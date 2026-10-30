"""The archive workflow (.github/workflows/red-build-logs.yml): whether a run archives (its due job,
whose script runs as GitHub runs it, bash -eo pipefail, with gh and the clock stood in), and which of
the archive job's steps fail the run when they fail."""
import datetime as dt
import os
import re
import shutil
import subprocess
import tempfile
import unittest

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
WORKFLOW = os.path.join(HERE, "..", ".github", "workflows", "red-build-logs.yml")
# (gh api ... --jq: the committer date of the last commit under archive/runs/, "" for none)
FAKE_GH = '#!/bin/sh\n[ -n "$FAKE_GH_FAILS" ] && exit 1\nprintf "%s\\n" "$FAKE_LAST"\n'
# (date +%s: the run's start; anything else, such as date -d "$last" +%s, is the real date's)
FAKE_DATE = '#!/bin/sh\nif [ "$#" = 1 ] && [ "$1" = +%s ]; then echo "$FAKE_NOW"; exit 0; fi\nexec {real} "$@"\n'


def stamp(iso):
    return int(dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())


class Due(unittest.TestCase):
    def due(self, last, now, event="schedule", attempt=1, gh_fails=False):
        """The due job's outputs for an archive last changed at `last` (as the commits API gives it;
        "" for no archive) and a run of `event` started at `now`: {"age": ..., "due": "true"} when
        due, {"age": ...} when not."""
        with open(WORKFLOW) as f:
            script = yaml.safe_load(f)["jobs"]["due"]["steps"][0]["run"]
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        for name, text in (("gh", FAKE_GH), ("date", FAKE_DATE.format(real=shutil.which("date")))):
            with open(os.path.join(d, name), "w") as f:
                f.write(text)
            os.chmod(os.path.join(d, name), 0o755)
        out = os.path.join(d, "outputs")
        env = dict(os.environ, PATH=d + os.pathsep + os.environ["PATH"], GITHUB_OUTPUT=out,
                   GITHUB_REPOSITORY="owner/repo", GITHUB_RUN_ATTEMPT=str(attempt), EVENT=event, BRANCH="main",
                   GH_TOKEN="token", FAKE_LAST=last, FAKE_NOW=str(stamp(now)), FAKE_GH_FAILS="1" if gh_fails else "")
        done = subprocess.run(["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script], env=env,
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        with open(out) as f:
            return dict(line.split("=", 1) for line in f.read().split())

    def test_twice_a_day_as_github_starts_the_schedule(self):
        # (the scheduled runs GitHub started in October 2026, about 05:50 and 17:30 UTC)
        self.assertEqual(self.due("2026-10-10T05:50:15Z", "2026-10-10T17:19:51Z"), {"age": "41376", "due": "true"})
        self.assertEqual(self.due("2026-10-10T17:20:29Z", "2026-10-11T05:43:54Z")["due"], "true")
        # (and the one it started in between: the archive changed in this half of the day)
        self.assertEqual(self.due("2026-10-11T17:41:40Z", "2026-10-11T21:57:10Z"), {"age": "15330"})

    def test_an_archive_made_by_hand_does_not_put_the_next_off_by_a_day(self):
        # (made by hand at 19:31: the run at 21:57 does not archive; the one at 05:55, 10 hours
        # later but in the next half of the day, does; by age alone it did not, nor did any until
        # 17:30, a day after)
        self.assertEqual(self.due("2026-10-11T19:31:33Z", "2026-10-11T21:57:10Z"), {"age": "8737"})
        self.assertEqual(self.due("2026-10-11T19:31:33Z", "2026-10-12T05:55:19Z"), {"age": "37426", "due": "true"})

    def test_the_halves_of_the_day_and_the_age(self):
        self.assertEqual(self.due("2026-10-12T11:59:59Z", "2026-10-12T12:00:00Z")["due"], "true")
        self.assertEqual(self.due("2026-10-12T12:00:00Z", "2026-10-12T22:59:59Z"), {"age": "39599"})
        self.assertEqual(self.due("2026-10-12T12:00:00Z", "2026-10-12T23:00:00Z")["due"], "true")  # (11 hours)
        self.assertEqual(self.due("2026-10-12T00:00:00Z", "2026-10-12T10:59:59Z"), {"age": "39599"})
        self.assertEqual(self.due("2026-10-11T23:59:59Z", "2026-10-12T00:00:00Z")["due"], "true")

    def test_by_hand_a_re_run_or_an_age_not_known(self):
        for kw in ({"event": "workflow_dispatch"}, {"attempt": 2}):
            self.assertEqual(self.due("2026-10-12T12:01:00Z", "2026-10-12T12:30:00Z", **kw),
                             {"age": "1740", "due": "true"}, kw)
        for last, kw in (("", {}), ("2026-10-12T12:01:00Z", {"gh_fails": True}), ("not a date", {})):
            self.assertEqual(self.due(last, "2026-10-12T12:30:00Z", **kw), {"age": "0", "due": "true"}, (last, kw))



class FailedSteps(unittest.TestCase):
    """GitHub takes a step with continue-on-error that fails for one that succeeded (its conclusion), though
    its outcome is failure: such a step fails the run only if the last step checks its outcome. The steps
    after a failed one that keep what the run took run unless the run is cancelled."""

    def setUp(self):
        with open(WORKFLOW) as f:
            self.steps = yaml.safe_load(f)["jobs"]["archive"]["steps"]

    def step(self, name):
        return next(s for s in self.steps if s.get("name") == name)

    def test_those_that_fail_without_failing_the_run(self):
        # (each says why: an error it need not stop on, a config earlier runs read, a cleanup the commit does again)
        checked = set(re.findall(r"steps\.([\w-]+)\.outcome == 'failure'", self.steps[-1]["if"]))
        self.assertEqual(checked, {"fetch", "verify"})
        quiet = [s["name"] for s in self.steps if s.get("continue-on-error") and s.get("id") not in checked]
        self.assertEqual(quiet, ["TestGrid's production manifests still deploy the commit the port follows",
                                 "Read TestGrid's config from its upload job's runs",
                                 "Remove what a cut-off fetch left"])  # fails: and Show the verification in INDEX.md

    def test_one_that_shows_the_verification_fails_the_run_after_the_commit(self):
        # (INDEX.md then still says the verification is pending: the run was successful all the same)
        show = self.step("Show the verification in INDEX.md")
        self.assertIn("--show-verifications", show["run"])
        self.assertNotIn("continue-on-error", show)  # fails: true
        names = [s.get("name") for s in self.steps]
        after = self.steps[names.index("Show the verification in INDEX.md") + 1:]
        self.assertEqual([s["name"] for s in after],
                         ["Commit the archive", "Keep the unpushed commit", "Fail the run if fetching or verifying failed"])
        self.assertEqual(after[0]["if"], "${{ !cancelled() }}")
        self.assertTrue(after[1]["if"].startswith("${{ failure() && "))


class Token(unittest.TestCase):
    """The archive job's token: the checkout keeps none in .git/config, where any later step could read it."""

    def setUp(self):
        with open(WORKFLOW) as f:
            self.job = yaml.safe_load(f)["jobs"]["archive"]
        self.steps = self.job["steps"]

    def test_the_checkout_keeps_none(self):
        checkouts = [s for s in self.steps if str(s.get("uses")).startswith("actions/checkout@")]
        self.assertEqual([s["with"].get("persist-credentials") for s in checkouts], [False])


if __name__ == "__main__":
    unittest.main()
