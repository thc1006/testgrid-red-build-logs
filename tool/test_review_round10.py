"""Review round 10 (W4b): TestGrid's config over time. Each test failed on 76191a2: the
config script, the history of the upload job's runs, and builds judged by the configs
over time."""
import email.utils
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module import: its tests are not collected again here)
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

NOW = tch.NOW
run, configs, loaded = tch.run, tch.configs, tch.loaded
E1, E2, E3 = tch.E1, tch.E2, tch.E3


# ------------------------------------------------------------------ testgrid_config.sh, offline

FAKE_DOCKER = r"""#!/usr/bin/env bash
# docker as testgrid_config.sh uses it: the "configurator" prints the TestGrid YAML of the
# commit checked out (config/testgrids/config.yaml), and fails where config/testgrids/FAIL is.
case "$1" in
  pull) exit 0 ;;
  image) echo "${*: -1}@sha256:0000000000000000000000000000000000000000000000000000000000000000"; exit 0 ;;
  run)
    shift
    src=""
    while [ $# -gt 0 ]; do
      case "$1" in
        -v) src=${2%%:*}; shift 2 ;;
        --rm) shift ;;
        --network|-w|--entrypoint) shift 2 ;;
        *) break ;;
      esac
    done
    if [ -e "$src/config/testgrids/FAIL" ]; then echo "configurator: cannot build this commit" >&2; exit 1; fi
    cat "$src/config/testgrids/config.yaml"
    exit 0 ;;
esac
exit 2
"""

UPLOAD_JOB = """postsubmits:
  kubernetes/test-infra:
  - name: post-test-infra-upload-testgrid-config
    spec:
      containers:
      - image: gcr.io/k8s-staging-test-infra/configurator:v1
        command: [configurator]
        args: [--yaml=config/testgrids, --default=config/testgrids/default.yaml, --prow-config=config/prow/config.yaml,
               --prow-job-config=config/jobs/, --output=gs://k8s-testgrid-config/k8s/config, --oneshot]
"""


def testgrid_yaml(base_options):
    group = {"name": "job", "gcs_prefix": "bucket/logs/job", "days_of_results": 15, "use_kubernetes_client": True}
    return json.dumps({"test_groups": [group], "dashboards": [
        {"name": d, "dashboard_tab": [{"name": "t", "test_group_name": "job", "base_options": base_options}]}
        for d in tgc.DASHBOARDS]})  # (JSON is YAML)


class Script(unittest.TestCase):
    """testgrid_config.sh run as the workflow runs it, offline: test-infra is a local repository
    (git's url.<local>.insteadOf), docker a stand-in (FAKE_DOCKER), and the upload job's runs a
    fixed listing (testgrid_config.py's "uploads"; every other command is the real one)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        tool = os.path.join(self.tmp, "tool")
        os.makedirs(tool)
        shutil.copy(os.path.join(HERE, "testgrid_config.sh"), tool)
        with open(os.path.join(tool, "testgrid_config.py"), "w") as f:
            f.write(f"import os, sys\nsys.path.insert(0, {HERE!r})\nimport testgrid_config\n"
                    "if sys.argv[1] == 'uploads':\n"
                    "    sys.stdout.write(open(os.environ['W4B_UPLOADS']).read())\n"
                    "    sys.exit(0)\n"
                    "sys.exit(testgrid_config.main(sys.argv[1:]))\n")
        self.script = os.path.join(tool, "testgrid_config.sh")
        bindir = os.path.join(self.tmp, "bin")
        os.makedirs(bindir)
        docker = os.path.join(bindir, "docker")
        with open(docker, "w") as f:
            f.write(FAKE_DOCKER)
        os.chmod(docker, os.stat(docker).st_mode | stat.S_IEXEC)
        # test-infra, as a repository GitHub would serve (fetch by SHA, partial clone)
        self.src = os.path.join(self.tmp, "test-infra")
        self.bare = os.path.join(self.tmp, "test-infra.git")
        self.git("init", "-q", "-b", "master", self.src)
        self.git("-C", self.src, "config", "user.email", "w4b@example.com")
        self.git("-C", self.src, "config", "user.name", "w4b")
        gitconfig = os.path.join(self.tmp, "gitconfig")
        with open(gitconfig, "w") as f:
            f.write(f'[url "file://{self.bare}"]\n\tinsteadOf = https://github.com/kubernetes/test-infra\n'
                    "[protocol \"file\"]\n\tallow = always\n")
        self.env = dict(os.environ, PATH=bindir + os.pathsep + os.environ["PATH"], PYTHON=sys.executable,
                        CONFIG_SECONDS="480", GIT_CONFIG_GLOBAL=gitconfig, GIT_CONFIG_NOSYSTEM="1",
                        W4B_UPLOADS=os.path.join(self.tmp, "uploads.json"))
        self.out = os.path.join(self.tmp, "testgrid-config.json")

    def git(self, *args):
        return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.strip()

    def commit(self, base_options, fail=False):
        """A commit of test-infra whose TestGrid config gives tab t these base options (whose
        configurator fails when `fail`); returns its SHA."""
        files = {"config/jobs/test-infra/test-infra-trusted.yaml": UPLOAD_JOB, "config/prow/config.yaml": "{}\n",
                 "config/testgrids/config.yaml": testgrid_yaml(base_options), "README.md": "not checked out\n"}
        for name, text in files.items():
            os.makedirs(os.path.dirname(os.path.join(self.src, name)), exist_ok=True)
            with open(os.path.join(self.src, name), "w") as f:
                f.write(text)
        failing = os.path.join(self.src, "config/testgrids/FAIL")
        if fail:
            open(failing, "w").close()
        elif os.path.exists(failing):
            os.remove(failing)
        self.git("-C", self.src, "add", "-A")
        self.git("-C", self.src, "commit", "-q", "-m", base_options or "none")
        return self.git("-C", self.src, "rev-parse", "HEAD")

    def listing(self, commits):
        """The upload job's runs, one per commit (oldest first), as `uploads` writes them."""
        shutil.rmtree(self.bare, True)
        self.git("clone", "-q", "--bare", self.src, self.bare)
        for k, v in (("uploadpack.allowFilter", "true"), ("uploadpack.allowAnySHA1InWant", "true")):
            self.git("-C", self.bare, "config", k, v)
        t = int(time.time()) - DAY
        runs = [{"build": snowflake(t + 600 * i), "commit": c, "started": t + 600 * i, "finished": t + 600 * i + 5,
                 "job": None, "recipe": tgc.recipe_id(c, None)} for i, c in enumerate(commits)]
        with open(self.env["W4B_UPLOADS"], "w") as f:
            json.dump({"after": t - 1, "runs": runs,
                       "build": [{"recipe": r["recipe"], "commit": r["commit"], "job": None} for r in reversed(runs)],
                       "compare": [{"build": r["build"], "recipe": r["recipe"], "prev": runs[i - 1]["recipe"] if i else None}
                                   for i, r in enumerate(runs)]}, f)
        return runs

    def built(self):
        """{commit: tab t's base options} of the configs the script wrote, and the runs."""
        got = tgc.load(self.out)
        return {source["test_infra"]: tabs["sig-release-master-blocking#t"]["base_options"]
                for tabs, source in got["configs"].values()}, got

    def run_script(self, **env):
        return subprocess.run(["bash", self.script, self.out], env=dict(self.env, **env), capture_output=True,
                              text=True, timeout=300)

    def test_baseline_two_commits_are_built(self):
        # (passes: the harness works, and the sparse, blobless checkout of each commit is right)
        a, b = self.commit("width=1"), self.commit("width=2")
        self.listing([a, b])
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        built, got = self.built()
        self.assertEqual(len(got["runs"]), 2)
        self.assertEqual(built, {a: "width=1", b: "width=2"})
        self.assertEqual(list(got["group_changes"].values()), [{}, {}])  # (the first; then nothing changed)

    def test_with_nothing_new_to_build_it_still_writes_its_file(self):
        """F-A. testgrid_config.sh:58: with no commit to build (no upload since the last run: the
        usual case between test-infra config changes), "$work"/extract-*.json matches nothing,
        bash passes it literally, and merge fails on the missing file: the script exits 1 and
        writes nothing, so the fetch warns that the config cannot be read and never marks it as
        read (after CONFIG_STALE_DAYS that is a serious warning that fails the workflow)."""
        self.listing([])
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        got = tgc.load(self.out)
        self.assertEqual((got["runs"], got["configs"], got["failed"]), ([], {}, {}))

    def test_a_listing_with_no_time_left_to_build_still_records_the_runs(self):
        """F-A (second face). When the budget is spent before the first build (a slow listing:
        16 days of runs on a new archive), the runs listed are lost the same way, instead of
        being recorded as runs whose config is not known yet ("the rest are left to the next run")."""
        self.listing([self.commit("width=1")])
        done = self.run_script(CONFIG_SECONDS="0")
        self.assertEqual(done.returncode, 0, done.stderr)
        got = tgc.load(self.out)
        self.assertEqual((len(got["runs"]), got["configs"]), (1, {}))

    def test_one_commit_that_cannot_be_built_does_not_lose_the_others(self):
        """F-B. testgrid_config.sh:38-55 run under set -e, and the configurator's failure exits 1:
        one commit that cannot be built (git fetch, the job's definition, docker pull, the
        configurator, or extract) loses every config already built and every run listed. The
        commit is first in line again next time, so a lasting failure freezes the archive's
        history of TestGrid's config: later uploads are never recorded, not even as unknown."""
        old, bad, new = self.commit("width=1"), self.commit("width=2", fail=True), self.commit("width=3")
        self.listing([old, bad, new])
        done = self.run_script()
        self.assertTrue(os.path.exists(self.out), done.stderr)  # fails: nothing written
        self.assertEqual(done.returncode, 0, done.stderr)
        built, got = self.built()
        self.assertEqual([r["commit"] for r in got["runs"]], [old, bad, new])
        self.assertEqual(sorted(built), sorted([old, new]))
        self.assertEqual(list(got["failed"]), [tgc.recipe_id(bad, None)])
        self.assertIn("cannot build this commit", got["failed"][tgc.recipe_id(bad, None)])
        # (how bad and new changed their test groups needs bad's config: not worked out)
        self.assertEqual(list(got["group_changes"]), [got["runs"][0]["build"]])


# ------------------------------------------------------------------ ConfigHistory and plan()

class History(unittest.TestCase):
    def test_a_run_seen_running_that_then_failed_is_dropped(self):
        """F-C. fetch_red_logs.py:2047-2050: a run the last listing saw running ("finished": None)
        and that failed is "not listed now", but `low` is the oldest build listed now, and plan()
        lists from just before that run, so every build listed is newer than it: it is never
        dropped. It stays "running" for CONFIG_KEEP_DAYS."""
        h = frl.ConfigHistory()
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b", finished=False)], configs(a={"d#t": E1}, b={"d#t": E2})))
        # the next listing, from a on (b still ran): b failed (upload_runs leaves it out), c uploaded since
        h.merge(loaded([run(NOW + 200, "c")], configs(c={"d#t": E3}), after=frl.created(run(NOW, "a")["build"])))
        self.assertEqual([r["commit"][0] for r in h.runs], ["a", "c"])  # fails: ['a', 'b', 'c']

    def test_a_failed_run_does_not_keep_the_config_before_it_in_force(self):
        """F-C, what it does: the run before the failed one is "in force" forever (its era never
        ends: every later build is judged by that config too), the failed run's config, which
        TestGrid never got, is in force until the next run, and when its group differs it is an
        event that never ends (every build of the last 7 days stays undecided)."""
        h = frl.ConfigHistory()
        changed = dict(E1, group_digest="1" * 64)
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b", finished=False)], configs(a={"d#t": E1}, b={"d#t": changed})))
        h.merge(loaded([run(NOW + 200, "c")], configs(c={"d#t": E1}), after=frl.created(run(NOW, "a")["build"])))
        # (fails: b's config was in force from b on, its event never ended; now a's config, which c
        # uploaded again, is in force all along, and nothing changed the group)
        self.assertEqual([e[2] for e in h.eras("d#t")], [frl.UNKNOWN, E1])
        self.assertEqual(h.events("d#t", NOW + 30 * DAY), [])


class Decode(unittest.TestCase):
    def test_runs_out_of_build_order_in_state_are_not_decoded_into_wrong_configs(self):
        """F-K. ConfigHistory.__init__ (fetch_red_logs.py:2011-2040) applies each run's changes
        in the order state.json lists the runs, and sorts them by build only afterwards: a list out
        of order (a hand merge of two state.json, say) gives runs configs they never had, silently
        (no run is dropped, nothing is "not known")."""
        h = frl.ConfigHistory()
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c")],
                       configs(a={"d#t": E1, "d#u": E1}, b={"d#t": E1, "d#u": E2}, c={"d#t": E3, "d#u": E2})))
        uploads, entries = h.encode()
        warnings = []
        again = frl.ConfigHistory([uploads[0], uploads[2], uploads[1]], entries, warnings)
        for got, want in zip(again.runs, h.runs, strict=True):
            self.assertIn(got["tabs"], (want["tabs"], None), got["commit"][0])  # fails: b gets c's d#t
        self.assertEqual(again.runs, h.runs)  # (put back in order, each checked by its digest)
        # two histories merged by hand: what does not match its digest is not known
        other = frl.ConfigHistory()
        other.merge(loaded([run(NOW + 150, "e")], configs(e={"d#t": E2})))
        mixed = frl.ConfigHistory(uploads + other.encode()[0][:1], dict(entries, **other.encode()[1]))
        self.assertEqual([(r["commit"][0], r["tabs"]) for r in mixed.runs],
                         [("a", h.runs[0]["tabs"]), ("b", h.runs[1]["tabs"]), ("e", {"d#t": E2}), ("c", None)])


class Plan(unittest.TestCase):
    """plan() against a fake GCS (as tch.Uploads)."""

    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def use_fake_gcs(self):
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)

    def test_runs_the_archive_has_built_are_not_built_again(self):
        """F-C, what it costs: with that failed run still "running" in state.json, every plan()
        lists from it again and puts every commit since then in "build", newest first, though
        their configs are known: each run rebuilds them, and its 8 minutes go to them before
        the runs whose config is not known yet."""
        self.use_fake_gcs()
        t = int(time.time()) - DAY
        a = self.upload(t, commit="a" * 40)
        b = self.upload(t + 60, finished={"timestamp": t + 65, "passed": False}, commit="b" * 40)
        c, d = self.upload(t + 120, commit="c" * 40), self.upload(t + 180, commit="d" * 40)
        state = {"config_uploads": [  # (as the fetch writes it after F-C)
            {"build": a, "commit": "a" * 40, "started": t, "finished": t + 5, "tabs": {}, "base": True},
            {"build": b, "commit": "b" * 40, "started": t + 60, "finished": None, "tabs": {}},
            {"build": c, "commit": "c" * 40, "started": t + 120, "finished": t + 125, "tabs": {}},
            {"build": d, "commit": "d" * 40, "started": t + 180, "finished": t + 185, "tabs": {}}]}
        self.assertEqual(tgc.plan(state, time.time())["build"], [])  # fails: ['d'*40, 'c'*40]

    def test_a_run_dropped_from_state_is_listed_again(self):
        """F-D. A run state.json holds malformed is dropped with the warning "(their configs are
        read again)", but the fetch then writes state.json without it, and plan() lists only
        runs made after the last one state.json knows: it is never listed again, and its config
        is lost for good (its time is covered by the run before it, as if it changed nothing)."""
        self.use_fake_gcs()
        t = int(time.time()) - DAY
        a, b, c = (self.upload(t + 60 * i, commit=x * 40) for i, x in enumerate("abc"))
        h = frl.ConfigHistory()
        h.merge(loaded([{k: r[k] for k in ("build", "commit", "started", "finished")}
                        for r in tgc.upload_runs(t - 1, gcs=self.srv.url)],
                       configs(a={"d#t": E1}, b={"d#t": E2}, c={"d#t": E3})))
        uploads, entries = h.encode()
        uploads[1]["started"] = "damaged"
        warnings = []
        h = frl.ConfigHistory(uploads, entries, warnings)  # (the fetch reads state.json)
        self.assertIn("are listed again", " ".join(warnings))
        uploads, entries = h.encode()  # (and writes it)
        out = tgc.plan({"config_uploads": uploads, "config_entries": entries, "config_relist_after": h.relist_after},
                       time.time())
        self.assertIn("b" * 40, [b["commit"] for b in out["build"]])  # fails: only c (whose delta was lost with b)


class Eras(unittest.TestCase):
    def test_the_config_before_stays_in_force_until_the_merger_has_run(self):
        """F-E. PICKUP (fetch_red_logs.py:1562) assumes TestGrid takes what the upload job uploads
        within a minute. It does not: the job writes gs://k8s-testgrid-config/k8s/config
        (--output in its prowjob.json), TestGrid's components read gs://k8s-testgrid/config
        (cluster/prod/updater.yaml: --config), and config_merger writes that every 15 minutes
        (cluster/prod/config_merger.yaml: --wait=15m, each merge up to 10 minutes; a merge that
        fails, e.g. one source unreadable, keeps the old config). So the old config can be in
        force ~25 minutes after the next run ended, and the update its change triggers can begin
        that much later than events() allows."""
        h = frl.ConfigHistory()
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b")],
                       configs(a={"d#t": E1}, b={"d#t": dict(E1, group_digest="1" * 64)})))
        b_finished = NOW + 105
        a_era = next(e for e in h.eras("d#t") if e[2] is not frl.UNKNOWN and e[0] == NOW)
        self.assertGreaterEqual(a_era[1], b_finished + 15 * 60)  # fails: b_finished + 60
        (_, hi, _), = h.events("d#t", NOW + DAY)
        self.assertGreaterEqual(hi, b_finished + 15 * 60 + frl.READ_GAP - frl.UPDATE_SPAN)  # fails: 14 min short


# ------------------------------------------------------------------ judging builds by the configs over time

class OverTime(ttc.ConfigWorld):
    """As test_config_history.ConfigOverTime (whose tests are not run again here)."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab
    late_junit = tch.ConfigOverTime.late_junit

    def finished_at(self, written):
        """b's finished.json and junit file written at `written` (it ran from 30 h ago until then)."""
        self.srv.route(f"/bucket/logs/job/{self.b}/finished.json",
                       (200, {"Last-Modified": email.utils.formatdate(written, usegmt=True)},
                        {"result": "FAILURE", "passed": False, "timestamp": int(written)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b)],
                             written={f"logs/job/{self.b}/artifacts/junit_01.xml": written})

    def test_control_hidden_all_along_is_not_red(self):
        # (passes: with the new config all along, b is not red)
        self.finished_at(self.now - 29 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))))
        self.assertNotIn("backfilled", run_["outcomes"])

    def test_control_finished_before_the_tab_hid_it_is_red(self):
        # (passes: b failed 29 h ago, the tab hid it 28 h ago; TestGrid showed it red for an hour)
        self.finished_at(self.now - 29 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 28 * HOUR, self.tab(ttc.entry(self.HIDE))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_control_no_change_no_late_read(self):
        # (passes: without the upload 10 h ago, the junit written 9 h ago is never read)
        self.late_junit(9 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))))
        self.assertNotIn("backfilled", run_["outcomes"])

    def test_a_build_running_when_its_tab_began_hiding_its_failure_was_never_red(self):
        """F-F. tab_verdict (fetch_red_logs.py:1915-1929) judges every column TestGrid may have
        read (col and col.alternatives, up to now) by every config of the tab's eras: a read made
        after a config ended counts as shown by it. b started 30 h ago and failed kubetest.Test
        29 h ago (its finished.json and junit written then); 29 h 45 min ago an upload made the
        tab hide kubetest.Test and job.Overall. Under the old config b's column was RUNNING, under
        the new one its failing rows are hidden: TestGrid never showed b red, yet the port
        archives it (its final read judged by the old config's filters)."""
        self.finished_at(self.now - 29 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                           (self.now - 29.75 * HOUR, self.tab(ttc.entry(self.HIDE))))
        self.assertNotIn("backfilled", run_["outcomes"])  # fails: backfilled
        self.assertIsNone(self.bdir(self.b))

    def test_a_read_made_under_the_new_config_is_not_judged_by_the_old_one(self):
        """F-F, with a re-read. An upload 10 h ago changed the tab's test
        group (TestGrid read b's column again after it) and made the tab hide kubetest.Test; b's
        junit file with the kubetest.Test failure was written 9 h ago, so only reads under the new
        config saw it, and TestGrid never showed b red. The port takes b as red (false red)."""
        self.late_junit(9 * HOUR)
        hides_and_changed = dict(ttc.entry(self.HIDE), group_digest="1" * 64)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(hides_and_changed)))
        self.assertNotIn("backfilled", run_["outcomes"])  # fails: backfilled
        self.assertIsNone(self.bdir(self.b))

    def test_renaming_a_tab_does_not_make_testgrid_read_its_columns_again(self):
        """F-G. events() compares the tab's own entry run to run: a tab renamed to t (or a new tab
        for an existing group) goes from None to a digest, which counts as its test group's config
        changing, though the group did not change and TestGrid read nothing again. With b's junit
        written after the rename, the port takes b as red; TestGrid never read that file (false red)."""
        self.late_junit(9 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, {"dash": {"u": ttc.entry("")}}),
                           (self.now - 10 * HOUR, {"dash": {"t": ttc.entry("")}}))
        self.assertNotIn("backfilled", run_["outcomes"])  # fails: backfilled


class WatchedAndShown(ttc.ConfigWorld):
    history = tch.ConfigOverTime.history
    tab = tch.ConfigOverTime.tab

    def watched_world(self, shown):
        w = snowflake(self.now - 6 * HOUR)
        written = self.now - 5 * HOUR
        self.srv.route(f"/bucket/logs/job/{w}/started.json", (200, {}, {"timestamp": int(self.now - 6 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{w}/finished.json",
                       (200, {"Last-Modified": email.utils.formatdate(written, usegmt=True)},
                        {"result": "FAILURE", "passed": False, "timestamp": int(written)}))
        self.srv.route(f"/bucket/logs/job/{w}/build-log.txt", (200, {}, b"log\n"))
        self.srv.route(f"/bucket/logs/job/{w}/artifacts/junit_01.xml", (200, {}, tch.KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)],
                             written={f"logs/job/{w}/artifacts/junit_01.xml": written})
        if shown:  # t shows w now, through its new base options (kubetest.Test and job.Overall hidden): not red
            self.show([self.visible, w], {"job.Pod": ([1, 1], ["", ""]), "kubetest.Up": ([1, 1], ["", ""])},
                      start=int(self.now - HOUR))
        path = self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 2 * HOUR, self.tab(ttc.entry(self.HIDE))))
        state = self.state()
        rec = {"job": "job", "build": w, "query": "bucket/logs/job", "started": None, "created": frl.created(w),
               "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}], "watch": True}
        state.update(tabs={"dash#t": {"scan": self.now - 5.5 * HOUR, "oldest": self.now - 60 * HOUR}},
                     watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 5.5 * HOUR}})
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.fetch_with(path)
        return w, self.runs()[-1]["outcomes"]

    def test_control_judged_from_gcs_it_is_red(self):
        # (passes: once TestGrid no longer shows w, the history makes it red)
        w, outcomes = self.watched_world(shown=False)
        self.assertIn(f"job/{w}", outcomes.get("backfilled", []))

    def test_a_watched_build_red_before_its_tab_hid_the_row_is_not_settled_by_what_the_tab_shows_now(self):
        """F-I. run() (fetch_red_logs.py:3577-3593) settles a watched build as "not-red" once every
        tab of it shows it not red now, ignoring the configs those tabs had since: w failed
        kubetest.Test 5 h ago, under a config that showed that row, and TestGrid read it within
        READ_GAP, so it was red on t until the upload 2 h ago hid the row. The history says so
        (the control above), yet the scan's "shows it not red now" decides: a missed red build."""
        w, outcomes = self.watched_world(shown=True)
        self.assertNotIn(f"job/{w}", outcomes.get("not-red", []))  # fails: settled as not red
        self.assertIn(f"job/{w}", outcomes.get("backfilled", []) + outcomes.get("archived", []))


class AbsentTab(ttc.ConfigWorld):
    def test_a_tab_testgrid_lists_but_its_config_lacks_is_not_judged_not_red(self):
        """F-H. A tab TestGrid lists now that the config read lacks gets the warnings "for it, the
        config read before, if any, is used" and (serious, here) "TestGrid's config was never read
        for 1 tabs (dash#t): rows their filters hide count as shown", but eras() says None
        ("TestGrid had no such tab") for all the config's time, and tab_verdict makes that
        "not-red": b, red on kubetest.Test by the tab's rows, is dropped for good (not watched;
        the tab's scan moves on; cross_check judges it the same way). This is what happens to
        every tab of a dashboard added to fetch_red_logs.DASHBOARDS but not to
        testgrid_config.DASHBOARDS (or a --dashboard run); for a tab that had a config before,
        only the non-serious warning is given."""
        self.fetch_with(self.config_of({"dash": {"other": ttc.entry("")}}))
        run_ = self.runs()[-1]
        self.assertIn("has no tab dash#t: it lacks tabs TestGrid shows", " ".join(run_["serious_warnings"]))
        decided = sum((run_["outcomes"].get(k, []) for k in ("backfilled", "watching", "held")), [])
        self.assertIn(f"job/{self.b}", decided)  # fails: outcomes {} (judged not red)


if __name__ == "__main__":
    unittest.main()
