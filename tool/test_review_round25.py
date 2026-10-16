"""Review of 2026-10-16: an outside review of 4ace4f3, and what testing it found. Each test
fails on 4ace4f3 for the reason its docstring gives; a test named test_control_* passes there
and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round25 -v
"""
import contextlib
import io
import os
import shutil
import sys
import tempfile
import time
import unittest
import zlib
from unittest import mock

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected here)
import test_testgrid_config as ttc  # noqa: E402
from test_fetch_red_logs import table as fetch_table  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj, snowflake  # noqa: E402


# ------------------------------------------------------------------ comparisons with GCS a run's deadline cut off

class CutOffComparisons(ArchiveWorld):
    """Six final builds TestGrid shows red: three in the run's seventh of the archive (compared
    with GCS, final or not), three not. A second run's deadline (1.8 s) comes while GCS has not
    answered for the first three (3.5 s)."""

    def setUp(self):
        super().setUp()
        t0 = int(time.time()) - 5 * DAY
        mine, other, t = [], [], t0
        while len(mine) < 3 or len(other) < 3:
            b = str(snowflake(t))
            (mine if zlib.crc32(f"job/{b}".encode()) % 7 == 0 else other).append(b)
            t -= 600
        self.mine, self.other = mine[:3], other[:3]
        builds = sorted(self.mine + self.other, key=frl.created, reverse=True)
        self.srv.table("dash", "t", {"query": "bucket/logs/job", "changelists": builds,
                                     "timestamps": [frl.created(b) * 1000 for b in builds],
                                     "tests": [{"name": "job.Overall", "statuses": [{"count": 6, "value": 12}],
                                                "messages": ["boom"] * 6}]})
        for b in builds:
            base = f"/bucket/logs/job/{b}"
            self.srv.route(base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": frl.created(b) + 600}))
            self.srv.route(base + "/build-log.txt", obj(b"log " + b.encode()))
        self.sevenths = mock.patch.object(frl, "gcs_sevenths", return_value={0})
        self.sevenths.start()
        self.addCleanup(self.sevenths.stop)
        self.assertEqual(self.fetch(), 0)  # (archives all six: final, as they finished 5 days ago)

    def keys(self, builds):
        return sorted(f"job/{b}" for b in builds)

    def slow(self, b):
        fast = obj(b"log " + b.encode())

        def serve(h):
            time.sleep(3.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                fast(h)
        return serve

    def test_control_comparisons_that_end(self):
        self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"already-archived": self.keys(self.mine + self.other)})
        self.assertEqual(run["serious_warnings"], [])

    def test_a_comparison_the_deadline_cut_off_is_not_checked(self):
        """run() gave a build whose worker the deadline cut off the outcome add_sightings names,
        "already-archived" for one this run did not update (fetch_red_logs.py:4828): a final build
        in the run's sevenths whose comparison with GCS did not end read as one compared and
        found unchanged, the run named no problem and exited 0."""
        for b in self.mine:
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", self.slow(b))
        code = self.fetch("--run-timeout-minutes", "0.03")
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"not-checked": self.keys(self.mine),  # fails: all "already-archived"
                                           "already-archived": self.keys(self.other)})
        self.assertEqual(code, 1)  # fails: 0
        self.assertEqual(run["serious_warnings"], [
            f"3 archived builds were not compared with GCS before the run's deadline ({', '.join(self.keys(self.mine))}): "
            "the next run compares them"])
        time.sleep(3.6)  # (the server's slow answers end before it closes)


class SeventhsOfRunsNotDone(unittest.TestCase):
    """gcs_sevenths: which sevenths of the archive a run compares with GCS again, final or not.
    4ace4f3 took the days since the run before, whatever that run did: a run cut off before it
    compared its sevenths, or whose verification found a problem, counted as having compared them,
    so the next run went on to the next day's seventh and left them for a week."""

    DAY = 20_000

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)

    def run_at(self, hours, compared=True, verified="ok"):
        """A run folder started `hours` after 00:00 UTC of day DAY: whether its fetch compared all
        it was due to, and its verification's outcome (None: no verification.json)."""
        started = self.DAY * 86400 + hours * 3600
        rel = frl.new_run_dir(self.root, started)[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       {"run": rel.rsplit("/", 1)[1], "started": started, "gcs_compared": compared})
        if verified is not None:
            frl.write_json(os.path.join(self.root, rel, "verification.json"), {"outcome": verified})

    def sevenths(self, hours):
        with mock.patch("time.time", return_value=(self.DAY + 30) * 86400):
            return frl.gcs_sevenths(self.root, self.DAY * 86400 + hours * 3600)

    def days(self, *days):
        return {(self.DAY + d) % 7 for d in days}

    def test_control_the_run_before_was_done(self):
        self.run_at(-18)
        self.assertEqual(self.sevenths(6), self.days(0))

    def test_control_the_first_run(self):
        self.assertEqual(self.sevenths(6), self.days(0))  # (nothing archived before it to compare again)

    def test_a_run_cut_off_leaves_its_day_to_the_next(self):
        self.run_at(-18)
        self.run_at(6, compared=False)  # (its deadline came before it compared its seventh)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_so_does_one_whose_verification_found_a_problem(self):
        self.run_at(-18)
        self.run_at(6, verified="problems")  # (such as "GCS changed since it was archived")
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_or_was_not_verified_to_the_end(self):
        self.run_at(-18)
        for i, outcome in enumerate(("timed out", "interrupted", "stopped", "incomplete", None)):
            self.run_at(6 + i, verified=outcome)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_a_run_not_done_on_the_day_of_the_last_done_one(self):
        # (the run done that day compared its seventh, but the later one may have found GCS changed since)
        self.run_at(1)
        self.run_at(5, compared=False)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_no_run_done_in_the_week_before(self):
        self.run_at(-50, compared=False)
        self.run_at(-20, verified="problems")
        self.assertEqual(self.sevenths(6), set(range(7)))  # fails: {DAY - 1, DAY}
        self.run_at(-8 * 24)  # (one done more than a week before: all seven still)
        self.assertEqual(self.sevenths(6), set(range(7)))  # fails: {DAY - 1, DAY}


class ComparedAgainAfterACutOff(ArchiveWorld):
    """Six final builds TestGrid shows red, three in today's seventh of the archive, none in
    tomorrow's. Run 1 compares them and is verified ok. Run 2, an hour later, is cut off while
    GCS has not answered for the three (it compares today's seventh again: the day's), and one of
    them changed in GCS since run 1; its verification finds that. Run 3 is tomorrow. (The clock
    is today's noon, UTC, from setUp on.)"""

    def setUp(self):
        real, start = time.time, time.time()
        self.noon, self.shift = int(real() // 86400) * 86400 + 12 * 3600, 0
        clock = mock.patch("time.time", lambda: self.noon + self.shift + (real() - start))
        clock.start()
        self.addCleanup(clock.stop)
        super().setUp()
        day = self.noon // 86400
        t, mine, other = self.noon - 5 * DAY, [], []
        while len(mine) < 3 or len(other) < 3:
            b = str(snowflake(t))
            seventh = zlib.crc32(f"job/{b}".encode()) % 7
            (mine if seventh == day % 7 else other if seventh != (day + 1) % 7 else []).append(b)
            t -= 600
        self.mine, self.other = mine[:3], other[:3]
        builds = sorted(self.mine + self.other, key=frl.created, reverse=True)
        self.srv.table("dash", "t", {"query": "bucket/logs/job", "changelists": builds,
                                     "timestamps": [frl.created(b) * 1000 for b in builds],
                                     "tests": [{"name": "job.Overall", "statuses": [{"count": 6, "value": 12}],
                                                "messages": ["boom"] * 6}]})
        for b in builds:
            base = f"/bucket/logs/job/{b}"
            self.srv.route(base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": frl.created(b) + 600}))
            self.srv.route(base + "/build-log.txt", obj(b"log " + b.encode()))
        self.slow_on = False

    def keys(self, builds):
        return sorted(f"job/{b}" for b in builds)

    def slow(self, data, generation):
        fast = obj(data, generation)

        def serve(h):
            if self.slow_on:
                time.sleep(3.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                fast(h)
        return serve

    def verify(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48",
                            "--record"])
        return code, frl.read_json(os.path.join(self.root, frl.run_dirs(self.root)[-1], "verification.json"))

    def test_a_build_left_by_a_run_cut_off_is_compared_by_the_next(self):
        """The fetch's run 3 compared tomorrow's seventh only (gcs_sevenths took the days since run 2,
        fetch_red_logs.py:3274-3278): the build GCS replaced, which run 2's verification found
        changed, was neither fetched again nor verified again, and run 3's verification passed."""
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.verify()[1]["outcome"], "ok")
        changed = self.mine[0]
        for b in self.mine:
            data, gen = (b"replaced " + b.encode(), "2") if b == changed else (b"log " + b.encode(), "1")
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", self.slow(data, gen))
        self.shift, self.slow_on = HOUR, True
        self.fetch("--run-timeout-minutes", "0.03")
        time.sleep(3.6)  # (the server's slow answers end)
        self.slow_on = False
        code, v = self.verify()
        self.assertEqual((code, v["outcome"]), (1, "problems"))
        self.assertTrue(any(f"job/{changed} build-log.txt: GCS changed since it was archived" in p for p in v["problems"]),
                        v["problems"])
        self.shift = DAY
        self.assertEqual(self.fetch(), 0)
        run3 = self.runs()[-1]
        self.assertEqual(run3["outcomes"], {"refreshed": [f"job/{changed}"],  # fails: all "already-archived"
                                            "already-archived": self.keys(self.mine[1:] + self.other)})
        self.assertEqual(self.read("build-log.txt", changed), b"replaced " + changed.encode())
        code, v = self.verify()
        day = self.noon // 86400
        self.assertEqual((code, v["outcome"], v["scope"]["sevenths"]), (0, "ok", sorted({day % 7, (day + 1) % 7})))


# ------------------------------------------------------------------ builds a scan saw, kept while a config is not known

RED_ON_TEST = (b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
               b'<testcase name="Up"/></testsuite>')


class PendingRanges(ttc.ConfigWorld):
    """A build made 6 h before a scan that saw it not red (tab t then hid its red rows: HIDE),
    which has since scrolled off; the run of the upload job an hour after that scan has a config
    not known yet (it shows every row). run() keeps the range "changed_pending", and drops it 14
    days after the scan."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def world(self, age):
        s0 = self.now - age
        made = s0 - 6 * HOUR
        b = str(snowflake(made))
        self.failed_in_gcs(b, self.now - made)
        self.junit(b, RED_ON_TEST)
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR,
                                        "changed_pending": [{"since": s0, "lo": made - HOUR, "hi": made + HOUR}]}})
        unknown = self.judged((self.now - 40 * DAY, self.tab(ttc.entry(self.HIDE))), (s0 + HOUR, None))
        return b, s0, made, unknown

    def known(self, s0):
        return self.judged((self.now - 40 * DAY, self.tab(ttc.entry(self.HIDE))), (s0 + HOUR, self.tab(ttc.entry(""))))

    def test_control_a_range_is_judged_once_its_config_is_known(self):
        b, s0, _, run = self.world(14 * DAY - 60)
        self.assertEqual(self.state()["tabs"]["dash#t"]["changed_pending"][0]["since"], s0)
        self.assertFalse(any(b in w or "seen not red by the scan" in w for w in run["warnings"]))
        self.assertEqual(self.known(s0)["outcomes"], {"backfilled": [f"job/{b}"]})

    def test_a_range_no_longer_judged_is_a_serious_warning(self):
        """run() dropped a range 14 days after its scan while a config of its tab in force since
        was still not known (fetch_red_logs.py:4545-4548), without a warning of its own: a build in
        it that the config, known later, shows red was never judged, and no run said so."""
        b, s0, made, run = self.world(14 * DAY + 60)
        self.assertNotIn("changed_pending", self.state()["tabs"]["dash#t"])
        self.assertIn(f"dash#t: the builds made from {frl.iso(made - HOUR)} to {frl.iso(made + HOUR)}, seen not red by "
                      f"the scan of {frl.iso(s0)}, cannot be judged from GCS: a config of the tab in force since that "
                      "scan is still not known 14 days later; no longer judged, though TestGrid may have shown them red",
                      run["serious_warnings"])  # fails: not there
        self.assertEqual(self.known(s0)["outcomes"], {})  # (dropped: not judged once the config is known)

    def test_so_are_those_of_a_tab_forgotten(self):
        """run() forgot a tab unlisted for 30 days (fetch_red_logs.py:4564-4566) with the ranges it
        kept for want of a config, and said nothing of them (a tab not listed is not read, so they
        are never judged)."""
        s0 = self.now - 20 * DAY
        pending = {"since": s0, "lo": s0 - 7 * HOUR, "hi": s0 - 5 * HOUR}
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR},
                             "dash#gone": {"scan": s0, "oldest": s0 - 60 * HOUR, "missing_since": self.now - 31 * DAY,
                                           "changed_pending": [pending]}})
        run = self.judged((self.now - 40 * DAY, {"dash": {"t": ttc.entry(""), "gone": ttc.entry("")}}))
        self.assertIn("dash#gone: unlisted for 30 days; forgetting it", run["warnings"])
        self.assertIn(f"dash#gone: the builds made from {frl.iso(pending['lo'])} to {frl.iso(pending['hi'])}, seen not "
                      f"red by the scan of {frl.iso(s0)}, cannot be judged from GCS: the tab is no longer listed, and is "
                      "forgotten; no longer judged, though TestGrid may have shown them red",
                      run["serious_warnings"])  # fails: not there


# ------------------------------------------------------------------ a tab the config lacks, then no longer listed

class LackingThenUnlisted(ttc.ConfigWorld):
    """TestGrid lists tab t; the latest config the archive knows (20 h ago) has no t. A build made
    10 h ago, red on t's rows, is in t's history gap: its config then is not known, so it is held.
    Then TestGrid stops listing t."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def setUp(self):
        super().setUp()
        self.made = self.now - 10 * HOUR
        self.b2 = str(snowflake(self.made))
        self.failed_in_gcs(self.b2, 10 * HOUR)
        self.junit(self.b2, RED_ON_TEST)
        self.configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                        (self.now - 20 * HOUR, {"dash": {"other": ttc.entry("")}})]
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b2}"]})

    def unlist(self):
        self.srv.summary("dash", {"other": "PASSING"})
        self.srv.table("dash", "other", fetch_table("bucket/logs/otherjob", [], {}))

    def test_control_held_while_listed(self):
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b2}"]})

    def test_still_held_once_unlisted(self):
        """history_rules took t's config since its last entry as not known only while TestGrid
        listed t ("lacking", fetch_red_logs.py:4401): once it no longer did, the build was judged
        as if t had been gone since that entry, not red, though run 1 saw TestGrid list t after the
        build was made."""
        self.unlist()
        run = self.judged(*self.configs)
        self.assertEqual(run["outcomes"], {"held": [f"job/{self.b2}"]})  # fails: {'not-red': [...]}
        self.assertEqual(list(self.state()["watch"]), [f"job/{self.b2}"])

    def test_judged_once_a_config_with_the_tab_is_known(self):
        self.unlist()
        self.judged(*self.configs)
        # (the run of the upload job 20 h ago, built at last, kept t: TestGrid showed the build red there)
        built = [self.configs[0], (self.now - 20 * HOUR, {"dash": {"t": ttc.entry(""), "other": ttc.entry("")}})]
        self.assertEqual(self.judged(*built)["outcomes"], {"backfilled": [f"job/{self.b2}"]})  # fails: {}


    def test_the_record_of_such_tabs(self):
        # (new: state.json keeps when TestGrid last listed each; a malformed record is dropped)
        self.assertEqual(set(self.state()["lacking_tabs"]), {"dash#t"})  # fails: no lacking_tabs
        warnings = []
        for bad in ([], {"dash#t": "yesterday"}, {"dash#t": -1.0}):
            frl.write_json(os.path.join(self.root, "state.json"), dict(self.state(), lacking_tabs=bad))
            self.assertNotIn("lacking_tabs", frl.load_state(self.root, warnings))
        self.assertEqual(warnings, ["state.json: dropped a malformed lacking_tabs record"] * 3)


# ------------------------------------------------------------------ INDEX.md shows the run's verification

class VerificationInTheIndex(ArchiveWorld):
    """A run that archived one red build, then its verification (cross_check.py --record), as the
    workflow runs them. The fetch writes INDEX.md before the run is verified: its row says pending."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.rel = frl.run_dirs(self.root)[-1]

    def verify(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "200",
                            "--record"])

    def row(self):
        with open(os.path.join(self.root, "INDEX.md")) as f:
            return next(line for line in f.read().splitlines() if line.startswith("| [") and f"]({self.rel}/)" in line)

    def show_verifications(self):
        with contextlib.redirect_stdout(io.StringIO()):
            return frl.main(["--archive", self.root, "--show-verifications"])

    def test_control_the_fetch_writes_pending(self):
        self.assertTrue(self.row().endswith(" | ok | pending |"), self.row())

    def test_the_verification_is_shown_before_the_commit(self):
        """The workflow committed INDEX.md as the fetch wrote it: its row of the run said pending
        whatever the verification recorded (fetch_red_logs.py:3665, run_rows), until the next run's
        fetch wrote it again (half a day later)."""
        self.assertEqual(self.verify(), 0)
        with open(os.path.join(self.root, "INDEX.md")) as f:
            before = f.read()
        self.assertEqual(self.show_verifications(), 0)  # fails: no --show-verifications
        self.assertEqual(self.row()[-len(f" | [ok]({self.rel}/verification.json) |"):],
                         f" | [ok]({self.rel}/verification.json) |")
        with open(os.path.join(self.root, "INDEX.md")) as f:
            after = f.read()
        self.assertEqual(after.replace(f" | [ok]({self.rel}/verification.json) |", " | pending |"), before)

    def test_so_are_its_problems(self):
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        self.assertEqual(self.verify(), 1)
        self.assertEqual(self.show_verifications(), 0)  # fails: no --show-verifications
        self.assertTrue(self.row().endswith(f" | [1 problem]({self.rel}/verification.json) |"), self.row())

    def test_and_that_none_was_recorded(self):
        self.assertEqual(self.show_verifications(), 0)  # fails: no --show-verifications
        self.assertTrue(self.row().endswith(" | ok | not recorded |"), self.row())

    def test_the_workflow_shows_it_after_the_verification_and_before_the_commit(self):
        with open(os.path.join(HERE, "..", ".github", "workflows", "red-build-logs.yml")) as f:
            steps = yaml.safe_load(f)["jobs"]["archive"]["steps"]
        names = [s.get("name") for s in steps]
        show = next(i for i, s in enumerate(steps) if "--show-verifications" in s.get("run", ""))  # fails: none
        self.assertLess(names.index("Verify the archive against TestGrid and GCS"), show)
        self.assertLess(show, names.index("Commit the archive"))
        self.assertEqual(steps[show]["if"], "${{ !cancelled() }}")  # (after a failed or timed-out verification too)



# ------------------------------------------------------------------ INDEX.md: what later runs still have to do

class LeftForLater(ttc.ConfigWorld):
    """A tab whose history gap reaches 40 days back (its older part is searched by the next runs),
    with a range a scan saw 3 days ago kept for a config of the tab not known yet."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def index(self):
        with open(os.path.join(self.root, "INDEX.md")) as f:
            return f.read()

    def test_control_nothing_left(self):
        self.judged((self.now - 60 * DAY, self.tab(ttc.entry(self.HIDE))))
        self.assertNotIn("## Left for later runs", self.index())

    def test_the_index_lists_what_later_runs_still_have_to_do(self):
        """INDEX.md said nothing of either: they were only in state.json (and the run that found the
        gap's older part warned of it)."""
        pending = {"since": self.now - 3 * DAY, "lo": self.now - 9 * DAY, "hi": self.now - 8 * DAY}
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * DAY, "oldest": self.now - 45 * DAY,
                                        "changed_pending": [pending]}})
        run = self.judged((self.now - 60 * DAY, self.tab(ttc.entry(self.HIDE))), (self.now - 2 * DAY, None))
        t = self.state()["tabs"]["dash#t"]
        lo, hi = t["unsearched"]
        self.assertEqual(run["left_for_later"], {"gap_parts": {"dash#t": [lo, hi]},  # fails: no left_for_later
                                                 "config_ranges": {"dash#t": {"ranges": 1, "oldest_scan": pending["since"]}}})
        self.assertIn("## Left for later runs\n\n"
                      f"- dash#t: the older part of its history gap, from {frl.iso(lo)} to {frl.iso(hi)}, is searched "
                      "in GCS by the next runs\n"
                      f"- dash#t: 1 range of builds a scan saw, waiting for a config of the tab not known yet (the "
                      f"oldest scan {frl.iso(pending['since'])}); each is dropped 14 days after its scan\n", self.index())


if __name__ == "__main__":
    unittest.main()
