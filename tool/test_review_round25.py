"""Review of 2026-10-16: an outside review of 4ace4f3, and what testing it found. Each test
fails on 4ace4f3 for the reason its docstring gives; a test named test_control_* passes there
and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round25 -v
"""
import contextlib
import io
import json
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
            "3 archived builds were not compared with GCS before the run reached its deadline "
            f"({', '.join(self.keys(self.mine))}): the next run compares them"])
        time.sleep(3.6)  # (the server's slow answers end before it closes)

    def test_or_one_an_interrupt_cut_off(self):
        for b in self.mine:
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", self.slow(b))
        wait = frl.cf.wait

        def interrupted(fs, *args, **kwargs):  # (as SIGINT while the run waits for its builds' workers)
            if isinstance(fs, dict) and fs and all(isinstance(b, dict) and "build" in b for b in fs.values()):
                raise KeyboardInterrupt
            return wait(fs, *args, **kwargs)
        with mock.patch.object(frl.cf, "wait", interrupted):
            self.assertEqual(self.fetch(), 130)
        run = self.runs()[-1]
        self.assertEqual((run["interrupted"], run["outcomes"].get("not-checked")), (True, self.keys(self.mine)))
        self.assertIn(f"3 archived builds were not compared with GCS before the run was interrupted "
                      f"({', '.join(self.keys(self.mine))}): the next run compares them", run["serious_warnings"])
        time.sleep(3.6)


class SeventhsOfRunsNotDone(unittest.TestCase):
    """gcs_sevenths: which sevenths of the archive a run compares with GCS again, final or not.
    4ace4f3 took the days since the run before, whatever that run did: a run cut off before it
    compared its sevenths, or whose verification found a difference, counted as having compared
    them, so the next run went on to the next day's seventh and left them for a week."""

    DAY = 20_000

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)

    def run_at(self, hours, compared=True, verified="ok", differ=0):
        """A run folder started `hours` after 00:00 UTC of day DAY: whether its fetch compared all
        it was due to, its verification's outcome (None: no verification.json), and how many builds
        the verification's comparisons with GCS found differing (None: it ended before they did)."""
        started = self.DAY * 86400 + hours * 3600
        rel = frl.new_run_dir(self.root, started)[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       {"run": rel.rsplit("/", 1)[1], "started": started, "gcs_compared": compared})
        if verified is not None:
            frl.write_json(os.path.join(self.root, rel, "verification.json"),
                           {"outcome": verified, "scope": {} if differ is None else {"sevenths": [], "differ": differ}})

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

    def test_so_does_one_whose_verification_found_a_difference(self):
        self.run_at(-18)
        self.run_at(6, verified="problems", differ=1)  # (such as "GCS changed since it was archived")
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_or_was_not_verified_to_the_end(self):
        self.run_at(-18)
        for i, outcome in enumerate(("timed out", "interrupted", "stopped", "incomplete", None)):
            self.run_at(6 + i, verified=outcome, differ=None)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_a_problem_of_another_kind_leaves_nothing_to_compare_again(self):
        """A run whose verification compared its sevenths, finding none differing, but found a
        problem of another kind (a tab it cannot read, say), was not done with them (d5bb3a8): while
        such a problem lasted, each run compared all seven, the whole archive, and so did its
        verification."""
        for day in range(7):
            self.run_at(6 - 24 * day, verified="problems")
        self.assertEqual(self.sevenths(30), self.days(1))  # fails: all seven
        self.run_at(10, verified="timed out")  # (cut off after its comparisons ended)
        self.assertEqual(self.sevenths(30), self.days(1))

    def test_a_run_not_done_on_the_day_of_the_last_done_one(self):
        # (the run done that day compared its seventh, but the later one may have found GCS changed since)
        self.run_at(1)
        self.run_at(5, compared=False)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_no_run_done_in_the_week_before(self):
        self.run_at(-50, compared=False)
        self.run_at(-20, verified="problems", differ=1)
        self.assertEqual(self.sevenths(6), set(range(7)))  # fails: {DAY} only
        self.run_at(-8 * 24)  # (one done more than a week before: all seven still)
        self.assertEqual(self.sevenths(6), set(range(7)))


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

    def world(self, age, then=lambda: None):
        s0 = self.now - age
        made = s0 - 6 * HOUR
        b = str(snowflake(made))
        self.failed_in_gcs(b, self.now - made)
        self.junit(b, RED_ON_TEST)
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR,
                                        "changed_pending": [{"since": s0, "lo": made - HOUR, "hi": made + HOUR}]}})
        then()
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

    def test_one_past_those_a_tab_keeps(self):
        """load_state kept the newest 60 ranges of a tab: an older one, while a config stayed not
        known through more than 60 runs in 14 days, went with no warning."""
        s0 = self.now - 8 * DAY
        ranges = [{"since": s0 + i * 3 * HOUR, "lo": s0 - 7 * HOUR + i * 3 * HOUR, "hi": s0 - 4 * HOUR + i * 3 * HOUR}
                  for i in range(61)]
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR,
                                        "changed_pending": ranges}})
        # (the upload job ran every 3 hours since s0, and none of its configs is known)
        run = self.judged((self.now - 40 * DAY, self.tab(ttc.entry(self.HIDE))),
                          *[(s0 - HOUR + i * 3 * HOUR, None) for i in range(64)])
        for p in ranges[:2]:  # (the run's own range, from the last scan, came after them)
            self.assertIn(f"dash#t: the builds made from {frl.iso(p['lo'])} to {frl.iso(p['hi'])}, seen not red by the "
                          f"scan of {frl.iso(p['since'])}, cannot be judged from GCS: more than 60 ranges of the tab wait "
                          "for a config; no longer judged, though TestGrid may have shown them red",
                          run["serious_warnings"])  # fails: not there
        self.assertEqual(self.state()["tabs"]["dash#t"]["changed_pending"][:-1], ranges[2:])

    def test_once_though_the_gap_search_fails(self):
        # (a run whose gap search fails keeps the tab's ranges for the next run: not the one it dropped)
        b, s0, made, run = self.world(14 * DAY + 60, then=self.listing_fails)
        self.assertEqual(sum("seen not red by the scan" in w for w in run["serious_warnings"]), 1)  # fails: 0
        run = self.judged((self.now - 40 * DAY, self.tab(ttc.entry(self.HIDE))), (s0 + HOUR, None))
        self.assertEqual(sum("seen not red by the scan" in w for w in run["serious_warnings"]), 0)
        self.assertNotIn("changed_pending", self.state()["tabs"]["dash#t"])

    def test_so_is_the_unsearched_part_of_a_tab_forgotten(self):
        """run() forgot a tab unlisted for 30 days with the older part of its history gap still to
        search, and said nothing of it (a tab not listed is not read, so it is never searched)."""
        s0 = self.now - 20 * DAY
        part = [s0 - 50 * DAY, s0 - 14 * DAY]
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR},
                             "dash#gone": {"scan": s0, "oldest": s0 - 60 * HOUR, "missing_since": self.now - 31 * DAY,
                                           "unsearched": part}})
        run = self.judged((self.now - 40 * DAY, {"dash": {"t": ttc.entry(""), "gone": ttc.entry("")}}))
        self.assertIn(f"dash#gone: the older part of its history gap, from {frl.iso(part[0])} to {frl.iso(part[1])}, "
                      "cannot be searched in GCS: the tab is no longer listed, and is forgotten; red builds in it may be "
                      "missing", run["serious_warnings"])  # fails: not there

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


    def test_control_a_config_just_uploaded_explains_the_tab(self):
        # (TestGrid may not have taken the config that dropped t yet, an upload 10 minutes ago: it lists t
        # under the config before, which hid the build's red row. Once it no longer lists t, the build is
        # not red, and nothing waits for t: no 60-day hold, ended by a warning that a config is not known.)
        configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry(self.HIDE)}}),
                   (self.now - 600, {"dash": {"other": ttc.entry("")}})]
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        self.assertEqual(self.judged(*configs)["outcomes"], {"held": [f"job/{self.b2}"]})
        self.assertNotIn("lacking_tabs", self.state())
        self.unlist()
        self.assertEqual(self.judged(*configs)["outcomes"], {"not-red": [f"job/{self.b2}"]})

    def test_the_record_of_such_tabs(self):
        # (new: state.json keeps when TestGrid last listed each; a malformed record is dropped)
        self.assertEqual(set(self.state()["lacking_tabs"]), {"dash#t"})  # fails: no lacking_tabs
        warnings = []
        for bad in ([], {"dash#t": "yesterday"}, {"dash#t": -1.0}):
            frl.write_json(os.path.join(self.root, "state.json"), dict(self.state(), lacking_tabs=bad))
            self.assertNotIn("lacking_tabs", frl.load_state(self.root, warnings))
        self.assertEqual(warnings, ["state.json: dropped a malformed lacking_tabs record"] * 3)


class LackingRecordAndLaterConfigs(ttc.ConfigWorld):
    """state.json records that a run saw TestGrid list tab t while the latest config the archive
    knew had no t (lacking_tabs), and configs are known since. A build made 10 h ago, red only on
    the rows HIDE hides, is in t's history gap. The record held t's configs after its last entry
    as not known, those that came into force after the record too (8c15963)."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged

    def setUp(self):
        super().setUp()
        self.b2 = str(snowflake(self.now - 10 * HOUR))
        self.failed_in_gcs(self.b2, 10 * HOUR)
        self.junit(self.b2, RED_ON_TEST)
        # C1 hides the build's red row, C2 has no t, C3 has t again, C4 (10 minutes ago) has no t
        self.configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry(self.HIDE)}}),
                        (self.now - 25 * DAY, {"dash": {"other": ttc.entry("")}}),
                        (self.now - 15 * DAY, {"dash": {"t": ttc.entry(self.HIDE), "other": ttc.entry("")}}),
                        (self.now - 600, {"dash": {"other": ttc.entry("")}})]

    def record(self, ago):
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       **({"lacking_tabs": {"dash#t": self.now - ago}} if ago is not None else {}))

    def unlisted(self):
        """TestGrid takes C4 and stops listing t: the outcomes of the next run."""
        self.srv.summary("dash", {"other": "PASSING"})
        self.srv.table("dash", "other", fetch_table("bucket/logs/otherjob", [], {}))
        return self.judged(*self.configs)["outcomes"]

    def test_control_without_a_record(self):
        self.record(None)
        # (TestGrid lists t, C4 not taken yet: t's config since C3 is not known)
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b2}"]})
        self.assertEqual(self.unlisted(), {"not-red": [f"job/{self.b2}"]})

    def test_a_record_from_before_c3(self):
        """A record from 20 days ago held the build for C4, which came into force long after it,
        for 61 days."""
        self.record(20 * DAY)
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b2}"]})
        self.assertEqual(self.unlisted(), {"not-red": [f"job/{self.b2}"]})  # fails: held

    def test_a_record_c3_explains(self):
        """C3 was built late: 20 hours ago C2 was the latest config the archive knew, while TestGrid
        listed t. The build stayed held 60 days, then the run failed with a warning that a config of
        t was not known, though all were."""
        self.record(20 * HOUR)
        self.assertEqual(self.judged(*self.configs)["outcomes"], {"held": [f"job/{self.b2}"]})
        self.assertEqual(self.unlisted(), {"not-red": [f"job/{self.b2}"]})  # fails: held

    def test_a_record_between_two_configs_without_the_tab(self):
        """C2 and C3 have no t, and a record from 2.5 days ago falls between them. The record held
        the build for C3 too, which came into force after it (31a580f): the two runs made one era
        in ConfigHistory.eras, which began before the record. The hold ended 60 days after the
        build was made, in a warning that a config of t was not known."""
        configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                   (self.now - 3 * DAY, {"dash": {"other": ttc.entry("")}}),
                   (self.now - 2 * DAY, {"dash": {"other": ttc.entry("")}}),
                   (self.now - 5 * HOUR, {"dash": {"t": ttc.entry(self.HIDE), "other": ttc.entry("")}})]
        self.record(2.5 * DAY)
        self.assertEqual(self.judged(*configs)["outcomes"], {})  # fails: held
        self.assertEqual(self.state()["watch"], {})  # (judged: not red, by C3 without t and C4 hiding its row)

    def test_a_config_just_uploaded_after_one_without_the_tab_too(self):
        """549f053 kept no record of a tab the latest config lacks if that config was uploaded so
        recently that TestGrid may not have taken it, though the config before lacked the tab too,
        so TestGrid listed it under neither. Once TestGrid no longer listed the tab, the build was
        judged not red; the run that saw it listed warned only that TestGrid may not have taken
        the latest config yet."""
        other = ttc.entry("", name="otherjob", gcs_prefix="bucket/logs/otherjob")
        configs = [(self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                   (self.now - 20 * HOUR, {"dash": {"other": other}}),
                   (self.now - 600, {"dash": {"other": other}})]
        self.record(None)
        run = self.judged(*configs)
        self.assertEqual(run["outcomes"], {"held": [f"job/{self.b2}"]})
        self.assertIn("it lacks tabs TestGrid shows", " ".join(run["serious_warnings"]))  # fails: not serious
        self.srv.summary("dash", {"other": "PASSING"})
        self.srv.table("dash", "other", fetch_table("bucket/logs/otherjob", [], {}))
        self.assertEqual(self.judged(*configs)["outcomes"], {"held": [f"job/{self.b2}"]})

    def later(self):
        """C2 (20 hours ago) has no t, C3 (5 hours ago) has t, hiding the build's red row."""
        return [(self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                (self.now - 20 * HOUR, {"dash": {"other": ttc.entry("")}}),
                (self.now - 5 * HOUR, {"dash": {"t": ttc.entry(self.HIDE), "other": ttc.entry("")}})]

    def test_control_without_a_record_c3_hides_it(self):
        self.record(None)
        self.assertEqual(self.judged(*self.later())["outcomes"], {})
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)

    def test_a_record_a_later_config_does_not_explain(self):
        """8 hours ago, after the build was made, a run saw TestGrid list t under a config the
        archive does not know (C2, the latest it knew, had no t): TestGrid may have shown the build
        red then. Once C3 was known, a config with t from after that, the record counted for
        nothing."""
        self.record(8 * HOUR)
        self.assertEqual(self.judged(*self.later())["outcomes"], {"held": [f"job/{self.b2}"]})  # fails: {}


class LackingRecordWhileAnUploadRan(ttc.ConfigWorld):
    """C1 (30 days ago) shows every row, C2 (2 days ago) has no t, and C3 has t, hiding the rows a
    build made 10 h ago is red on: C3's run of the upload job began 70 minutes ago and ended 40
    minutes ago. A lacking_tabs record says a run saw TestGrid list t while C2 was the latest
    config known."""

    def history(self, *configs):
        """As ConfigOverTime.history, each config (started, finished, dashboards)."""
        runs, built = [], {}
        for i, (started, finished, dashboards) in enumerate(configs):
            commit = f"{i + 1:040x}"
            runs.append({"build": str(snowflake(started)), "commit": commit, "started": int(started),
                         "finished": int(finished)})
            built[commit] = ({f"{d}#{t}": e for d, tabs in dashboards.items() for t, e in tabs.items()},
                             {"test_infra": commit})
        h = frl.ConfigHistory()
        h.merge(tch.loaded(runs, built))
        uploads, entries = h.encode()
        frl.write_json(os.path.join(self.root, "state.json"),
                       dict(self.state(), config_uploads=uploads, config_entries=entries))
        return ttc.write_config(os.path.join(self.dir, "now.json"), [runs[-1]], {
            runs[-1]["commit"]: {"committed": runs[-1]["started"] - 60, "image": "img", "dashboards": configs[-1][2]}})

    def judged(self, ago):
        b2 = str(snowflake(self.now - 10 * HOUR))
        self.failed_in_gcs(b2, 10 * HOUR)
        self.junit(b2, RED_ON_TEST)
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       lacking_tabs={"dash#t": self.now - ago})
        self.fetch_with(self.history(
            (self.now - 30 * DAY - 5, self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
            (self.now - 2 * DAY - 5, self.now - 2 * DAY, {"dash": {"other": ttc.entry("")}}),
            (self.now - 70 * 60, self.now - 40 * 60, {"dash": {"t": ttc.entry(self.HIDE), "other": ttc.entry("")}})))
        return self.runs()[-1]["outcomes"], f"job/{b2}"

    def test_control_a_record_after_c3_was_uploaded(self):
        outcomes, _ = self.judged(30 * 60)
        self.assertEqual(outcomes, {})  # (TestGrid may have had C3 then, which has t: not red by it)

    def test_a_record_while_c3_ran(self):
        """C3's run had not uploaded it when the record was made: TestGrid did not have it, and
        showed t under a config not known. C3 counted as t's last entry before the record, by its
        run's start."""
        outcomes, b2 = self.judged(60 * 60)
        self.assertEqual(outcomes, {"held": [b2]})  # fails: {}


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

    def test_a_tab_no_longer_listed(self):
        # (it is not read: its gap is searched, and its ranges looked at, only if TestGrid lists it again)
        s0, gone = self.now - 20 * DAY, self.now - 5 * DAY
        part, pending = [s0 - 50 * DAY, s0 - 14 * DAY], {"since": s0, "lo": s0 - 7 * HOUR, "hi": s0 - 5 * HOUR}
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 60 * HOUR},
                             "dash#gone": {"scan": s0, "oldest": s0 - 60 * HOUR, "missing_since": gone,
                                           "unsearched": part, "changed_pending": [pending]}})
        self.judged((self.now - 40 * DAY, {"dash": {"t": ttc.entry(""), "gone": ttc.entry("")}}))
        by = frl.iso(gone + 30 * DAY)
        self.assertIn(f"- dash#gone: the older part of its history gap, from {frl.iso(part[0])} to {frl.iso(part[1])}, "
                      f"is searched in GCS if TestGrid lists the tab again by {by}\n"
                      "- dash#gone: 1 range of builds a scan saw, waiting for a config of the tab not known yet (the "
                      f"oldest scan {frl.iso(s0)}); looked at again if TestGrid lists the tab again by {by}\n",
                      self.index())  # fails: no such list

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



# ------------------------------------------------------------------ a build taken out of the archive for good

class TakenOut(ArchiveWorld):
    """A red build archived, then taken out by hand (as for a log that must not stay public):
    excluded.json names it. TestGrid still shows it red."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.calls = []
        self.srv.route(self.base + "/build-log.txt", obj(b"a log that must not stay public\n", "1", self.calls))
        self.assertEqual(self.fetch(), 0)

    def exclude(self, text=None):
        with open(os.path.join(self.root, "excluded.json"), "w") as f:
            f.write(text if text is not None else json.dumps({self.key: {"reason": "it holds a token"}}) + "\n")

    def state(self):
        return frl.read_json(os.path.join(self.root, "state.json"))

    def test_control_a_build_removed_by_hand_comes_back(self):
        shutil.rmtree(self.bdir())
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived": [self.key]})

    def test_a_build_taken_out_is_removed_and_never_fetched_again(self):
        """No run read a list of builds taken out: one removed by hand was archived again by the
        next run, as TestGrid still showed it red, and nothing kept a run from it."""
        self.exclude()
        asked = len(self.calls)
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.bdir())  # fails: still there
        self.assertEqual(self.runs()[-1]["outcomes"], {"excluded": [self.key]})
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.bdir(), len(self.calls)), (None, asked))  # (its log is not asked for again)
        self.assertEqual(self.check()[0], 0)  # (cross_check.py does not count it as missing)

    def test_one_waiting_to_be_archived_is_dropped(self):
        shutil.rmtree(self.bdir())
        self.srv.route(self.base + "/build-log.txt", (503, {}, b""))
        self.assertEqual(self.fetch(), 1)  # (the log could not be fetched: retried by the next runs)
        self.assertIn(self.key, self.state()["unresolved"])
        self.exclude()
        self.assertEqual(self.fetch(), 0)  # fails: 1, as it is retried
        self.assertNotIn(self.key, self.state()["unresolved"])
        self.assertEqual(self.runs()[-1]["outcomes"], {"excluded": [self.key]})

    def test_a_list_that_cannot_be_used_stops_the_run(self):
        # (it could fetch a build taken out again)
        runs = len(frl.run_dirs(self.root))
        for text in ("{", "[]", "null", json.dumps({"job/x": {"reason": "why"}}), json.dumps({self.key: {}}),
                     json.dumps({self.key: {"reason": " "}})):
            self.exclude(text)
            self.assertEqual(self.fetch(), 1, text)  # fails: 0
            self.assertEqual(len(frl.run_dirs(self.root)), runs, text)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("cannot use the archive's excluded.json", out)

    def test_a_build_no_run_can_have_stops_the_run(self):
        """A build named with a 0 first (no build of Prow's has one) can never be one archived: the
        build meant stayed, and nothing said so."""
        self.exclude(json.dumps({f"job/0{self.build}": {"reason": "it holds a token"}}))
        self.assertEqual(self.fetch(), 1)  # fails: 0, the build still archived

    def test_whatever_is_in_its_folder(self):
        for name in ("notes", "old"):  # (folders a person left there: the fetch writes none)
            os.mkdir(os.path.join(self.bdir(), name))
        self.exclude()
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.bdir())  # fails: still there

    def test_a_folder_that_cannot_go_is_an_error_of_the_run(self):
        self.exclude()
        with mock.patch.object(frl, "remove_build", side_effect=PermissionError("not allowed")):
            self.assertEqual(self.fetch(), 1)  # fails: 0
        run = self.runs()[-1]
        self.assertEqual(run["errors"], [{"build": self.key, "error": "taking it out (excluded.json) failed: "
                                                                      "PermissionError: not allowed"}])
        self.assertEqual(run["outcomes"], {"excluded": [self.key]})

    def test_cross_check_reports_one_still_archived(self):
        self.exclude()
        code, out = self.check()  # (before a run took it out)
        self.assertEqual(code, 1)  # fails: 0, not checked
        self.assertIn(f"{self.key}: taken out (excluded.json), but still archived", out)



class TakenOutInTheWindow(ArchiveWorld):
    """A red build made 5 h ago (inside cross_check.py's 48 h window), which GCS lists as it lists
    every build, archived, then taken out."""

    def setUp(self):
        super().setUp()
        self.build = snowflake(int(time.time()) - 5 * HOUR)
        self.key, self.base = f"job/{self.build}", f"/bucket/logs/job/{self.build}"
        self.show([self.build], {"job.Overall": ([12], ["boom"])}, start=int(time.time()) - 5 * HOUR)
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"a log that must not stay public\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.assertEqual(self.fetch(), 0)

    def test_it_is_not_missing(self):
        """cross_check.py's look at GCS's failed builds of the window took one taken out for a missed
        red build ("MISSED failed build"), and counted it as archived among TestGrid's red builds."""
        with open(os.path.join(self.root, "excluded.json"), "w") as f:
            json.dump({self.key: {"reason": "it holds a token"}}, f)
        self.assertEqual(self.fetch(), 0)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48"])
        self.assertEqual(code, 0, out.getvalue())  # fails: 1, MISSED
        self.assertIn("1 red builds in window, 0 archived, 1 taken out (excluded.json), ", out.getvalue())


class TakenOutOfAnOlderRun(ArchiveWorld):
    """A red build a run archived 3.8 days ago, then taken out: INDEX.md rewrites the READMEs of
    recent run folders only."""

    def archived(self, ago, log=True):
        """Its run folder, of a run `ago` seconds ago (without a file, as GCS had none, if not log)."""
        self.finished(ago=ago + 0.1 * DAY)
        if log:
            self.srv.route(self.base + "/build-log.txt", obj(b"a log that must not stay public\n"))
        real = time.time
        with mock.patch("time.time", lambda: real() - ago):
            self.assertEqual(self.fetch(), 0)
        return frl.run_dirs(self.root)[0]

    def exclude(self):
        with open(os.path.join(self.root, "excluded.json"), "w") as f:
            json.dump({self.key: {"reason": "it holds a token"}}, f)

    def readme(self, rel):
        with open(os.path.join(self.root, rel, "README.md")) as f:
            return f.read()

    def test_once_the_run_that_took_it_out_was_cut_off(self):
        """Only a run that removed a build's folder rewrote the README of the run that archived it,
        and only as it ended: one cut off before (by a signal, as a step's timeout or a cancel sends)
        left the row, with its link, for good."""
        rel = self.archived(3.8 * DAY)
        self.exclude()
        with mock.patch.object(frl, "scan", side_effect=KeyboardInterrupt):
            self.assertEqual(self.fetch(), 130)
        self.assertIsNone(self.bdir())  # (taken out before the scan)
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn(self.build, self.readme(rel))  # fails: its row, with a link to its log

    def test_once_a_person_removed_its_folder_too(self):
        rel = self.archived(3.8 * DAY)
        shutil.rmtree(self.bdir())
        self.exclude()
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn(self.build, self.readme(rel))  # fails: its row, with a link to its log

    def test_the_readme_of_a_run_that_fetched_it_again(self):
        # (GCS replaced its log: a run 4 days ago fetched it again, into the folder of the run before)
        rel = self.archived(4.8 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"a log replaced, that must not stay public\n", "2"))
        real = time.time
        with mock.patch("time.time", lambda: real() - 4 * DAY):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})
        later = frl.run_dirs(self.root)[1]
        self.exclude()
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn(self.build, self.readme(rel))
        self.assertNotIn(f"/{self.key}/", self.readme(later))  # fails: "- refreshed: [job/…](…/job/…/)"
        self.assertIn(f"- refreshed: {self.key}\n", self.readme(later))  # (as its run.json says)

    def test_one_archived_without_a_file(self):
        """Its row links the build on Prow only (no file of it is archived): linking() looked for
        a link to its folder."""
        rel = self.archived(3.8 * DAY, log=False)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived-without-log": [self.key]})
        self.exclude()
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn(self.build, self.readme(rel))  # fails: its row

    def test_a_readme_not_utf8(self):
        """(a byte a hand edit left): linking() could not decode it, and passed it over."""
        rel = self.archived(3.8 * DAY)
        with open(os.path.join(self.root, rel, "README.md"), "ab") as f:
            f.write(b"\xff\n")
        self.exclude()
        self.assertEqual(self.fetch(), 0)
        with open(os.path.join(self.root, rel, "README.md"), "rb") as f:
            self.assertNotIn(self.build.encode(), f.read())  # fails: its row, with a link to its log

    def test_a_readme_that_is_a_link_is_not_followed(self):
        """linking() read a README.md that is a link, as no other reader of the archive does: one
        to /dev/zero, in a run folder older than INDEX.md's, made every run read until it ran out
        of memory."""
        rel = self.archived(3.8 * DAY)
        elsewhere = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, elsewhere)
        outside = os.path.join(elsewhere, "README.md")
        with open(outside, "w") as f:
            f.write(f"[x](../../../runs/{rel}/{self.key}/)\n")
        readme = os.path.join(self.root, frl.run_dirs(self.root)[0], "README.md")
        os.remove(readme)
        os.symlink(outside, readme)
        self.assertEqual(frl.linking(self.root, frozenset({self.key})), set())  # fails: the folder, by the file linked

    def test_its_runs_readme_lists_it_no_more(self):
        self.finished(ago=3.9 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"a log that must not stay public\n"))
        real = time.time
        with mock.patch("time.time", lambda: real() - 3.8 * DAY):
            self.assertEqual(self.fetch(), 0)
        rel = frl.run_dirs(self.root)[0]
        with open(os.path.join(self.root, "excluded.json"), "w") as f:
            json.dump({self.key: {"reason": "it holds a token"}}, f)
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.bdir())
        with open(os.path.join(self.root, rel, "README.md")) as f:
            self.assertNotIn(self.build, f.read())  # fails: its row, with a link to its log


# ------------------------------------------------------------------ a tab the latest config dropped, with a build watched

class ATabTheConfigDropped(ttc.ConfigWorld):
    """The latest config, uploaded 3.5 days ago, dropped tab t, and TestGrid no longer lists t; a
    run 4 days ago read t's config last. A build of t made 10 h ago, still running, is watched."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged

    def test_no_config_of_it_is_left_to_read(self):
        """The run that judged the build warned "TestGrid's config was last read ... for 1 tabs
        (dash#t): changes to their filters or test groups since then are not followed", a serious
        warning, though it read the latest config, which has no t."""
        b = snowflake(self.now - 10 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 10 * HOUR)}))
        self.srv.summary("dash", {"other": "PASSING"})
        self.srv.table("dash", "other", fetch_table("bucket/logs/otherjob", [], {}))
        self.put_state(tabs={"dash#t": {"scan": self.now - 3.5 * DAY, "oldest": self.now - 4 * DAY,
                                        "missing_since": self.now - 3.4 * DAY}},
                       tab_configs={"dash#t": {"config": ttc.entry(""), "read": self.now - 4 * DAY}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - 9 * HOUR}})
        other = ttc.entry("", name="otherjob", gcs_prefix="bucket/logs/otherjob")
        run = self.judged((self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                          (self.now - 3.5 * DAY, {"dash": {"other": other}}))
        self.assertEqual(run["outcomes"], {"not-red": [f"job/{b}"]})  # (t had no tab while it ran)
        self.assertEqual(run["serious_warnings"], [])  # fails: TestGrid's config was last read ...


if __name__ == "__main__":
    unittest.main()
