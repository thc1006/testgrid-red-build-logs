"""Review wave W6b: TestGrid's config over time (e5df0fb, 35c5616, 3395a24) and the run logic.
Each test fails on bff8ac5 for the reason its docstring gives; a test named test_control_*
passes and shows the finding's scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round15 -v
"""
import json
import os
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
from test_integrity import HOUR  # noqa: E402


# ------------------------------------------------------------------ F1: the script cannot import its module

class ScriptImports(unittest.TestCase):
    def test_testgrid_config_py_runs_the_way_testgrid_config_sh_runs_it(self):
        """testgrid_config.sh runs every step as `"$python" -I "$here/testgrid_config.py" ...`
        (lines 35, 36, 57, 68, 94), and the workflow sets PYTHON=tool/.venv/bin/python. Python's
        -I (isolated mode) leaves the script's own folder off sys.path, and testgrid_config.py
        imports its neighbours goregexp and testgrid_rule (lines 64-65) without putting that
        folder on sys.path (fetch_red_logs.py does), while the venv does not install them
        ([tool.uv] package = false). So the script's first step (uploads) dies with
        ModuleNotFoundError, set -e ends the script, and no config is written, on every run:
        the fetch says the config "could not be read", keeps judging by the tab configs
        state.json holds (read 2026-10-11T19:31Z), never builds a history, and 3 days later
        every run fails ("TestGrid's config was last read ..."). Run as the workflow runs it,
        tool/testgrid_config.sh exits 1 within a second. The tests that run testgrid_config.sh
        (test_review_round10.Script, test_review_round13.ScriptSteps) replace
        testgrid_config.py with a stub that puts the tool's folder on sys.path, so they pass.
        (Here: the script's `recipes` step, which reads only the file it is given.)"""
        recipe = "0" * 12 + "-" + "1" * 12
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "uploads.json")
            with open(path, "w") as f:
                json.dump({"build": [{"recipe": recipe, "commit": "2" * 40, "job": None}]}, f)
            done = subprocess.run([sys.executable, "-I", os.path.join(HERE, "testgrid_config.py"), "recipes", path],
                                  capture_output=True, text=True, timeout=120)
        self.assertEqual(done.returncode, 0, done.stderr[-300:])  # fails: ModuleNotFoundError: goregexp
        self.assertEqual(done.stdout.split(), [recipe, "2" * 40])


# ------------------------------------------------------------------ F2: the run before a failing config

class NeverBuildsBefore(unittest.TestCase):
    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def test_the_run_before_a_config_that_cannot_be_built_is_not_rebuilt_on_every_archive_run(self):
        """README ("When something fails", TestGrid's config row): "A run whose config cannot be
        built is tried again on every run, but the runs around it are not rebuilt to compare
        with it until it is built" (e5df0fb's message: "remembered so its neighbours are not
        rebuilt each run"). plan() skips the known run after it (testgrid_config.py:777-778),
        but for the failing run itself it still adds the run before it (a, whose config is
        known) to the configs to build, to compare with (lines 779-785): every archive run
        rebuilds a's config (git fetch, checkout, configurator, extract: a config takes about
        25 s on a GitHub runner, of the 480 s budget) for a comparison that cannot happen, for
        as long as c fails (W5b's test of the same scenario checks only e)."""
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        t = int(time.time()) - DAY
        for i, c in enumerate("ace"):  # (c's config cannot be built; a's and e's are known)
            self.upload(t + 60 * i, commit=c * 40)
        tabs = {"d#t": tch.E1, "d#u": dict(tch.E1, test_group="other")}
        h = frl.ConfigHistory()
        h.merge(tch.loaded([{k: r[k] for k in ("build", "commit", "started", "finished")}
                            for r in tgc.upload_runs(t - 1, gcs=self.srv.url)],
                           tch.configs(a={"d#t": tch.E1}, e=tabs), failed={ttc.recipe_of("c" * 40): "the configurator failed"}))
        uploads, entries = h.encode()
        state = {"config_uploads": uploads, "config_entries": entries}
        for _ in range(2):  # (the next archive runs: c fails again each time)
            built = [b["commit"][0] for b in tgc.plan(state, time.time())["build"]]
            self.assertIn("c", built)
            self.assertNotIn("e", built)
            self.assertNotIn("a", built)  # fails: ['c', 'a'], each time


# ------------------------------------------------------------------ F4: the tabulator applies new filters late

class TabulatorLag(ttc.ConfigWorld):
    """self.b (made 30 hours ago, red only on kubetest.Test, which HIDE hides) was on the tab
    when it was scanned an hour ago; it shows only builds of the last hour now (round14's world)."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def setUp(self):
        super().setUp()
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR}})

    def test_control_a_filter_change_taken_long_before_the_last_scan_was_seen_by_it(self):
        # (passes: taken 2 hours before the scan, the tab showed the new filters by then)
        upload = self.now - HOUR - frl.PICKUP - 2 * HOUR
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))), (upload, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {})

    def test_a_filter_change_taken_shortly_before_the_last_scan_may_be_shown_after_it(self):
        """TestGrid's tabulator applies a tab's new filters only when it next writes the tab: on
        a new config it re-inits its queue, which keeps the next time of every group it has
        (util/queue Queue.Init skips names it holds; FixPersistent loads only once), so a tab
        is rewritten at its group's next grid write (the updater's pubsub, 5 s later) or within
        its --wait=1h (cluster/prod/tabulator.yaml), not at PICKUP. config_changed ends the old
        config at the next run's finish + PICKUP (ConfigHistory.eras): a filter change taken 20
        minutes before the last scan counts as seen by it, though the tab may still have shown
        the old filters then and switched after it. b, on the tab at that scan (not red: HIDE
        hid its red row), red by the new filters, scrolled off since: TestGrid may have shown
        it red after that scan, and it is never searched (a missed red)."""
        upload = self.now - HOUR - frl.PICKUP - 20 * 60
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))), (upload, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})  # fails: {}


# ------------------------------------------------------------------ F5: a watched build keeps a later `since`

class WatchedSince(ttc.ConfigWorld):
    """Tab t hid kubetest.Test (HIDE) until run B (4 h ago) showed every row; run C (3 h ago)
    hid it again. self.b (made 30 h ago, red only on kubetest.Test) was on t all along, so t
    showed it red from B's switch until C's. A scan 5 h ago (S0) saw it before B's switch; that
    scan's range of builds was kept "changed_pending" (a config not known then); the last scan
    (S1, 1 h ago) came after C's switch."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def world(self, watched_since):
        s0, s1 = self.now - 5 * HOUR, self.now - HOUR
        state = {"tabs": {"dash#t": {"scan": s1, "oldest": self.now - 60 * HOUR,
                                     "changed_pending": [{"since": s0, "lo": self.now - 60 * HOUR,
                                                          "hi": self.now - 2 * HOUR}]}}}
        if watched_since is not None:
            rec = {"job": "job", "build": self.b, "query": "bucket/logs/job", "started": None,
                   "created": frl.created(self.b), "watch": True,
                   "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": None, "red_cells": [],
                             "judged_from_gcs": True, "since": watched_since}]}
            state["watch"] = {f"job/{self.b}": {"rec": rec, "first_seen": s1}}
        self.put_state(**state)
        return self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))),
                           (self.now - 4 * HOUR, self.tab(ttc.entry(""))),
                           (self.now - 3 * HOUR, self.tab(ttc.entry(self.HIDE))))

    def test_control_a_build_not_watched_is_judged_from_the_earliest_scan_that_saw_it(self):
        # (passes: the pending range from S0, now known to have changed the filters, finds b red)
        run_ = self.world(watched_since=None)
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_watched_build_found_in_a_range_of_an_earlier_scan_is_judged_from_that_scan(self):
        """backfill_gaps (fetch_red_logs.py:3835-3846) takes, per tab, the earliest `since` of
        the ranges that list a build (3395a24: min), but for a build watched already it only
        drops its tab's "since" for a range without one: a range with an earlier `since` (a
        pending range of an earlier scan, resolved now) leaves the record's later one. b is
        watched with t since S1 (a later range listed it, as list_build_ids reaches 6 h before
        a range's start, while it was not decided); S0's range, known now to have changed t's
        filters, lists it too, but it is judged from S1 on only: t's red display of it (B to C,
        all before S1) is not counted, it is "not red" and dropped from the watch list, though
        TestGrid showed it red after S0 saw it (a missed red)."""
        run_ = self.world(watched_since=self.now - HOUR)
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})  # fails: {'not-red': [...]}


# ------------------------------------------------------------------ F3: an "unknown" that is never known

OTHER = dict(ttc.GROUP, name="other", gcs_prefix="bucket/logs/other")


class ForeverUnknown(unittest.TestCase):
    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def test_a_group_change_known_in_full_is_not_unknown_for_good(self):
        """3395a24's group_change gives "unknown" ("where the archive cannot tell yet") for a test
        group that run b's own comparison (merge, against a's whole config) says "changed" when no
        tab of a, the run before, showed it (fetch_red_logs.py:2604-2608): a job put on the
        archive's dashboard by the commit that also changed its group (as promotions to
        sig-release-master-informing do, with testgrid-alert-email or testgrid-num-failures-to-alert,
        both test group fields). b's group_changes are known, so plan() never compares it again,
        and a's tabs will never show the group: the event stays "unknown" for as long as the
        history keeps b (77 days). config_changed returns None for a scan inside its window (after
        the tab's own switch), and run() keeps that range "changed_pending" and drops it after 14
        days, never searched, without a word (and a definite "changed" of another group in the
        same run is hidden behind it: events() ranks "unknown" first)."""
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        t = int(time.time()) - DAY
        self.upload(t, commit="a" * 40)
        self.upload(t + 60, commit="b" * 40)
        runs = [{k: r[k] for k in ("build", "commit", "started", "finished")} for r in tgc.upload_runs(t - 1, gcs=self.srv.url)]
        e_other = {"test_group": "other", "base_options": "", "group": OTHER, "group_digest": tgc.group_digest(OTHER)}
        h = frl.ConfigHistory()
        h.merge(tch.loaded(runs, tch.configs(a={"d#u": e_other}, b={"d#u": e_other, "d#t": ttc.entry("")}),
                           changes={runs[1]["build"]: {"job": "changed"}}))
        uploads, entries = h.encode()
        self.assertEqual(tgc.plan({"config_uploads": uploads, "config_entries": entries}, time.time())["build"], [])
        hows = [how for _, _, how in h.events("d#t", time.time(), "bucket/logs/job")]
        rule = frl.history_rules(h, ["d#t"], time.time(), (), {"d#t": "bucket/logs/job"})["d#t"]
        since = runs[1]["finished"] + frl.PICKUP + 600  # (a scan after t appeared, while an update may still read)
        self.assertNotIn("unknown", hows)  # fails: ['unknown']
        self.assertIsNotNone(frl.config_changed(rule, since, since - DAY))


if __name__ == "__main__":
    unittest.main()
