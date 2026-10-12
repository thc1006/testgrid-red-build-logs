"""Builds the last scan saw that the tab no longer shows, after its config changed (run()'s
gaps, "changed"): TestGrid may have shown them red after that scan (its tabulator applies a
tab's new filters to the grid it has; an update after a change to the test group reads them
again) and dropped them before this one (they scrolled off, or TestGrid dropped its oldest
columns to keep the group's grid under its size limit: shrinkGridInline), unseen by any scan.
They are judged by what the tab may have shown since that scan."""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected again here)
import test_review_round11 as r11  # noqa: E402
import test_review_round9 as r9  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

MINUTE = 60


class ChangedSinceTheLastScan(ttc.ConfigWorld):
    """self.b (made 30 hours ago, red only on kubetest.Test, which HIDE hides) was on the tab
    when it was scanned an hour ago; it shows only builds of the last hour now."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    tab = tch.ConfigOverTime.tab

    def setUp(self):
        super().setUp()
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR}})

    def test_a_build_shown_red_after_its_tabs_filter_changed_is_taken(self):
        # (the tab showed every row from half an hour ago on: b, still in its grid then, was red)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))),
                           (self.now - HOUR / 2, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertIn("its config changed since a scan", " ".join(run_["warnings"]))
        self.assertNotIn("since", str(self.meta(self.b)["tabs"]))

    def test_without_a_change_the_last_scan_is_what_it_showed(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))))
        self.assertEqual(run_["outcomes"], {})
        self.assertIsNone(self.bdir(self.b))
        self.assertNotIn("its config changed", " ".join(run_["warnings"]))

    def test_a_change_before_the_last_scan_was_seen_by_it(self):
        # (the filter changed 10 hours ago, nine before the last scan: that scan saw what it showed)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))),
                           (self.now - 10 * HOUR, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {})

    def waited_for(self):
        # (a run of the upload job whose config is not known yet, after the last scan: whether it changed
        # the tab is decided once it is known, by a later run; the builds that scan saw are kept until then)
        configs = [(self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))), (self.now - HOUR / 2, None)]
        run_ = self.judged(*configs)
        self.assertEqual(run_["outcomes"], {})
        pending = self.state()["tabs"]["dash#t"]["changed_pending"]
        self.assertEqual([(p["since"], p["lo"]) for p in pending], [(self.now - HOUR, self.now - 60 * HOUR)])
        return configs

    def test_a_config_not_known_yet_is_waited_for(self):
        self.waited_for()

    def test_a_config_known_later_that_changed_the_filters_takes_them(self):
        configs = self.waited_for()
        run_ = self.read_later(configs, ttc.entry(""))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertNotIn("changed_pending", self.state()["tabs"]["dash#t"])

    def test_a_config_known_later_with_the_same_filters_does_not(self):
        configs = self.waited_for()
        run_ = self.read_later(configs, ttc.entry(self.HIDE + "&width=20"))  # (drawn differently, the same rows)
        self.assertEqual(run_["outcomes"], {})
        self.assertNotIn("changed_pending", self.state()["tabs"]["dash#t"])

    def read_later(self, configs, entry):
        """The next run, which knows the config of the run that was not known."""
        unknown = self.state()["config_uploads"][-1]
        path = ttc.write_config(os.path.join(self.dir, "later.json"),
                                [{k: unknown[k] for k in ("build", "commit", "started", "finished")}],
                                {unknown["commit"]: {"committed": unknown["started"] - 60, "image": "img",
                                                     "dashboards": self.tab(entry)}})
        self.fetch_with(path)
        return self.runs()[-1]

    def test_a_change_to_the_test_group_reads_them_again_only_while_recent(self):
        # (c, 10 days old, its failing junit file written 5 days ago, long after its reads; the last scan
        # saw it. A "changed" group reads again the builds of the last 7 days only; one that kept 7 days
        # of results or fewer is read again in full, back to its new days of results)
        for before, red in ((15, False), (7, True)):
            self.setUp()
            c = snowflake(self.now - 10 * DAY)
            base = f"/bucket/logs/job/{c}"
            self.srv.route(f"{base}/started.json", (200, {}, {"timestamp": int(self.now - 10 * DAY)}))
            self.srv.route(f"{base}/finished.json", (200, {}, {"result": "SUCCESS", "passed": True,
                                                               "timestamp": int(self.now - 10 * DAY + HOUR)}))
            self.srv.route(f"{base}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
            self.srv.route(f"{base}/build-log.txt", (200, {}, b"log\n"))
            name = f"logs/job/{c}/artifacts/junit_01.xml"
            self.srv.route(f"/bucket/{name}", (200, {}, tch.KUBETEST_FAILED))
            self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b, c)],
                                 written={name: self.now - 5 * DAY})
            self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 13 * DAY}})
            run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry("", days_of_results=before))),
                               (self.now - HOUR / 2, self.tab(ttc.entry("", days_of_results=16))))
            self.assertEqual(f"job/{c}" in run_["outcomes"].get("backfilled", []), red, before)



class ConfigsNotKnownForDays(ttc.ConfigWorld):
    """A run of the upload job the archive listed four days ago whose config is not known yet:
    the run fails if that config could not be built, or if this run built none (the backlog
    is not going down); while configs are being built, newest first, it waits its turn."""

    def fetched(self, failed, built_other):
        old = {"build": str(snowflake(self.now - 6 * DAY)), "commit": "1" * 40, "started": int(self.now - 6 * DAY) - 5,
               "finished": int(self.now - 6 * DAY)}
        new = {"build": str(snowflake(self.now - 2 * HOUR)), "commit": "2" * 40, "started": int(self.now - 2 * HOUR) - 5,
               "finished": int(self.now - 2 * HOUR)}
        state = self.state()
        state["config_uploads"] = [dict(old, tabs=None, listed=self.now - 4 * DAY,
                                        **({"failed": "the configurator failed"} if failed else {}))]
        frl.write_json(os.path.join(self.root, "state.json"), state)
        built = {"2" * 40: {"committed": new["started"] - 60, "image": "img", "dashboards": self.tab(ttc.entry(""))}} \
            if built_other else {}
        self.fetch_with(ttc.write_config(os.path.join(self.dir, "cfg.json"), [old, new], built,
                                         after=frl.created(old["build"]) - 1,
                                         failed={"1" * 40: "the configurator failed"} if failed else None))
        return self.runs()[-1]

    tab = tch.ConfigOverTime.tab

    def test_waiting_its_turn_while_others_are_built(self):
        run_ = self.fetched(failed=False, built_other=True)
        self.assertIn("runs of its upload job", " ".join(run_["warnings"]))
        self.assertNotIn("still not known", " ".join(run_["serious_warnings"]))

    def test_one_that_could_not_be_built_is_serious(self):
        run_ = self.fetched(failed=True, built_other=True)
        self.assertIn("still not known", " ".join(run_["serious_warnings"]))

    def test_none_built_is_serious(self):
        run_ = self.fetched(failed=False, built_other=False)
        self.assertIn("still not known", " ".join(run_["serious_warnings"]))


class AChangeJustBeforeTheBuild(r11.World):
    """The update after a change to the test group reads every build of the last 7 days again,
    and puts each build without a start it reads at the start of a good column below it; the
    updates after it take those off one an update, oldest first, each reading every newer
    column again (truncateRunning, hintStarted, readColumns). A build made just after that
    update is read again by each of them, though the change came before it was made."""

    def test_a_chain_of_holds_begun_before_it_reads_it_later(self):
        t = self.now - 30 * HOUR
        self.at(t - 10 * HOUR, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)  # G1
        self.at(t - 9 * HOUR, started=None, finished=5 * MINUTE, podinfo=5 * MINUTE)  # N1: never started
        self.at(t - 8 * HOUR, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)  # G2
        self.at(t - 7 * HOUR, started=None, finished=5 * MINUTE, podinfo=5 * MINUTE)  # N2: never started
        b = self.at(t + 30 * MINUTE, started=MINUTE, finished=3 * MINUTE, podinfo=4 * MINUTE,
                    junits=self.failing_junit(t + 4 * HOUR))  # (done in 4 minutes; its junit file 3 h later)
        events = [(t, t + 10 * MINUTE, "changed")]
        # TestGrid: U0 (t..t+10m) reads G1, N1, G2, N2 again: N1 at G1's start, N2 at G2's. U1 (by
        # t+2h30m) cuts at N1, puts it at the epoch, reads G2, N2 (at G2's start again) and B (new);
        # U2 (by t+4h50m) cuts at N2 and reads B again, with its junit file: red. B's own reads end
        # by t+3h14m.
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now, events=events)
        self.assertEqual(frl.verdict(col, "job")[0], "red")
        # (without the change: no read of it after its own)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")


class W5cFindings(unittest.TestCase):
    """The two bugs review W5c found by mutation testing."""

    def test_a_failed_run_made_in_the_second_listing_began_is_dropped(self):
        # (plan() lists the runs made after `after` by the millisecond; merge drops a run it saw running
        # that a listing from before it no longer has, by the same measure)
        now = tch.NOW
        a = {"build": snowflake(now + 0.1), "commit": "a" * 40, "started": now, "finished": now + 5}
        b = {"build": snowflake(now + 0.7), "commit": "b" * 40, "started": now, "finished": None}  # (same second)
        h = frl.ConfigHistory()
        h.merge(tch.loaded([a, b], tch.configs(a={"d#t": tch.E1}, b={"d#t": tch.E2}), after=now - 100))
        c = {"build": snowflake(now + 200), "commit": "c" * 40, "started": now + 200, "finished": now + 205}
        h.merge(tch.loaded([c], tch.configs(c={"d#t": tch.E3}), after=tgc.made(a["build"])))  # (b failed: not listed)
        self.assertEqual([r["commit"][0] for r in h.runs], ["a", "c"])
        self.assertEqual([e for _, _, e in h.eras("d#t")], [frl.UNKNOWN, tch.E1, tch.E3])


class TheColumnMadeLater(r9.JobWorld):
    """made_columns: an update that makes a build's column 20 minutes after it read the build
    began when it read it (it reads within its 20 minutes, and makes each column within them),
    so its stop is the read's time less the days of results."""

    def test_its_stop_is_of_an_update_that_began_at_the_read(self):
        # (passed 23.5 h after it started, no podinfo.json: its Pod row waits an hour; t failed; a tab of
        # one day of results, which hid t until 28 h after it started)
        b = self.build(40 * HOUR, finished=MINUTE + 23.5 * HOUR, junits=[("junit_01.xml", MINUTE + HOUR, r9.FAILING)])
        start = frl.created(b) + MINUTE
        col = self.column(b, days=1)
        hide_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$", days_of_results=1))
        show_t = tgc.TabRule(ttc.entry("", days_of_results=1))
        switch = start + 28 * HOUR
        rule = {"eras": [(-math.inf, switch, hide_t), (switch, math.inf, show_t)], "days": 1}
        # (no column TestGrid made is shown after it got too old but a too-old one: not red by the config
        # that shows t)
        self.assertEqual(frl.tab_verdict(col, "job", rule, frl.created(b))[0], "not-red")


class Since(unittest.TestCase):
    """tab_verdict(since=...): only what the tab may have shown from then on counts."""

    @staticmethod
    def col(red):
        c = tr.Column([], [], {"PASS"}, {"job.Overall": "PASS", "t": "FAIL" if red else "PASS"})
        c.has_finished, c.partial, c.finished_seen = True, None, True
        return c

    def test_a_red_read_replaced_before_since_does_not_count(self):
        red, fine = self.col(True), self.col(False)
        fine.reads = [(fine, [(2000.0, math.inf, 2000.0)]), (red, [(1000.0, 2000.0, 1000.0)])]
        fine.alternatives, fine.open, fine.now = [red], False, 3000.0
        rule = {"eras": [(-math.inf, math.inf, tgc.TabRule(ttc.entry("")))]}
        self.assertEqual(frl.tab_verdict(fine, "job", rule, 900.0)[0], "red")
        self.assertEqual(frl.tab_verdict(fine, "job", rule, 900.0, since=2500.0)[0], "not-red")
        self.assertEqual(frl.tab_verdict(fine, "job", rule, 900.0, since=1500.0)[0], "red")
        self.assertEqual(frl.tab_verdict(fine, "job", {}, 900.0, since=2500.0)[0], "not-red")  # (no eras)

    def test_what_a_config_change_is(self):
        e1, e2 = tgc.TabRule(ttc.entry("")), tgc.TabRule(ttc.entry(ttc.ConfigWorld.HIDE))
        rule = {"eras": [(-math.inf, 1000.0, frl.UNKNOWN), (500.0, 2000.0, e1), (1900.0, math.inf, e2)]}
        self.assertTrue(frl.config_changed(rule, 1500.0))  # (e1 in force until after it, then e2)
        self.assertTrue(frl.config_changed(rule, 2500.0))  # (TestGrid's tabulator may show e1's filters an hour more)
        self.assertFalse(frl.config_changed(rule, 2000.0 + frl.TABULATE))
        self.assertTrue(frl.config_changed({"events": [(100.0, 3000.0, "changed")]}, 2500.0))
        self.assertFalse(frl.config_changed({"events": [(100.0, 2000.0, "changed")]}, 2500.0))
        self.assertFalse(frl.config_changed({}, 0.0))


if __name__ == "__main__":
    unittest.main()
