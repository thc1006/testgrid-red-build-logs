"""Regression tests for the round-8 review (TestGrid's config, the read TestGrid judged a
build by, interim responses): each failed before its fix."""
import email.utils
import json
import os
import random
import shutil
import sys
import tempfile
import time
import unittest
import urllib.parse
from unittest import mock

import urllib3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import goregexp  # noqa: E402
import test_review_round7 as r7  # noqa: E402  (module imports: their tests are not collected again here)
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer  # noqa: E402
from test_fetch_red_logs import table as ttc_table  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

class _AnyText(str):
    def __eq__(self, other):
        return isinstance(other, str) and bool(other)

    __hash__ = str.__hash__


ANY_TEXT = _AnyText("any message")
KUBETEST_FAILED = (b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                   b'<testcase name="Up"/></testsuite>')


class RememberedMismatch(ttc.ConfigWorld):
    """A tab whose table showed a row its config's filters hide does not follow that config:
    that was forgotten as soon as one read of its table failed, and a build red on the row
    TestGrid showed was judged by the wrong filter and dropped. Builds made before the tab
    agrees again are now judged by the rows its table showed too, and cross_check does the same."""

    def show_kubetest(self):
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))

    def failing_kubetest(self, ago):
        b = snowflake(self.now - ago)
        self.failed_in_gcs(b, ago)
        self.srv.route(f"/bucket/logs/job/{b}/artifacts/junit_01.xml", (200, {}, KUBETEST_FAILED))
        return b

    def test_a_mismatch_counts_until_the_tab_agrees_even_when_its_table_cannot_be_read(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        path = self.config(self.HIDE)
        self.show_kubetest()
        self.assertEqual(self.fetch_with(path), 1)
        self.assertIn("does not match the tab", " ".join(self.runs()[-1]["serious_warnings"]))
        record = self.state()["tab_configs"]["dash#t"]["mismatch"]
        self.assertIsNone(record["until"])
        # the table cannot be read; a watched build red only on kubetest.Test is judged by the
        # rows the table showed too
        w = self.failing_kubetest(5 * DAY)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)])
        state = self.state()
        state["watch"] = {f"job/{w}": {"rec": self.watched(w), "first_seen": self.now - 5 * DAY}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.srv.table("dash", "t", (404, {}, b""))
        self.assertEqual(self.fetch_with(path), 1)  # (the tab error)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{w}"]})
        self.assertEqual([c["test"] for c in self.meta(w)["tabs"][0]["red_cells"]], ["kubetest.Test"])
        self.assertEqual(self.state()["tab_configs"]["dash#t"]["mismatch"], record)
        # the table agrees again: the record closes, and a build made after that is judged by the config
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Up": ([1], [""])}, start=int(self.now - HOUR))
        self.assertEqual(self.fetch_with(path), 0)
        until = self.state()["tab_configs"]["dash#t"]["mismatch"]["until"]
        self.assertGreater(until, record["since"])
        # (the record moved ten hours back, for a build made after it closed whose reads by
        # TestGrid are over)
        state = self.state()
        state["tab_configs"]["dash#t"]["mismatch"].update(since=self.now - 20 * HOUR, until=self.now - 10 * HOUR)
        y = snowflake(self.now - 8 * HOUR)
        self.failed_in_gcs(y, 8 * HOUR)
        self.srv.route(f"/bucket/logs/job/{y}/artifacts/junit_01.xml", (200, {}, KUBETEST_FAILED))
        self.srv.route(f"/bucket/logs/job/{y}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 7 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{y}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w, y)])
        state["watch"] = {f"job/{y}": {"rec": self.watched(y), "first_seen": self.now - 7 * HOUR}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        with mock.patch.object(frl, "gcs_sevenths", return_value=set()):  # (w, final, not in this run's sevenths)
            self.assertEqual(self.fetch_with(path), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{y}"]})

    def test_cross_check_judges_by_the_runs_mismatch_record(self):
        self.earlier_run(self.now - 3 * DAY)
        self.show_kubetest()
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 1)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})
        shutil.rmtree(self.bdir(self.b))  # (as if the fetch had missed it)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"MISSED red build job/{self.b}", out)
        # judged by the config alone, it would pass unnoticed
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        run = frl.read_json(run_path)
        for t in run["tabs"]:
            t.pop("config_mismatch", None)
        run["serious_warnings"] = []
        frl.write_json(run_path, run)
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)


class HeldBuilds(ttc.ConfigWorld):
    """A build only its tab's unfollowed config kept undecided was dropped after 14 days, with
    a reason that was false and not serious; it is now held until the port follows that
    config (60 days at most), and judged then."""

    def held(self, ago):
        b = snowflake(self.now - ago)
        self.failed_in_gcs(b, ago)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, b)])
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - ago + HOUR}})
        return b

    def test_held_past_14_days_and_judged_once_the_config_is_followed(self):
        b = self.held(14 * DAY + HOUR)
        self.assertEqual(self.fetch_with(self.config("", build_override_configuration_value="x")), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"held": [f"job/{b}"]})
        self.assertIn("cannot be judged from GCS", " ".join(run["serious_warnings"]))
        self.assertIn(f"job/{b}", self.state()["watch"])
        self.assertEqual(self.fetch_with(self.config("")), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{b}"]})

    def test_dropped_after_60_days_with_a_serious_warning(self):
        b = self.held(61 * DAY)
        self.assertEqual(self.fetch_with(self.config("", build_override_configuration_value="x")), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"stopped-holding": [f"job/{b}"]})
        warning = next(w for w in run["serious_warnings"] if b in w)
        self.assertIn("build_override_configuration_value", warning)
        self.assertIn("though TestGrid may have shown it red", warning)
        self.assertEqual(self.state()["watch"], {})

    def test_an_unlisted_tab_with_a_config_problem_is_serious(self):
        w = snowflake(self.now - 5 * DAY)
        self.failed_in_gcs(w, 5 * DAY)
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "gone", "tab_status": "FAILING", "red_cells": []}])
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 2 * DAY, "oldest": self.now - 9 * DAY,
                                           "missing_since": self.now - DAY}},
                       tab_configs={"dash#gone": {"config": ttc.entry("", build_override_configuration_value="x"),
                                                  "read": self.now - DAY}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 5 * DAY}})
        self.assertEqual(self.fetch_with(self.config("")), 1)
        self.assertIn("dash#gone: TestGrid's config for the tab", " ".join(self.runs()[-1]["serious_warnings"]))


class Staleness(ttc.ConfigWorld):
    """The config's age was taken from one record for all tabs: a config never read was never
    serious, a dropped record let every tab's config be used for ever, and a config without
    a tab refreshed the record while the tab used one a month old."""

    def setUp(self):
        super().setUp()
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})

    def test_a_config_never_read_is_serious(self):
        missing = os.path.join(self.dir, "missing.json")
        self.assertEqual(self.fetch_with(missing), 1)
        self.assertIn("TestGrid's config was never read for 1 tabs (dash#t)", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_each_tabs_config_ages_on_its_own(self):
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        state = self.state()
        state["tab_configs"]["dash#t"]["read"] = self.now - 4 * DAY
        state.pop("testgrid_config")
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.assertEqual(self.fetch_with(os.path.join(self.dir, "missing.json")), 1)
        self.assertIn("TestGrid's config was last read", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_a_config_without_the_tab_does_not_make_its_old_entry_new(self):
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        state = self.state()
        state["tab_configs"]["dash#t"]["read"] = self.now - 30 * DAY
        frl.write_json(os.path.join(self.root, "state.json"), state)
        other = self.config_of({"elsewhere": {"x": ttc.entry("")}})
        self.assertEqual(self.fetch_with(other), 1)
        run = self.runs()[-1]
        self.assertIn("has no tab dash#t", " ".join(run["warnings"]))
        # (TestGrid shows the tab, under a config not known: a serious warning of its own)
        self.assertIn("it lacks tabs TestGrid shows", " ".join(run["serious_warnings"]))
        self.assertAlmostEqual(run["tabs"][0]["config_read"], self.now - 30 * DAY, delta=1)


class MalformedEntries(ttc.ConfigWorld):
    """An entry valid_entry accepted (custom_evaluator_rule_set a list) crashed the fetch."""

    def test_a_field_of_the_wrong_type_is_refused_not_a_crash(self):
        path = self.config_of({"dash": {"t": ttc.entry("", custom_evaluator_rule_set=["x"])}})
        self.fetch_with(path)
        self.assertIn("its config as written: dash#t: an entry not as extract writes it",
                      " ".join(self.runs()[-1]["warnings"]))
        for bad in (ttc.entry("", custom_evaluator_rule_set=["x"]), ttc.entry("", custom_evaluator_rule_set={"rules": 1}),
                    ttc.entry("", ignore_skip="yes"), ttc.entry("", name=5), ttc.entry("", test_name_config="x")):
            self.assertFalse(tgc.valid_entry(bad), bad)
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       tab_configs={"dash#t": {"config": ttc.entry("", custom_evaluator_rule_set=["x"])}})
        self.fetch()
        self.assertIn("dropped the malformed TestGrid config", " ".join(self.runs()[-1]["warnings"]))


class LastReadWorld(unittest.TestCase):
    """A build that started 40 hours ago, with its started.json and a succeeded pod."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        frl.BACKOFF = 0
        self.now = time.time()
        self.start = int(self.now - 40 * HOUR)
        self.build = snowflake(self.start)
        self.srv.route(f"/bucket/logs/job/{self.build}/started.json", (200, {}, {"timestamp": self.start}))
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        self.srv.gcs_listing("bucket", [])

    def podinfo(self, written_after, failed=False):
        pod = {"pod": {"status": {"phase": "Failed" if failed else "Succeeded",  # (a failed sidecar fails the Pod row)
                                  "containerStatuses": [{"name": "sidecar", "state": {"terminated": {"exitCode": 1}}}]
                                  if failed else []}}}
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + written_after, usegmt=True)}, pod))

    def junit(self, written_after):
        name = f"logs/job/{self.build}/artifacts/junit_01.xml"
        self.srv.route("/bucket/" + name, (200, {}, b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'))
        self.srv.gcs_listing("bucket", [], written={name: self.start + written_after})

    def finished(self, written_after, passed=True):
        body = {"timestamp": self.start + written_after, "passed": passed, "result": "SUCCESS" if passed else "FAILURE"}
        self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + written_after, usegmt=True)}, body))

    def judged(self, days):
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, days, self.now)
        return frl.verdict(col, "job")


class TestGridsLastRead(LastReadWorld):
    """A build without finished.json 24 hours after it started is FAIL at TestGrid's next read
    and never read again, from what GCS held then: one that finished later keeps that FAIL,
    and its junit files and podinfo.json written later are not in it; on a tab of one day of
    results it is too old unless the update that read it began before the deadline. A
    podinfo.json written after the Pod row stopped waiting is not in the column either. The
    port judged all of these by GCS now."""

    def test_never_finished(self):
        self.assertEqual(self.judged(15), ("red", [{"test": "job.Overall", "status": "FAIL",
                                                    "message": "it did not finish within 24 hours"}]))
        # a tab of one day: the update that read it may have begun before the deadline
        self.assertEqual(self.judged(1)[0], "red")

    def test_finished_long_after_the_deadline_keeps_its_fail(self):
        self.finished(30 * HOUR)
        self.assertEqual(self.judged(15)[1][0]["message"], "it did not finish within 24 hours")
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertFalse(any(c.finished_seen for c in (col, *col.alternatives)))  # (written too late to be read)

    def test_what_was_written_after_the_deadline_read_is_not_in_it(self):
        self.finished(30 * HOUR, passed=False)
        self.junit(30 * HOUR)
        self.podinfo(30 * HOUR, failed=True)
        state, cells = self.judged(15)
        self.assertEqual((state, [c["test"] for c in cells]), ("red", ["job.Overall"]))
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertEqual(frl.verdict(col, "job", shows=lambda row: row != "job.Overall")[0], "not-red")
        # written within the read's window instead, they may be in it
        self.junit(25 * HOUR)
        self.assertIn("t", [c["test"] for c in self.judged(15)[1]])

    def test_a_podinfo_written_after_the_pod_row_stopped_waiting_is_not_in_it(self):
        self.finished(2 * HOUR, passed=True)
        self.podinfo(2 * HOUR + 5 * HOUR, failed=True)  # (an hour of waiting, and the read window, past)
        self.assertEqual(self.judged(15)[0], "not-red")
        self.podinfo(2 * HOUR + 30 * 60, failed=True)
        self.assertEqual(self.judged(15), ("red", [{"test": "job.Pod", "status": "FAIL", "message": ANY_TEXT}]))

    def test_finished_soon_after_the_deadline_is_red_if_either_read_is(self):
        self.finished(25 * HOUR)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        reads = (col, *col.alternatives)
        self.assertTrue(any(c.finished_seen for c in reads))  # (the first read after the deadline may come after it)
        self.assertTrue(any(c.cells.get("job.Overall") == "FAIL" and not c.finished_seen for c in reads))
        self.assertEqual(frl.verdict(col, "job")[0], "red")

    def test_finished_in_time_is_judged_as_it_finished(self):
        self.finished(23 * HOUR)
        self.assertEqual(self.judged(1)[0], "not-red")  # passed
        self.finished(23 * HOUR, passed=False)
        self.assertEqual(self.judged(1)[0], "red")
        self.assertEqual(self.judged(15)[0], "red")

    def test_without_last_modified_a_build_counts_as_read_finished(self):
        self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                       (200, {}, {"timestamp": self.start + 30 * HOUR, "passed": True, "result": "SUCCESS"}))
        self.assertEqual(self.judged(15)[0], "not-red")


class InterimLimits(unittest.TestCase):
    """MAX_INTERIM interim responses were refused at the last one (49 taken), and slow ones
    held a request for MAX_INTERIM read timeouts."""

    def setUp(self):
        frl.BACKOFF = 0
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)
        self.addCleanup(frl.bind_context, frl._DEFAULT_CONTEXT)

    def raw(self, script):
        srv = r7.RawServer(script)
        self.addCleanup(srv.close)
        return srv

    def test_exactly_max_interim_are_taken(self):
        def script(n, s, read_request):
            while (path := read_request()) is not None:
                count = int(path.strip("/"))
                s.sendall(b"HTTP/1.1 103 Early Hints\r\n\r\n" * count + r7.response(200, path.encode()))
        srv = self.raw(script)
        self.assertEqual(frl.request(f"{srv.url}/{frl.MAX_INTERIM}"), f"/{frl.MAX_INTERIM}".encode())
        with self.assertRaisesRegex(urllib3.exceptions.ProtocolError, "interim responses"):
            frl.fetch_once(f"{srv.url}/{frl.MAX_INTERIM + 1}", frl.read_body, None, 10, "GET")

    def test_slow_interim_responses_are_cut_off_in_time(self):
        def script(n, s, read_request):
            while read_request() is not None:
                for _ in range(20):
                    s.sendall(b"HTTP/1.1 102 Processing\r\n\r\n")
                    time.sleep(0.1)
                s.sendall(r7.response(200, b"late"))
        srv = self.raw(script)
        start = time.monotonic()
        with mock.patch.object(frl, "INTERIM_SECONDS", 0.5):
            with self.assertRaisesRegex(urllib3.exceptions.ProtocolError, "interim responses for more than"):
                frl.fetch_once(srv.url + "/x", frl.read_body, None, 10, "GET")
        self.assertLess(time.monotonic() - start, 1.5)


class FilterLimits(unittest.TestCase):
    """Filters Go rejects for their height or size were applied, nested repetitions grew the
    automaton exponentially, and nothing bounded the time a filter could take."""

    def test_patterns_beyond_gos_limits_are_broken(self):
        # (Go's maxHeight and maxSize, as its parser counts them)
        tgc.go_regexp("(" * 999 + "a" + ")" * 999)
        with self.assertRaisesRegex(tgc.Broken, "expression nests too deeply"):
            tgc.go_regexp("(" * 1000 + "a" + ")" * 1000)
        with self.assertRaisesRegex(tgc.Broken, "expression too large"):
            tgc.go_regexp("(?:a{1000}b{1000}){1}" * 1700)
        tgc.go_regexp("a" * 5000)

    def test_nested_repetitions_compile_to_one_copy_each(self):
        r = tgc.go_regexp("((((((a+)+)+)+)+)+)+b")
        self.assertLess(len(r.matcher.states), 40)
        self.assertTrue(r.search("xaaab"))
        self.assertFalse(r.search("a" * 3000))
        r = tgc.go_regexp("x{3,}y")
        self.assertEqual([r.search(t) for t in ("xxy", "xxxy", "xxxxxy")], [False, True, True])

    def test_a_filter_out_of_time_is_a_filter_problem(self):
        rule = tgc.TabRule(ttc.entry("exclude-filter-by-regex=a"))
        rule.budget = tgc.Budget(0)
        with self.assertRaises(tgc.FilterTooSlow):
            rule.shows("b")
        self.assertIn("took more than", rule.filter_problem)
        with self.assertRaises(tgc.Unsupported):
            rule.shows("b")


class FilterOutOfTime(ttc.ConfigWorld):
    def test_judged_by_the_tables_rows_with_a_serious_warning(self):
        with mock.patch.object(tgc, "FILTER_SECONDS", 0.0):
            self.assertEqual(self.fetch_with(self.config(self.HIDE)), 1)
        run = self.runs()[-1]
        self.assertIn("the filters cannot be applied here", " ".join(run["serious_warnings"]))
        self.assertEqual(run["outcomes"], {"backfilled": [f"job/{self.b}"]})  # kubetest.Test counts as shown


class CacheBound(unittest.TestCase):
    def test_the_cache_is_emptied_by_size(self):
        r = tgc.go_regexp("[a-z].{20}~").matcher
        with mock.patch.object(goregexp, "CACHE_ELEMENTS", 500):
            for i in range(50):
                r.search("abcdefghijklmnopqrstuvwxyz" * 3 + str(i))
                self.assertLessEqual(r.cached, 500 + 2 * len(r.states) + 1)


if __name__ == "__main__":
    unittest.main()


class Wave2(ttc.ConfigWorld):
    """The second review of the fixes above: a run whose filters could not be applied closed
    a tab's mismatch record; a tab missing from a config that loaded was never serious; a held
    build was settled as not red because another tab of its job showed it; a new tab's record
    was lost with its scans when its first gap search failed; a held build's tab was forgotten
    after 30 days, and its config with it; a held build dropped by the error path was said to
    have been watched 14 days."""

    def gap_build(self, ago):
        y = snowflake(self.now - ago)
        self.failed_in_gcs(y, ago)
        self.srv.route(f"/bucket/logs/job/{y}/artifacts/junit_01.xml", (200, {}, KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, y)])
        return y

    def show_rows(self, *rows):
        self.show([self.visible], {r: ([1], [""]) for r in rows}, start=int(self.now - HOUR))

    def two_tabs(self, t2_shows=()):
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        builds = [self.visible, *t2_shows]
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", builds,
                                               {"job.Pod": ([1] * len(builds), [""] * len(builds)),
                                                "kubetest.Up": ([1] * len(builds), [""] * len(builds))},
                                               start=int(self.now - HOUR) * 1000))
        return self.config_of({"dash": {"t": ttc.entry("", build_override_configuration_value="x"),
                                        "t2": ttc.entry(self.HIDE)}})

    def test_a_run_whose_filters_cannot_be_applied_leaves_the_record_open(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        self.show_rows("job.Pod", "kubetest.Test")
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 1)
        self.fetch_with(self.config(self.HIDE + "&mystery-option=1"))
        self.assertIsNone(self.state()["tab_configs"]["dash#t"]["mismatch"]["until"])
        # the table agrees now; a gap build red only on kubetest.Test is judged by its rows too
        state = self.state()
        state["tabs"]["dash#t"].update(scan=self.now - 20 * HOUR, oldest=self.now - 21 * HOUR)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.show_rows("job.Pod", "kubetest.Up")
        y = self.gap_build(10 * HOUR)
        self.fetch_with(self.config(self.HIDE))
        self.assertIn(f"job/{y}", self.runs()[-1]["outcomes"].get("backfilled", []))

    def test_a_listed_tab_missing_from_a_config_read_is_serious(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        other = self.config_of({"elsewhere": {"x": ttc.entry("")}})
        self.assertEqual(self.fetch_with(other), 1)
        self.assertIn("TestGrid's config was never read for 1 tabs (dash#t)", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_a_held_build_is_not_settled_by_another_tab_of_its_job(self):
        w = snowflake(self.now - 2 * HOUR)
        self.failed_in_gcs(w, 2 * HOUR)
        path = self.two_tabs(t2_shows=[w])  # t2 shows it, not red (its filters hide the failing rows); t does not
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#t2": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{w}": {"rec": self.watched(w), "first_seen": self.now - 2 * HOUR}})
        self.fetch_with(path)
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{w}"]})
        self.assertIn(f"job/{w}", self.state()["watch"])

    def test_a_watched_build_is_judged_on_its_tabs_that_no_longer_show_it(self):
        w = snowflake(self.now - 2 * HOUR)
        self.failed_in_gcs(w, 2 * HOUR)
        self.srv.route(f"/bucket/logs/job/{w}/artifacts/junit_01.xml", (200, {}, KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)])
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", [self.visible, w],
                                               {"job.Pod": ([1, 1], ["", ""]), "kubetest.Up": ([1, 1], ["", ""])},
                                               start=int(self.now - HOUR) * 1000))
        path = self.config_of({"dash": {"t": ttc.entry(""), "t2": ttc.entry(self.HIDE)}})
        rec = self.watched(w)
        rec["tabs"].append({"dashboard": "dash", "tab": "t2", "tab_status": "PASSING", "red_cells": []})
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#t2": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 2 * HOUR}})
        self.fetch_with(path)
        # t2 shows it (not red there); t, which shows every row, no longer does: red there by the rule
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{w}"]})
        self.assertEqual([t["tab"] for t in self.meta(w)["tabs"]], ["t"])

    def test_a_mismatch_record_survives_a_failed_first_gap_search(self):
        self.earlier_run(self.now - 3 * DAY)
        self.put_state()  # (dash#t never scanned: its first scan searches GCS since the archive began)
        self.show_rows("job.Pod", "kubetest.Test")
        saved = self.srv.httpd.prefix_routes.get("/storage/v1/b/bucket/o?")
        self.listing_fails()
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("dash#t", self.state()["tabs"])  # (searched again next run)
        self.assertIsNone(self.state()["tab_configs"]["dash#t"]["mismatch"]["until"])
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = saved
        self.show_rows("job.Pod", "kubetest.Up")
        self.fetch_with(self.config(self.HIDE))
        self.assertIn(f"job/{self.b}", self.runs()[-1]["outcomes"].get("backfilled", []))

    def test_a_held_build_keeps_its_tabs_config_after_the_tab_is_forgotten(self):
        w = snowflake(self.now - 20 * DAY)
        self.srv.route(f"/bucket/logs/job/{w}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 20 * DAY + HOUR)}))
        self.srv.route(f"/bucket/logs/job/{w}/started.json", (200, {}, {"timestamp": int(self.now - 20 * DAY)}))
        self.srv.route(f"/bucket/logs/job/{w}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)])
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "gone", "tab_status": "FAILING", "red_cells": []}])
        rules = {"rules": [{"name": "x", "computed_status": "TEST_STATUS_FAIL"}]}
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 31 * DAY, "oldest": self.now - 40 * DAY,
                                           "missing_since": self.now - 31 * DAY}},
                       tab_configs={"dash#gone": {"config": ttc.entry("", custom_evaluator_rule_set=rules),
                                                  "read": self.now - DAY}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 20 * DAY}})
        # (TestGrid's config had the tab while it may have shown the build; it is gone from it now)
        path = self.config_of({"dash": {"t": ttc.entry(""), "gone": ttc.entry("", custom_evaluator_rule_set=rules)}})
        self.fetch_with(path)
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{w}"]})
        self.fetch_with(self.config_of({"dash": {"t": ttc.entry("")}}, uploaded=self.now - DAY))
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{w}"]})
        self.assertNotIn("dash#gone", self.state()["tabs"])
        self.assertIn("dash#gone", self.state()["tab_configs"])

    def test_the_error_path_names_the_held_limit(self):
        b = snowflake(self.now - 61 * DAY)
        self.failed_in_gcs(b, 61 * DAY)
        self.listing_fails()  # its junit files cannot be listed: archive() raises
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - 61 * DAY}})
        self.fetch_with(self.config("", build_override_configuration_value="x"))
        error = next(e["error"] for e in self.runs()[-1]["errors"] if e["build"] == f"job/{b}")
        self.assertIn(f"no longer watched after {frl.HELD_DAYS} days", error)


class MutationGaps(ttc.ConfigWorld):
    """Behaviour the mutation test of the change found no test for."""

    def test_a_column_testgrid_could_not_read_is_red_on_a_tab_hiding_only_the_jobs_overall_row(self):
        col = frl.tr.Column(["started"], [], {"TOOL_FAIL"}, None)
        col.has_finished = False
        hides = tgc.TabRule(ttc.entry("exclude-filter-by-regex=" + urllib.parse.quote(r"^job\.Overall$"))).shows
        self.assertEqual(frl.verdict(col, "job", shows=hides)[0], "red")  # (its row is "Overall")

    def test_a_watched_build_is_judged_by_the_tabs_filters_and_days(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{self.b}": {"rec": self.watched(self.b), "first_seen": self.now - DAY}})
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{self.b}"]})  # its red rows are hidden
        # a build whose start is two days before it was made is too old for one day of results
        old = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(old, 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{old}/started.json", (200, {}, {"timestamp": int(self.now - 30 * HOUR - 2 * DAY)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, old)])
        state = self.state()
        state["watch"] = {f"job/{old}": {"rec": self.watched(old), "first_seen": self.now - DAY}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.assertEqual(self.fetch_with(self.config("", days_of_results=1)), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{old}"]})

    def test_cross_check_follows_the_runs_group_problems_and_days(self):
        self.earlier_run(self.now - 3 * DAY)
        self.assertEqual(self.fetch_with(self.config("", build_override_configuration_value="x")), 1)  # b held
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)
        self.assertIn(f"job/{self.b}: TestGrid no longer lists it and it is not decided yet", out)
        # too old for one day of results: not missed either
        old = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(old, 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{old}/started.json", (200, {}, {"timestamp": int(self.now - 30 * HOUR - 2 * DAY)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b, old)])
        self.fetch_with(self.config("", days_of_results=1))
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{old}", out)

    def test_a_corrupt_config_file_is_a_warning_not_a_crash(self):
        for body in (b"{", b"\xff\xfe", b"[]", b'{"format": 1, "source": [], "dashboards": {}}', b"null"):
            path = os.path.join(self.dir, "bad.json")
            with open(path, "wb") as f:
                f.write(body)
            self.fetch_with(path)
            self.assertIn("TestGrid's config could not be read", " ".join(self.runs()[-1]["warnings"]), body)

    def test_each_check_of_the_config_record_in_state(self):
        good = {"read": self.now - HOUR, "source": {"test_infra": "a" * 40, "committed": int(self.now - DAY), "image": "i"}}
        for bad in ({**good, "read": "x"}, {**good, "source": []}, {**good, "source": {**good["source"], "test_infra": 1}},
                    {**good, "source": {**good["source"], "committed": -1}}):
            self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                           testgrid_config=bad)
            self.fetch()
            self.assertIn("dropped a malformed testgrid_config record", " ".join(self.runs()[-1]["warnings"]), bad)
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}}, testgrid_config=good)
        self.fetch()
        self.assertNotIn("dropped a malformed testgrid_config record", " ".join(self.runs()[-1]["warnings"]))

    def test_the_staleness_threshold(self):
        for age, serious in ((2.5 * DAY, False), (3.5 * DAY, True)):
            self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                           tab_configs={"dash#t": {"config": ttc.entry(self.HIDE), "read": self.now - age}})
            self.fetch()
            self.assertEqual("TestGrid's config was last read" in " ".join(self.runs()[-1]["serious_warnings"]), serious, age)
        # a config read now is new, whatever was read before
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("TestGrid's config was last read", " ".join(self.runs()[-1]["warnings"]))

    def test_the_absent_and_unknown_config_warnings_name_only_their_tabs(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        self.fetch_with(self.config(self.HIDE))
        warnings = " ".join(self.runs()[-1]["warnings"])
        self.assertNotIn("has no tab", warnings)
        self.assertNotIn("no TestGrid config known", warnings)
        # a watched build of a tab without any config: judged with a warning naming that tab
        w = snowflake(self.now - 2 * DAY)
        self.failed_in_gcs(w, 2 * DAY)
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "other", "tab_status": "FAILING", "red_cells": []}])
        state = self.state()
        state["watch"] = {f"job/{w}": {"rec": rec, "first_seen": self.now - DAY}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.fetch_with(self.config(self.HIDE))
        warnings = " ".join(self.runs()[-1]["warnings"])
        self.assertIn("no TestGrid config known for 1 tabs (dash#other)", warnings)


class HttpsPools(unittest.TestCase):
    """Production requests go over HTTPS, and through proxies: their connections take
    FinalResponse too (the HTTP tests only cover plain HTTP)."""

    def test_https_and_proxied_pools_skip_interim_responses(self):
        pool = frl.HTTP.connection_from_url("https://storage.googleapis.com/x")
        self.assertIs(pool.ConnectionCls.response_class, frl.FinalResponse)
        self.assertIs(frl.HTTP.connection_from_url("http://127.0.0.1:9/x").ConnectionCls.response_class,
                      frl.FinalResponse)
        with mock.patch.dict(frl._PROXIES, {"https": "http://u:p@127.0.0.1:9"}, clear=True), \
                mock.patch.dict(frl._PROXY_POOLS, {}, clear=True):
            proxied = frl.pool_for("https://storage.googleapis.com/x")
            self.assertIsNot(proxied, frl.HTTP)
            self.assertIs(proxied.connection_from_url("https://storage.googleapis.com/x").ConnectionCls.response_class,
                          frl.FinalResponse)


class ConfigRoundTrip(unittest.TestCase):
    """From the configurator's YAML through extract, valid_entry and TabRule, per option."""

    def rule(self, **group):
        g = dict(ttc.GROUP, **group)
        out = tgc.extract({"test_groups": [g], "dashboards": [{"name": "d", "dashboard_tab": [
            {"name": "t", "test_group_name": "job", "base_options": ""}]}]}, ["d"])
        entry = out["d"]["t"]
        self.assertTrue(tgc.valid_entry(entry), entry)
        return tgc.TabRule(entry)

    def test_every_option_the_updater_paints_cells_by_survives_extract(self):
        for option, value in (("build_override_strftime", "%Y"), ("build_override_configuration_value", "x")):
            self.assertIn(option, self.rule(**{option: value}).group_problem or "", option)
        # (kept, and followed: the cell options, the source of its builds; options TestGrid production
        # does not apply)
        rule = self.rule(test_name_config={"name_format": "%s", "name_elements": [{"target_config": "Tests name"}]},
                         disable_prowjob_analysis=True, disable_merged_status=True,
                         custom_evaluator_rule_set={"rules": [{"computed_status": 12}]})
        self.assertEqual((rule.group_problem, rule.options),
                         (None, tr.Options(merge=False, pod=False, all_tests="FAIL", names=("%s", ("Tests name",)))))
        rule = self.rule(result_source={"gcs_config": {"gcs_prefix": "x/y"}}, ignore_skip=True, ignore_old_results=True)
        self.assertEqual((rule.prefix, rule.not_updated, rule.group_problem), ("x/y", None, None))
        self.assertTrue({"result_source", "ignore_skip", "ignore_old_results"} <= set(rule.entry["group"]))
        self.assertIsNone(self.rule(num_columns_recent=3, alert_stale_results_hours=4, short_text_metric="m",
                                    user_property="p").group_problem)  # (fields that set no result)
        rule = self.rule()
        self.assertNotIn("num_columns_recent", rule.entry["group"])

    def test_a_group_without_use_kubernetes_client_is_skipped_by_testgrid(self):
        group = {k: v for k, v in ttc.GROUP.items() if k != "use_kubernetes_client"}
        rule = tgc.TabRule({"test_group": "job", "base_options": "", "group": group})
        self.assertIsNone(rule.group_problem)  # (what TestGrid shows is known: no new column)
        self.assertIn("never updates the group", rule.not_updated)
        self.assertIsNone(rule.prefix)

    def test_a_tab_without_its_test_group_is_a_valid_entry_with_a_problem(self):
        entry = {"test_group": "missing", "base_options": "", "group": None}
        self.assertTrue(tgc.valid_entry(entry))
        self.assertIn("not in the config", tgc.TabRule(entry).group_problem)

    def test_load_raises_only_value_errors(self):
        rng = random.Random(5)
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        scalars = [None, True, 0, -1, 1.5, "", "x", [], {}, [1], {"a": 1}, "a" * 12 + "-" + "b" * 12, "c" * 40, 1 << 41]
        commit, recipe = "c" * 40, ttc.recipe_of("c" * 40)
        run = {"build": "2109000000000000000", "commit": commit, "started": 1_790_000_000, "finished": 1_790_000_005,
               "recipe": recipe}
        good = {"format": tgc.FORMAT, "after": 1_789_000_000, "runs": [run], "group_changes": {run["build"]: {}},
                "failed": {}, "configs": {recipe: {"commit": commit, "committed": 1_789_990_000, "image": "i",
                                                   "dashboards": {"d": {"t": ttc.entry("")}}}}}

        def mutate(value, depth=0):
            if depth > 4 or not isinstance(value, (dict, list)) or rng.random() < 0.15:
                return rng.choice(scalars) if rng.random() < 0.5 else value
            if isinstance(value, list):
                return [mutate(v, depth + 1) for v in value] + ([rng.choice(scalars)] if rng.random() < 0.1 else [])
            out = {k: mutate(v, depth + 1) for k, v in value.items() if rng.random() > 0.05}
            if rng.random() < 0.1:
                out[rng.choice(["extra", "x", "group", "duplicate", "base_options"])] = rng.choice(scalars)
            return out
        accepted = 0
        for _ in range(3000):
            with open(path, "w") as f:
                json.dump(mutate(good), f)
            try:
                loaded = tgc.load(path)
            except ValueError:
                continue
            accepted += 1
            for tabs, _ in loaded["configs"].values():
                for entry in tabs.values():
                    tgc.TabRule(entry)  # (never raises for an entry load accepts)
        self.assertGreater(accepted, 10)  # (some mutations keep it valid: the loop does reach TabRule)


class UploadJobArguments(unittest.TestCase):
    def test_printing_args_edges(self):
        self.assertEqual(tgc.printing_args(["--yaml=x", "--output"]), ["--yaml=x", "--oneshot", "--print-text", "--output-yaml"])
        self.assertEqual(tgc.printing_args(["-output=gs://b/o", "--print-text=false", "-oneshot", "--output-yaml=true",
                                            "--prowjob-url-prefix=https://x/?a=b"]),
                         ["--prowjob-url-prefix=https://x/?a=b", "--oneshot", "--print-text", "--output-yaml"])
        self.assertEqual(tgc.printing_args(["--outputs=x"]), ["--outputs=x", "--oneshot", "--print-text", "--output-yaml"])


class Wave3(ttc.ConfigWorld):
    """The third review: a renamed tab, or an unreadable one beside a tab showing a build alike,
    had the build judged by the rule; a later read on a tab of one day of results was always
    too old; a listed tab without scans lost its config record when the config could not be
    read; a podinfo.json without a pod left a build undecided for good."""

    def test_a_tab_renamed_is_decided_by_the_tab_that_shows_the_build_alike(self):
        w = snowflake(self.now - 3 * HOUR)
        self.failed_in_gcs(w, 3 * HOUR)
        self.srv.route(f"/bucket/logs/job/{w}/artifacts/junit_01.xml", (200, {}, KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)])
        self.srv.summary("dash", {"t2": "PASSING"})  # dash#t was renamed dash#t2
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", [self.visible, w],
                                               {"job.Pod": ([1, 1], ["", ""]), "kubetest.Up": ([1, 1], ["", ""])},
                                               start=int(self.now - HOUR) * 1000))
        # (TestGrid's config had t while it may have shown the build, then t2 instead of it)
        for t, outcome in ((ttc.entry(self.HIDE), "not-red"), (ttc.entry(""), "backfilled")):
            # with other base options, t2 says nothing about t: t judges it by the rule (red there)
            self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 5 * HOUR}},
                           tab_configs={"dash#t": {"config": t, "read": self.now - HOUR}})
            self.fetch_with(self.config_of({"dash": {"t": t}}))
            self.put_state(**{k: v for k, v in self.state().items() if k in ("config_uploads", "config_entries")},
                           tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 5 * HOUR}},
                           tab_configs={"dash#t": {"config": t, "read": self.now - HOUR}},
                           watch={f"job/{w}": {"rec": self.watched(w), "first_seen": self.now - 2 * HOUR}})
            self.fetch_with(self.config_of({"dash": {"t2": ttc.entry(self.HIDE)}}, uploaded=self.now - HOUR))
            self.assertIn(f"job/{w}", self.runs()[-1]["outcomes"].get(outcome, []), outcome)

    def test_a_listed_tab_without_scans_keeps_its_config_record(self):
        self.earlier_run(self.now - 3 * DAY)
        self.put_state()
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))
        self.listing_fails()  # (its first gap search fails: no scans kept for it)
        self.fetch_with(self.config(self.HIDE))
        self.assertIn("mismatch", self.state()["tab_configs"]["dash#t"])
        self.fetch_with(os.path.join(self.dir, "missing.json"))  # the config cannot be read now
        self.assertEqual(set(self.state()["tab_configs"]["dash#t"]), {"config", "read", "mismatch"})


class Wave3LastRead(LastReadWorld):
    def test_a_later_read_on_a_tab_of_one_day_counts_what_it_adds(self):
        # finished.json (timestamp 21.5 h) written at 23.4 h, a failed pod's podinfo.json at 23.5 h:
        # the read after the Pod row stopped waiting, or one right after podinfo.json came
        body = {"timestamp": self.start + int(21.5 * HOUR), "passed": True, "result": "SUCCESS"}
        self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + 23.4 * HOUR, usegmt=True)}, body))
        self.podinfo(int(23.5 * HOUR), failed=True)
        self.assertEqual(self.judged(1)[0], "red")

    def test_a_podinfo_without_a_pod_does_not_end_the_wait(self):
        self.finished(2 * HOUR, passed=True)
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + 2 * HOUR + 60, usegmt=True)}, {}))
        self.assertEqual(self.judged(15)[0], "not-red")  # (decided once the Pod row stopped waiting)

