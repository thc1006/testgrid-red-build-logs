"""Targeted tests from re-running the round-7 mutation review's residual mutants on 0734e94:
each kills mutants the rest of the suite let survive. Each test name starts with the mutant
id(s) it targets."""
import contextlib
import datetime as dt
import hashlib
import io
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest
import zlib
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
import testgrid_rule as tr  # noqa: E402
import test_mutation_killers_r7 as k7  # noqa: E402
import test_review_round4 as r4  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, run_record, table  # noqa: E402
from test_integrity import HOUR, obj, snowflake  # noqa: E402

OVERALL_FAIL = {"test": "job.Overall", "status": "FAIL", "message": "it did not pass; Prow result FAILURE"}


def tempdir(test):
    d = tempfile.mkdtemp()
    test.addCleanup(shutil.rmtree, d)
    return d


def write(path, data):
    with open(path, "wb") as f:
        f.write(data)


class BackfillNote(unittest.TestCase):
    def test_BN3_a_pod_row_gives_its_message(self):
        note = frl.backfill_note([{"red_cells": [{"test": "job.Pod", "status": "FAIL", "message": "sidecar exited 2: "}]}])
        self.assertIn("by TestGrid's rule it was red (its pod: sidecar exited 2: )", note)

    def test_BN4_BN5_BN7_each_reason_once_then_the_failing_tests_sorted(self):
        tool_fail = {"test": "Overall", "status": "TOOL_FAIL", "message": "x"}
        tabs = [{"red_cells": [tool_fail, {"test": "b", "status": "FAIL", "message": None},
                               {"test": "a", "status": "FAIL", "message": None}]},
                {"red_cells": [tool_fail, {"test": "b", "status": "FAIL", "message": None}]}]
        self.assertIn("by TestGrid's rule it was red (TestGrid could not read it from GCS (TOOL_FAIL); "
                      "failing tests: a, b)", frl.backfill_note(tabs))


class JunitAtTheLimit(unittest.TestCase):
    def test_G10_a_junit_file_of_exactly_the_size_limit_is_read(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        frl.BACKOFF = 0
        build, now = "2109000000000000000", int(time.time())
        base = f"/bucket/logs/job/{build}"
        failing = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'
        srv.route(base + "/started.json", (200, {}, json.dumps({"timestamp": frl.created(build)}).encode()))
        srv.route(base + "/finished.json", (200, {}, json.dumps({"timestamp": now - 60, "passed": True}).encode()))
        srv.route(base + "/artifacts/junit_01.xml", (200, {}, failing))
        srv.gcs_listing("bucket", [])
        with mock.patch.object(tr, "JUNIT_MAX_SIZE", len(failing)):  # (TestGrid: only a larger one is too large)
            col = frl.gcs_column(srv.url, "bucket/logs/job", build)
        self.assertEqual((col.malformed, col.red_rows()), ([], ["t"]))


class Listing(unittest.TestCase):
    def listing(self, *items):
        with mock.patch.object(frl, "get_json", return_value={"items": list(items)}):
            return frl.list_objects("http://x", "bucket", "logs/job/1/")

    def test_LO3_LO4_only_objects_of_the_folder_with_plain_sizes(self):
        self.assertEqual(self.listing({"name": "logs/job/1/a", "size": "3"}), {"logs/job/1/a": (3, False, None)})
        for item in ({"name": "logs/job/2/a", "size": "3"}, {"name": "logs/job/1/a", "size": "-3"},
                     {"name": "logs/job/1/a", "size": "+3"}, {"name": "logs/job/1/a", "size": " 3"}):
            with self.subTest(item), self.assertRaises(frl.IntegrityError):
                self.listing(item)


class IndexRows(unittest.TestCase):
    def test_BR2_a_build_taken_from_gcs_still_names_its_red_test(self):
        e = {"time": time.time(), "job": "j", "build": "1", "tabs": ["d#t"], "red_tests": ["t1", "t2"],
             "red_test_count": 2, "from_gcs": True, "prow_url": "u", "status": "x"}
        self.assertIn("| t1 (+1) (taken from GCS: red by TestGrid's rule, not seen red on TestGrid) |",
                      "".join(frl.build_rows([e], files=False)))

    def test_IE6_a_build_shown_red_with_its_cells_omitted_was_not_taken_from_gcs(self):
        # (a mass failure past MAX_RED_CELLS: the tab listed none of the build's red cells)
        meta = {"job": "job", "build": "1", "query": "b/logs/job", "started": None, "created": time.time() - 3600,
                "log": None, "podinfo": None, "tabs": [{"dashboard": "d", "tab": "m", "red_cells": [], "red_cells_omitted": 7}]}
        self.assertFalse(frl.index_entry("job", "1", meta, time.time())["from_gcs"])


class Place(unittest.TestCase):
    def setUp(self):
        self.d = tempdir(self)
        self.final = os.path.join(self.d, "build-log.txt")

    def download(self, n):
        tmp = os.path.join(self.d, ".dl.part")
        write(tmp, b"x" * n)
        return tmp

    def test_K1_a_file_of_exactly_max_bytes_is_stored_whole(self):
        self.assertEqual(frl.place(self.download(10), self.final, 10), [])
        self.assertEqual(os.listdir(self.d), ["build-log.txt"])

    def test_K3_K4_a_file_of_exactly_1000_parts_is_stored_in_parts(self):
        parts = frl.place(self.download(2000), self.final, 2)
        self.assertEqual((len(parts), parts[-1]), (1000, {"file": "build-log.txt.999", "bytes": 2}))
        with self.assertRaises(frl.LocalIOError):
            frl.place(self.download(2001), self.final, 2)


class Parts(unittest.TestCase):
    def setUp(self):
        self.d = tempdir(self)

    def test_PP2_parts_are_listed_in_order_under_their_names(self):
        write(os.path.join(self.d, "build-log.txt.000"), b"ab")
        write(os.path.join(self.d, "build-log.txt.001"), b"cd")
        swapped = [{"file": "build-log.txt.001", "bytes": 2}, {"file": "build-log.txt.000", "bytes": 2}]
        with self.assertRaises(ValueError):
            frl.part_paths(self.d, "build-log.txt", swapped)
        self.assertFalse(frl.intact(self.d, {"log": {"file": "build-log.txt", "bytes": 4, "file_bytes": 4, "md5": "x",
                                                     "parts": swapped}}))

    def test_IN2_LD2_each_part_has_the_size_meta_json_records(self):
        write(os.path.join(self.d, "build-log.txt.000"), b"abc")
        write(os.path.join(self.d, "build-log.txt.001"), b"d")
        info = {"file": "build-log.txt", "bytes": 4, "file_bytes": 4, "md5": hashlib.md5(b"abcd").hexdigest(),
                "parts": [{"file": "build-log.txt.000", "bytes": 2}, {"file": "build-log.txt.001", "bytes": 2}]}
        self.assertFalse(frl.intact(self.d, {"log": info}, deep=True))  # (the content is right, its parts are not)
        with self.assertRaises(ValueError):
            cc.local_digest(os.path.dirname(self.d), os.path.basename(self.d), "build-log.txt", info)


class MergeTabs(unittest.TestCase):
    JUDGED = {"dashboard": "d", "tab": "t", "red_cells": [OVERALL_FAIL], "judged_from_gcs": True}

    def test_MT3_a_later_judgement_from_gcs_does_not_replace_the_first(self):
        later = dict(self.JUDGED, red_cells=[{"test": "job.Pod", "status": "FAIL", "message": "x"}])
        self.assertEqual(frl.merge_tabs([self.JUDGED], [later]), ([self.JUDGED], False))

    def test_MT5_testgrid_cells_that_replace_a_judgement_from_gcs_are_written(self):
        target = tempdir(self)
        frl.write_json(os.path.join(target, "meta.json"), {"tabs": [self.JUDGED], "archived_by_run": "R0"})
        shown = {"dashboard": "d", "tab": "t", "tab_status": "FAILING",
                 "red_cells": [{"test": "job.Overall", "status": "FAIL", "message": "boom"}]}
        frl.add_sightings(target, {"tabs": [shown]}, "R1")
        self.assertEqual(frl.read_json(os.path.join(target, "meta.json"))["tabs"], [shown])


class TestGridRule(unittest.TestCase):
    def test_ST9_a_container_that_exited_with_a_negative_code_failed(self):
        now = int(time.time())
        pod = json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [
            {"name": "sidecar", "state": {"terminated": {"exitCode": -1}}}]}}}).encode()
        fin = b'{"timestamp": %d, "passed": true}' % (now - 7200)
        col = tr.column("job", b'{"timestamp": %d}' % (now - 3 * 3600), fin, pod, [], now - 15 * 86400, now)
        self.assertEqual((col.cells["job.Pod"], col.notes["job.Pod"]), ("FAIL", "sidecar exited -1: "))

    def test_D9_a_lone_low_surrogate_escape_is_the_replacement_character(self):
        self.assertEqual(tr.json_unquote(b'"a\\udc00b"'), "a�b")
        self.assertEqual(tr.json_unquote(b'"\\ud83d\\ude00"'), "\U0001f600")

    def test_D25_json_nests_10000_deep_and_no_deeper(self):
        tr.json_value(b"[" * 10000 + b"]" * 10000)
        with self.assertRaises(tr.JSONSyntaxError):
            tr.json_value(b"[" * 10001 + b"]" * 10001)


class Inventory(unittest.TestCase):
    def test_INV3_INV4_INV5_INV6_INV8_the_inventory_counts_each_kind_of_record(self):
        root = tempdir(self)
        frl.write_json(os.path.join(root, "state.json"), {"watch": {"job/9": {}}, "unresolved": {}, "given_up": {}})
        log = {"file": "build-log.txt", "bytes": 1}
        archived = {("job", "1"): ("r/1", {"log": log, "finished": None}),  # (an older record: no settled, no final)
                    ("job", "2"): ("r/2", {"log": log, "finished": 5, "settled": False, "final": True}),
                    ("job", "3"): ("r/3", {"log": log, "finished": 5, "settled": True, "final": True})}
        self.assertEqual(cc.inventory(archived, root),
                         "3 builds: 3 with a log, 1 log without a finish time, 1 still compared with GCS on every run, "
                         "1 never settled, 1 watched")


class CrossCheckOneTabUnread(r4.World):
    def test_UJ5_a_job_one_of_whose_tabs_cannot_be_read_now_is_not_judged(self):
        # (that tab may still list the build: it is not known to be gone from TestGrid)
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        seen = snowflake(self.now - HOUR)
        self.show([seen], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.table("dash", "t2", (500, {}, b""))
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, seen)])
        k7.CrossCheckJudging.report(self, [{"dashboard": "dash", "tab": t, "query": "bucket/logs/job",
                                            "oldest": self.now - HOUR} for t in ("t", "t2")])
        code, out = k7.CrossCheckJudging.check(self)
        self.assertIn("cannot re-read dash#t2", out)
        self.assertNotIn(f"MISSED red build job/{b}", out)
        # (once both tabs are read, it is judged: missed)
        self.srv.table("dash", "t2", table("bucket/logs/job", [seen], {"job.Overall": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))
        code, out = k7.CrossCheckJudging.check(self)
        self.assertIn(f"MISSED red build job/{b}", out)


class CrossCheckLateLog(r4.World):
    def test_CB1_only_builds_compared_with_gcs_today_are_asked_for_a_late_log(self):
        day = int(self.now // 86400)  # (one run: a seventh of the older builds, by the day, are compared)
        hours = next(h for h in range(20, 60)
                     if zlib.crc32(f"job/{snowflake(self.now - h * HOUR)}".encode()) % 7 not in {day % 7, (day + 1) % 7})
        b = snowflake(self.now - hours * HOUR)
        self.show([b], {"job.Overall": ([12], [""])}, start=int(self.now - hours * HOUR))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.meta(b)["log"])  # archived without a log: GCS had none
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"late log\n"))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "1"])
        self.assertNotIn("GCS now has a build-log.txt", out.getvalue())
        _, out = self.check()  # (within the window: asked)
        self.assertIn(f"job/{b}: GCS now has a build-log.txt that is not archived", out)


class RunSummaryLine(r4.World):
    def test_R14_a_build_found_running_in_a_gap_is_not_counted_as_retried(self):
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        red, running = snowflake(self.now - 30 * HOUR), snowflake(self.now - 10 * HOUR)
        self.failed_in_gcs(red, 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{running}/started.json", (200, {}, {"timestamp": int(self.now - 10 * HOUR)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (red, running, visible)])
        out = io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
            frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url, "--gcs", self.srv.url])
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{red}"], "watching": [f"job/{running}"]})
        self.assertIn(" 0 selected, 0 retried, 0 re-compared with GCS, 1 taken from GCS", out.getvalue())


class TabDays(r4.World):
    def test_R17_a_tab_read_now_judges_by_the_days_its_history_shows(self):
        visible, old = snowflake(self.now - HOUR), snowflake(self.now - 25 * DAY)
        t = table("bucket/logs/job", [visible, old], {"job.Overall": ([1, 1], ["", ""])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 25 * DAY) * 1000)]
        self.srv.table("dash", "t", t)  # (it shows 25 days: 26 days of results, not TestGrid's default 15)
        b = snowflake(self.now - 30 * HOUR)  # watched; its started.json says 20 days before it was made
        made = frl.created(b)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(made - 20 * DAY)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(made + HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible, old)])
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{b}"]})


class SightingOfAnUnreadableRecord(r4.World):
    def test_R20_a_meta_json_that_is_not_json_does_not_stop_the_run(self):
        def corrupt(b):
            write(os.path.join(self.bdir(b), "meta.json"), b"{")
        b = k7.Sightings.two_runs(self, 60, between=corrupt)  # (final: only the gap search's sighting reads it)
        runs = self.runs()
        self.assertEqual(len(runs), 2)
        self.assertTrue(any(f"job/{b}/meta.json" in (e["error"] or "") for e in runs[-1]["errors"]), runs[-1]["errors"])


class Charts(unittest.TestCase):
    NOW = dt.datetime(2026, 10, 10, 17, 20, 33, tzinfo=dt.UTC).timestamp()
    C = report.THEMES["light"]

    def summary(self):
        return report.summarize([{"time": self.NOW - k * DAY, "tabs": ["d#t"]} for k in (0, 1, 3)], [], self.NOW)

    def test_RT1_the_legend_gives_the_time_the_data_ends(self):
        self.assertIn(">Oct 10 so far (to 17:20 UTC)</text>", report.render_trend(self.summary(), "light"))

    def test_RT2_only_the_day_in_progress_is_drawn_unfinished(self):
        svg = report.render_trend(self.summary(), "light")
        self.assertEqual((svg.count('fill-opacity="0.35"'), svg.count("<path ")), (2, 2))  # (the legend's and today's)

    def test_RT3_RT4_weekly_ticks_end_at_today_which_stands_out(self):
        s = self.summary()
        tick = r'font-size="11" font-weight="400" fill="([^"]+)" text-anchor="middle" >([A-Z][a-z]{2} \d{1,2})</text>'
        ticks = re.findall(tick, report.render_trend(s, "light"))
        self.assertEqual(ticks, [(self.C["ink2"] if i == 29 else self.C["muted"], report.short_day(s["days"][i]))
                                 for i in (1, 8, 15, 22, 29)])

    def test_RH1_the_first_of_a_month_is_labelled_with_its_month(self):
        now = dt.datetime(2026, 10, 5, 12, tzinfo=dt.UTC).timestamp()
        svg = report.render_heatmap(report.summarize([{"time": now, "tabs": ["d#t"]}], [], now), "light")
        self.assertIn(">Sep 22</text>", svg)
        self.assertIn(">Oct 1</text>", svg)

    def test_RC2_RC3_RS1_a_tab_counts_as_seen_up_to_each_scan_and_within_its_window(self):
        now = dt.datetime(2026, 10, 10, 3, 0, tzinfo=dt.UTC).timestamp()
        started = now - 2 * HOUR
        run = run_record(started, [{"dashboard": "d", "tab": "t", "oldest": now - 5 * DAY}], since_hours=48)
        row = report.summarize([{"time": now - DAY, "tabs": ["d#t"]}], [run], now)["rows"][0]
        self.assertEqual((row["seen"], row["spans"]),
                         ([(started - 48 * HOUR, started)], [(started - 48 * HOUR, started - 6 * HOUR)]))

    def test_RH3_a_day_the_last_scan_saw_has_a_cell_though_its_builds_may_still_run(self):
        now = dt.datetime(2026, 10, 10, 3, 0, tzinfo=dt.UTC).timestamp()
        run = run_record(now - 2 * HOUR, [{"dashboard": "d", "tab": "t", "oldest": now - 5 * DAY}])
        s = report.summarize([{"time": now - 2 * DAY, "tabs": ["d#t"]}], [run], now)
        # (seen from Oct 5 03:00 to the scan, Oct 10 01:00: a "0" cell for Oct 5, 6, 7, 9 and 10, today)
        self.assertEqual(report.render_heatmap(s, "light").count(f'height="22" rx="3" fill="{self.C["empty"]}"'), 5)

    def test_RH2_RH4_the_heatmap_marks_today(self):
        svg = report.render_heatmap(self.summary(), "light")
        self.assertIn(f'font-size="11" font-weight="600" fill="{self.C["ink"]}" text-anchor="middle" >10</text>', svg)
        self.assertIn(">to Oct 10</text>", svg)

    def test_UB1_the_bar_of_the_day_in_progress_has_a_strong_outline(self):
        self.assertIn(f'fill="{self.C["bar"]}" fill-opacity="0.35" stroke="{self.C["edge"]}"',
                      report.render_trend(self.summary(), "light"))

    def test_WC1_WC4_the_charts_alt_texts_say_when_their_data_ends(self):
        root = tempdir(self)
        md = "\n".join(report.write_charts(root, [{"time": self.NOW - k * DAY, "tabs": ["d#t"]} for k in (0, 1, 3)],
                                           self.NOW))
        self.assertIn('alt="Red builds per day, data to Oct 10 17:20 UTC. ', md)
        self.assertIn('alt="Red builds per tab, 14 days to Oct 10 17:20 UTC. ', md)  # fails: "to Oct 10 to 17:20 UTC"


if __name__ == "__main__":
    unittest.main()
