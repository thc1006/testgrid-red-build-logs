"""Targeted tests from the round-7 mutation-testing review: each kills mutants the rest of
the suite let survive. Each test name starts with the mutant id(s) it targets."""
import contextlib
import datetime as dt
import gzip
import io
import json
import threading
import os
import random
import sys
import tempfile
import time
import unittest
import urllib.parse
import zlib
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
import testgrid_rule as tr  # noqa: E402
import test_review_round4 as r4  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, run_record, send, table  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj, snowflake  # noqa: E402

OVERALL_FAIL = {"test": "job.Overall", "status": "FAIL", "message": "it did not pass; Prow result FAILURE"}


def files_like(target, prefix):
    return sorted(n for n in os.listdir(target) if n.startswith(prefix))


class Sightings(r4.World):
    """An archived build whose column TestGrid painted red on a second tab, which it
    scrolled off of during a history gap, gains that tab (backfill_gaps "sightings")."""

    def two_runs(self, b_ago, between=None):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        b = snowflake(self.now - b_ago * HOUR)
        self.failed_in_gcs(b, b_ago * HOUR)  # finished an hour after it started
        # run 1: t shows b red; t2 cannot be read (so it keeps its old scan time)
        self.srv.table("dash", "t", table("bucket/logs/job", [b], {"job.Overall": ([12], ["boom"])},
                                          start=int((self.now - b_ago * HOUR) * 1000)))
        self.srv.table("dash", "t2", (500, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json"])
        self.put_state(tabs={"dash#t2": {"scan": self.now - (b_ago + 10) * HOUR,
                                         "oldest": self.now - (b_ago + 20) * HOUR}})
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived": [f"job/{b}"]})
        if between:
            between(b)
        # run 2: b scrolled off both tabs; t2 loads, and its history gap holds b
        visible = snowflake(self.now - HOUR)
        for tab in ("t", "t2"):
            self.srv.table("dash", tab, table("bucket/logs/job", [visible], {"job.Overall": ([1], [""])},
                                              start=int((self.now - HOUR) * 1000)))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible)])
        self.fetch()
        return b

    def assert_gained_t2(self, b):
        run = self.runs()[-1]
        key = f"job/{b}"
        self.assertEqual(sum(v.count(key) for v in run["outcomes"].values()), 1, run["outcomes"])
        self.assertEqual((run["rechecked"], run["backfilled_from_gcs"], run["retried_from_state"]), (1, 0, 0))
        tabs = self.meta(b)["tabs"]
        self.assertEqual([(t["tab"], bool(t.get("judged_from_gcs"))) for t in tabs], [("t", False), ("t2", True)])
        self.assertEqual(tabs[0]["red_cells"], [{"test": "job.Overall", "status": "FAIL", "message": "boom"}])
        self.assertEqual(tabs[1]["red_cells"], [OVERALL_FAIL])

    def test_BG1_BG7_BG12_R4_R6_R8_a_build_still_compared_with_gcs_gains_the_tab(self):
        self.assert_gained_t2(self.two_runs(30))  # finished 29 h ago: not final yet

    def test_BG1_BG12_R6_R8_a_final_build_gains_the_tab(self):
        b = self.two_runs(60)  # finished 59 h ago: final, so only the gap search brings it back
        self.assertTrue(self.meta(b)["final"])
        self.assert_gained_t2(b)

    def test_BG8_an_undecided_column_adds_no_tab(self):
        b = self.two_runs(30, between=lambda b: self.srv.route(f"/bucket/logs/job/{b}/started.json", (403, {}, b"")))
        self.assertEqual([t["tab"] for t in self.meta(b)["tabs"]], ["t"])


class HiddenRows(r4.World):
    """A tab that hides its job's Overall row (its table has job.Pod but no job.Overall)
    never showed a build red for Overall alone, so the gap search does not take it, and
    cross_check does not call it missed."""

    def test_S1_R16_BG6_UJ7_UJ15_a_gap_build_red_only_on_a_hidden_row_is_not_red(self):
        self.earlier_run(self.now - 50 * HOUR)  # the archive began before b
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"job.Pod": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)  # Overall FAIL; no podinfo.json long after: Pod not red
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible)])
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn("backfilled", self.runs()[-1]["outcomes"])
        self.assertIsNone(self.bdir(b))
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_J3_J4_R19_a_watched_build_red_only_on_a_hidden_row_is_dropped(self):
        self.show([snowflake(self.now)], {"job.Pod": ([1], [""])}, start=int(self.now))
        b = snowflake(self.now - 5 * HOUR)
        self.failed_in_gcs(b, 5 * HOUR)
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{b}"]})


class WatchedFoundRed(r4.World):
    def test_A2_J5_J6_a_watched_build_found_red_keeps_its_red_cells(self):
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))
        b = snowflake(self.now - 5 * HOUR)
        self.failed_in_gcs(b, 5 * HOUR)
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{b}"]})
        (tab,) = self.meta(b)["tabs"]
        self.assertEqual((tab["red_cells"], tab.get("judged_from_gcs")), ([OVERALL_FAIL], True))


class LargeFiles(ArchiveWorld):
    def test_FD3_a_log_over_a_mebibyte_is_intact_on_the_next_run(self):
        self.finished(ago=3 * 86400)
        self.srv.route(self.base + "/build-log.txt", obj(random.Random(1).randbytes(3 << 20)))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual(self.check()[0], 0)

    def test_FD3_parts_over_a_mebibyte_are_intact_on_the_next_run(self):
        self.finished(ago=3 * 86400)
        self.srv.route(self.base + "/build-log.txt", obj(random.Random(2).randbytes(5 << 20)))
        self.assertEqual(self.fetch("--max-file-mb", "2"), 0)
        self.assertEqual(len(self.meta()["log"]["parts"]), 3)
        self.assertEqual(self.fetch("--max-file-mb", "2"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})

    def test_K6_a_part_built_from_several_reads_stays_within_the_limit(self):
        self.finished(ago=3 * 86400)
        log = b"0123456789" * 5
        self.srv.route(self.base + "/build-log.txt", obj(log))
        with mock.patch.object(frl, "PART_STEP", 3):
            self.assertEqual(self.fetch("--max-file-mb", str(8 / (1 << 20))), 0)
        parts = self.meta()["log"]["parts"]
        self.assertTrue(all(0 < p["bytes"] <= 8 for p in parts), parts)
        self.assertEqual(b"".join(self.read(p["file"]) for p in parts), log)


class StaleParts(ArchiveWorld):
    def test_K2_a_log_that_shrinks_below_the_limit_leaves_no_parts(self):
        self.finished(ago=3 * HOUR)  # not final: compared with GCS again
        limit = ("--max-file-mb", str(8 / (1 << 20)))
        self.srv.route(self.base + "/build-log.txt", obj(b"a first log, long enough to split\n", "1"))
        self.assertEqual(self.fetch(*limit), 0)
        self.assertGreater(len(files_like(self.bdir(), "build-log.txt.")), 1)
        self.srv.route(self.base + "/build-log.txt", obj(b"short\n", "2"))
        self.assertEqual(self.fetch(*limit), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})
        self.assertEqual(files_like(self.bdir(), "build-log"), ["build-log.txt"])

    def test_SL1_a_log_stored_in_the_other_format_leaves_no_parts(self):
        self.finished(ago=3 * HOUR)
        limit = ("--max-file-mb", str(8 / (1 << 20)))
        self.srv.route(self.base + "/build-log.txt", obj(b"a first log, long enough to split\n", "1"))
        self.assertEqual(self.fetch("--gzip", *limit), 0)
        self.assertTrue(files_like(self.bdir(), "build-log.txt.gz."))
        self.srv.route(self.base + "/build-log.txt", obj(b"short\n", "2"))
        self.assertEqual(self.fetch(*limit), 0)
        self.assertEqual(files_like(self.bdir(), "build-log"), ["build-log.txt"])

    def test_CPB1_parts_left_by_a_killed_run_are_cleaned_up(self):
        target = os.path.join(self.root, "runs", "2026-10", "2026-10-01T000000Z", "job", "7")
        os.makedirs(target)
        for name in ("build-log.txt.000", "build-log.txt.001"):
            open(os.path.join(target, name), "wb").close()
        warnings = []
        frl.clean_partial_builds(self.root, warnings)
        self.assertEqual((os.path.exists(target), warnings), (False, []))

    def test_IN4_a_meta_json_with_a_bad_parts_list_is_not_intact(self):
        target = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, target)
        with open(os.path.join(target, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        meta = {"log": {"file": "build-log.txt", "bytes": 4, "file_bytes": 4, "md5": "x",
                        "parts": [{"file": "elsewhere", "bytes": 4}]}}
        self.assertFalse(frl.intact(target, meta, deep=True))


class TableLimits(unittest.TestCase):
    def test_TO1_TB7_TO6_long_messages_are_accepted_up_to_a_mebibyte_each(self):
        # 1.2 MB of messages in one row, none over 1 MiB; then one of exactly 1 MiB
        for msgs in (["x" * 600_000, "y" * 5_000, "z" * 600_000], ["x" * (1 << 20), "y" * 5_000, "z"]):
            t = table("bucket/logs/job", ["3", "2", "1"], {"r": ([12, 12, 12], msgs)})
            self.assertEqual(frl.red_columns(t)[1][0]["message"], "y" * 5_000)

    def test_RC1_RC2_RC9_the_red_cell_limit_counts_red_cells_only(self):
        def listed(reds):
            return sum(len(c) for c in reds.values()), sum(c.omitted for c in reds.values())
        builds = [str(i) for i in range(30, 10, -1)]
        with mock.patch.object(frl, "MAX_RED_CELLS", 10):
            ten = table("bucket/logs/job", builds, {"r": ([12] * 10 + [1] * 10, [""] * 20), "s": ([1] * 20, [""] * 20)})
            self.assertEqual((len(frl.red_columns(ten)), listed(frl.red_columns(ten))), (10, (10, 0)))  # green: not counted
            eleven = table("bucket/logs/job", builds, {"r": ([12] * 11 + [1] * 9, [""] * 20)})
            self.assertEqual((len(frl.red_columns(eleven)), listed(frl.red_columns(eleven))), (11, (10, 1)))
            split = table("bucket/logs/job", builds,  # 12 red cells in two rows of two runs each
                          {"a": ([12] * 6 + [1] * 14, [""] * 20), "b": ([1] * 14 + [12] * 6, [""] * 20)})
            self.assertEqual((len(frl.red_columns(split)), listed(frl.red_columns(split))), (12, (10, 2)))
        self.assertEqual(frl.MAX_RED_CELLS, 500_000)


class RuleEdges(unittest.TestCase):
    def column(self, started_ago, finished=None, podinfo=None):
        now = int(time.time())
        return tr.column("job", b'{"timestamp": %d}' % (now - started_ago), finished, podinfo, [], now - 15 * 86400, now)

    def test_C4_a_build_running_23_hours_is_still_running(self):
        self.assertEqual(self.column(23 * 3600 + 1800).results, {"RUNNING"})
        self.assertEqual(self.column(24 * 3600 + 60).results, {"FAIL"})

    def test_ST1_a_pod_that_succeeded_passes_whatever_its_containers_say(self):
        pod = json.dumps({"pod": {"status": {"phase": "Succeeded", "containerStatuses": [
            {"name": "sidecar", "state": {"terminated": {"exitCode": 1}}}]}}}).encode()
        fin = b'{"timestamp": %d, "passed": true}' % (int(time.time()) - 60)
        self.assertEqual(self.column(3600, fin, pod).cells["job.Pod"], "PASS")


class ReadLimits(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        frl.BACKOFF = 0

    def test_RA2_RA4_RA5_bodies_are_bounded_plain_or_gzipped(self):
        body = b"x" * 1000
        self.srv.route("/plain", (200, {}, body))
        self.srv.route("/gz", (200, {"Content-Encoding": "gzip"}, gzip.compress(body)))
        for path in ("/plain", "/gz"):
            with self.subTest(path):
                self.assertEqual(frl.read_artifact(self.srv.url + path, 1000), body)
                with self.assertRaisesRegex(frl.IntegrityError, "limit"):
                    frl.read_artifact(self.srv.url + path, 999)


class Listing(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        frl.BACKOFF = 0
        now = int(time.time())
        self.build = snowflake(now - 12 * HOUR)  # (made 12 hours ago: TestGrid shows the last DAYS_OF_RESULTS days)
        self.put("started.json", json.dumps({"timestamp": frl.created(self.build)}).encode())
        self.put("finished.json", json.dumps({"timestamp": now - 60, "passed": True}).encode())

    def put(self, name, body):
        self.srv.route(f"/bucket/logs/job/{self.build}/{urllib.parse.quote(name)}", (200, {}, body))

    def test_LO1_a_junit_file_on_the_second_listing_page_is_read(self):
        self.put("artifacts/junit_01.xml", b'<testsuite><testcase name="t"><failure/></testcase></testsuite>')
        folder = f"logs/job/{self.build}/"
        pages = {None: ([folder + "finished.json", folder + "started.json"], "p2"),
                 "p2": ([folder + "artifacts/junit_01.xml"], None)}

        def serve(h):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            names, token = pages[q.get("pageToken", [None])[0]]
            page = {"items": [{"name": n, "size": "10"} for n in names]}
            if token:
                page["nextPageToken"] = token
            send(h, 200, {}, json.dumps(page).encode())
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        self.assertEqual(frl.gcs_column(self.srv.url, "bucket/logs/job", self.build).red_rows(), ["t"])

    def test_G12_a_junit_object_name_is_quoted_in_its_url(self):
        self.put("artifacts/junit a#b.xml", b'<testsuite><testcase name="t"/></testsuite>')
        self.srv.gcs_listing("bucket", [])
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual((col.results, col.cells.get("t")), ({"PASS"}, "PASS"))


class Holdings(unittest.TestCase):
    def test_H1_to_H9_each_row_counts_its_own_kind(self):
        def e(**kw):
            base = {"has_log": False, "files": [], "remote": [], "in_parts": False, "settled": True, "final": True,
                    "backfilled": False, "from_gcs": False}
            return dict(base, **kw)
        entries = [e(has_log=True, files=["l"], in_parts=True, settled=False, final=False, backfilled=True, from_gcs=True),
                   e(has_log=True, files=["l", "n"], node_logs=True),
                   e(files=["p"], backfilled=True),
                   e(remote=[("podinfo.json", "u")], final=None),
                   e(settled=False, final=True, comparison_given_up=True, comparison_failing_since=1_790_000_000.0),
                   e()]
        waiting = [{"status": "waiting for GCS upload", "on_disk": True}, {"status": "given up: x", "on_disk": True},
                   {"status": "given up: y", "on_disk": False}]
        rows = dict(line.rsplit(" | ", 1) for line in frl.holdings(entries, waiting, {"outcomes": {"watching": ["a"]}})
                    if line.startswith("| ") and not line.startswith("| |"))
        counts = {k.strip("| ").replace("\\", ""): int(v.strip(" |")) for k, v in rows.items()}
        self.assertEqual(list(counts.values()), [6, 2, 1, 2, 1, 1, 1, 2, 1, 2, 1, 1, 2, 1], counts)  # fails: no row for it
        # the count of watched builds is the run's (state.json's), not only its "watching" outcomes
        self.assertIn("| 3 |", frl.holdings(entries, waiting, {"watched": 3, "outcomes": {"watching": ["a"]}})[-1])

    def test_IE2_IE3_IE4_IE5_index_entry_fields(self):
        base = {"job": "job", "build": "1", "query": "b/logs/job", "started": None, "created": time.time() - 3600}
        tg = {"dashboard": "d", "tab": "t", "red_cells": [{"test": "job.Overall", "status": "FAIL", "message": "x"}]}
        judged = dict(tg, tab="t2", judged_from_gcs=True)
        pod = {"file": "podinfo.json", "bytes": 4, "parts": [{"file": "podinfo.json.000", "bytes": 4}]}
        m = dict(base, tabs=[tg], log=None, podinfo=pod)
        entry = frl.index_entry("job", "1", m, time.time(), "runs/2026-10/x/job/1")
        self.assertEqual((entry["has_log"], entry["in_parts"]), (False, True))
        old_backfill = dict(base, tabs=[dict(tg, red_cells=[])], backfilled="note")
        self.assertTrue(frl.index_entry("job", "1", old_backfill, time.time())["from_gcs"])
        shown_later = dict(base, tabs=[judged, tg], backfilled="note")
        self.assertFalse(frl.index_entry("job", "1", shown_later, time.time())["from_gcs"])



class DaysOfResults(unittest.TestCase):
    def test_G2_BG3_a_tabs_days_of_results_decide_what_is_too_old(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        frl.BACKOFF = 0
        build = snowflake(time.time() - 30 * HOUR)
        made = frl.created(build)
        base = f"/bucket/logs/job/{build}"
        srv.route(base + "/started.json", (200, {}, {"timestamp": made - 20 * DAY}))  # 20 days before it was made
        srv.route(base + "/finished.json", (200, {}, {"timestamp": made + 3600, "passed": False}))
        srv.route(base + "/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        srv.gcs_listing("bucket", [])
        self.assertEqual(frl.verdict(frl.gcs_column(srv.url, "bucket/logs/job", build, 15), "job")[0], "not-red")
        self.assertEqual(frl.verdict(frl.gcs_column(srv.url, "bucket/logs/job", build, 60), "job")[0], "red")
        srv.route(base + "/started.json", (200, {}, {"timestamp": made - 15 * DAY + 600}))  # just within 15 days
        self.assertEqual(frl.verdict(frl.gcs_column(srv.url, "bucket/logs/job", build, 15), "job")[0], "red")


class CrossCheckJudging(r4.World):
    """cross_check's judging of builds TestGrid no longer lists, from a run report written here."""

    def report(self, tabs):
        self.earlier_run(self.now - 3 * DAY)  # the archive began before the builds
        name = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime(self.now))
        d = os.path.join(self.root, "runs", name[:7], name)
        os.makedirs(d)
        frl.write_json(os.path.join(d, "run.json"), dict(run_record(self.now, tabs), run=name, dir=f"runs/{name[:7]}/{name}",
                                                          errors=[], warnings=[], serious_warnings=[], finished=self.now))

    def check(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48"])
        return code, out.getvalue()

    def test_UJ8_a_missed_build_names_only_its_own_jobs_tabs(self):
        self.srv.summary("dash", {"t": "FAILING", "u": "FAILING"})
        seen_job, seen_job2 = snowflake(self.now - HOUR), snowflake(self.now - HOUR + 60)
        self.show([seen_job], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.table("dash", "u", table("bucket/logs/job2", [seen_job2], {"job2.Overall": ([1], [""])},
                                          start=int((self.now - HOUR) * 1000)))
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, seen_job)]
                             + [f"logs/job2/{seen_job2}/finished.json"])
        self.report([{"dashboard": "dash", "tab": "t", "query": "bucket/logs/job", "oldest": self.now - HOUR},
                     {"dashboard": "dash", "tab": "u", "query": "bucket/logs/job2", "oldest": self.now - HOUR}])
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"MISSED red build job/{b} (dash#t): TestGrid no longer lists it", out)


class Rotation(ArchiveWorld):
    def test_CG4_CG5_an_old_build_is_compared_with_gcs_once_a_week(self):
        self.finished(ago=3 * DAY)
        calls = []
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1", calls))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 0)
        meta_path = os.path.join(self.bdir(), "meta.json")
        frl.write_json(meta_path, dict(self.meta(), started=time.time() - 30 * DAY))  # long before any window
        (rel,) = frl.run_dirs(self.root)
        run_path = os.path.join(self.root, rel, "run.json")
        run = frl.read_json(run_path)
        compared, due = [], []
        seventh = zlib.crc32(self.key.encode()) % 7
        for k in range(14):  # the same run checked as if on each of fourteen days in a row
            started = run["started"] - k * DAY
            frl.write_json(run_path, dict(run, started=started))
            before = calls.count("HEAD")
            with contextlib.redirect_stdout(io.StringIO()):
                cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--run", run_path,
                         "--max-run-age-hours", "1000"])
            compared.append(calls.count("HEAD") - before)
            due.append(int(int(started // DAY) % 7 == seventh))
        self.assertEqual(compared, due)  # its own day of the week, and only then: twice in fourteen days
        self.assertEqual(sum(compared), 2)


class ReportLabels(unittest.TestCase):
    def test_RS2_AO2_the_charts_say_when_their_data_ends(self):
        now = dt.datetime(2026, 10, 10, 17, 20, 33, tzinfo=dt.UTC).timestamp()
        s = report.summarize([], [], now)
        self.assertEqual(report.as_of(s), "Oct 10 17:20 UTC")


class UnreadableFlag(r4.World):
    def test_R3_a_build_looked_at_again_keeps_no_unreadable_flag(self):
        self.gap_world()
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        body = json.dumps({"result": "FAILURE", "passed": False, "timestamp": int(self.now - 29 * HOUR)}).encode()
        lock, seen = threading.Lock(), []

        def finished(h):  # the gap search's read fails; the second look in the run succeeds
            with lock:
                seen.append(1)
                first = len(seen) <= frl.RETRIES
            send(h, 500, {}, b"") if first else send(h, 200, {}, body)
        self.srv.route(f"/bucket/logs/job/{self.red30h}/finished.json", finished)
        self.fetch()
        run = self.runs()[-1]
        self.assertIn(f"job/{self.red30h}", run["outcomes"]["backfilled"])
        self.assertEqual(run["backfilled_from_gcs"], len(run["outcomes"]["backfilled"]))  # (also the one looked at again)
        self.assertNotIn("unreadable", self.meta(self.red30h))


if __name__ == "__main__":
    unittest.main()
