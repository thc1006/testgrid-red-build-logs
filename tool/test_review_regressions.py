"""Regression cases R01-R13 from an external review of commit 036e196, ported to
the per-run layout. Each failed against that commit; all must pass now."""
import contextlib
import datetime as dt
import io
import json
import os
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
from test_fetch_red_logs import FakeServer, gcs_object, gzip_stored, run_record, table  # noqa: E402


class LocalArchive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.server = FakeServer()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.server.close)
        self.start = int(time.time()) - 20 * 3600
        self.build = str((self.start * 1000 - frl.PROW_EPOCH_MS) << 22)
        self.key = f"job/{self.build}"
        self.base = f"/bucket/logs/job/{self.build}"
        self.server.summary("dash", {"t": "FAILING"})
        self.server.table("dash", "t", table("bucket/logs/job", [self.build],
                                             {"test": ([12], ["test failed"])}, start=self.start * 1000))
        self.server.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.server.route(self.base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": self.start + 600}))

    def fetch(self, *extra):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return frl.main(["--archive", str(self.root), "--dashboard", "dash", "--testgrid", self.server.url,
                             "--gcs", self.server.url, "--max-concurrency", "4", "--start-concurrency", "2", *extra])

    def check(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", str(self.root), "--testgrid", self.server.url,
                            "--gcs", self.server.url, "--max-concurrency", "4"])
        return code, out.getvalue()

    def target(self):
        (found,) = self.root.glob(f"runs/*/*/job/{self.build}")
        return found

    def test_R01_corrupt_only_podinfo_must_fail_verification(self):
        self.server.route(self.base + "/podinfo.json", gcs_object(b'{"phase":"Failed"}\n'))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.check()[0], 0)
        path = self.target() / "podinfo.json"
        path.write_bytes(b"x" * path.stat().st_size)  # ordinary same-size disk corruption
        code, output = self.check()
        self.assertEqual(code, 1, output)

    def test_R02_changed_gzip_object_must_fail_remote_verification(self):
        self.server.route(self.base + "/build-log.txt", gzip_stored(b"original evidence\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.check()[0], 0)
        self.server.route(self.base + "/build-log.txt", gzip_stored(b"updated evidence\n"))
        code, output = self.check()
        self.assertEqual(code, 1, output)

    def test_R03_reporting_failure_must_preserve_retry_checkpoint(self):
        self.server.route(self.base + "/finished.json", (404, {}, b""))
        with mock.patch.object(report, "write_charts", side_effect=OSError("simulated chart write failure")):
            self.assertEqual(self.fetch("--max-job-hours", "24"), 1)  # recorded and failed, not lost
        state_path = self.root / "state.json"
        self.assertTrue(state_path.exists(), "rendering failed before the retry state was saved")
        self.assertIn(self.key, json.loads(state_path.read_text())["unresolved"])
        (run_json,) = self.root.glob("runs/*/*/run.json")
        errors = [e["error"] for e in json.loads(run_json.read_text())["errors"]]
        self.assertTrue(any("simulated chart write failure" in e for e in errors), errors)

    def test_R10_finished_metadata_and_final_log_must_be_refreshed(self):
        self.server.route(self.base + "/finished.json", (404, {}, b""))
        self.server.route(self.base + "/build-log.txt", gcs_object(b"first upload\n"))
        self.assertEqual(self.fetch(), 0)
        self.server.route(self.base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": self.start + 600}))
        self.server.route(self.base + "/build-log.txt", gcs_object(b"final upload\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.target() / "build-log.txt").read_bytes(), b"final upload\n")
        self.assertEqual(json.loads((self.target() / "meta.json").read_text())["result"], "FAILURE")

    def test_R11_log_finalization_must_continue_after_leaving_testgrid(self):
        self.server.route(self.base + "/finished.json", (404, {}, b""))
        self.server.route(self.base + "/build-log.txt", gcs_object(b"first upload\n"))
        self.assertEqual(self.fetch(), 0)
        self.server.table("dash", "t", table("bucket/logs/job", [self.build],
                                             {"test": ([1], [""])}, start=self.start * 1000))
        self.server.route(self.base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": self.start + 600}))
        self.server.route(self.base + "/build-log.txt", gcs_object(b"final upload\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.target() / "build-log.txt").read_bytes(), b"final upload\n")


class DataAndReporting(unittest.TestCase):
    def test_R04_nonobject_state_must_not_be_silently_discarded(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "state.json").write_text('["damaged state"]')
            warnings = []
            frl.load_state(root, warnings)
            self.assertTrue(any("state.json" in w for w in warnings), warnings)

    def test_R05_malformed_retry_collection_must_be_reported(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, "state.json").write_text('{"unresolved": ["damaged retry list"]}')
            warnings = []
            frl.load_state(root, warnings)
            self.assertTrue(any("state.json" in w for w in warnings), warnings)

    def test_R06_negative_verification_window_must_be_rejected(self):
        with mock.patch.object(cc, "verify", return_value=0), contextlib.redirect_stderr(io.StringIO()):
            for flag, value in (("--hours", "-1"), ("--hours", "nan"), ("--lag-minutes", "-5"),
                                ("--lag-minutes", "inf"), ("--max-concurrency", "0")):
                with self.assertRaises(SystemExit) as raised:
                    cc.main(["--archive", "unused", flag, value])
                self.assertEqual(raised.exception.code, 2, flag)

    def test_R07_glob_metacharacters_in_archive_path_are_literal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp, "archive[1]")
            run_dir = root / "runs" / "2026-10" / "2026-10-01T000000Z"
            run_dir.mkdir(parents=True)
            (run_dir / "run.json").write_text(json.dumps(run_record(time.time(), [])))
            self.assertEqual(len(report.load_runs(str(root))), 1)
            self.assertEqual(cc.latest_run(str(root)), str(run_dir / "run.json"))

    def test_R08_collector_must_reject_negative_rle_lengths(self):
        t = table("bucket/logs/job", ["1"], {"test": ([12], [""])})
        t["tests"][0]["statuses"] = [{"value": 1, "count": -1}, {"value": 12, "count": 2}]
        with self.assertRaises(ValueError):
            frl.red_columns(t)

    def test_R09_verifier_must_reject_negative_rle_lengths(self):
        t = table("bucket/logs/job", ["1"], {"test": ([12], [""])})
        t["tests"][0]["statuses"] = [{"value": 1, "count": -1}, {"value": 12, "count": 2}]
        with self.assertRaises(ValueError):
            list(cc.columns(t))

    def test_R12_trend_must_not_connect_across_unknown_days(self):
        now = time.time()
        summary = report.summarize([], [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * 86400}])], now)
        days = summary["days"]
        summary["panels"][0]["avg"] = {days[i]: float(i) for i in (1, 2, 10, 11)}
        tree = ET.fromstring(report.render_trend(summary, "light"))
        lines = tree.findall("{http://www.w3.org/2000/svg}polyline")
        self.assertEqual(len(lines), 4, "two segments each need a foreground and outline; no gap bridging")

    def test_R13_removing_a_tab_must_not_certify_its_uncovered_past(self):
        now = report.day_start(dt.datetime.now(dt.UTC).date()) + 12 * 3600
        older = run_record(now - 86400, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * 86400},
                                         {"dashboard": "d", "tab": "broken", "error": "temporarily unavailable"}])
        newer = run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * 86400}])
        past = report.day_of(now - 5 * 86400)
        self.assertNotIn(past, report.summarize([], [older], now)["panels"][0]["complete"])
        self.assertNotIn(past, report.summarize([], [older, newer], now)["panels"][0]["complete"])

    def test_R13_a_removed_tab_does_not_poison_later_days(self):
        now = report.day_start(report.day_of(time.time())) + 12 * 3600
        runs = [run_record(now - 20 * 86400, [{"dashboard": "d", "tab": "a", "oldest": now - 40 * 86400},
                                              {"dashboard": "d", "tab": "b", "error": "failed"}]),
                run_record(now - 10 * 86400, [{"dashboard": "d", "tab": "a", "oldest": now - 40 * 86400}]),
                run_record(now, [{"dashboard": "d", "tab": "a", "oldest": now - 40 * 86400}])]
        complete = report.summarize([], runs, now)["panels"][0]["complete"]
        self.assertIn(report.day_of(now - 5 * 86400), complete)
        self.assertNotIn(report.day_of(now - 25 * 86400), complete)


if __name__ == "__main__":
    unittest.main(verbosity=2)
