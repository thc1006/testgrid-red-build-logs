"""Targeted tests from a fourth mutation-testing review: each kills mutants the rest of
the suite let survive. Each test name starts with the mutant id(s) it targets."""
import contextlib
import datetime as dt
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r3 as r3  # noqa: E402  (a module import: its tests are not collected again here)
from test_fetch_red_logs import DAY, run_record, send, table  # noqa: E402
from test_integrity import HOUR, obj, snowflake  # noqa: E402


def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def run_folder(root, started, report=None):
    """A run folder (and its run.json) for a run that started at `started`."""
    name = dt.datetime.fromtimestamp(started, dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
    os.makedirs(os.path.join(root, "runs", name[:7], name), exist_ok=True)
    frl.write_json(os.path.join(root, "runs", name[:7], name, "run.json"),
                   report if report is not None else dict(run_record(started, []), run=name))
    return name


class Unit(unittest.TestCase):
    def test_GI03_GI05_go_int64_is_exactly_the_int64_range(self):
        for v in (0, -5, -(1 << 63), 1 << 62, (1 << 63) - 1, 1_791_000_000):
            self.assertTrue(frl.go_int64(v), v)
        for v in (1 << 63, -(1 << 63) - 1, True, 1.0, "5", None):
            self.assertFalse(frl.go_int64(v), v)

    def test_DC06_cross_check_decodes_exactly_the_int64_range(self):
        for v in (0, -5, -(1 << 63), 1 << 62, (1 << 63) - 1):
            self.assertTrue(cc.decodable({"timestamp": v}, {"timestamp": int}), v)
        for v in (1 << 63, -(1 << 63) - 1, True, 1.0, "5"):
            self.assertFalse(cc.decodable({"timestamp": v}, {"timestamp": int}), v)

    def test_JM03_JM04_JM05_malformed_remembered_margins_are_ignored(self):
        now = time.time()
        for entry in ({"job_hours": ["20"]}, {"job_hours": {"abc": now + HOUR}}, {"job_hours": {"20": "soon"}},
                      {"job_hours": {"20": None}}, {"job_hours": {"20": 1e30}}):
            with self.subTest(entry):
                self.assertEqual(frl.job_margin(entry, None, now, 6), (6, {}))

    def test_JM06_JM18_a_remembered_margin_binds_until_its_time_but_never_beyond_a_week(self):
        now = time.time()
        # cut from 30 h to 2 h, then to 3 h within a day: builds started under 30 h may still run
        entry = {"current_job_hours": 2, "job_hours": {"30": now + 29 * HOUR, "2": now + HOUR}}
        self.assertEqual(frl.job_margin(entry, 3, now, 6)[0], 30)
        self.assertEqual(frl.job_margin({"job_hours": {"30": now + 30 * DAY}}, None, now, 6), (6, {}))

    def test_JM19_an_absurd_margin_is_ignored(self):
        now = time.time()
        self.assertEqual(frl.job_margin({"current_job_hours": 1000}, None, now, 6), (6, {}))
        self.assertEqual(frl.job_margin({}, 500, now, 6), (6, {}))

    def test_SF06_a_finish_a_day_and_a_half_before_the_build_id_is_not_trusted(self):
        now = time.time()
        created = int(now - 10 * HOUR)
        meta = {"created": created, "finished": created - 36 * HOUR, "archived_at": now - 5 * HOUR, "log": {"md5": "x"}}
        self.assertEqual(frl.settle_flags(meta, now, now), (False, False))

    def test_WR01_WR02_malformed_warning_lists_in_a_run_report(self):
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            readme = os.path.join(root, rel, "README.md")
            frl.write_run_readme(root, rel, {"warnings": "abc", "serious_warnings": []}, [])
            self.assertNotIn("## Warnings", slurp(readme))
            frl.write_run_readme(root, rel, {"warnings": ["state", "x"], "serious_warnings": "state.json"}, [])
            self.assertIn("- state\n", slurp(readme).split("## Warnings")[1])
            frl.write_run_readme(root, rel, {"warnings": ["x"], "serious_warnings": 5}, [])
            self.assertIn("- x\n", slurp(readme).split("## Warnings")[1])

    def test_CR01_CR04_CR05_CR06_text_allows_tab_and_newline_but_no_other_control(self):
        self.assertTrue(frl.is_text("line one\n\tline two"))
        for bad in ("a\x0bb", "a\rb", "a\x0cb", "a\x1bb"):
            self.assertFalse(frl.is_text(bad), repr(bad))
        t = table("bucket/logs/job", ["1"], {"o": ([12], ["first line\n\tsecond line"])})
        self.assertEqual(frl.red_columns(t)[0][0]["message"], "first line\n\tsecond line")

    def test_AD02_the_earliest_copy_of_a_duplicate_wins(self):
        with tempfile.TemporaryDirectory() as root:
            for name in ("2026-10-01T000000Z", "2026-10-02T000000Z"):
                d = os.path.join(root, "runs", "2026-10", name, "job", "123")
                os.makedirs(d)
                frl.write_json(os.path.join(d, "meta.json"), {})
            found, duplicates = frl.archived_builds(root)
            self.assertEqual(found, {"job/123": "runs/2026-10/2026-10-01T000000Z/job/123"})
            self.assertEqual(len(duplicates), 1)


class Migration(unittest.TestCase):
    """migrate_old_layout() resuming after earlier migrations."""
    OLD, NEW = "20261001T120000.000000Z", "2026-10-01T120000Z"

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.t = dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC).timestamp()
        os.makedirs(os.path.join(self.root, "runs"))

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def old_report(self, old, t):
        frl.write_json(self.path("runs", f"{old}.json"), dict(run_record(t, []), run=old, outcomes={}))

    def old_build(self, build, old, **changes):
        d = self.path("logs", "job", build)
        os.makedirs(d)
        with open(os.path.join(d, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        meta = {"job": "job", "build": build, "query": "bucket/logs/job", "started": self.t - HOUR,
                "created": self.t - HOUR, "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}],
                "log_url": f"https://gcs/bucket/logs/job/{build}/build-log.txt", "podinfo": None,
                "log": {"bytes": 4, "md5": frl.file_md5(os.path.join(d, "build-log.txt")), "file": "build-log.txt",
                        "file_bytes": 4}, "archived_by_run": old}
        meta.update(changes)
        frl.write_json(os.path.join(d, "meta.json"), meta)

    def folder(self, name, report):
        """A run folder an earlier migration (or run) made, with this run.json."""
        os.makedirs(self.path("runs", name[:7], name))
        frl.write_json(self.path("runs", name[:7], name, "run.json"), report)

    def migrate(self):
        warnings = []
        frl.migrate_old_layout(self.root, warnings)
        return warnings

    def test_MF01_MF02_odd_run_reports_do_not_stop_a_migration(self):
        self.folder("2026-10-02T000000Z", ["not a report"])
        self.folder("2026-10-03T000000Z", {"run": "x", "started": self.t, "migrated_from": ["not", "a", "name"]})
        b = snowflake(self.t - HOUR)
        self.old_build(b, self.OLD)
        self.old_report(self.OLD, self.t)
        self.assertEqual(self.migrate(), [])
        self.assertTrue(os.path.exists(self.path("runs", "2026-10", self.NEW, "job", b, "build-log.txt")))

    def test_MF05_an_old_run_from_the_same_second_gets_its_own_folder(self):
        self.folder(self.NEW, {"run": self.NEW, "started": self.t, "migrated_from": self.OLD})
        other = "20261001T120000.500000Z"
        self.old_report(other, self.t + 0.5)
        self.assertEqual(self.migrate(), [])
        self.assertEqual(frl.read_json(self.path("runs", "2026-10", self.NEW, "run.json"))["migrated_from"], self.OLD)
        self.assertEqual(frl.read_json(self.path("runs", "2026-10", self.NEW + "-2", "run.json"))["migrated_from"], other)

    def test_MF06_a_late_build_joins_the_suffixed_folder_its_run_was_migrated_to(self):
        other = "20261001T120000.500000Z"  # same second as OLD, so an earlier migration gave it NEW-2
        self.folder(self.NEW, {"run": self.NEW, "started": self.t, "migrated_from": self.OLD})
        self.folder(self.NEW + "-2", {"run": self.NEW + "-2", "started": self.t + 0.5, "migrated_from": other})
        b = snowflake(self.t - HOUR)
        self.old_build(b, other)
        self.assertEqual(self.migrate(), [])
        self.assertTrue(os.path.exists(self.path("runs", "2026-10", self.NEW + "-2", "job", b, "meta.json")))
        self.assertEqual(len(frl.run_dirs(self.root)), 2)

    def test_MF07_a_late_build_links_the_folders_of_runs_migrated_before(self):
        older, older_new = "20261001T110000.000000Z", "2026-10-01T110000Z"
        self.folder(self.NEW, {"run": self.NEW, "started": self.t, "migrated_from": self.OLD})
        self.folder(older_new, {"run": older_new, "started": self.t - HOUR, "migrated_from": older})
        b = snowflake(self.t - HOUR)
        self.old_build(b, self.OLD, repaired_by_run=older)
        self.assertEqual(self.migrate(), [])
        m = frl.read_json(self.path("runs", "2026-10", self.NEW, "job", b, "meta.json"))
        self.assertEqual((m["archived_by_run"], m["repaired_by_run"]), (self.NEW, older_new))

    def test_MF08_a_folder_with_an_unreadable_report_is_not_taken_over(self):
        os.makedirs(self.path("runs", "2026-10", self.NEW))
        with open(self.path("runs", "2026-10", self.NEW, "run.json"), "w") as f:
            f.write("{broken")
        self.old_report(self.OLD, self.t)
        self.migrate()
        self.assertEqual(slurp(self.path("runs", "2026-10", self.NEW, "run.json")), "{broken")
        self.assertEqual(frl.read_json(self.path("runs", "2026-10", self.NEW + "-2", "run.json"))["migrated_from"], self.OLD)


class GapWorld(r3.World):
    """r3.Gap's world: TestGrid shows one passing build from an hour ago; the last scan was 40 h ago,
    so GCS is searched from 46 h ago."""

    def setUp(self):
        super().setUp()
        self.visible = snowflake(self.now - HOUR)
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})

    def listing(self, *builds):
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (*builds, self.visible)])

    def gap_build(self, hours, finished=None, started="created"):
        b = snowflake(self.now - hours * HOUR)
        self.gcs_build(b, finished=finished, started=started)
        return b

    def ago(self, hours):
        return int(self.now - hours * HOUR)


class GapRule(GapWorld):
    """Builds in a history gap judged by TestGrid's Overall-cell rule."""

    def judged(self, builds, backfilled, watching, reasons):
        self.listing(*builds)
        self.assertEqual(self.fetch(), 0)
        run = self.runs()[-1]
        want = {k: sorted(f"job/{b}" for b in v) for k, v in (("backfilled", backfilled), ("watching", watching)) if v}
        self.assertEqual(run["outcomes"], want)
        for b, reason in reasons.items():
            self.assertIn(f"by TestGrid's rule it was red ({reason}", self.meta(b)["backfilled"])
        return run

    def test_PR10_BR02_BW04_RR02_RR03_RT01_deadline_and_passed(self):
        late = self.gap_build(26)  # no finished.json, started 26 h ago: past TestGrid's 24-hour deadline
        young = self.gap_build(22)  # still within it: still running
        false_success = self.gap_build(30, (200, {}, {"result": "SUCCESS", "passed": False, "timestamp": self.ago(2)}))
        no_time = self.gap_build(31, (200, {}, {"result": "SUCCESS", "passed": True}))  # passed, but never finished
        no_result = self.gap_build(32, (200, {}, {"passed": False, "timestamp": self.ago(2)}))
        run = self.judged([late, young, false_success, no_time, no_result],
                          backfilled=[late, false_success, no_time, no_result], watching=[young],
                          reasons={late: "it did not finish within 24 hours", no_time: "it did not finish within 24 hours",
                                   false_success: "it did not pass; Prow result SUCCESS)",
                                   no_result: "it did not pass; Prow result none)"})
        self.assertEqual((run["backfilled_from_gcs"], run["retried_from_state"]), (4, 0))

    def test_RS02_RS03_RS04_RF02_BR01_RR01_RR04_unreadable_or_undecodable_artifacts_are_red(self):
        bad_started = self.gap_build(8, started=(200, {}, b"{not json"))
        list_started = self.gap_build(9, started=(200, {}, []))
        list_finished = self.gap_build(10, (200, {}, []))
        cut_finished = self.gap_build(11, (200, {}, b"{cut"))
        bad_result = self.gap_build(12, (200, {}, {"timestamp": self.ago(2), "passed": True, "result": ["SUCCESS"]}))
        builds = [bad_started, list_started, list_finished, cut_finished, bad_result]
        self.judged(builds, backfilled=builds, watching=[],
                    reasons={b: "an artifact is malformed)" for b in builds})

    def test_RF05_GI05_RS05_times_as_testgrid_reads_them(self):
        # TestGrid (deadline and overallCell in pkg/updater/gcs.go): a finish time of 0 is not
        # "finished > 0", and its deadline, an hour after it, is long past, so it is FAIL at once;
        # 2**62 is a finish time, so a passed build is not red however bogus the time; a start
        # time of 0 is no start time (Go decodes a missing one as 0 too): shown as running
        zero_finished = self.gap_build(5, (200, {}, {"timestamp": 0, "result": "FAILURE", "passed": False}))
        huge_finished = self.gap_build(6, (200, {}, {"timestamp": 1 << 62, "result": "SUCCESS", "passed": True}))
        zero_started = self.gap_build(7, started=(200, {}, {"timestamp": 0}))
        self.judged([zero_finished, huge_finished, zero_started], backfilled=[zero_finished],
                    watching=[zero_started], reasons={zero_finished: "it did not finish within 24 hours"})
        self.assertIsNone(self.meta(zero_finished)["finished"])  # not a time the archive can date

    def test_BW01_BR03_never_started_and_passed_builds_are_neither_watched_nor_backfilled(self):
        never = snowflake(self.now - 16 * HOUR)  # no started.json, no finished.json: TestGrid never lists it
        self.gcs_build(never, started=None, log=False)
        passed = self.gap_build(17, (200, {}, {"result": "SUCCESS", "timestamp": self.ago(2)}), started=(500, {}, b""))
        self.judged([never, passed], backfilled=[], watching=[], reasons={})
        self.assertEqual(self.state()["watch"], {})
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{passed}/started.json"), 0)


class GapTimeout(GapWorld):
    def slow(self, body, delay=3.5):
        def serve(h):
            time.sleep(delay)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                send(h, 200, {}, json.dumps(body).encode())
        return serve

    def test_TE01_TE06_a_gcs_listing_that_outlasts_the_run_fails_its_gap(self):
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = self.slow({"prefixes": []})
        self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), 1)  # 1.8 s
        run = self.runs()[-1]
        self.assertTrue(any("history gap failed: TimeoutError" in e["error"] for e in run["errors"]), run["errors"])
        self.assertLess(self.state()["tabs"]["dash#t"]["scan"], self.now - 39 * HOUR)

    def test_TE04_TE07_a_lookup_that_outlasts_the_run_fails_its_gap(self):
        b = snowflake(self.now - 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       self.slow({"result": "FAILURE", "passed": False, "timestamp": self.ago(1)}))
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log\n"))
        self.listing(b)
        self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), 1)  # 1.8 s
        run = self.runs()[-1]
        self.assertTrue(any("history gap failed: TimeoutError" in e["error"] for e in run["errors"]), run["errors"])
        self.assertEqual(run["outcomes"], {})
        self.assertLess(self.state()["tabs"]["dash#t"]["scan"], self.now - 39 * HOUR)


class WatchedRed(GapWorld):
    def test_AW04_a_watched_build_found_red_is_tracked_as_red_without_its_watch_flag(self):
        b = snowflake(self.now - 10 * HOUR)
        self.gcs_build(b)  # still running
        self.listing(b)
        self.fetch()
        key = f"job/{b}"
        self.assertIn(key, self.state()["watch"])
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", self.failed())
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)  # found red, but its log cannot be fetched
        self.assertNotIn("watch", self.state()["unresolved"][key]["rec"])
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)  # a failed lookup does not make a known red build "watched" again
        state = self.state()
        self.assertIn(key, state["unresolved"])
        self.assertNotIn(key, state["watch"])
        waiting = slurp(os.path.join(self.root, "INDEX.md")).split("## Not archived yet")[1].split("\n## ")[0]
        self.assertIn(b, waiting)


class Scans(r3.World):
    def show_recent(self):
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        return visible

    def test_FS04_a_new_tab_in_a_young_archive_is_searched_back_to_the_archive_start_only(self):
        run_folder(self.root, self.now - 3 * DAY)
        visible = self.show_recent()
        before, after = snowflake(self.now - 5 * DAY), snowflake(self.now - 2 * DAY)
        for b in (before, after):
            self.gcs_build(b, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (before, after, visible)])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{after}"]})

    def test_FS10_FS11_FS14_a_gap_past_the_horizon_is_searched_14_days_back_and_fails_the_run(self):
        visible = self.show_recent()
        self.put_state(tabs={"dash#t": {"scan": self.now - 20 * DAY, "oldest": self.now - 21 * DAY}})
        old, recent = snowflake(self.now - 17 * DAY), snowflake(self.now - 2 * DAY)
        for b in (old, recent):
            self.gcs_build(b, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (old, recent, visible)])
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"backfilled": [f"job/{recent}"]})
        self.assertTrue(any("searched 14 days back" in w and "may be missing" in w for w in run["serious_warnings"]),
                        run["warnings"])

    def test_LS07_a_gap_search_still_failing_after_14_days_is_tried_again(self):
        # never given up while it fails (each run fails instead); it never reaches back past 14 days
        self.show_recent()
        self.put_state(tabs={"dash#t": {"scan": self.now - 20 * DAY, "oldest": self.now - 21 * DAY}})
        self.srv.httpd.prefix_routes.clear()  # the GCS listing answers 404
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(any("the next run tries again" in w for w in self.runs()[-1]["warnings"]))
        self.assertEqual(self.state()["tabs"]["dash#t"]["scan"], self.now - 20 * DAY)

    def test_LS06_a_failed_first_search_of_a_new_tab_is_tried_again(self):
        run_folder(self.root, self.now - 3 * DAY)
        visible = self.show_recent()
        self.srv.httpd.prefix_routes.clear()
        self.assertEqual(self.fetch(), 1)
        self.assertNotIn("dash#t", self.state()["tabs"])
        red = snowflake(self.now - 2 * DAY)
        self.gcs_build(red, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{red}"]})

    def test_FS13_FS15_a_tab_showing_only_placeholders_keeps_its_last_scan(self):
        p = snowflake(self.now - HOUR)
        self.show([p], {"o": ([6], ["grid exceeds maximum size"])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        self.assertEqual(self.fetch(), 0)
        self.assertAlmostEqual(self.state()["tabs"]["dash#t"]["scan"], self.now - 40 * HOUR, delta=1)

    def test_JC03_since_hours_honours_the_margin_in_effect_at_the_last_scan(self):
        start = int(self.now - 25 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.gcs_build(b, finished=self.failed(ago=24 * HOUR))
        scan = self.now - 12 * HOUR  # a 20 h timeout was cut to 2 h before that scan; 20 h bound until 2 h ago
        self.put_state(tabs={"dash#t": {"scan": scan, "oldest": self.now - 30 * HOUR, "current_job_hours": 2,
                                        "job_hours": {"20": scan + 10 * HOUR, "2": scan + 2 * HOUR}}})
        self.fetch("--since-hours", "12")
        self.assertIn(f"job/{b}", self.outcomes().get("archived", []))


class Runs(r3.World):
    def given_up(self, days):
        b = snowflake(self.now - days * DAY)
        self.quiet()
        rec = {"job": "job", "build": b, "query": "bucket/logs/job", "started": None, "created": frl.created(b),
               "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        self.put_state(given_up={f"job/{b}": {"rec": rec, "first_seen": self.now - days * DAY, "last_error": "x"}})
        return f"job/{b}"

    def test_GU06_a_given_up_build_stays_listed_until_44_days(self):
        key = self.given_up(41)
        self.fetch()
        self.assertIn(key, self.state()["given_up"])

    def test_GU04_dropping_a_given_up_build_is_reported(self):
        key = self.given_up(50)
        self.fetch()
        self.assertTrue(any(w.startswith(f"{key}: given up on more than 30 days ago") for w in self.runs()[-1]["warnings"]))

    def test_OR05_a_red_build_failing_for_two_days_is_retried_not_given_up(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", (503, {}, b""))
        rec = {"job": "job", "build": self.build, "query": "bucket/logs/job", "started": self.start,
               "created": frl.created(self.build), "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        self.put_state(unresolved={self.key: {"rec": rec, "first_seen": self.now - 2 * DAY, "last_error": "x"}})
        self.assertEqual(self.fetch(), 1)
        state = self.state()
        self.assertIn(self.key, state["unresolved"])
        self.assertEqual(state["given_up"], {})

    def test_RT02_rechecked_builds_are_not_counted_as_retried(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.quiet()  # off TestGrid now, but not final yet
        self.fetch()
        run = self.runs()[-1]
        self.assertEqual((run["rechecked"], run["retried_from_state"]), (1, 0))

    def test_AB01_AB03_AB04_AB05_a_scan_timeout_keeps_retry_clocks_and_reports_what_it_left(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()  # archived, not final yet: re-checked by every run (an archived final one never is)
        other = snowflake(self.now - 2 * DAY)
        rec = {"job": "job", "build": other, "query": "bucket/logs/job", "started": int(self.now - 2 * DAY),
               "created": frl.created(other), "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        state = self.state()
        state["unresolved"] = {f"job/{other}": {"rec": rec, "first_seen": self.now - 3 * DAY, "last_error": "x"}}
        frl.write_json(os.path.join(self.root, "state.json"), state)

        def slow_summary(h):
            time.sleep(1.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                send(h, 200, {}, json.dumps({"t": {"overall_status": "FAILING"}}).encode())
        self.srv.route("/dash/summary", slow_summary)
        self.fetch("--run-timeout-minutes", "0.01")
        run, state = self.runs()[-1], self.state()
        self.assertAlmostEqual(state["unresolved"][f"job/{other}"]["first_seen"], self.now - 3 * DAY, delta=1)
        self.assertIn(f"job/{other}", [e["build"] for e in run["errors"]])
        self.assertEqual(run["outcomes"], {"not-checked": [self.key]})
        self.assertNotIn(self.key, state["unresolved"])
        self.assertNotIn(self.key, [e["build"] for e in run["errors"]])
        time.sleep(1.6)


class Store(r3.World):
    def test_RF04_a_result_that_is_not_short_clean_text_is_not_stored(self):
        start = int(self.now - 3 * DAY)
        builds = [snowflake(start - i * HOUR) for i in range(3)]
        self.show(builds, {"o": ([12, 12, 12], ["", "", ""])}, start=start)
        for b, result in zip(builds, ("x" * 100, "FAIL\x00ED", "\ud800"), strict=True):
            self.gcs_build(b, finished=(200, {}, {"result": result, "passed": False, "timestamp": start + HOUR}))
        self.assertEqual(self.fetch(), 0)
        for b in builds:
            self.assertIsNone(self.meta(b)["result"], b)

    def test_ST02_a_red_build_with_nothing_in_gcs_15_hours_after_it_started_is_recorded(self):
        start = int(self.now - 15 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"archived-without-log": [f"job/{b}"]})


class Recheck(r3.World):
    def test_RC03_a_recovered_log_is_recorded_even_if_the_podinfo_check_fails(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Failed"}'))
        self.fetch()  # no log: archived with its podinfo.json
        self.assertIsNone(self.meta()["log"])
        self.srv.route(self.base + "/build-log.txt", obj(b"late log\n"))
        self.srv.route(self.base + "/podinfo.json", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        m = self.meta()
        self.assertIsNotNone(m["log"])
        self.assertEqual((m["log"]["md5"], m["log_recovered_by_run"]),
                         (frl.file_md5(os.path.join(self.bdir(), "build-log.txt")), self.runs()[-1]["run"]))

    def test_RC18_a_recovered_log_with_a_changed_podinfo_is_reported_as_recovered(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Pending"}', "1"))
        self.fetch()
        self.srv.route(self.base + "/build-log.txt", obj(b"late log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Failed"}', "2"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"log-recovered": [self.key]})
        self.assertEqual(self.meta()["refreshed"], ["podinfo.json"])

    def test_RC20_a_copy_that_cannot_be_compared_for_two_days_is_still_compared(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        m["archived_at"] = self.now - 2 * DAY
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        self.assertNotIn("stopped-comparing", self.outcomes())
        self.assertFalse(self.meta()["final"])


class Check(r3.World):
    def missed(self, out):
        return sorted(set(re.findall(r"MISSED red build job/([0-9]+)", out)))

    def test_TG02_TG07_TG08a_TG09_TG11_TJ01_TJ02_UL02_unlisted_builds_are_judged_by_testgrids_rule(self):
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(self.now - HOUR))

        def gcs(hours, finished=None, started="created"):
            b = snowflake(self.now - hours * HOUR)
            self.gcs_build(b, finished=finished, started=started)
            return b
        stamp = int(self.now - 2 * HOUR)
        late = gcs(26)  # no finished.json, past the 24-hour deadline
        young = gcs(22)  # within it
        false_success = gcs(30, (200, {}, {"result": "SUCCESS", "passed": False, "timestamp": stamp}))
        no_time = gcs(31, (200, {}, {"result": "SUCCESS", "passed": True}))
        list_started = gcs(32, started=(200, {}, []))  # running, but started.json is no object: TOOL_FAIL
        no_start = gcs(33, started=(200, {}, {}))
        just_finished = gcs(34, (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - 600)}))
        builds = [late, young, false_success, no_time, list_started, no_start, just_finished, visible]
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in builds])
        self.assertEqual(self.fetch(), 0)  # a fresh archive: nothing to search, nothing archived
        run_folder(self.root, self.now - 50 * HOUR)  # ... although the archive ran when these were on TestGrid
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertNotIn("verification stopped", out)
        self.assertEqual(self.missed(out), sorted([late, false_success, no_time, list_started]))

    def test_OS01_a_build_older_than_the_oldest_column_with_results_is_checked(self):
        visible, ph = snowflake(self.now - HOUR), snowflake(self.now - 40 * HOUR)
        t = table("bucket/logs/job", [visible, ph], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 40 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)
        lost = snowflake(self.now - 30 * HOUR)  # not on TestGrid at all, between the two columns
        self.gcs_build(lost, finished=self.failed(ago=29 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (visible, ph, lost)])
        self.fetch()
        run_folder(self.root, self.now - 50 * HOUR)
        code, out = self.check()
        self.assertEqual(self.missed(out), [lost])

    def test_OS02_a_build_newer_than_the_oldest_column_is_not_judged_as_scrolled_off(self):
        old, new = snowflake(self.now - 4 * DAY), snowflake(self.now - 2 * HOUR)
        t = table("bucket/logs/job", [new, old], {"o": ([1, 1], ["", ""])})
        t["timestamps"] = [int((self.now - 2 * HOUR) * 1000), int((self.now - 4 * DAY) * 1000)]
        self.srv.table("dash", "t", t)
        lagging = snowflake(self.now - 3 * HOUR)  # failed 2 h ago; TestGrid has not listed it yet
        self.gcs_build(lagging, finished=self.failed(ago=2 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (old, new, lagging)])
        self.fetch()
        run_folder(self.root, self.now - 5 * DAY)
        code, out = self.check()
        self.assertEqual((code, self.missed(out)), (0, []), out)

    def test_SO06_a_red_build_without_files_is_not_missed_within_twice_its_tabs_own_margin(self):
        start = int(self.now - 15 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.srv.route(f"/bucket/logs/job/{b}/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "11h20m"}}}))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})  # the fetch waits 2 x 11.58 h for its files
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_MB02_a_red_build_that_finished_hours_ago_is_missed_even_if_prow_says_success(self):
        start = int(self.now - 8 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([1], [""])}, start=start)  # not red yet when the fetch looks
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 5 * HOUR)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json"])
        self.assertEqual(self.fetch(), 0)
        self.show([b], {"o": ([1], [""]), "test-x": ([12], ["boom"])}, start=start)  # a red cell by verification time
        code, out = self.check()
        self.assertIn(f"PROBLEM: MISSED red build job/{b}", out)
        self.assertEqual(code, 1, out)

    def test_MB07_a_red_build_that_started_long_ago_without_finished_json_is_missed(self):
        start = int(self.now - 30 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([4], [""])}, start=start)  # running when the fetch looks
        self.assertEqual(self.fetch(), 0)
        self.show([b], {"o": ([12], ["Build did not complete within 24 hours"])}, start=start)
        code, out = self.check()
        self.assertIn(f"PROBLEM: MISSED red build job/{b}", out)
        self.assertEqual(code, 1, out)

    def test_UL04_a_build_testgrid_lists_with_results_is_judged_by_its_cells_not_as_unlisted(self):
        start = int(self.now - 30 * HOUR)
        b = snowflake(start - 120)  # its pod started two minutes after the build was made
        self.show([b], {"o": ([12], ["Build did not complete within 24 hours"])}, start=start)
        self.srv.route(f"/bucket/logs/job/{b}/prowjob.json", (200, {}, {"spec": {"decoration_config": {"timeout": "16h"}}}))
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": start}))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/started.json"])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})  # the fetch waits 2 x 16.25 h for its files
        run_folder(self.root, self.now - 50 * HOUR)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_UL03_an_archived_build_that_scrolled_off_is_not_missed(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/started.json", (200, {}, {"timestamp": self.start}))
        self.fetch()
        recent = snowflake(self.now - HOUR)
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (self.build, recent)])
        run_folder(self.root, self.now - 5 * DAY)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_UL07_a_tab_without_result_columns_is_not_a_crash(self):
        p = snowflake(self.now - HOUR)
        self.show([p], {"o": ([6], ["grid exceeds maximum size"])}, start=int(self.now - HOUR))
        c = snowflake(self.now - 10 * HOUR)
        self.gcs_build(c, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (c, p)])
        self.fetch()
        run_folder(self.root, self.now - 50 * HOUR)
        code, out = self.check()
        self.assertNotIn("verification stopped", out)
        self.assertEqual(code, 0, out)

    def test_SO01_a_red_build_without_files_is_not_missed_while_the_fetch_waits_for_it(self):
        start = int(self.now - 8 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_SO04_an_old_run_report_without_tab_margins_uses_its_own_max_job_hours(self):
        start = int(self.now - 30 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        rel = frl.new_run_dir(self.root, time.time())[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       dict(run_record(time.time(), [{"dashboard": "dash", "tab": "t", "query": "bucket/logs/job",
                                                      "oldest": start}], max_job_hours=20),
                            run="r", finished=time.time()))
        code, out = self.check()
        self.assertNotIn("MISSED", out)


if __name__ == "__main__":
    unittest.main()
