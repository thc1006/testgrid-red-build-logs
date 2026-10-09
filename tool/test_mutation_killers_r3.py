"""Targeted tests from a second mutation-testing review: each kills mutants the rest of
the suite let survive. Each test name starts with the mutant id(s) it targets."""
import base64
import datetime as dt
import gzip
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import DAY, gzip_stored, run_record, send, table  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj, snowflake  # noqa: E402


def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def composite(data, stored=None):
    """A GCS object with a crc32c and no md5 (a composite upload); `stored` is its gzip form, if any."""
    raw = stored if stored is not None else data
    crc = base64.b64encode(frl.crc32c(raw).to_bytes(4, "big")).decode()

    def serve(h):
        headers = {"x-goog-stored-content-encoding": "gzip" if stored is not None else "identity",
                   "x-goog-stored-content-length": str(len(raw)), "x-goog-hash": f"crc32c={crc}"}
        body = data
        if stored is not None and "gzip" in h.headers.get("Accept-Encoding", ""):
            body, headers["Content-Encoding"] = stored, "gzip"
        send(h, 200, headers, body)
    return serve


class Unit(unittest.TestCase):
    def test_CR01_CR02_CR03_CR08_crc32c_matches_the_castagnoli_check_values(self):
        self.assertEqual(frl.crc32c(b"123456789"), 0xE3069283)
        self.assertEqual(frl.crc32c(b"\x00" * 32), 0x8A9136AA)
        self.assertEqual(frl.crc32c(b"\xff" * 32), 0x62A8AB43)
        self.assertEqual(frl.crc32c(b"6789", frl.crc32c(b"12345")), 0xE3069283)

    def test_SF01_a_finish_shortly_before_the_build_id_time_still_settles(self):
        now = time.time()
        created = int(now - 10 * HOUR)
        meta = {"created": created, "finished": created - HOUR, "archived_at": now - 5 * HOUR, "log": {"md5": "x"}}
        self.assertEqual(frl.settle_flags(meta, now, now), (True, False))

    def test_SF03_a_copy_is_not_settled_90_minutes_after_the_build_finished(self):
        now = time.time()
        meta = {"created": int(now - 5 * HOUR), "finished": now - 90 * 60, "archived_at": now, "log": {"md5": "x"}}
        self.assertFalse(frl.settle_flags(meta, now, now)[0])
        self.assertTrue(frl.settle_flags(meta, now + 31 * 60, now)[0])

    def test_LD01_LD02_an_abandoned_worker_leaves_folders_alone(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        ctx.abandoned.set()
        errors = []
        with tempfile.TemporaryDirectory() as d:
            empty = os.path.join(d, "job", "1")
            os.makedirs(empty)

            def worker():
                frl.bind_context(ctx)
                try:
                    frl.remove_build_dir(empty)
                except Exception as e:  # noqa: BLE001
                    errors.append(e)
            t = threading.Thread(target=worker)
            t.start()
            t.join()
            self.assertEqual(errors, [])
            self.assertTrue(os.path.isdir(empty))

    def test_WF01_WF02_a_lone_surrogate_is_written_visibly(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "x.md")
            frl.write_file(path, "job \udcff\n")
            self.assertEqual(slurp(path), "job \\udcff\n")

    def test_RN02_RN03_run_counts_include_every_update(self):
        run = {"outcomes": {"archived": ["a"], "archived-without-log": ["b"], "backfilled": ["c"], "repaired": ["d"],
                            "refreshed": ["e"], "log-recovered": ["f"], "already-archived": ["g"], "not-red": ["h"]},
               "errors": [{"build": None, "error": "x"}], "serious_warnings": ["w"], "tabs": [{"error": "e"}, {}]}
        self.assertEqual(frl.run_counts(run), (3, 3, 3))

    def test_RN06_RN07_warnings_are_listed_and_escaped_in_the_run_readme(self):
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            frl.write_run_readme(root, rel, {"warnings": ["tab <img src=x> | y"], "serious_warnings": []}, [])
            text = slurp(os.path.join(root, rel, "README.md"))
            self.assertIn("## Warnings", text)
            self.assertIn("- tab \\<img src=x\\> \\| y", text)

    def test_RN05_a_serious_warning_is_listed_once(self):
        lost = "dash#t: no longer listed on its dashboard"
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            frl.write_run_readme(root, rel, {"warnings": [lost, "a note"], "serious_warnings": [lost]}, [])
            text = slurp(os.path.join(root, rel, "README.md"))
            self.assertEqual(text.count("no longer listed"), 1)
            self.assertIn("- a note", text.split("## Warnings")[1])


class Migration(unittest.TestCase):
    """migrate_old_layout() on its own: logs/<job>/<build>/ and runs/<old id>.json."""
    OLD, NEW = "20261001T120000.000000Z", "2026-10-01T120000Z"

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)
        self.t = dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC).timestamp()
        self.build = snowflake(self.t - HOUR)
        os.makedirs(os.path.join(self.root, "runs"))

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def old_report(self, name=None, **fields):
        report = dict(run_record(self.t, []), run=self.OLD, outcomes={})
        report.update(fields)
        frl.write_json(self.path("runs", f"{name or self.OLD}.json"), report)

    def old_build(self, build=None, files=("build-log.txt",), changes=None, drop=()):
        build = build or self.build
        d = self.path("logs", "job", build)
        os.makedirs(d)
        for name in files:
            with open(os.path.join(d, name), "wb") as f:
                f.write(b"log\n")
        meta = {"job": "job", "build": build, "query": "bucket/logs/job", "started": self.t - HOUR,
                "created": self.t - HOUR, "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}],
                "log_url": f"https://gcs/bucket/logs/job/{build}/build-log.txt", "podinfo": None,
                "log": {"bytes": 4, "md5": frl.file_md5(os.path.join(d, "build-log.txt")) if "build-log.txt" in files
                        else None, "file": "build-log.txt", "file_bytes": 4}, "archived_by_run": self.OLD}
        meta.update(changes or {})
        for k in drop:
            meta.pop(k)
        if "meta.json" not in files:
            frl.write_json(os.path.join(d, "meta.json"), meta)
        return d

    def migrate(self):
        warnings = []
        frl.migrate_old_layout(self.root, warnings)
        return warnings

    def test_MG01_MG16_MG19_a_report_whose_run_archived_nothing_is_migrated(self):
        self.old_report()
        self.assertEqual(self.migrate(), [])
        self.assertFalse(os.path.exists(self.path("runs", f"{self.OLD}.json")))
        moved = frl.read_json(self.path("runs", "2026-10", self.NEW, "run.json"))
        self.assertEqual((moved["run"], moved["migrated_from"]), (self.NEW, self.OLD))

    def test_MG02_MG03_MG04_unusable_reports_are_reported_and_left(self):
        self.old_report("20261001T110000.000000Z")  # names another run
        self.old_report("20261001T100000.000000Z", run="20261001T100000.000000Z", started=None)
        self.old_report("20261001T090000.000000Z", run=None)
        report = frl.read_json(self.path("runs", "20261001T090000.000000Z.json"))
        del report["run"]
        frl.write_json(self.path("runs", "20261001T090000.000000Z.json"), report)
        warnings = self.migrate()
        self.assertEqual(sum("cannot be moved" in w for w in warnings), 3, warnings)
        self.assertEqual(len([n for n in os.listdir(self.path("runs")) if n.endswith(".json")]), 3)

    def test_MG05_an_old_meta_of_another_build_stays_in_place(self):
        d = self.old_build(build="12345", changes={"build": "999"})
        warnings = self.migrate()
        self.assertTrue(os.path.exists(os.path.join(d, "meta.json")))
        self.assertTrue(any("logs/job/12345 has no usable meta.json" in w for w in warnings), warnings)

    def test_MG06_an_old_meta_without_its_run_is_reported_not_a_crash(self):
        d = self.old_build(drop=["archived_by_run"])
        warnings = self.migrate()
        self.assertTrue(os.path.exists(os.path.join(d, "build-log.txt")))
        self.assertTrue(any("has no usable meta.json" in w for w in warnings), warnings)

    def test_MG07_a_log_next_to_a_corrupt_meta_is_kept(self):
        d = self.old_build(files=("build-log.txt", "meta.json"))
        with open(os.path.join(d, "meta.json"), "w") as f:
            f.write("{corrupt")
        warnings = self.migrate()
        self.assertTrue(os.path.exists(os.path.join(d, "build-log.txt")))
        self.assertTrue(any("has no usable meta.json" in w for w in warnings), warnings)

    def test_MG08_an_unfinished_old_download_is_cleared(self):
        d = self.path("logs", "job", self.build)
        os.makedirs(d)
        open(os.path.join(d, "build-log.txt.part"), "w").close()
        self.assertEqual(self.migrate(), [])
        self.assertFalse(os.path.exists(self.path("logs")))

    def test_MG09_a_build_whose_old_run_left_no_report_is_dated_by_its_id(self):
        self.old_build()
        self.assertEqual(self.migrate(), [])
        self.assertTrue(os.path.exists(self.path("runs", "2026-10", self.NEW, "job", self.build, "build-log.txt")))

    def test_MG12_an_occupied_destination_is_reported_not_a_crash(self):
        os.makedirs(self.path("runs", "2026-10", self.NEW))  # migrated before; a late old run added a build
        frl.write_json(self.path("runs", "2026-10", self.NEW, "run.json"),
                       dict(run_record(self.t, []), run=self.NEW, migrated_from=self.OLD))
        d = self.old_build()
        dest = self.path("runs", "2026-10", self.NEW, "job", self.build)
        os.makedirs(dest)
        open(os.path.join(dest, "notes.txt"), "w").close()
        warnings = self.migrate()
        self.assertTrue(os.path.exists(os.path.join(d, "meta.json")))
        self.assertTrue(any("is also archived at" in w for w in warnings), warnings)

    def test_MG13_MG14_a_moved_build_keeps_its_times_and_run_links(self):
        self.old_report()
        self.old_build(changes={"repaired_by_run": self.OLD, "log_recovered_by_run": self.OLD})
        self.assertEqual(self.migrate(), [])
        m = frl.read_json(self.path("runs", "2026-10", self.NEW, "job", self.build, "meta.json"))
        self.assertEqual(m["archived_at"], self.t)
        self.assertEqual((m["repaired_by_run"], m["log_recovered_by_run"]), (self.NEW, self.NEW))

    def test_MG17_leftovers_in_logs_are_reported(self):
        self.old_build()
        with open(self.path("logs", "notes.txt"), "w") as f:
            f.write("x")
        warnings = self.migrate()
        self.assertTrue(any("some of logs/ could not be moved" in w for w in warnings), warnings)


class World(ArchiveWorld):
    def setUp(self):
        super().setUp()
        self.now = time.time()

    def quiet(self):
        """TestGrid shows one passing build and nothing red."""
        self.show([snowflake(self.now)], {"o": ([1], [""])}, start=int(self.now))

    def state(self):
        return frl.read_json(os.path.join(self.root, "state.json"))

    def put_state(self, **parts):
        frl.write_json(os.path.join(self.root, "state.json"), parts)

    def outcomes(self):
        return self.runs()[-1]["outcomes"]

    def tab(self):
        (tab,) = [t for t in self.runs()[-1]["tabs"] if t.get("tab") == "t"]
        return tab

    def watched(self, build):
        """A state.json watch list holding one build a history gap showed still running."""
        rec = {"job": "job", "build": build, "query": "bucket/logs/job", "started": None, "created": frl.created(build),
               "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}], "watch": True}
        return {f"job/{build}": {"first_seen": self.now - HOUR, "rec": rec}}

    def gcs_build(self, b, finished=None, started="created", log=True):
        if finished is not None:
            self.srv.route(f"/bucket/logs/job/{b}/finished.json", finished)
        if started == "created":  # it started when its ID was made
            started = (200, {}, {"timestamp": frl.created(b)})
        if started is not None:
            self.srv.route(f"/bucket/logs/job/{b}/started.json", started)
        if log:
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log " + b.encode()))

    def failed(self, ago=HOUR, **extra):
        return (200, {}, dict({"result": "FAILURE", "passed": False, "timestamp": int(self.now - ago)}, **extra))


class Gap(World):
    """TestGrid shows one passing build from an hour ago; the last scan was 40 h ago."""

    def setUp(self):
        super().setUp()
        self.visible = snowflake(self.now - HOUR)
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})

    def listing(self, *builds):
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (*builds, self.visible)])

    def test_PR03_a_finished_json_without_passed_is_red(self):
        b = snowflake(self.now - 30 * HOUR)
        self.gcs_build(b, finished=(200, {}, {"result": "FAILURE", "timestamp": int(self.now - HOUR)}))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{b}"]})

    def test_HS01_HS03_an_unreadable_started_json_still_means_started(self):
        b = snowflake(self.now - 30 * HOUR)
        self.gcs_build(b, finished=self.failed(), started=(200, {}, b"{not json"))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{b}"]})

    def test_BW01_a_passed_gap_build_needs_no_started_json(self):
        b = snowflake(self.now - 30 * HOUR)
        self.gcs_build(b, finished=(200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - HOUR)}),
                       started=(500, {}, b""))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{b}/started.json"), 0)

    def test_BW02_a_gap_build_whose_finished_json_has_no_time_is_watched(self):
        b = snowflake(self.now - 10 * HOUR)  # started 10 h ago: within TestGrid's 24-hour deadline
        self.gcs_build(b, finished=(200, {}, {"result": "FAILURE", "passed": False}))
        self.listing(b)
        self.fetch()
        self.assertIn(f"job/{b}", self.state()["watch"])

    def test_BW04_a_running_build_in_two_gap_tabs_keeps_both_tabs(self):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        self.srv.table("dash", "t2", table("bucket/logs/job", [self.visible], {"o": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap})
        b = snowflake(self.now - 10 * HOUR)  # started 10 h ago: within TestGrid's 24-hour deadline
        self.gcs_build(b)  # no finished.json: still running
        self.listing(b)
        self.fetch()
        self.assertEqual(sorted(t["tab"] for t in self.state()["watch"][f"job/{b}"]["rec"]["tabs"]), ["t", "t2"])

    def test_RW04_a_watched_build_found_again_in_a_gap_is_queued_once(self):
        b = snowflake(self.now - 10 * HOUR)  # started 10 h ago: within TestGrid's 24-hour deadline
        self.gcs_build(b)  # still running
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       watch=self.watched(b))
        self.listing(b)
        self.fetch()
        self.assertEqual(self.outcomes(), {"watching": [f"job/{b}"]})

    def test_BW06_a_gap_build_already_queued_for_retry_is_looked_up_once(self):
        b = snowflake(self.now - 30 * HOUR)
        self.gcs_build(b, finished=self.failed())  # started, so a second lookup would find it red
        rec = {"job": "job", "build": b, "query": "bucket/logs/job", "started": int(self.now - 30 * HOUR),
               "created": frl.created(b), "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       unresolved={f"job/{b}": {"rec": rec, "first_seen": self.now - DAY, "last_error": "x"}})
        self.listing(b)
        self.fetch()
        self.assertEqual(self.outcomes(), {"archived": [f"job/{b}"]})

    def test_RW11_newly_watched_builds_are_not_counted_as_retried(self):
        b = snowflake(self.now - 10 * HOUR)  # started 10 h ago: within TestGrid's 24-hour deadline
        self.gcs_build(b)
        self.listing(b)
        self.fetch()
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["retried_from_state"]), ({"watching": [f"job/{b}"]}, 0))


class Watch(World):
    """A build a history gap showed still running is in state.json's watch list; TestGrid shows nothing red."""

    def setUp(self):
        super().setUp()
        self.quiet()

    def watch(self, ago, finished=None, started="created"):
        b = snowflake(self.now - ago)
        self.gcs_build(b, finished=finished, started=started)
        self.put_state(watch=self.watched(b))
        return f"job/{b}"

    def test_AW01_a_long_job_is_watched_past_14_hours(self):
        key = self.watch(20 * HOUR)
        self.fetch()
        self.assertEqual(self.outcomes(), {"watching": [key]})

    def test_AW02_watching_stops_after_give_up_days(self):
        key = self.watch(20 * DAY, started=(200, {}, {}))  # no start time: TestGrid's deadline cannot apply
        self.fetch()
        self.assertEqual(self.outcomes(), {"stopped-watching": [key]})
        self.assertEqual(self.state()["watch"], {})
        run = self.runs()[-1]
        self.assertTrue(any(key in w and "no longer watched" in w for w in run["warnings"]), run["warnings"])
        self.assertEqual(run["serious_warnings"], [])  # nothing red was lost

    def test_AW03_a_finished_json_without_a_time_keeps_it_watched(self):
        key = self.watch(5 * HOUR, finished=(200, {}, {"result": "FAILURE", "passed": False}))
        self.fetch()
        self.assertEqual(self.outcomes(), {"watching": [key]})

    def test_AW05_a_watched_build_that_never_started_is_not_red(self):
        key = self.watch(5 * HOUR, finished=self.failed(), started=None)
        self.fetch()
        self.assertEqual(self.outcomes(), {"not-red": [key]})

    def test_RW06_a_failed_lookup_keeps_a_watched_build_watched(self):
        key = self.watch(5 * HOUR, finished=(500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        self.assertIn(key, self.state()["watch"])
        self.assertNotIn(key, self.state()["unresolved"])

    def test_RW07_a_scan_timeout_keeps_watched_builds_watched(self):
        key = self.watch(5 * HOUR)

        def slow_summary(h):
            time.sleep(1.5)
            try:
                send(h, 200, {}, json.dumps({"t": {"overall_status": "FAILING"}}).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass
        self.srv.route("/dash/summary", slow_summary)
        self.fetch("--run-timeout-minutes", "0.01")
        self.assertIn(key, self.state()["watch"])
        self.assertNotIn(key, self.state()["unresolved"])
        time.sleep(1.6)

    def test_RW01_a_watched_build_now_red_on_testgrid_is_archived_once(self):
        key = self.watch(5 * HOUR, finished=self.failed())
        b = key.split("/")[1]
        self.show([b], {"o": ([12], ["boom"])}, start=int(self.now - 5 * HOUR))
        self.fetch()
        self.assertEqual(sum(v.count(key) for v in self.outcomes().values()), 1, self.outcomes())

    def test_RW03_a_build_both_unresolved_and_watched_is_queued_once(self):
        key = self.watch(5 * HOUR, finished=self.failed())
        state = self.state() or {}
        frl.write_json(os.path.join(self.root, "state.json"), dict(state, unresolved={key: dict(
            state["watch"][key], rec={k: v for k, v in state["watch"][key]["rec"].items() if k != "watch"})}))
        self.fetch()
        self.assertEqual(sum(v.count(key) for v in self.outcomes().values()), 1, self.outcomes())

    def test_RW10_a_malformed_watch_list_is_reported(self):
        self.put_state(watch=[])
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(any("watch is a list" in w for w in self.runs()[-1]["serious_warnings"]))


class Margins(World):
    def prowjob(self, build, timeout, delay=0):
        def serve(h):
            time.sleep(delay)
            try:
                send(h, 200, {}, json.dumps({"spec": {"decoration_config": {"timeout": timeout}}}).encode())
            except (BrokenPipeError, ConnectionResetError):
                pass
        self.srv.route(f"/bucket/logs/job/{build}/prowjob.json", serve)

    def test_MJ01_a_failed_prowjob_read_keeps_the_saved_margin(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.prowjob(self.build, "11h20m")
        self.fetch()
        self.srv.route(self.base + "/prowjob.json", (503, {}, b""))
        self.fetch()
        self.assertAlmostEqual(self.state()["tabs"]["dash#t"]["current_job_hours"], 11.58, places=2)
        self.assertAlmostEqual(self.tab()["max_job_hours"], 11.58, places=2)

    def test_MJ05_the_gap_floor_uses_the_tabs_own_margin(self):
        self.show([self.build], {"o": ([1], [""])}, start=int(self.now - 20 * HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 10 * HOUR, "oldest": self.now - 100 * HOUR,
                                        "current_job_hours": 20,
                                        "job_hours": {"20": self.now + 10 * HOUR}}})
        self.fetch()
        self.assertTrue(any("dash#t: TestGrid only shows builds since" in w for w in self.runs()[-1]["warnings"]))

    def test_MJ06_since_hours_keeps_builds_a_long_job_was_running_at_the_last_scan(self):
        start = int(self.now - 25 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.gcs_build(b, finished=self.failed(ago=24 * HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 12 * HOUR, "oldest": self.now - 30 * HOUR,
                                        "current_job_hours": 20,
                                        "job_hours": {"20": self.now + 10 * HOUR}}})
        self.fetch("--since-hours", "12")
        self.assertIn(f"job/{b}", self.outcomes().get("archived", []))

    def test_MJ07_a_long_job_without_uploads_is_pending(self):
        start = int(self.now - 15 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.prowjob(b, "20h")
        self.fetch()
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})

    def test_MJ08_an_absurd_saved_margin_is_ignored(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 5 * DAY, "current_job_hours": 1000,
                                        "job_hours": {"1000": self.now + DAY}}})
        self.fetch()
        self.assertEqual(self.tab()["max_job_hours"], 6)

    def test_MJ09_MJ10_a_slow_prowjob_json_is_not_waited_for(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.prowjob(self.build, "11h", delay=2)
        with mock.patch.object(frl, "PROWJOB_WAIT", 0.2):
            self.fetch()
        self.assertEqual(self.tab()["max_job_hours"], 6)

    def test_MJ11_the_margin_comes_from_the_newest_build(self):
        newer = snowflake(self.start + HOUR)
        self.show([newer, self.build], {"o": ([1, 1], ["", ""])}, start=self.start + HOUR)
        self.prowjob(newer, "11h")
        self.prowjob(self.build, "1h")
        self.fetch()
        self.assertAlmostEqual(self.tab()["max_job_hours"], 11.25, places=2)


class NoLog(World):
    def test_AN02_AN03_a_young_finished_build_without_a_log_is_recorded_without_a_note(self):
        start = int(self.now - 2 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        self.gcs_build(b, finished=self.failed(), log=False)
        self.fetch()
        self.assertEqual(self.outcomes(), {"archived-without-log": [f"job/{b}"]})
        self.assertNotIn("note", self.meta(b))

    def test_RF02_RF03_an_unreadable_finished_json_means_the_build_is_over(self):
        for body in (b"{not json", b"[]"):
            with self.subTest(body):
                shutil.rmtree(os.path.join(self.root, "runs"), ignore_errors=True)
                start = int(self.now - HOUR)
                b = snowflake(start)
                self.show([b], {"o": ([12], [""])}, start=start)
                self.gcs_build(b, finished=(200, {}, body), log=False)
                self.fetch()
                self.assertEqual(self.outcomes(), {"archived-without-log": [f"job/{b}"]})


class Recheck(World):
    def test_RC01_RC04_a_refetched_log_is_recorded_even_if_the_podinfo_check_fails(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Failed"}'))
        self.fetch()  # no log: archived with its podinfo.json
        self.srv.route(self.base + "/build-log.txt", obj(b"first\n", "1"))
        self.fetch()  # the log appears
        self.assertTrue(self.meta()["log"] and self.meta()["podinfo"])
        self.srv.route(self.base + "/build-log.txt", obj(b"second, longer\n", "2"))
        self.srv.route(self.base + "/podinfo.json", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        m = self.meta()
        self.assertEqual(frl.file_md5(os.path.join(self.bdir(), "build-log.txt")), m["log"]["md5"])
        self.assertEqual((m.get("refreshed"), m.get("refreshed_by_run")), (["build-log.txt"], self.runs()[-1]["run"]))

    def test_RC02_a_refetch_of_the_same_log_is_not_a_refresh(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", gzip_stored(b"evidence\n"))
        self.fetch()
        m = self.meta()
        m["log"].pop("gcs")  # as written before identities were kept
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.fetch()
        self.assertEqual(self.outcomes(), {"already-archived": [self.key]})
        self.assertEqual(self.meta()["log"]["gcs"]["encoding"], "gzip")

    def test_RC03_a_refetch_of_the_same_podinfo_is_not_a_refresh(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", gzip_stored(b'{"phase": "Failed"}'))
        self.fetch()
        m = self.meta()
        m["podinfo"].pop("gcs")
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.fetch()
        self.assertEqual(self.outcomes(), {"already-archived": [self.key]})

    def test_AN06_a_repair_keeps_the_backfilled_label(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        m["backfilled"] = "taken from GCS"
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        with open(os.path.join(self.bdir(), "build-log.txt"), "wb") as f:
            f.write(b"XXX\n")
        self.fetch()  # TestGrid still shows it: the record comes from TestGrid, without the label
        self.assertEqual(self.outcomes(), {"repaired": [self.key]})
        self.assertEqual(self.meta().get("backfilled"), "taken from GCS")


class Crc(World):
    def test_CR04_a_gzip_stored_object_without_md5_is_checked_by_the_crc32c_of_its_stored_bytes(self):
        self.finished(ago=3 * DAY)
        content = b"composite gzip log\n" * 50
        self.srv.route(self.base + "/build-log.txt", composite(content, gzip.compress(content)))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["log"]["verified"])
        self.assertEqual(self.read("build-log.txt"), content)

    def test_CR09_a_large_object_without_md5_is_checked_across_chunks(self):
        self.finished(ago=3 * DAY)
        data = os.urandom(300_000)
        self.srv.route(self.base + "/build-log.txt", composite(data))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("build-log.txt"), data)


class GivenUp(World):
    def given_up(self, days):
        b = snowflake(self.now - days * DAY)
        self.quiet()
        rec = {"job": "job", "build": b, "query": "bucket/logs/job", "started": None, "created": frl.created(b),
               "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        self.put_state(given_up={f"job/{b}": {"rec": rec, "first_seen": self.now - days * DAY, "last_error": "x"}})
        return f"job/{b}"

    def test_GU01_a_given_up_build_stays_listed_for_30_days_after_giving_up(self):
        key = self.given_up(35)
        self.fetch()
        self.assertIn(key, self.state()["given_up"])

    def test_GU02_a_given_up_build_is_dropped_after_that(self):
        key = self.given_up(50)
        self.fetch()
        self.assertNotIn(key, self.state()["given_up"])


class Check(World):
    def archived(self, ago=3 * DAY, **fetch_args):
        self.finished(ago=ago)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.fetch(*fetch_args.get("args", ()))

    def test_CC02_same_size_damage_to_an_unsettled_copy_is_a_problem(self):
        self.archived(ago=60)
        self.assertFalse(self.meta()["settled"])
        with open(os.path.join(self.bdir(), "build-log.txt"), "wb") as f:
            f.write(b"XXX\n")
        code, out = self.check()
        self.assertEqual(code, 1, out)

    def test_CC03_a_regzipped_unsettled_copy_is_a_problem(self):
        self.archived(ago=60, args=("--gzip",))
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "wb") as f:
            f.write(gzip.compress(b"log\n", compresslevel=1, mtime=0))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("bytes on disk", out)

    def test_CC04_an_unsettled_copy_with_a_bad_file_name_is_a_problem(self):
        self.archived(ago=60)
        m = self.meta()
        m["log"]["file"] = "notes.txt"
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        code, out = self.check()
        self.assertEqual(code, 1, out)

    def test_CC05_a_final_copy_that_never_settled_must_still_match_gcs(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.fetch()  # no finished.json: never settles
        m = self.meta()
        m["final"] = True  # 14 days later
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.srv.route(self.base + "/build-log.txt", obj(b"changed\n", "2"))
        code, out = self.check()
        self.assertEqual(code, 1, out)

    def test_CC07_a_leftover_logs_folder_is_a_problem(self):
        self.archived()
        os.makedirs(os.path.join(self.root, "logs", "job"))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("old-layout logs/", out)

    def unlisted(self, started):
        """A failed build created 30 h ago that TestGrid no longer lists and the archive lacks:
        the tab now shows only builds from the last hour."""
        recent = snowflake(self.now - HOUR)
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", self.failed(ago=29 * HOUR))
        if started is not None:
            self.srv.route(f"/bucket/logs/job/{b}/started.json", started)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.build, b, recent)])
        return b

    def earlier_run(self, started):
        name = dt.datetime.fromtimestamp(started, dt.UTC).strftime("%Y-%m-%dT%H%M%SZ")
        os.makedirs(os.path.join(self.root, "runs", name[:7], name), exist_ok=True)
        return os.path.join(self.root, "runs", name[:7], name, "run.json")

    def test_CC08_CC10_CC11_a_red_build_gone_from_testgrid_since_the_first_run_is_missed(self):
        self.archived()
        b = self.unlisted(started=(200, {}, {}))
        frl.write_json(self.earlier_run(self.now - 50 * HOUR), dict(run_record(self.now - 50 * HOUR, []), run="old"))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn(f"MISSED red build job/{b}: TestGrid no longer lists it", out)

    def test_CC09_CC15_a_build_gone_before_the_first_run_is_only_noted(self):
        self.archived()
        b = self.unlisted(started=(200, {}, {"timestamp": 1}))
        # a later report whose start is not a time: neither the run to verify nor when the archive began
        frl.write_json(self.earlier_run(self.now + 60), {"run": "odd", "started": True})
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn(f"job/{b}: FAILURE, not listed on TestGrid", out)

    def test_CC12_an_unreachable_started_json_is_not_called_a_miss(self):
        self.archived()
        b = self.unlisted(started=(500, {}, b""))
        frl.write_json(self.earlier_run(self.now - 50 * HOUR), dict(run_record(self.now - 50 * HOUR, []), run="old"))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn(f"cannot check job/{b}: started.json", out)
        self.assertNotIn("MISSED", out)

    def test_CC14_a_run_json_that_is_not_an_object_is_skipped(self):
        self.archived()
        frl.write_json(self.earlier_run(self.now - 50 * HOUR), ["not a report"])
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_CC16_the_latest_run_is_verified(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.assertEqual(self.fetch(), 1)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_CC17_a_long_job_still_running_is_not_missed(self):
        start = int(self.now) - 10 * HOUR
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)
        rel = frl.new_run_dir(self.root, time.time())[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       dict(run_record(time.time(), [{"dashboard": "dash", "tab": "t", "query": "bucket/logs/job",
                                                      "oldest": start, "max_job_hours": 11.6}]),
                            run="r", finished=time.time()))
        code, out = self.check()
        self.assertNotIn("MISSED", out)


if __name__ == "__main__":
    unittest.main()
