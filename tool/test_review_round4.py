"""Regression tests for the round-4 review: each failed before its fix."""
import concurrent.futures as cf
import json
import os
import sys
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r3 as r3  # noqa: E402  (a module import: its tests are not collected again here)
from test_fetch_red_logs import DAY, run_record, send, table  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj, snowflake  # noqa: E402
from test_mutation_killers_r4 import run_folder  # noqa: E402


def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


class World(ArchiveWorld):
    def setUp(self):
        super().setUp()
        self.now = time.time()

    def state(self):
        return frl.read_json(os.path.join(self.root, "state.json"))

    def put_state(self, **parts):
        frl.write_json(os.path.join(self.root, "state.json"), parts)

    def earlier_run(self, started):
        """A run folder with its report from `started` (the archive began then, if it is the first)."""
        name = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime(started))
        d = os.path.join(self.root, "runs", name[:7], name)
        os.makedirs(d, exist_ok=True)
        frl.write_json(os.path.join(d, "run.json"), dict(run_record(started, []), run=name, dir=f"runs/{name[:7]}/{name}"))
        return d

    def failed_in_gcs(self, b, ago):
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - ago + HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - ago)}))
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log of " + b.encode()))

    def gap_world(self):
        """TestGrid shows one passing build from an hour ago; GCS has builds that failed 30 h and 5 days ago."""
        self.visible = snowflake(self.now - HOUR)
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.red30h, self.red5d = snowflake(self.now - 30 * HOUR), snowflake(self.now - 5 * DAY)
        self.failed_in_gcs(self.red30h, 30 * HOUR)
        self.failed_in_gcs(self.red5d, 5 * DAY)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (self.red30h, self.red5d, self.visible)])

    def listing_fails(self):
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = lambda h: send(h, 503, {}, b"")

    def watched(self, build):
        return {"job": "job", "build": build, "query": "bucket/logs/job", "started": None, "created": frl.created(build),
                "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}], "watch": True}


class GapSearchRetryTest(World):
    def test_a_failed_first_scan_search_is_retried_until_it_succeeds(self):
        self.earlier_run(self.now - 20 * DAY)  # the archive began 20 days ago
        self.put_state(tabs={})  # the tab was never scanned (new on the dashboard)
        self.gap_world()
        self.listing_fails()
        self.assertEqual(self.fetch(), 1)
        self.assertIn("the next run tries again", " ".join(self.runs()[-1]["warnings"]))
        self.assertNotIn("dash#t", self.state()["tabs"])  # not marked as scanned
        self.gap_world()  # GCS heals
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(sorted(self.runs()[-1]["outcomes"]["backfilled"]),
                         sorted([f"job/{self.red30h}", f"job/{self.red5d}"]))
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_a_failed_search_of_a_gap_older_than_14_days_is_retried_too(self):
        self.earlier_run(self.now - 30 * DAY)
        scan = self.now - 20 * DAY
        self.put_state(tabs={"dash#t": {"scan": scan, "oldest": self.now - 25 * DAY}})
        self.gap_world()
        self.listing_fails()
        self.assertEqual(self.fetch(), 1)
        self.assertEqual(self.state()["tabs"]["dash#t"]["scan"], scan)  # kept, so the gap is searched again
        self.gap_world()
        self.assertEqual(self.fetch(), 1)  # what lies before the 14-day horizon may still be missing
        self.assertEqual(sorted(self.runs()[-1]["outcomes"]["backfilled"]),
                         sorted([f"job/{self.red30h}", f"job/{self.red5d}"]))
        self.assertEqual(self.fetch(), 0)


    def test_one_unreadable_build_does_not_hold_back_the_rest_of_its_gap(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 6 * DAY, "oldest": self.now - 7 * DAY}})
        self.gap_world()
        self.srv.route(f"/bucket/logs/job/{self.red5d}/finished.json", (503, {}, b""))
        self.assertEqual(self.fetch(), 1)  # looked at twice in this run, and failed: an error
        self.assertEqual(self.runs()[-1]["outcomes"].get("backfilled"), [f"job/{self.red30h}"])
        self.assertIn(f"job/{self.red5d}", self.state()["watch"])  # looked at again next run
        self.failed_in_gcs(self.red5d, 5 * DAY)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"].get("backfilled"), [f"job/{self.red5d}"])


class WatchAgeTest(World):
    def setUp(self):
        super().setUp()
        self.show([snowflake(self.now)], {"o": ([1], [""])}, start=int(self.now))  # nothing red

    def test_a_watched_build_that_cannot_be_read_is_dropped_after_14_days(self):
        b = snowflake(self.now - 40 * DAY)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (403, {}, b"denied"))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - 40 * DAY}})
        self.assertEqual(self.fetch(), 1)
        errors = [e["error"] for e in self.runs()[-1]["errors"]]
        self.assertTrue(any("no longer watched after 14 days" in e for e in errors), errors)
        self.assertEqual(self.state()["watch"], {})
        self.assertEqual(self.fetch(), 0)

    def test_a_younger_one_stays_watched(self):
        b = snowflake(self.now - 3 * DAY)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (403, {}, b"denied"))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - 3 * DAY}})
        self.assertEqual(self.fetch(), 1)
        self.assertIn(f"job/{b}", self.state()["watch"])


class DamagedReportTest(World):
    def first_run(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        return os.path.join(self.root, frl.run_dirs(self.root)[-1], "run.json")

    def test_fields_of_the_wrong_type_in_a_run_report_are_skipped(self):
        path = self.first_run()
        frl.write_json(path, dict(frl.read_json(path), outcomes={"refreshed": [{"job": "job"}, ["x"]]},
                                  red_builds_seen="<b>bold</b>"))
        self.assertEqual(self.fetch(), 0)
        readme = slurp(os.path.join(os.path.dirname(path), "README.md"))
        self.assertNotIn("<b>", readme)

    def test_a_lone_surrogate_in_a_run_report_or_meta_json_does_not_stop_the_index(self):
        path = self.first_run()
        report = frl.read_json(path)
        report["tabs"] = [dict(report["tabs"][0], dashboard="dash\ud800")]
        with open(path, "w", encoding="ascii") as f:  # stored as the JSON escape \ud800
            json.dump(report, f)
        m = self.meta()
        m["tabs"][0]["tab"] = "t\ud800"
        with open(os.path.join(self.bdir(), "meta.json"), "w", encoding="ascii") as f:
            json.dump(m, f)
        before = slurp(os.path.join(self.root, "INDEX.md"))
        time.sleep(1.1)  # INDEX.md says when it was written, to the second
        self.assertEqual(self.fetch(), 0)
        self.assertNotEqual(slurp(os.path.join(self.root, "INDEX.md")), before)


class TimeEdgeTest(World):
    def test_times_testgrid_reads_as_zero_agree_between_fetch_and_check(self):
        self.earlier_run(self.now - 50 * HOUR)
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        zero_finish, zero_start = snowflake(self.now - 10 * HOUR), snowflake(self.now - 31 * HOUR)
        self.srv.route(f"/bucket/logs/job/{zero_finish}/started.json", (200, {}, {"timestamp": int(self.now - 10 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{zero_finish}/finished.json",
                       (200, {}, {"timestamp": 0, "passed": False, "result": "FAILURE"}))
        self.srv.route(f"/bucket/logs/job/{zero_start}/started.json", (200, {}, {"timestamp": 0}))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (zero_finish, zero_start, visible)])
        self.assertEqual(self.fetch(), 0)
        outcomes = self.runs()[-1]["outcomes"]
        self.assertEqual(outcomes.get("backfilled"), [f"job/{zero_finish}"])  # TestGrid: FAIL at once
        self.assertEqual(outcomes.get("watching"), [f"job/{zero_start}"])  # TestGrid: running, never red
        code, out = self.check()
        self.assertEqual(code, 0, out)


class PlaceholderTest(World):
    def test_a_red_build_left_only_in_a_placeholder_column_is_still_reported_missed(self):
        self.earlier_run(self.now - 50 * HOUR)
        recent, lost = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(lost, 30 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (lost, recent)])
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        # the last scan was an hour ago, so this run does not search back to `lost`: as if an earlier run lost it
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR}})
        self.fetch()
        self.assertIsNone(self.bdir(lost))
        t = table("bucket/logs/job", [recent, lost], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 30 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn(f"MISSED red build job/{lost}", out)


class MarginPerJobTest(World):
    def test_cross_check_takes_the_longest_margin_of_a_jobs_tabs(self):
        start = int(self.now - 15 * HOUR)
        b = snowflake(start)
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        for tab in ("t", "t2"):
            self.srv.table("dash", tab, table("bucket/logs/job", [b], {"o": ([12], [""])}, start=start * 1000))
        # t knows its job can run 20 hours; t2 is new, and prowjob.json cannot be read
        self.put_state(tabs={"dash#t": {"scan": self.now - 12 * HOUR, "oldest": self.now - 100 * HOUR,
                                        "current_job_hours": 20, "job_hours": {"20": self.now + 10 * HOUR}}})
        self.earlier_run(self.now - 60 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/started.json"])
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"pending": [f"job/{b}"]})  # no log yet, within 2 x 20 h
        code, out = self.check()
        self.assertNotIn("MISSED", out)
        self.assertEqual(code, 0, out)

    def test_a_build_retried_while_its_tab_cannot_be_read_keeps_the_tabs_known_margin(self):
        b = snowflake(self.now - 15 * HOUR)
        rec = {"job": "job", "build": b, "query": "bucket/logs/job", "started": int(self.now - 15 * HOUR),
               "created": frl.created(b), "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}]}
        self.put_state(tabs={"dash#t": {"scan": self.now - 12 * HOUR, "oldest": self.now - 100 * HOUR,
                                        "current_job_hours": 20, "job_hours": {"20": self.now + 10 * HOUR}}},
                       unresolved={f"job/{b}": {"rec": rec, "first_seen": self.now - 12 * HOUR, "last_error": None}})
        self.srv.table("dash", "t", lambda h: send(h, 500, {}, b""))
        self.assertEqual(self.fetch(), 1)  # the tab failed
        self.assertEqual(self.runs()[-1]["outcomes"], {"pending": [f"job/{b}"]})  # not recorded without a log yet


class MigrationResumeTest(World):
    def test_a_migration_cut_off_before_a_move_finishes_it_next_run(self):
        import hashlib
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        old_id = "20261001T120000.000000Z"
        d = os.path.join(self.root, "logs", "job", self.build)
        os.makedirs(d)
        os.makedirs(os.path.join(self.root, "runs"))
        with open(os.path.join(d, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        frl.write_json(os.path.join(d, "meta.json"), {
            "job": "job", "build": self.build, "query": "bucket/logs/job", "started": self.start, "created": self.start,
            "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}],
            "result": "FAILURE", "finished": self.start + 600, "log_url": f"{self.srv.url}{self.base}/build-log.txt",
            "log": {"bytes": 4, "md5": hashlib.md5(b"log\n").hexdigest(), "verified": True, "file": "build-log.txt",
                    "file_bytes": 4}, "podinfo": None, "archived_by_run": old_id})
        real = os.rename

        def cut_off(src, dst):
            if src == d:
                raise KeyboardInterrupt  # killed after meta.json was rewritten, before the move
            return real(src, dst)
        with mock.patch.object(os, "rename", side_effect=cut_off):
            self.assertEqual(self.fetch(), 130)
        self.assertEqual(self.fetch(), 0)
        m = self.meta()
        folder = os.path.basename(os.path.dirname(os.path.dirname(self.bdir())))
        self.assertEqual(m["archived_by_run"], folder)
        self.assertEqual(m["archived_at"], frl.folder_time(folder))
        self.assertFalse(os.path.exists(os.path.join(self.root, "logs")))


class JobFolderTest(World):
    def test_a_worker_never_removes_the_job_folder_another_worker_may_use(self):
        job = os.path.join(self.root, "runs", "2026-10", "2026-10-09T000000Z", "job")
        a, b = os.path.join(job, "1"), os.path.join(job, "2")
        os.makedirs(a)
        real_mkdir = os.mkdir

        def mkdir(path, mode=0o777):
            if path == b:  # worker A's cleanup lands while worker B makes its folder
                frl.remove_build_dir(a)
            return real_mkdir(path, mode)
        with mock.patch.object(os, "mkdir", side_effect=mkdir):
            os.makedirs(b, exist_ok=True)
        self.assertTrue(os.path.isdir(b))

    def test_the_next_run_removes_an_empty_job_folder(self):
        job = os.path.join(self.root, "runs", "2026-10", "2026-10-09T000000Z", "job")
        os.makedirs(job)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertFalse(os.path.exists(job))


class LatestRunTest(World):
    def test_a_future_dated_report_does_not_pose_as_the_latest_run(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))  # the real run cannot archive the red build
        self.assertEqual(self.fetch(), 1)
        self.earlier_run(self.now + 30 * DAY)  # damage, or a runner with a wrong clock
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("the run reported an error", out)


class BorrowedStartTest(World):
    def test_a_column_testgrid_dates_by_an_earlier_one_is_archived_not_rejected(self):
        ok, failed = snowflake(self.now - 2 * HOUR), snowflake(self.now - 3 * HOUR)
        t = table("bucket/logs/job", [ok, failed], {"o": ([1, 14], ["", "Failed to download"])})
        # TestGrid could not read `failed`: TOOL_FAIL, dated by an earlier column
        t["timestamps"] = [int((self.now - 2 * HOUR) * 1000), int((self.now - 5 * DAY) * 1000)]
        self.srv.table("dash", "t", t)
        self.srv.route(f"/bucket/logs/job/{failed}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - 2 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{failed}/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"].get("archived"), [f"job/{failed}"])
        self.assertIsNone(self.meta(failed)["started"])  # dated by its build ID instead
        code, out = self.check()
        self.assertEqual(code, 0, out)


class SharedGapBuildTest(World):
    def test_a_build_in_two_tabs_gaps_is_listed_under_both(self):
        red, bad = snowflake(self.now - 30 * HOUR), snowflake(self.now - 40 * HOUR)
        self.failed_in_gcs(red, 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{bad}/finished.json", (403, {}, b""))
        tab = {"query": "bucket/logs/job", "dashboard": "dash", "oldest": self.now - HOUR, "status": "FAILING"}
        gaps = [("dash#t", dict(tab, tab="t"), self.now - 50 * HOUR), ("dash#t2", dict(tab, tab="t2"), self.now - 32 * HOUR)]

        def listing(gcs, query, lo, hi):  # GCS lists by name, which need not be by age
            return [b for b in (red, bad) if lo <= frl.created(b) <= hi]
        with mock.patch.object(frl, "list_build_ids", side_effect=listing), cf.ThreadPoolExecutor(4) as pool:
            out = frl.backfill_gaps(gaps, self.srv.url, pool, time.monotonic() + 60, {}, set())
        self.assertEqual([r["build"] for r in out["dash#t"]], [red])  # its search goes on without `bad`
        self.assertEqual([r["build"] for r in out["dash#t2"]], [red])
        self.assertEqual([w["build"] for w in out["watch"]], [bad])  # looked at again until it can be read



# Repros of the defects the round-4 mutation review found (each failed before its fix)
class MutationReviewRules(unittest.TestCase):
    def test_D4_cross_check_treats_a_zero_start_time_unlike_a_missing_one(self):
        """Go decodes a missing started.json timestamp as 0, so TestGrid cannot tell the two
        apart. fetch_red_logs treats both as "no start time" (not red, watched); cross_check
        treats a missing one the same way but takes 0 (or a negative time) as a real start in
        1970, so the build is red by its 24-hour deadline and can be reported as MISSED."""
        now = time.time()
        self.assertEqual(cc.testgrid_red(None, {"timestamp": 0}, now), cc.testgrid_red(None, {}, now))
        self.assertEqual(cc.testgrid_red(None, {"timestamp": 0}, now),
                         frl.painted_red(None, {"timestamp": None, "malformed": False}, now))


class MutationReviewDefects(r3.World):
    def show_recent(self):
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        return visible

    def test_D1_a_failed_first_search_of_a_new_tab_in_an_old_archive_is_tried_again(self):
        """A tab new to an archive older than GIVE_UP_DAYS gets one try at its first-scan
        gap search: last_scan() falls back to the archive's first run, so a single failed
        GCS listing counts as "failing for 14 days" and the search is given up for good."""
        run_folder(self.root, self.now - 30 * DAY)  # the archive has run for a month
        visible = self.show_recent()  # a tab it has never scanned shows only the last hour
        red = snowflake(self.now - 2 * DAY)
        self.gcs_build(red, finished=self.failed())
        self.srv.httpd.prefix_routes.clear()  # this run's GCS listing fails (404)
        self.assertEqual(self.fetch(), 1)
        first = self.runs()[-1]
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        self.fetch()  # the listing works again
        self.assertEqual(self.outcomes().get("backfilled"), [f"job/{red}"])  # fails: never searched again
        given_up = [w for w in first["warnings"] if "after 14 days the search is given up" in w]
        self.assertEqual(given_up, [], "the very first attempt was reported as 14 days of failures")

    def test_D2_cross_check_misses_a_red_build_testgrid_lists_only_as_a_placeholder(self):
        """cross_check's unlisted check skips every build in `statuses`, which includes
        TestGrid's grey "grid exceeds maximum size" placeholder columns. The fetch treats such
        a column as not shown and backfills it by TestGrid's rule (test_B02); cross_check only
        notes it as "TestGrid paints it UNKNOWN", so a missed one is never reported. The same
        build absent from the table altogether is reported as MISSED."""
        visible, ph = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        t = table("bucket/logs/job", [visible, ph], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 30 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)
        self.gcs_build(ph, finished=self.failed(ago=29 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (visible, ph)])
        self.assertEqual(self.fetch(), 0)  # a fresh archive has no gap to search, so ph is not archived ...
        run_folder(self.root, self.now - 50 * HOUR)  # ... although the archive ran while TestGrid showed it
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{ph}", out)
        self.assertEqual(code, 1, out)

    def test_D5_cross_check_uses_one_tabs_margin_where_the_fetch_uses_the_largest(self):
        """Two tabs show the same job. One still remembers a 20 h timeout (cut to 2 h a few hours
        ago, so builds started under it may still run); the other is new and has the default 6 h.
        The fetch waits 2 x max over the build's tabs (40 h) for a red build with nothing in
        GCS; cross_check's settle_of keeps whichever tab comes last for the query (6 h, so 12 h)
        and reports the build as MISSED while the fetch is still waiting for it."""
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        start = int(self.now - 30 * HOUR)
        b = snowflake(start)  # red on TestGrid ("did not complete within 24 hours"), nothing in GCS yet
        self.show([b], {"o": ([12], [""])}, start=start)
        self.srv.table("dash", "t2", table("bucket/logs/job", [b], {"o": ([12], [""])}, start=start * 1000))
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 40 * HOUR, "current_job_hours": 2,
                                        "job_hours": {"20": self.now + 10 * HOUR, "2": self.now + HOUR}}})
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})  # the fetch waits 40 h for its files
        code, out = self.check()
        self.assertNotIn("MISSED", out)
        self.assertEqual(code, 0, out)

    def test_D3_one_failing_lookup_drops_the_red_builds_its_gap_search_already_found(self):
        """A gap search that fails on one build discards the red builds it had already
        judged (out[key] is replaced by the exception), so a build whose lookup keeps failing
        holds back every other red build of that gap until the search is given up after
        GIVE_UP_DAYS, and then they are never archived."""
        self.show_recent()
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        red, bad = snowflake(self.now - 30 * HOUR), snowflake(self.now - 10 * HOUR)
        self.gcs_build(red, finished=self.failed())
        self.gcs_build(bad, finished=(500, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, bad)])
        self.assertEqual(self.fetch(), 1)  # the failed lookup fails the run, as it should ...
        self.assertEqual(self.outcomes().get("backfilled"), [f"job/{red}"])  # ... but red was judged red

    def test_D3b_a_red_build_found_by_a_second_tabs_gap_search_is_lost_with_the_first_tabs(self):
        """Worse with two tabs of one job: t2's gap search finds `red` and succeeds (t2 is
        marked searched), but the record was filed under t, whose search then failed."""
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        self.show_recent()  # t: results from 1 h ago
        t2_oldest = snowflake(self.now - 20 * HOUR)  # t2: results from 20 h ago, so `bad` is outside its gap
        self.srv.table("dash", "t2", table("bucket/logs/job", [t2_oldest], {"o": ([1], [""])},
                                           start=int((self.now - 20 * HOUR) * 1000)))
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap})
        red, bad = snowflake(self.now - 30 * HOUR), snowflake(self.now - 10 * HOUR)
        self.gcs_build(red, finished=self.failed())
        self.gcs_build(bad, finished=(500, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, bad, t2_oldest)])
        self.assertEqual(self.fetch(), 1)
        self.assertGreater(self.state()["tabs"]["dash#t2"]["scan"], time.time() - HOUR)  # t2 counts as searched
        self.assertEqual(self.outcomes().get("backfilled"), [f"job/{red}"])



if __name__ == "__main__":
    unittest.main()
