"""Regression tests for the round-5 review: each failed before its fix."""
import datetime as dt
import hashlib
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
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r3 as r3  # noqa: E402  (module imports: their tests are not collected again here)
import test_review_round4 as r4  # noqa: E402
from test_fetch_red_logs import run_record, send, table  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402


class D1SharedGapRedAndWatched(r4.World):
    """One build in two tabs' gaps (one job shown on two tabs). Its first finished.json request
    fails (any GCS error after retries), so one tab's lookup puts it on the watch list while the
    other tab's lookup finds it red. run() schedules BOTH records: two workers archive the same
    folder at once, the run counts it twice, and meta.json keeps only one of the two tabs."""

    def test_a_build_is_archived_once_with_both_tabs_even_if_its_two_lookups_disagree(self):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        visible = snowflake(self.now - HOUR)
        for tab in ("t", "t2"):
            self.srv.table("dash", tab, table("bucket/logs/job", [visible], {"o": ([1], [""])},
                                              start=int((self.now - HOUR) * 1000)))
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap})
        red = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(red, 30 * HOUR)
        body = json.dumps({"result": "FAILURE", "passed": False, "timestamp": int(self.now - 29 * HOUR)}).encode()
        lock, seen = threading.Lock(), []

        def finished(h):  # the first request fails, later ones succeed
            with lock:
                seen.append(1)
                first = len(seen) == 1
            send(h, 403, {}, b"denied") if first else send(h, 200, {}, body)
        self.srv.route(f"/bucket/logs/job/{red}/finished.json", finished)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        self.fetch()
        run = self.runs()[-1]
        key = f"job/{red}"
        tabs = sorted(t["tab"] for t in self.meta(red)["tabs"])
        self.assertEqual(sum(v.count(key) for v in run["outcomes"].values()), 1, run["outcomes"])
        self.assertEqual(tabs, ["t", "t2"])


class D2MigrationInvisibleFolder(unittest.TestCase):
    """An old-layout meta.json whose archived_by_run matches RUN_RE but is not a real time (damage).
    r9 left the build in logs/ with a serious "old layout" warning; r10 moves it into a run folder
    run_dirs() never lists, with no warning, so the archive, the index and cross_check lose it."""

    def test_a_build_naming_an_impossible_run_folder_is_not_hidden(self):
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        t = dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC).timestamp()
        b = snowflake(t - HOUR)
        d = os.path.join(root, "logs", "job", b)
        os.makedirs(d)
        os.makedirs(os.path.join(root, "runs"))
        with open(os.path.join(d, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        frl.write_json(os.path.join(d, "meta.json"), {
            "job": "job", "build": b, "query": "bucket/logs/job", "started": t - HOUR, "created": t - HOUR,
            "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}], "podinfo": None,
            "log_url": f"https://gcs/bucket/logs/job/{b}/build-log.txt",
            "log": {"bytes": 4, "md5": hashlib.md5(b"log\n").hexdigest(), "file": "build-log.txt", "file_bytes": 4},
            "archived_by_run": "2026-02-30T120000Z"})  # matches RUN_RE; February 30th does not exist
        warnings = []
        frl.migrate_old_layout(root, warnings)
        known, _ = frl.archived_builds(root)
        self.assertTrue(f"job/{b}" in known or any("old layout" in w for w in warnings), (warnings, known))


class D3PassedGapBuildCheck(r4.World):
    """A gap build that passed is decided without reading started.json (by design), so the fetch
    does not archive it; cross_check reads that started.json, finds it malformed (TestGrid: TOOL_FAIL,
    red) and fails the run: the two halves disagree (pre-existing since r9)."""

    def run_it(self, started):
        self.earlier_run(self.now - 50 * HOUR)
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        p = snowflake(self.now - 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{p}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 29 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{p}/started.json", started)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (p, visible)])
        code = self.fetch()
        ccode, out = self.check()
        self.assertEqual((code, ccode), (0, 0), out)

    def test_undecodable_started_json(self):
        self.run_it((200, {}, {"timestamp": "yesterday"}))

    def test_unparseable_started_json(self):
        self.run_it((200, {}, b"{not json"))



class BogusColumnDateTest(unittest.TestCase):
    def test_a_column_dated_in_the_future_counts_as_undated(self):
        now = time.time()
        build = snowflake(now - HOUR)
        for stamp in (int((now + 30 * 86400) * 1000), 10 ** 400, -5):
            with self.subTest(stamp=stamp):
                t = table("bucket/logs/job", [build], {"o": ([12], [""])})
                t["timestamps"] = [stamp]
                _, _, oldest, found, _ = frl.read_tab(t, now)
                self.assertEqual([(b, start) for b, start, _ in found], [(build, None)])
                self.assertIsNone(oldest)
                self.assertEqual([start for _, start, _ in cc.columns(t)], [frl.created(build)])


class JsonOrFetchTest(unittest.TestCase):
    """A file that is not JSON is malformed (red to TestGrid); one that cannot be fetched is not."""

    def test_the_fetch_tells_a_bad_file_from_a_failed_read(self):
        with mock.patch.object(frl, "get_json", side_effect=frl.BadJSON("bad JSON")):
            self.assertTrue(frl.read_finished("base")["malformed"])
            self.assertTrue(frl.read_started("base")["malformed"])
        with mock.patch.object(frl, "get_json", side_effect=frl.IntegrityError("got 2 of 100 bytes")):
            with self.assertRaises(frl.IntegrityError):
                frl.read_finished("base")
            with self.assertRaises(frl.IntegrityError):
                frl.read_started("base")

    def test_cross_check_tells_them_apart_too(self):
        with mock.patch.object(cc, "get_json", side_effect=frl.BadJSON("bad JSON")):
            self.assertEqual(cc.try_json("url"), cc.MALFORMED)
        with mock.patch.object(cc, "get_json", side_effect=frl.IntegrityError("got 2 of 100 bytes")):
            self.assertIsInstance(cc.try_json("url"), cc.Unreachable)


class MalformedMissTest(r4.World):
    def test_cross_check_reports_a_missed_build_whose_finished_json_is_not_json(self):
        self.earlier_run(self.now - 50 * HOUR)
        recent, lost = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.srv.route(f"/bucket/logs/job/{lost}/finished.json", (200, {}, b"{not json"))
        self.srv.route(f"/bucket/logs/job/{lost}/started.json", (200, {}, {"timestamp": int(self.now - 30 * HOUR)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (lost, recent)])
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        # the last scan was an hour ago, so this run does not search back to `lost`: as if an earlier run lost it
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR}})
        self.fetch()
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn(f"MISSED red build job/{lost}", out)



# Repros of the defects the round-5 mutation review found (each failed before its fix)
class MutationReviewDefects(r3.World):
    def test_D1_a_build_red_in_one_tabs_gap_and_unreadable_in_anothers_is_archived_once(self):
        """Two tabs of one job have the same history gap. The build's lookup for one tab fails
        (403, not retried) and the other finds it red: backfill_gaps puts it both in that tab's
        list and on the watch list, run()'s `taken` only dedupes the lists, so the build is queued
        twice and two workers archive it into the same folder at once. Same with a build that
        finishes between the two lookups (running for one tab, red for the other)."""
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        visible = snowflake(self.now - HOUR)
        for tab in ("t", "t2"):
            self.srv.table("dash", tab, table("bucket/logs/job", [visible], {"o": ([1], [""])},
                                              start=int((self.now - HOUR) * 1000)))
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap})
        red = snowflake(self.now - 30 * HOUR)
        self.gcs_build(red)
        lock, hits = threading.Lock(), []

        def finished(h):  # the first read is refused, later reads work
            with lock:
                hits.append(1)
                first = len(hits) == 1
            if first:
                send(h, 403, {}, b"")
            else:
                send(h, 200, {}, json.dumps({"result": "FAILURE", "passed": False,
                                             "timestamp": int(self.now - 29 * HOUR)}).encode())
        self.srv.route(f"/bucket/logs/job/{red}/finished.json", finished)
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        self.fetch()
        outcomes = self.outcomes()
        self.assertEqual(sum(v.count(f"job/{red}") for v in outcomes.values()), 1, outcomes)

    def test_D2_a_watched_build_that_never_started_is_decided_even_if_its_finished_json_is_malformed(self):
        """test_AW05: a watched build without started.json is "not-red" once finished.json exists
        (TestGrid never lists it). With a malformed or unreadable finished.json, `stamp` is None, so
        archive() keeps it "watching" for 14 days and then warns it is "still not finished"."""
        self.quiet()
        for finished in ((200, {}, {"result": "FAILURE", "timestamp": "yesterday"}), (200, {}, b"{cut")):
            with self.subTest(finished=finished[2]):
                b = snowflake(self.now - 5 * HOUR)
                self.gcs_build(b, finished=finished, started=None)
                self.put_state(watch=self.watched(b))
                self.fetch()
                self.assertEqual(self.outcomes(), {"not-red": [f"job/{b}"]})

    def test_D4_a_watched_build_found_red_but_pending_is_tracked_as_red(self):
        """A watched build red by TestGrid's 24-hour deadline whose files are not in GCS yet (a
        long job: pending for 2 x its margin) is stored in `unresolved` with its watch flag, as
        run() saves the record it queued, not the one archive() returned. A later failed lookup
        then treats it as never found red: it moves back to `watch` (so INDEX.md no longer lists
        it as not archived), and 14 days after it was made it is dropped as "never found red"
        (test_AW04 covers the same leak through the exception path)."""
        self.quiet()
        b = snowflake(self.now - 25 * HOUR)  # started 25 h ago, no finished.json: red by the deadline
        self.gcs_build(b, log=False)  # nothing uploaded yet
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 100 * HOUR,
                                        "current_job_hours": 20, "job_hours": {"20": self.now + 10 * HOUR}}},
                       watch=self.watched(b))
        self.fetch()
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (500, {}, b""))  # a later lookup fails
        self.fetch()
        state = self.state()
        self.assertIn(f"job/{b}", state["unresolved"])
        self.assertNotIn(f"job/{b}", state["watch"])


class MutationReviewMigration(unittest.TestCase):
    def test_D3_two_old_runs_from_the_same_second_get_their_own_folders_in_one_migration(self):
        """folder_for() reuses an existing folder without run.json as "one an earlier, cut-off
        migration made". Within one migration, the folder just made for the first run has no
        run.json yet (reports are written after the builds), so a second old run from the same
        second is put in it too, and its report overwrites the first's (test_MF05 covers the
        case where the first run was migrated by an earlier migration)."""
        root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, root)
        os.makedirs(os.path.join(root, "runs"))
        t = dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC).timestamp()
        olds = ("20261001T120000.000000Z", "20261001T120000.500000Z")
        for old, started in zip(olds, (t, t + 0.5), strict=True):
            frl.write_json(os.path.join(root, "runs", f"{old}.json"), dict(run_record(started, []), run=old, outcomes={}))
        frl.migrate_old_layout(root, [])
        migrated = sorted(frl.read_json(os.path.join(root, rel, "run.json"))["migrated_from"] for rel in frl.run_dirs(root))
        self.assertEqual(migrated, sorted(olds))


if __name__ == "__main__":
    unittest.main()
