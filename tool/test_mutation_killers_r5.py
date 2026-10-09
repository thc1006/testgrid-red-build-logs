"""Targeted tests from a fifth mutation-testing review (the round-4 changes): each kills mutants
the rest of the suite let survive. Each test name starts with the mutant id(s) it targets."""
import datetime as dt
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
import test_mutation_killers_r3 as r3  # noqa: E402  (module imports: their tests are not collected again here)
import test_mutation_killers_r4 as r4  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, run_record, table  # noqa: E402
from test_integrity import HOUR, obj, snowflake  # noqa: E402


def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


class Unit(unittest.TestCase):
    def test_RT11_a_start_less_than_a_day_before_the_build_id_is_the_columns_own(self):
        now = time.time()
        build = snowflake(now - 3 * HOUR)
        for before, kept in ((2 * HOUR, True), (23 * HOUR, True), (25 * HOUR, False)):
            start = frl.created(build) - before
            t = table("bucket/logs/job", [build], {"o": ([12], [""])}, start=start * 1000)
            _, _, oldest, found, _ = frl.read_tab(t, now)
            self.assertEqual((found[0][1], oldest), (start, start) if kept else (None, None), before)

    def test_CC10_CC11_CC14_CC15_cross_check_dates_each_column_as_the_fetch_does(self):
        # the fetch keeps a column's own start unless it is more than a day before the build
        # was made (then the column is undated); cross_check dates an undated column by its build ID
        now = time.time()
        build = snowflake(now - 3 * HOUR)
        made = frl.created(build)
        for offset in (-3 * DAY, -25 * HOUR, -DAY - 1, -DAY, -23 * HOUR, -2 * HOUR, 0, 2 * HOUR, 20 * HOUR):
            t = table("bucket/logs/job", [build], {"o": ([12], [""])}, start=(made + offset) * 1000)
            _, _, _, ((_, fetch_start, _),), _ = frl.read_tab(t, now)
            (_, check_start, _), = cc.columns(t)
            self.assertEqual(check_start, made if fetch_start is None else fetch_start, offset)
            self.assertEqual(check_start, made if offset < -DAY else made + offset, offset)

    def test_LB10_LB12_an_empty_search_range_asks_gcs_nothing(self):
        with mock.patch.object(frl, "get_json", side_effect=AssertionError("GCS was asked")):
            self.assertEqual(frl.list_build_ids("http://127.0.0.1:9", "bucket/logs/job", 2_000_000_000, 1_900_000_000), [])

    def test_LB11_the_search_range_includes_both_ends(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        t = int(time.time()) - 5 * HOUR
        b = snowflake(t)
        srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json"])
        self.assertEqual(frl.list_build_ids(srv.url, "bucket/logs/job", t, t), [b])

    def test_RF10_RF11_stamp_is_the_finish_time_only_when_go_reads_it_as_an_int64(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        cases = [(0, 0), (-5, -5), (1 << 62, 1 << 62), ("123", None), (True, None), (1.7e9, None), (1 << 63, None),
                 (None, None)]
        for i, (raw, want) in enumerate(cases):
            srv.route(f"/b/logs/j/{i}/finished.json", (200, {}, {"timestamp": raw, "passed": False}))
            fin = frl.read_finished(f"{srv.url}/b/logs/j/{i}")
            self.assertIs(type(fin["stamp"]), type(want), raw)
            self.assertEqual(fin["stamp"], want, raw)

    def test_WR10_WR12_WR14_a_run_readme_shows_only_well_formed_counts_and_entries(self):
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            readme = os.path.join(root, rel, "README.md")
            for seen in (True, None, 2.5, "7"):
                frl.write_run_readme(root, rel, {"tabs": [], "red_builds_seen": seen,
                                                 "outcomes": {"refreshed": [5, "job/1"]}}, [])
                text = slurp(readme)
                self.assertIn("0 tabs read (0 failed), ? red builds on TestGrid.", text, seen)
                self.assertIn("- refreshed: job/1\n", text)
                self.assertNotIn("- refreshed: 5", text)
            frl.write_run_readme(root, rel, {"tabs": [], "red_builds_seen": 7}, [])
            self.assertIn("0 tabs read (0 failed), 7 red builds on TestGrid.", slurp(readme))

    def test_XT10_XT11_svg_text_drops_lone_surrogates(self):
        self.assertEqual(report.xml_text("a\ud800b\udbffc\udc00d\udfffe"), "a b c d e")

    def test_WC11_charts_are_written_as_utf_8_whatever_the_locale(self):
        with tempfile.TemporaryDirectory() as root:
            code = (f"import sys, time; sys.path.insert(0, {HERE!r}); import report; "
                    f"report.write_charts({root!r}, [], time.time())")
            env = dict(os.environ, LC_ALL="C", LANG="C", PYTHONUTF8="0", PYTHONCOERCECLOCALE="0")  # an ASCII locale
            subprocess.run([sys.executable, "-c", code], env=env, check=True, capture_output=True)
            self.assertIn("UTC days · a build counts once per dashboard", slurp(os.path.join(root, "charts", "trend-light.svg")))


class Migration(unittest.TestCase):
    OLD, NEW = "20261001T120000.000000Z", "2026-10-01T120000Z"

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, self.root)
        self.t = dt.datetime(2026, 10, 1, 12, tzinfo=dt.UTC).timestamp()
        os.makedirs(os.path.join(self.root, "runs"))
        self.b = snowflake(self.t - HOUR)

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def old_build(self, old):
        d = self.path("logs", "job", self.b)
        os.makedirs(d)
        with open(os.path.join(d, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        frl.write_json(os.path.join(d, "meta.json"), {
            "job": "job", "build": self.b, "query": "bucket/logs/job", "started": self.t - HOUR, "created": self.t - HOUR,
            "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}],
            "log_url": f"https://gcs/bucket/logs/job/{self.b}/build-log.txt", "podinfo": None,
            "log": {"bytes": 4, "md5": frl.file_md5(os.path.join(d, "build-log.txt")), "file": "build-log.txt",
                    "file_bytes": 4}, "archived_by_run": old})

    def migrate(self):
        warnings = []
        frl.migrate_old_layout(self.root, warnings)
        return warnings

    def test_MG21_a_meta_naming_what_only_starts_like_a_run_folder_stays_in_place(self):
        self.old_build(self.NEW + "-x")
        warnings = self.migrate()
        self.assertTrue(any("no known time" in w for w in warnings), warnings)
        self.assertTrue(os.path.isfile(self.path("logs", "job", self.b, "meta.json")))
        self.assertFalse(os.path.exists(self.path("runs", "2026-10", self.NEW + "-x")))

    def test_MG23_a_migration_cut_off_while_rewriting_meta_json_finishes_on_the_next_run(self):
        self.old_build(self.OLD)
        frl.write_json(self.path("runs", f"{self.OLD}.json"), dict(run_record(self.t, []), run=self.OLD, outcomes={}))
        real = frl.write_json

        def cut_off(path, data):
            if path.endswith(os.path.join("job", self.b, "meta.json")):
                raise KeyboardInterrupt  # killed while the build's meta.json is rewritten
            return real(path, data)
        with mock.patch.object(frl, "write_json", side_effect=cut_off), self.assertRaises(KeyboardInterrupt):
            frl.migrate_old_layout(self.root, [])
        self.assertEqual(self.migrate(), [])
        found, _ = frl.archived_builds(self.root)
        rel = found[f"job/{self.b}"]
        m = frl.read_json(self.path(rel, "meta.json"))
        self.assertEqual(m["archived_by_run"], rel.split("/")[2])


class GapRule(r4.GapWorld):
    def test_RR12_RR13_a_build_that_did_not_pass_did_not_pass_however_bogus_its_finish_time(self):
        b = self.gap_build(30, (200, {}, {"timestamp": 1 << 62, "passed": False, "result": "FAILURE"}))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{b}"]})
        self.assertIn("by TestGrid's rule it was red (it did not pass; Prow result FAILURE)", self.meta(b)["backfilled"])

    def test_BR10_BR11_a_build_that_passed_with_a_zero_finish_time_is_red(self):
        b = self.gap_build(30, (200, {}, {"timestamp": 0, "passed": True, "result": "SUCCESS"}))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{b}"]})
        self.assertIn("by TestGrid's rule it was red (it did not finish within 24 hours)", self.meta(b)["backfilled"])

    def test_BG19_a_gap_build_keeps_the_status_of_the_tab_whose_gap_held_it(self):
        b = self.gap_build(30, self.failed())
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual([(t["tab"], t["tab_status"]) for t in self.meta(b)["tabs"]], [("t", "FAILING")])

    def test_BR12_a_build_that_passed_is_decided_without_its_started_json_whatever_its_finish_time(self):
        b = self.gap_build(30, (200, {}, {"timestamp": 1 << 62, "passed": True, "result": "SUCCESS"}),
                           started=(403, {}, b""))
        self.listing(b)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {})
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{b}/started.json"), 0)

    def test_GS10_a_failed_gap_search_keeps_the_margins_read_now_and_drops_missing_since(self):
        # the tab had a 3 h timeout and was briefly unlisted; prowjob.json now says 20 h
        self.srv.route(f"/bucket/logs/job/{self.visible}/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "20h"}}}))
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR,
                                        "missing_since": self.now - DAY,
                                        "current_job_hours": 3, "job_hours": {"3": self.now + HOUR}}})
        self.srv.httpd.prefix_routes.clear()  # the gap's GCS listing fails
        self.assertEqual(self.fetch(), 1)
        tab = self.state()["tabs"]["dash#t"]
        self.assertAlmostEqual(tab["scan"], self.now - 40 * HOUR, delta=1)  # searched again next run
        self.assertEqual(tab.get("current_job_hours"), 20.25)
        self.assertIn("20.25", tab.get("job_hours", {}))
        self.assertNotIn("missing_since", tab)  # listed again


class Watched(r3.World):
    def test_AW10_a_watched_build_that_passed_with_a_far_future_finish_time_is_dropped(self):
        self.quiet()
        b = snowflake(self.now - 5 * HOUR)
        self.gcs_build(b, finished=(200, {}, {"timestamp": 1 << 62, "passed": True, "result": "SUCCESS"}))
        self.put_state(watch=self.watched(b))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"not-red": [f"job/{b}"]})
        self.assertEqual(self.state()["watch"], {})

    def test_WE11_WE14_an_unreadable_watched_build_is_dropped_once_the_build_is_14_days_old(self):
        self.quiet()
        b = snowflake(self.now - 15 * DAY)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (403, {}, b"denied"))
        self.put_state(watch=self.watched(b))  # first seen an hour ago
        self.assertEqual(self.fetch(), 1)
        errors = [e["error"] for e in self.runs()[-1]["errors"]]
        self.assertTrue(any("no longer watched after 14 days" in e for e in errors), errors)
        self.assertEqual(self.state()["watch"], {})


class Margins(r3.World):
    def test_HB15_every_tab_of_a_build_counts_for_its_margin(self):
        start = int(self.now - 15 * HOUR)
        b = snowflake(start)
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        for tab in ("t", "t2"):
            self.srv.table("dash", tab, table("bucket/logs/job", [b], {"o": ([12], [""])}, start=start * 1000))
        # t2, the build's second tab, knows its job can run 20 hours
        self.put_state(tabs={"dash#t2": {"scan": self.now - 12 * HOUR, "oldest": self.now - 100 * HOUR,
                                         "current_job_hours": 20, "job_hours": {"20": self.now + 10 * HOUR}}})
        self.fetch()
        self.assertEqual(self.outcomes(), {"pending": [f"job/{b}"]})  # nothing in GCS yet: waits 2 x 20 h


class Horizon(r3.World):
    def test_LB10_LB12_a_gap_entirely_before_the_14_day_horizon_needs_no_gcs_listing(self):
        recent, old = snowflake(self.now - HOUR), snowflake(self.now - 16 * DAY)
        t = table("bucket/logs/job", [recent, old], {"o": ([1, 1], ["", ""])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 16 * DAY) * 1000)]
        self.srv.table("dash", "t", t)  # TestGrid still shows 16 days ...
        self.put_state(tabs={"dash#t": {"scan": self.now - 20 * DAY, "oldest": self.now - 30 * DAY}})  # ... after 20 unseen
        self.srv.httpd.prefix_routes.clear()  # a GCS listing would fail
        self.assertEqual(self.fetch(), 1)  # red builds before the 14-day horizon may be missing: a serious warning
        self.assertEqual(self.runs()[-1]["errors"], [])  # but nothing was searched, so nothing failed
        self.assertGreater(self.state()["tabs"]["dash#t"]["scan"], self.now - HOUR)


class LatestRun(r3.World):
    def test_LR13_a_newest_run_folder_without_a_usable_report_is_skipped(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        verified = self.runs()[-1]["run"]
        for content in (None, ["not a report"]):  # a run killed before it wrote run.json, or a damaged one
            rel = frl.new_run_dir(self.root, time.time())[1]
            if content is not None:
                frl.write_json(os.path.join(self.root, rel, "run.json"), content)
            code, out = self.check()
            self.assertIn(f"verifying run {verified}:", out, content)
            self.assertEqual(code, 0, out)
            if content is not None:
                os.remove(os.path.join(self.root, rel, "run.json"))
            os.rmdir(os.path.join(self.root, rel))


class Check(r3.World):
    def cut_off_run(self, started):
        """A run folder from `started` holding an archived build but no run.json: the run was killed."""
        name = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime(started))
        b = snowflake(started)
        d = os.path.join(self.root, "runs", name[:7], name, "job", b)
        os.makedirs(d)
        frl.write_json(os.path.join(d, "meta.json"), {
            "job": "job", "build": b, "query": "bucket/logs/job", "started": int(started), "created": frl.created(b),
            "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}], "result": None,
            "finished": None, "log_url": f"{self.srv.url}/bucket/logs/job/{b}/build-log.txt", "log": None,
            "podinfo": None, "archived_by_run": name, "archived_at": started, "settled": False, "final": False})

    def test_AS12_the_archive_began_with_its_first_run_folder_even_one_without_a_report(self):
        recent, lost = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.gcs_build(lost, finished=self.failed(ago=29 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (lost, recent)])
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.assertEqual(self.fetch(), 0)  # a fresh archive has no gap to search, so `lost` is not archived ...
        self.cut_off_run(self.now - 50 * HOUR)  # ... although a (killed) run was archiving while TestGrid showed it
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{lost}", out)
        self.assertEqual(code, 1, out)

    def test_SH12_SH14_a_build_judged_as_no_longer_shown_is_not_noted_again(self):
        recent, lost = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.gcs_build(lost, finished=self.failed(ago=29 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (lost, recent)])
        self.show([recent], {"o": ([1], [""])}, start=int(self.now - HOUR))
        self.fetch()
        r4.run_folder(self.root, self.now - 50 * HOUR)
        t = table("bucket/logs/job", [recent, lost], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 30 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)  # `lost` is left only in a placeholder column
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{lost}", out)
        self.assertNotIn(f"job/{lost}: FAILURE", out)  # judged once, not also noted as "not red, not archived"

    def test_SO13_SO14_a_tab_margin_that_is_not_a_positive_number_falls_back_to_the_runs_own(self):
        start = int(self.now - 30 * HOUR)
        b = snowflake(start)
        self.show([b], {"o": ([12], [""])}, start=start)  # red, nothing in GCS yet
        for bad in (True, 0):
            rel = frl.new_run_dir(self.root, time.time())[1]
            tab = {"dashboard": "dash", "tab": "t", "query": "bucket/logs/job", "oldest": start, "max_job_hours": bad}
            frl.write_json(os.path.join(self.root, rel, "run.json"),
                           dict(run_record(time.time(), [tab], max_job_hours=20), run="r", finished=time.time()))
            code, out = self.check()
            self.assertNotIn("MISSED", out, bad)  # within 2 x 20 h the fetch may still be waiting for its files


if __name__ == "__main__":
    unittest.main()
