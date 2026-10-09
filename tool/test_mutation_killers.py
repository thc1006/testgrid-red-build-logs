"""Targeted tests from a mutation-testing review: each kills mutants that the rest of
the suite let survive. Each test name starts with the mutant id(s) it targets."""
import gzip
import os
import sys
import time
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, run_record, send, table  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj, snowflake  # noqa: E402


def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def lines(path):
    return slurp(path).splitlines()


class H(dict):
    def get_all(self, name):
        return [self[name]] if name in self else None


class Unit(unittest.TestCase):
    def test_S04_S11_logless_build_is_followed_for_give_up_days(self):
        now = time.time()
        meta = {"finished": now - 5 * DAY, "archived_at": now - 2 * DAY, "created": now - 5 * DAY, "log": None}
        self.assertEqual(frl.settle_flags(meta, now, now), (True, False))
        meta["archived_at"] = now - 15 * DAY
        self.assertEqual(frl.settle_flags(meta, now, now), (True, True))

    def test_S08_bogus_finished_timestamp_is_ignored(self):
        now = time.time()
        meta = {"finished": "soon", "archived_at": now, "created": now, "log": {"md5": "x"}}
        self.assertEqual(frl.settle_flags(meta, now, now), (False, False))

    def test_I01_I02_I03_I04_I05_I09_same_object_edges(self):
        idn = {"encoding": "identity"}
        self.assertFalse(frl.same_object(dict(idn, md5="a", bytes=1), {"encoding": "gzip", "md5": "a", "bytes": 1}))
        self.assertFalse(frl.same_object(dict(idn, crc32c="x", generation="1"), dict(idn, crc32c="y", generation="1")))
        self.assertTrue(frl.same_object(dict(idn, md5="a", crc32c="c", bytes=1), dict(idn, md5=None, crc32c="c", bytes=1)))
        self.assertFalse(frl.same_object(dict(idn), dict(idn)))
        self.assertFalse(frl.same_object(dict(idn, md5="a", bytes=1), dict(idn, md5="a", bytes=2)))

    def test_I07_a_non_dict_identity_falls_back_to_md5(self):
        self.assertEqual(frl.recorded_identity({"gcs": "x", "md5": "m", "bytes": 3})["md5"], "m")

    def test_G02_a_member_after_padding_in_a_later_chunk_is_rejected(self):
        g = frl.Gunzip()
        list(g.feed(gzip.compress(b"a") + b"\0" * 8))
        with self.assertRaises(frl.IntegrityError):
            list(g.feed(gzip.compress(b"b")))

    def test_G07_a_truncated_second_member_is_rejected(self):
        with self.assertRaises(frl.IntegrityError):
            list(frl.gunzipped([gzip.compress(b"a") + gzip.compress(b"b" * 1000)[:-5]]))

    def test_T02_messages_must_be_clean_text(self):
        for bad in ("bad\ud800", "nul\x00"):
            with self.subTest(bad), self.assertRaises(ValueError):
                frl.red_columns(table("b/logs/j", ["1"], {"o": ([12], [bad])}))

    def test_T03_c1_controls_are_not_text(self):
        self.assertFalse(frl.is_text("a\x85b"))

    def test_RT03_a_build_id_from_the_future_fails_the_tab(self):
        t = table("bucket/logs/job", ["99999999999999999999"], {"o": ([12], [""])})
        t["timestamps"] = [0]
        with self.assertRaisesRegex(ValueError, "unexpected build id"):
            frl.read_tab(t, time.time())

    def test_H01_H02_header_validation(self):
        with self.assertRaises(frl.IntegrityError):
            frl.header_int(H({"Content-Length": "١٢"}), "Content-Length")
        self.assertIsNone(frl.remote_identity(H({"x-goog-generation": "abc"}))["generation"])

    def test_L01_L02_odd_run_folders_are_ignored(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            for rel in ("runs/2026-01/2026-02-01T000000Z", "runs/2026-02/2026-02-30T000000Z"):
                os.makedirs(os.path.join(root, rel))
            self.assertEqual(frl.run_dirs(root), [])

    def test_MD01_MD02_md_cell_line_separators_and_markers(self):
        self.assertNotIn(" ", frl.md_cell("a b"))
        self.assertEqual(frl.md_cell("+ x"), "\\+ x")
        self.assertEqual(frl.md_cell("a b"), report.md_cell("a b"))

    def test_P06_P14_averages_and_week_need_complete_windows(self):
        now = report.day_start(report.day_of(time.time())) + 12 * HOUR
        entries = [{"time": now - k * DAY - 600, "tabs": ["d#t"]} for k in range(1, 30)]
        runs = [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY}])]
        p = report.summarize(entries, runs, now)["panels"][0]
        days = report.summarize(entries, [], now)["days"]
        self.assertEqual(min(p["avg"]), days[6])
        runs = [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 9 * DAY}])]
        p = report.summarize(entries, runs, now)["panels"][0]
        self.assertIsNone(p["week"])

    def test_P08_a_lone_average_day_is_a_dot(self):
        now = time.time()
        s = report.summarize([], [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY}])], now)
        s["panels"][0]["avg"] = {s["days"][5]: 1.0}
        self.assertIn('r="2.5"', report.render_trend(s, "light"))

    def test_P12_a_tabs_own_job_hours_limit_its_coverage(self):
        now = report.day_start(report.day_of(time.time())) + 12 * HOUR
        run = run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY, "max_job_hours": 30}])
        p = report.summarize([], [run], now)["panels"][0]
        self.assertNotIn(report.day_of(now - DAY), p["complete"])

    def test_P13_a_dashboard_no_run_listed_has_no_complete_days(self):
        now = time.time()
        s = report.summarize([{"time": now - 2 * DAY, "tabs": ["x#y"]}],
                             [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY}])], now)
        self.assertEqual([p["complete"] for p in s["panels"] if p["dashboard"] == "x"], [set()])

    def test_MD10_only_a_true_interrupted_flag_marks_a_run_interrupted(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            rel = frl.new_run_dir(root, time.time())[1]
            frl.write_json(os.path.join(root, rel, "run.json"), {"run": "x", "interrupted": "no", "outcomes": {}})
            self.assertTrue(frl.run_rows(root, report.day_of(time.time() - DAY))[0].endswith("| ok |"))

    def test_P09_svg_text_drops_noncharacters(self):
        self.assertNotIn("￾", report.xml_text("a￾b"))


class Http(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer()
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)

    def tearDown(self):
        self.srv.close()
        frl.bind_context(frl._DEFAULT_CONTEXT)

    def once(self, path, first_status, headers=None):
        calls = []

        def h(r):
            calls.append(time.monotonic())
            send(r, first_status, headers or {}, b"") if len(calls) == 1 else send(r, 200, {}, b"ok")
        self.srv.route(path, h)
        return calls

    def test_RQ01_retry_after_on_429_is_honoured(self):
        calls = self.once("/x", 429, {"Retry-After": "1"})
        self.assertEqual(frl.request(self.srv.url + "/x"), b"ok")
        self.assertGreaterEqual(calls[1] - calls[0], 0.9)

    def test_RQ02_backoff_doubles(self):
        waits = []
        self.srv.route("/x", (500, {}, b""))
        with mock.patch.object(frl, "BACKOFF", 1.0), mock.patch.object(frl.random, "uniform", return_value=1.0), \
                mock.patch.object(self.ctx.abandoned, "wait", side_effect=lambda w: waits.append(w) or False):
            with self.assertRaises(urllib.error.HTTPError):
                frl.request(self.srv.url + "/x")
        self.assertEqual(waits, [1.0, 2.0, 4.0])

    def test_RQ03_no_new_attempt_after_the_run_is_abandoned(self):
        self.srv.route("/x", (200, {}, b"ok"))
        self.ctx.abandoned.set()
        with self.assertRaises(frl.Abandoned):
            frl.request(self.srv.url + "/x")
        self.assertEqual(self.srv.hits("/x"), 0)

    def test_RQ04_504_is_pushback(self):
        self.once("/x", 504)
        frl.request(self.srv.url + "/x")
        self.assertEqual(self.ctx.limit.pushbacks, 1)

    def test_RQ05_408_is_retried(self):
        self.once("/x", 408)
        self.assertEqual(frl.request(self.srv.url + "/x"), b"ok")

    def test_SH01_a_redirect_may_not_change_scheme(self):
        self.srv.route("/a", (302, {"Location": self.srv.url.replace("http:", "https:") + "/b"}, b""))
        with self.assertRaisesRegex(urllib.error.HTTPError, "refused redirect"):
            frl.request(self.srv.url + "/a")

    def test_RB02_a_body_without_length_is_capped(self):
        def h(r):
            r.send_response(200)
            r.end_headers()
            r.wfile.write(b'{"a": "' + b"x" * 5000 + b'"}')
        self.srv.route("/big", h)
        with mock.patch.object(frl, "MAX_JSON_BYTES", 1000), self.assertRaisesRegex(frl.IntegrityError, "limit"):
            frl.get_json(self.srv.url + "/big")

    def test_RB04_a_short_body_is_an_integrity_error(self):
        def h(r):
            r.send_response(200)
            r.send_header("Content-Length", "10")
            r.end_headers()
            r.wfile.write(b"123")
        self.srv.route("/n", h)
        with self.assertRaises(frl.IntegrityError):
            frl.get_json(self.srv.url + "/n")

    def test_U01_unchanged_when_gcs_lost_the_object(self):
        self.assertTrue(frl.unchanged_in_gcs(self.srv.url + "/gone", {"md5": "x", "bytes": 1}))

    def test_B08_B09_C10_C11_listing_follows_pages(self):
        now = time.time()
        ids = [snowflake(now - h * HOUR) for h in (3, 2)]

        def page(h):
            token = "pageToken=t2" in h.path
            body = {"prefixes": [f"logs/job/{ids[1 if token else 0]}/"]}
            if not token:
                body["nextPageToken"] = "t2"
            send(h, 200, {}, __import__("json").dumps(body).encode())
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = page
        self.assertEqual(frl.list_build_ids(self.srv.url, "bucket/logs/job", now - 5 * HOUR, now), ids)
        self.assertEqual(sorted(cc.list_builds(self.srv.url, "bucket/logs/job", int(now - 5 * HOUR))), sorted(ids))

    def test_B10_B11_prowjob_timeouts(self):
        self.srv.route("/b/logs/j/1/prowjob.json", (200, {}, {"spec": {"decoration_config": {"timeout": "11h"}}}))
        self.srv.route("/b/logs/j/2/prowjob.json", (200, {}, {"spec": {"decoration_config": {"timeout": "200h"}}}))
        self.assertEqual(frl.prow_job_hours(self.srv.url, "b/logs/j", "1"), 11.25)
        self.assertIsNone(frl.prow_job_hours(self.srv.url, "b/logs/j", "2"))

    def test_C08_list_builds_drops_ids_longer_than_20_digits(self):
        self.srv.gcs_listing("bucket", [f"logs/job/{'9' * 21}/finished.json"])
        self.assertEqual(cc.list_builds(self.srv.url, "bucket/logs/job", 1_600_000_000), [])

    def test_C12_a_listing_that_never_ends_is_an_error(self):
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = (200, {}, {"prefixes": [], "nextPageToken": "again"})
        with mock.patch.object(frl, "MAX_LIST_PAGES", 3), self.assertRaises(ValueError):
            cc.list_builds(self.srv.url, "bucket/logs/job", 1)


class Archive(ArchiveWorld):
    def test_D01_a_download_without_length_is_capped(self):
        self.finished(ago=3 * DAY)

        def h(r):
            r.send_response(200)
            r.send_header("x-goog-stored-content-encoding", "identity")
            r.end_headers()
            r.wfile.write(b"x" * 100)
        self.srv.route(self.base + "/build-log.txt", h)
        with mock.patch.object(frl, "MAX_DOWNLOAD_BYTES", 50):
            self.assertEqual(self.fetch(), 1)
        self.assertIn("byte limit", self.runs()[-1]["errors"][0]["error"])

    def test_D03_an_open_failure_is_local(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        real_open = open

        def fake_open(path, *a, **k):
            if str(path).endswith(".part") and "build-log" in str(path) and a and a[0] == "wb":
                raise OSError(28, "No space left on device")
            return real_open(path, *a, **k)
        with mock.patch("builtins.open", fake_open):
            self.assertEqual(self.fetch(), 1)
        self.assertIn("LocalIOError", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.srv.hits(self.base + "/build-log.txt"), 1)

    def test_D04_a_flush_failure_is_local(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))

        class Full(gzip.GzipFile):
            failed = False

            def close(self):
                if not Full.failed:
                    Full.failed = True
                    raise OSError(28, "No space left on device")
                super().close()
        with mock.patch.object(frl.gzip, "GzipFile", Full):
            self.assertEqual(self.fetch("--gzip"), 1)
        self.assertIn("LocalIOError", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.srv.hits(self.base + "/build-log.txt"), 1)

    def test_D05_a_download_without_checksums_is_not_verified(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", (200, {"x-goog-stored-content-encoding": "identity"}, b"log\n"))
        self.fetch()
        self.assertFalse(self.meta()["log"]["verified"])

    def test_U01_an_object_gcs_deleted_is_not_an_error(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        del self.srv.httpd.routes[self.base + "/build-log.txt"]
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})

    def test_U02_RC06_an_old_record_gains_its_identity(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1"))
        self.fetch()
        m = self.meta()
        m["log"].pop("gcs")
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.fetch()
        self.assertEqual(self.meta()["log"]["gcs"]["generation"], "1")

    def test_RC02_log_recovery_is_recorded(self):
        self.finished(ago=2 * HOUR)
        self.fetch()
        self.srv.route(self.base + "/build-log.txt", obj(b"late\n"))
        self.fetch()
        self.assertEqual(self.meta()["log_recovered_by_run"], self.runs()[-1]["run"])

    def test_RC03_podinfo_is_not_fetched_for_a_build_with_a_log(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": 1}'))
        self.fetch()
        self.fetch()
        self.assertIsNone(self.meta()["podinfo"])
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})

    def test_A03_a_young_red_build_with_nothing_in_gcs_is_pending(self):
        start = int(time.time()) - 7 * HOUR
        build = snowflake(start)
        self.show([build], {"o": ([12], [""])}, start=start)
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"pending": [f"job/{build}"]})

    def test_A04_A05_A06_A08_a_repair_keeps_the_history(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        first = self.meta()
        self.srv.summary("dash", {"t": "FAILING", "u": "FAILING"})
        self.show([self.build], {"o": ([1], [""])})
        self.srv.table("dash", "u", table("bucket/logs/job", [self.build], {"p": ([12], ["y"])}, start=self.start * 1000))
        with open(os.path.join(self.bdir(), "build-log.txt"), "wb") as f:
            f.write(b"XXXX")
        self.fetch()
        m, run = self.meta(), self.runs()[-1]
        self.assertEqual(run["outcomes"], {"repaired": [self.key]})
        self.assertEqual((m["archived_by_run"], m["archived_at"]), (first["archived_by_run"], first["archived_at"]))
        self.assertEqual(m["repaired_by_run"], run["run"])
        self.assertEqual([t["tab"] for t in m["tabs"]], ["t", "u"])

    def test_A07_a_meta_json_of_another_build_is_not_trusted(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        path = os.path.join(self.bdir(), "meta.json")
        frl.write_json(path, dict(frl.read_json(path), build="1"))
        self.fetch()
        self.assertEqual(self.meta()["build"], self.build)

    def test_A11_a_corrupt_meta_json_is_archived_again(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        with open(os.path.join(self.bdir(), "meta.json"), "w") as f:
            f.write("{corrupt")
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})

    def test_RB01_a_pending_build_leaves_no_folders(self):
        self.fetch("--max-job-hours", "200")  # nothing in GCS yet, and the job may still be running
        (rel,) = frl.run_dirs(self.root)
        self.assertEqual(sorted(os.listdir(os.path.join(self.root, rel))), ["README.md", "run.json"])

    def test_L06_L07_L14_symlinks_to_run_or_build_folders_are_not_followed(self):
        import shutil
        import tempfile
        outside = tempfile.mkdtemp()
        try:
            victim = os.path.join(outside, "2020-01-01T000000Z", "job", "1")
            os.makedirs(victim)
            open(os.path.join(victim, "build-log.txt"), "w").close()
            os.makedirs(os.path.join(self.root, "runs"))
            os.symlink(outside, os.path.join(self.root, "runs", "2020-01"))
            self.finished(ago=3 * DAY)
            self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
            self.fetch()
            os.symlink(self.bdir(), os.path.join(self.root, frl.run_dirs(self.root)[0], "job", "2"))
            self.assertTrue(os.path.exists(os.path.join(victim, "build-log.txt")))
            self.assertEqual(sorted(frl.archived_builds(self.root)[0]), [self.key])
        finally:
            shutil.rmtree(outside)

    def test_L08_a_cut_off_folder_with_stray_files_is_not_an_archived_build(self):
        d = os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", "9")
        os.makedirs(d)
        open(os.path.join(d, "notes.txt"), "w").close()
        self.assertEqual(frl.archived_builds(self.root)[0], {})

    def damage(self):
        with open(os.path.join(self.bdir(), "build-log.txt"), "wb") as f:
            f.write(b"XXXX")

    def off_testgrid(self):
        self.show([snowflake(time.time())], {"o": ([1], [""])}, start=int(time.time()))

    def test_RN01_a_given_up_build_is_not_rechecked(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.damage()
        m = self.meta()
        rec = {k: m[k] for k in ("job", "build", "query", "started", "created", "tabs")}
        state = frl.read_json(os.path.join(self.root, "state.json"))
        state["given_up"] = {self.key: {"rec": rec, "first_seen": time.time() - 20 * DAY, "last_error": "x"}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.off_testgrid()
        hits = self.srv.hits(self.base + "/build-log.txt")
        self.fetch()
        self.assertEqual(self.srv.hits(self.base + "/build-log.txt"), hits)

    def test_RN02_an_unresolved_archived_build_is_queued_once(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.damage()
        self.srv.route(self.base + "/build-log.txt", (503, {}, b""))
        self.fetch()
        self.assertIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.off_testgrid()
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})

    def test_RN03_a_final_build_is_not_queued_again(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.off_testgrid()
        self.fetch()
        self.assertEqual(self.runs()[-1]["rechecked"], 0)

    def test_L11_L12_a_cleaned_run_folder_goes_away(self):
        d = os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", "9")
        os.makedirs(d)
        open(os.path.join(d, ".build-log.txt.x.part"), "w").close()
        self.fetch()
        self.assertFalse(os.path.exists(os.path.join(self.root, "runs", "2020-01")))


class Backfill(ArchiveWorld):
    def test_RN05_a_gap_build_already_queued_for_retry_is_not_backfilled_too(self):
        now = time.time()
        visible, failed = snowflake(now - HOUR), snowflake(now - 30 * HOUR)
        self.show([visible], {"o": ([1], [""])}, start=int(now - HOUR))
        rec = {"job": "job", "build": failed, "query": "bucket/logs/job", "started": int(now - 30 * HOUR),
               "created": frl.created(failed), "tabs": [{"dashboard": "dash", "tab": "t", "red_cells": []}]}
        frl.write_json(os.path.join(self.root, "state.json"), {
            "tabs": {"dash#t": {"scan": now - 40 * HOUR, "oldest": now - 60 * HOUR}},
            "unresolved": {f"job/{failed}": {"rec": rec, "first_seen": now - DAY, "last_error": "x"}}})
        self.srv.route(f"/bucket/logs/job/{failed}/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(now)}))
        self.srv.route(f"/bucket/logs/job/{failed}/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{failed}/finished.json", f"logs/job/{visible}/finished.json"])
        self.fetch()
        outcomes = self.runs()[-1]["outcomes"]
        self.assertEqual(sum(v.count(f"job/{failed}") for v in outcomes.values()), 1, outcomes)


class Gap(ArchiveWorld):
    """BackfillTest's world: TestGrid shows one passing build from an hour ago; the last scan was 40 h ago."""

    def setUp(self):
        super().setUp()
        self.now = time.time()
        self.visible = snowflake(self.now - HOUR)
        frl.write_json(os.path.join(self.root, "state.json"),
                       {"tabs": {"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}}})

    def failed_build(self, b, finished=None, started=True):
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       finished or (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - HOUR)}))
        if started:
            self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 2 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log " + b.encode()))

    def listing(self, *builds):
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (*builds, self.visible)])

    def archived(self):
        return sorted(k for v in self.runs()[-1]["outcomes"].values() for k in v)

    def test_B01_a_build_created_just_before_the_floor_is_backfilled(self):
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 46 * HOUR - 1800)  # floor is 46 h ago; created 30 min before it
        self.failed_build(b)
        self.listing(b)
        self.fetch()
        self.assertEqual(self.archived(), [f"job/{b}"])

    def test_B02_a_red_build_testgrid_only_shows_as_a_placeholder_is_backfilled(self):
        ph = snowflake(self.now - 30 * HOUR)
        t = table("bucket/logs/job", [self.visible, ph], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 30 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)
        self.failed_build(ph)
        self.listing(ph)
        self.fetch()
        self.assertEqual(self.archived(), [f"job/{ph}"])  # its results are gone from TestGrid

    def test_B06_a_build_newer_than_the_oldest_column_is_not_backfilled(self):
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 1800)  # not on TestGrid yet
        self.failed_build(b)
        self.listing(b)
        self.fetch()
        self.assertEqual(self.archived(), [])

    def test_B04_B05_a_failed_finished_json_read_is_never_dropped(self):
        # the builds that cannot be read are errors and stay watched; the rest of the gap is taken
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        bad1, bad2, good = (snowflake(self.now - h * HOUR) for h in (30, 29, 28))
        for b in (bad1, bad2):
            self.failed_build(b, finished=(500, {}, b""))
        self.failed_build(good)
        self.listing(bad1, bad2, good)
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"].get("backfilled"), [f"job/{good}"])
        self.assertEqual(sorted(e["build"] for e in run["errors"]), sorted([f"job/{bad1}", f"job/{bad2}"]))
        self.assertEqual(sorted(frl.read_json(os.path.join(self.root, "state.json"))["watch"]),
                         sorted([f"job/{bad1}", f"job/{bad2}"]))

    def test_B03_an_aborted_build_testgrid_paints_red_is_backfilled_one_never_started_is_not(self):
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        aborted, never = snowflake(self.now - 30 * HOUR), snowflake(self.now - 29 * HOUR)
        self.failed_build(aborted, finished=(200, {}, {"result": "ABORTED", "passed": False,
                                                        "timestamp": int(self.now - HOUR)}))
        self.failed_build(never, started=False)  # crier wrote finished.json for a pod that never ran
        self.listing(aborted, never)
        self.fetch()
        self.assertEqual(self.archived(), [f"job/{aborted}"])

    def test_MD04_backfilled_builds_count_as_new_in_the_run_table(self):
        self.show([self.visible], {"o": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 30 * HOUR)
        self.failed_build(b)
        self.listing(b)
        self.fetch()
        row = [ln for ln in lines(os.path.join(self.root, "INDEX.md")) if ln.startswith("| [")][0]
        self.assertIn("/) | 1 | 0 |", row)


class Reports(ArchiveWorld):
    def test_MD05_a_failed_tab_counts_as_a_problem(self):
        self.srv.summary("dash", {"t": "FAILING", "u": "FAILING"})
        self.srv.table("dash", "u", (500, {}, b""))
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        row = [ln for ln in lines(os.path.join(self.root, "INDEX.md")) if ln.startswith("| [")][0]
        self.assertIn("| 1 problems |", row)

    def test_MD08_an_earlier_run_readme_shows_the_settled_copy(self):
        self.finished(ago=60)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        first = frl.run_dirs(self.root)[0]
        self.assertIn("(provisional)", slurp(os.path.join(self.root, first, "README.md")))
        self.finished(ago=2 * HOUR)
        self.fetch()
        self.assertNotIn("(provisional)", slurp(os.path.join(self.root, first, "README.md")))

    def test_MD06_MD09_readme_lists_only_its_own_builds_and_old_records_are_not_provisional(self):
        e = {"time": time.time() - HOUR, "job": "j", "build": "1", "tabs": ["d#t"], "red_tests": [], "files": ["x/y"],
             "remote": [], "settled": None, "prow_url": "u", "dir": "runs/2026-10/2026-10-01T000000Z-2/j/1"}
        os.makedirs(os.path.join(self.root, "runs/2026-10/2026-10-01T000000Z"))
        frl.write_run_readme(self.root, "runs/2026-10/2026-10-01T000000Z", None, [e])
        self.assertIn("(0)", slurp(os.path.join(self.root, "runs/2026-10/2026-10-01T000000Z/README.md")))
        self.assertNotIn("provisional", "\n".join(frl.build_rows([e])))

    def test_C20_a_red_build_that_finished_hours_before_the_run_is_missed(self):
        start = int(time.time()) - 8 * HOUR
        b = snowflake(start)
        self.show([b], {"o": ([1], [""])}, start=start)
        self.srv.gcs_listing("bucket", [])
        self.fetch()
        self.show([b], {"o": ([12], [""])}, start=start)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "FAILURE", "timestamp": int(time.time()) - 5 * HOUR}))
        code, out = self.check()
        self.assertIn("MISSED red build", out)

    def test_C05_an_object_gcs_deleted_is_not_a_verification_problem(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.fetch()
        del self.srv.httpd.routes[self.base + "/build-log.txt"]
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_C13_C14_an_unreachable_finished_json_of_a_red_build_is_a_problem(self):
        start = int(time.time()) - 2 * HOUR
        b = snowflake(start)
        self.show([b], {"o": ([1], [""])}, start=start)  # not red when the fetch runs
        self.fetch()
        self.show([b], {"o": ([12], [""])}, start=start)  # red by verification time
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (500, {}, b""))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("cannot check red build", out)

    def test_C15_an_unreadable_finished_json_in_gcs_is_only_noted(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        other = snowflake(time.time() - 5 * HOUR)
        self.srv.route(f"/bucket/logs/job/{other}/finished.json", (500, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (self.build, other)])
        self.fetch()
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn("cannot read finished.json", out)


class Checkpoints(ArchiveWorld):
    def test_RN08_a_damaged_state_json_alone_fails_the_run(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        with open(os.path.join(self.root, "state.json"), "w") as f:
            f.write("{corrupt")
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(self.runs()[-1]["serious_warnings"])

    def test_RN11_RN12_a_kill_while_drawing_keeps_state_and_report(self):
        with mock.patch.object(frl.report, "write_charts", side_effect=KeyboardInterrupt):
            self.assertEqual(self.fetch("--max-job-hours", "200"), 130)
        self.assertIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])
        self.assertIn("the run stopped before it finished", [e["error"] for e in self.runs()[-1]["errors"]])

    def test_C16_a_record_without_a_settled_flag_is_checked_strictly(self):
        self.finished(ago=2 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"first\n", "1"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.fetch()
        m = self.meta()
        m.pop("settled")
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.srv.route(self.base + "/build-log.txt", obj(b"second\n", "2"))
        self.assertEqual(self.check()[0], 1)

    def test_C17_an_unsettled_copy_missing_on_disk_is_a_problem(self):
        self.finished(ago=60)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json"])
        self.fetch()
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        code, out = self.check()
        self.assertEqual(code, 1, out)


if __name__ == "__main__":
    unittest.main()
