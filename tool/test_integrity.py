"""Tests for strict input checks, content integrity over time, the per-run layout,
history-gap backfill, and hostile or odd input."""
import contextlib
import copy
import datetime as dt
import glob
import gzip
import hashlib
import io
import json
import os
import random
import re
import shutil
import sys
import tempfile
import threading
import time
import tracemalloc
import unittest
import urllib.error
import xml.etree.ElementTree as ET
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, md5_b64, run_record, send, table  # noqa: E402

HOUR = 3600


def snowflake(t):
    """A Prow build ID created at Unix time t."""
    return str((int(t * 1000) - frl.PROW_EPOCH_MS) << 22)


def obj(data, generation="1", calls=None):
    """A GCS object route that records each request's method in `calls`."""
    def serve(h):
        if calls is not None:
            calls.append(h.command)
        send(h, 200, {"x-goog-stored-content-encoding": "identity", "x-goog-stored-content-length": str(len(data)),
                      "x-goog-hash": f"crc32c=AAAAAA==,md5={md5_b64(data)}", "x-goog-generation": generation,
                      "Content-Length": str(len(data))}, data)
    return serve



def slurp(path):
    with open(path, encoding="utf-8") as f:
        return f.read()

class StrictTableTest(unittest.TestCase):
    GOOD = table("bucket/logs/job", ["3", "2", "1"], {"t": ([1, 12, 1], ["", "x", ""])})

    def variant(self, **changes):
        t = copy.deepcopy(self.GOOD)
        for path, value in changes.items():
            if path == "runs":
                t["tests"][0]["statuses"] = value
            elif path == "row":
                t["tests"][0] = value
            elif path in ("messages", "name"):
                t["tests"][0][path] = value
            else:
                t[path] = value
        return t

    def test_the_good_table_decodes(self):
        self.assertEqual(sorted(frl.red_columns(self.GOOD)), [1])
        self.assertEqual([cells for _, _, cells in cc.columns(self.GOOD)], [{1}, {12}, {1}])

    def test_invalid_run_length_encodings_are_rejected_by_both_decoders(self):
        cases = {
            "negative count": [{"value": 1, "count": -1}, {"value": 12, "count": 1}, {"value": 1, "count": 3}],
            "zero count": [{"value": 1, "count": 0}, {"value": 12, "count": 3}],
            "bool count": [{"value": 12, "count": True}, {"value": 1, "count": 2}],
            "float count": [{"value": 12, "count": 1.0}, {"value": 1, "count": 2}],
            "string count": [{"value": 12, "count": "3"}],
            "float status": [{"value": 12.0, "count": 1}, {"value": 1, "count": 2}],
            "unknown status": [{"value": 99, "count": 3}],
            "negative status": [{"value": -1, "count": 3}],
            "bool status": [{"value": True, "count": 3}],
            "too wide": [{"value": 1, "count": 4}],
            "too narrow": [{"value": 1, "count": 2}],
            "empty row": [],
        }
        for name, runs in cases.items():
            t = self.variant(runs=runs)
            with self.subTest(name), self.assertRaises(ValueError):
                frl.red_columns(t)
            with self.subTest("cross_check " + name), self.assertRaises((ValueError, KeyError, TypeError)):
                list(cc.columns(t))

    def test_invalid_table_shapes_are_rejected(self):
        cases = {
            "run not an object": self.variant(runs=[[1, 3]]),
            "statuses not a list": self.variant(runs={"value": 1, "count": 3}),
            "row not an object": self.variant(row=["t"]),
            "name not a string": self.variant(name=["x"]),
            "name with a lone surrogate": self.variant(name="bad\ud800"),
            "messages a string": self.variant(messages="xyz"),
            "a message not a string": self.variant(messages=["", 5, ""]),
            "tests not a list": self.variant(tests={"t": 1}),
            "changelists a string": self.variant(changelists="321"),
            "a build listed twice": self.variant(changelists=["3", "3", "1"]),
            "timestamps too short": self.variant(timestamps=[1, 2]),
            "not an object": ["table"],
        }
        for name, t in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                frl.red_columns(t)

    def test_the_reported_case_no_longer_picks_column_minus_one(self):
        t = {"query": "b/logs/job", "changelists": ["3003", "3002", "3001"], "timestamps": [3, 2, 1],
             "tests": [{"name": "t", "statuses": [{"value": 1, "count": -1}, {"value": 12, "count": 1},
                                                  {"value": 1, "count": 3}], "messages": ["", "", ""]}]}
        with self.assertRaises(ValueError):
            frl.red_columns(t)

    def test_queries_that_could_leave_the_bucket_or_redirect_requests_are_rejected(self):
        now = time.time()
        for query in ("bucket/logs/job#/x", "bucket/../other/job", "bucket/logs/./job", "bucket/logs/job?x=1",
                      "Bucket/logs/job", "bucket", "bucket/logs/job/", "bucket/logs/jo b", "bucket/logs/job,x"):
            t = dict(table(query.rsplit("/", 1)[0] + "/job", ["1"], {"o": ([12], [""])}), query=query)
            with self.subTest(query), self.assertRaises(ValueError):
                frl.read_tab(t, now)

    def test_a_column_dated_before_its_build_existed_counts_as_undated(self):
        # TestGrid dates a column it could not read by an earlier one; such a start is not the build's
        now = time.time()
        build = snowflake(now - HOUR)
        t = table("bucket/logs/job", [build], {"o": ([12], [""])}, start=1000)
        _, _, oldest, found, _ = frl.read_tab(t, now)
        self.assertEqual([(b, start) for b, start, _ in found], [(build, None)])  # dated by its build ID
        self.assertIsNone(oldest)  # and it says nothing about how far back the tab reaches

    def test_a_bad_build_id_in_any_column_fails_the_tab(self):
        t = table("bucket/logs/job", ["2", "abc"], {"o": ([12, 1], ["", ""])})
        with self.assertRaisesRegex(ValueError, "unexpected build id"):
            frl.read_tab(t, time.time())

    def test_placeholder_columns_do_not_count_as_history(self):
        now = time.time()
        builds = [snowflake(now - h * HOUR) for h in (1, 2, 30)]
        t = table("bucket/logs/job", builds, {"a": ([1, 1, 6], ["", "", "grid exceeds maximum size"]),
                                              "b": ([12, 1, 0], ["x", "", ""])})
        t["timestamps"] = [int((now - h * HOUR) * 1000) for h in (1, 2, 30)]
        _, _, oldest, _, shown = frl.read_tab(t, now)
        self.assertAlmostEqual(oldest, now - 2 * HOUR, delta=1)
        self.assertEqual(shown, set(builds[:2]))

    def test_columns_agree_with_a_plain_decoder_on_random_tables(self):
        rng = random.Random(1)
        for n in range(1500):
            width = rng.randrange(0, 60)
            sparse = n % 2 == 0  # long runs half of the time, cell-by-cell the rest
            raw = []
            for _ in range(rng.randrange(0, 30)):
                row, value = [], rng.randrange(16)
                for _ in range(width):
                    if not sparse or rng.random() < 0.05:
                        value = rng.randrange(16)
                    row.append(value)
                raw.append(row)
            builds = [str(i + 1) for i in range(width)]
            t = {"query": "b/logs/job", "changelists": builds,
                 "timestamps": [0 if i % 7 == 0 else 1_700_000_000_000 + i for i in range(width)],
                 "tests": [{"name": str(i), "statuses": self.rle(r)} for i, r in enumerate(raw)]}
            want = [(b, (t["timestamps"][i] // 1000 if t["timestamps"][i] else cc.created(b)), {r[i] for r in raw})
                    for i, b in enumerate(builds)]
            self.assertEqual(list(cc.columns(t)), want, n)
            reds = {i for i in range(width) if any(r[i] in frl.RED for r in raw)}
            self.assertEqual(set(frl.red_columns(t)), reds, n)

    @staticmethod
    def rle(cells):
        out = []
        for v in cells:
            if out and out[-1]["value"] == v:
                out[-1]["count"] += 1
            else:
                out.append({"value": v, "count": 1})
        return out


class GunzipTest(unittest.TestCase):
    def test_an_empty_stream_is_not_a_valid_gzip_stream(self):
        with self.assertRaises(frl.IntegrityError):
            frl.Gunzip().finish()
        with self.assertRaises(frl.IntegrityError):
            list(frl.gunzipped([b""]))

    def test_zeros_before_any_member_are_rejected(self):
        with self.assertRaises(frl.IntegrityError):
            list(frl.Gunzip().feed(b"\0" * 32))

    def test_garbage_after_a_member_is_rejected(self):
        with self.assertRaises(frl.IntegrityError):
            list(frl.gunzipped([gzip.compress(b"abc") + b"garbage"]))

    def test_a_cut_member_is_rejected(self):
        with self.assertRaises(frl.IntegrityError):
            list(frl.gunzipped([gzip.compress(b"abc" * 100)[:-1]]))

    def test_output_comes_in_bounded_steps_whatever_the_ratio(self):
        size = 64 << 20
        bomb = gzip.compress(b"\0" * size, compresslevel=9)
        pieces = tracemalloc.start() or []
        total = 0
        for piece in frl.gunzipped(bomb[i:i + (64 << 10)] for i in range(0, len(bomb), 64 << 10)):
            self.assertLessEqual(len(piece), frl.GUNZIP_STEP)
            total += len(piece)
            pieces.append(len(piece)) if len(pieces) < 3 else None
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        self.assertEqual(total, size)
        self.assertLess(peak, 4 << 20, f"peak {peak} bytes for a {len(bomb)} byte stream")

    def test_random_members_and_splits_match_gzip(self):
        rng = random.Random(7)
        for n in range(300):
            members = []
            for _ in range(rng.randrange(1, 4)):
                kind = rng.randrange(3)
                data = (b"x" * rng.randrange(0, 900_000) if kind == 0 else
                        rng.randbytes(rng.randrange(0, 5000)) if kind == 1 else
                        b"line %d\n" * 0 + b"".join(b"line %d\n" % i for i in range(rng.randrange(0, 20_000))))
                members.append(data)
            stream = b"".join(gzip.compress(m, compresslevel=rng.choice([1, 6, 9])) for m in members)
            if rng.random() < 0.2:
                stream += b"\0" * rng.randrange(1, 40)
            cuts = sorted(rng.sample(range(1, len(stream)), min(len(stream) - 1, rng.randrange(0, 8))))
            chunks = [stream[a:b] for a, b in zip([0, *cuts], [*cuts, len(stream)], strict=True)]
            self.assertEqual(b"".join(frl.gunzipped(chunks)), b"".join(members), n)

    def test_an_empty_local_gz_file_is_not_intact(self):
        with tempfile.TemporaryDirectory() as d:
            open(os.path.join(d, "build-log.txt.gz"), "wb").close()
            meta = {"log": {"file": "build-log.txt.gz", "file_bytes": 0, "bytes": 0, "md5": hashlib.md5(b"").hexdigest()}}
            self.assertTrue(frl.intact(d, meta))  # the size matches ...
            self.assertFalse(frl.intact(d, meta, deep=True))  # ... but it is not a gzip file


class HttpTest(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer()
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)

    def tearDown(self):
        self.srv.close()
        frl.bind_context(frl._DEFAULT_CONTEXT)

    def test_retry_after_is_honoured(self):
        calls = []

        def throttled(h):
            calls.append(time.monotonic())
            send(h, 503, {"Retry-After": "1"}, b"") if len(calls) == 1 else send(h, 200, {}, b"ok")
        self.srv.route("/x", throttled)
        self.assertEqual(frl.request(self.srv.url + "/x"), b"ok")
        self.assertGreaterEqual(calls[1] - calls[0], 0.9)

    def test_retry_after_is_capped(self):
        calls = []

        def throttled(h):
            calls.append(time.monotonic())
            send(h, 429, {"Retry-After": "3600"}, b"") if len(calls) == 1 else send(h, 200, {}, b"ok")
        self.srv.route("/x", throttled)
        with mock.patch.object(frl, "RETRY_AFTER_CAP", 0.2):
            self.assertEqual(frl.request(self.srv.url + "/x"), b"ok")
        self.assertLess(calls[1] - calls[0], 1.5)

    def test_a_wait_that_would_outlast_the_run_is_not_started(self):
        self.ctx.deadline = time.monotonic() + 0.5
        self.srv.route("/x", (503, {"Retry-After": "30"}, b""))
        t0 = time.monotonic()
        with self.assertRaises(urllib.error.HTTPError):
            frl.request(self.srv.url + "/x")
        self.assertLess(time.monotonic() - t0, 0.4)
        self.assertEqual(self.srv.hits("/x"), 1)

    def test_abandoning_the_run_ends_a_wait_at_once(self):
        self.srv.route("/x", (503, {"Retry-After": "30"}, b""))
        threading.Timer(0.3, self.ctx.abandoned.set).start()
        t0 = time.monotonic()
        with self.assertRaises(frl.Abandoned):
            frl.request(self.srv.url + "/x")
        self.assertLess(time.monotonic() - t0, 2)

    def test_redirects_stay_on_the_same_host(self):
        other = FakeServer()
        try:
            other.route("/x", (200, {}, b"from another host"))
            self.srv.route("/away", (302, {"Location": other.url + "/x"}, b""))
            with self.assertRaises(urllib.error.HTTPError):
                frl.request(self.srv.url + "/away")
            self.assertEqual(other.hits("/x"), 0)
            self.srv.route("/a", (302, {"Location": "/b"}, b""))
            self.srv.route("/b", (200, {}, b"same host"))
            self.assertEqual(frl.request(self.srv.url + "/a"), b"same host")
        finally:
            other.close()

    def test_json_constants_deep_nesting_and_size_are_refused(self):
        self.srv.route("/nan", (200, {}, b'{"a": NaN}'))
        self.srv.route("/deep", (200, {}, b"[" * 200_000 + b"]" * 200_000))
        self.srv.route("/big", (200, {}, b'{"a": "' + b"x" * 5000 + b'"}'))
        for path in ("/nan", "/deep"):
            with self.subTest(path), self.assertRaises((ValueError, frl.IntegrityError)):
                frl.get_json(self.srv.url + path)
        with mock.patch.object(frl, "MAX_JSON_BYTES", 1000), self.assertRaisesRegex(frl.IntegrityError, "limit"):
            frl.get_json(self.srv.url + "/big")

    def test_malformed_checksum_and_length_headers_are_integrity_errors(self):
        for headers in ({"x-goog-hash": "md5=!!notbase64!!"}, {"x-goog-hash": "md5=AAAA"},
                        {"x-goog-stored-content-length": "12a"}, {"x-goog-stored-content-length": "-1"},
                        {"Content-Length": "abc"}):
            with self.subTest(headers):
                class H(dict):
                    def get_all(self, name):
                        return [self[name]] if name in self else None
                with self.assertRaises(frl.IntegrityError):
                    frl.remote_identity(H(headers))
                    frl.header_int(H(headers), "Content-Length")


class ArchiveWorld(unittest.TestCase):
    """One red build, started three hours ago, on one tab."""

    def setUp(self):
        self.srv = FakeServer()
        self.root = tempfile.mkdtemp()
        self.start = int(time.time()) - 4 * DAY
        self.build = snowflake(self.start)
        self.key = f"job/{self.build}"
        self.base = f"/bucket/logs/job/{self.build}"
        self.srv.summary("dash", {"t": "FAILING"})
        self.show([self.build], {"o": ([12], ["boom"])})
        self.srv.gcs_listing("bucket", [])

    def tearDown(self):
        self.srv.close()
        shutil.rmtree(self.root)

    def show(self, builds, rows, start=None):
        self.srv.table("dash", "t", table("bucket/logs/job", builds, rows, start=(start or self.start) * 1000))

    def finished(self, ago, result="FAILURE"):
        self.srv.route(self.base + "/finished.json", (200, {}, {"result": result, "timestamp": int(time.time() - ago)}))

    def fetch(self, *extra):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url,
                             "--gcs", self.srv.url, *extra])

    def check(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "200"])
        return code, out.getvalue()

    def bdir(self, build=None):
        found = glob.glob(os.path.join(self.root, "runs", "*", "*", "job", build or self.build))
        return found[0] if found else None

    def meta(self, build=None):
        return frl.read_json(os.path.join(self.bdir(build), "meta.json"))

    def runs(self):
        return [frl.read_json(os.path.join(self.root, rel, "run.json")) for rel in frl.run_dirs(self.root)]

    def read(self, name, build=None):
        with open(os.path.join(self.bdir(build), name), "rb") as f:
            return f.read()


class ContentOverTimeTest(ArchiveWorld):
    def test_a_log_replaced_in_gcs_is_fetched_again_in_place(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"first upload\n", "1"))
        self.assertEqual(self.fetch(), 0)
        folder, m = self.bdir(), self.meta()
        self.assertEqual((m["settled"], m["final"]), (True, False))
        self.assertEqual(m["log"]["gcs"]["generation"], "1")
        self.srv.route(self.base + "/build-log.txt", obj(b"final upload, longer\n", "2"))
        self.assertEqual(self.fetch(), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"refreshed": [self.key]})
        self.assertEqual(self.bdir(), folder)  # stays in the folder of the run that first archived it
        self.assertEqual(self.read("build-log.txt"), b"final upload, longer\n")
        m = self.meta()
        self.assertEqual((m["refreshed"], m["refreshed_by_run"], m["log"]["gcs"]["generation"]),
                         (["build-log.txt"], run["run"], "2"))
        self.assertEqual(m["archived_by_run"], self.runs()[0]["run"])
        readme = slurp(os.path.join(self.root, run["dir"], "README.md"))
        self.assertIn("## Archived earlier, updated by this run", readme)
        self.assertEqual(self.check()[0], 0)

    def test_the_same_content_under_a_new_generation_is_not_fetched_again(self):
        calls = []
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1", calls))
        self.fetch()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "7", calls))
        self.fetch()
        self.assertEqual(calls, ["GET", "HEAD"])
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})

    def test_a_final_build_is_not_compared_again(self):
        calls = []
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1", calls))
        self.fetch()
        self.assertTrue(self.meta()["final"])
        hits = self.srv.hits(self.base + "/finished.json")
        self.fetch()
        self.assertEqual(calls, ["GET"])
        self.assertEqual(self.srv.hits(self.base + "/finished.json"), hits)

    def test_a_copy_taken_right_after_the_build_finished_is_provisional_until_it_settles(self):
        self.finished(ago=60)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.assertFalse(self.meta()["settled"])
        index = slurp(os.path.join(self.root, "INDEX.md"))
        self.assertIn("(provisional)", index)
        self.finished(ago=3 * HOUR)  # time passes
        self.fetch()
        self.assertTrue(self.meta()["settled"])
        self.assertNotIn("(provisional)", slurp(os.path.join(self.root, "INDEX.md")))

    def test_finished_times_that_cannot_be_right_never_make_a_copy_final(self):
        for bogus in (1, self.start - 3 * DAY):  # 1970, and days before the build was created
            with self.subTest(bogus):
                shutil.rmtree(os.path.join(self.root, "runs"), ignore_errors=True)
                self.srv.route(self.base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": bogus}))
                self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
                self.fetch()
                self.assertEqual((self.meta()["settled"], self.meta()["final"]), (False, False))

    def test_finished_json_values_that_cannot_be_stored_are_dropped(self):
        for body in (b'{"result": "FAILURE", "timestamp": 1e999}', b'{"result": "\\ud800", "timestamp": 5}',
                     b'{"result": ["x"], "timestamp": "soon"}'):
            with self.subTest(body):
                shutil.rmtree(os.path.join(self.root, "runs"), ignore_errors=True)
                self.srv.route(self.base + "/finished.json", (200, {}, body))
                self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
                self.fetch()
                m = self.meta()
                self.assertIsNotNone(m, body)
                self.assertTrue(m["log"]["verified"])
                self.assertEqual(m["final"], False)

    def test_a_copy_that_never_settles_is_shown_as_such_once_final(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        m["archived_at"] = time.time() - 15 * DAY
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.fetch()
        self.assertEqual((self.meta()["settled"], self.meta()["final"]), (False, True))
        self.assertIn("(never settled", slurp(os.path.join(self.root, "INDEX.md")))

    def test_a_log_archived_before_finished_json_existed_is_followed_until_it_settles(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        self.assertEqual((m["result"], m["settled"], m["final"]), (None, False, False))
        self.show([snowflake(time.time())], {"o": ([1], [""])}, start=int(time.time()))  # off TestGrid now
        self.finished(ago=3 * HOUR)
        self.fetch()
        m = self.meta()
        self.assertEqual((m["result"], m["settled"], m["final"]), ("FAILURE", True, False))

    def test_a_podinfo_json_that_appears_later_is_fetched(self):
        self.finished(ago=3 * HOUR)
        self.fetch()
        self.assertIsNone(self.meta()["podinfo"])
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": "pending"}'))
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})
        self.assertEqual(self.read("podinfo.json"), b'{"pod": "pending"}')

    def test_a_podinfo_json_changed_in_gcs_is_fetched_again(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Pending"}', "1"))
        self.fetch()
        self.srv.route(self.base + "/podinfo.json", obj(b'{"phase": "Failed"}', "2"))
        self.fetch()
        self.assertEqual(self.read("podinfo.json"), b'{"phase": "Failed"}')
        self.assertEqual(self.meta()["refreshed"], ["podinfo.json"])

    def test_a_record_from_before_identities_is_compared_once_without_downloading(self):
        calls = []
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n", "1", calls))
        self.fetch()
        m = self.meta()
        for k in ("settled", "final", "archived_at"):
            m.pop(k)
        m["log"].pop("gcs")
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.fetch()
        self.assertEqual(calls, ["GET", "HEAD"])
        self.assertEqual((self.meta()["settled"], self.meta()["final"]), (True, True))

    def test_a_failed_comparison_keeps_the_copy_and_tries_again(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed", run["errors"][0]["error"])
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])
        self.assertEqual(self.read("build-log.txt"), b"log\n")
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)

    def test_cross_check_notes_but_does_not_fail_on_an_unsettled_copy(self):
        self.finished(ago=60)
        self.srv.route(self.base + "/build-log.txt", obj(b"first\n"))
        self.fetch()
        self.srv.route(self.base + "/build-log.txt", obj(b"second\n", "2"))
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn("not settled yet", out)

    def test_cross_check_fails_on_a_changed_settled_podinfo(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"a": 1}', "1"))
        self.fetch()
        self.assertEqual(self.check()[0], 0)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"a": 2}', "2"))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("podinfo.json: GCS changed since it was archived", out)

    def test_cross_check_compares_an_old_gzip_record_by_content(self):
        from test_fetch_red_logs import gzip_stored
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", gzip_stored(b"evidence\n"))
        self.fetch()
        m = self.meta()
        m["log"].pop("gcs")  # as written before identities were kept
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.assertEqual(self.check()[0], 0)
        self.srv.route(self.base + "/build-log.txt", gzip_stored(b"changed evidence\n"))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("gunzipped md5 differs", out)


class LayoutTest(ArchiveWorld):
    def test_each_run_keeps_what_it_archived_in_its_own_folder(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"first\n"))
        self.fetch()
        second = snowflake(self.start + HOUR)
        self.srv.route(f"/bucket/logs/job/{second}/finished.json",
                       (200, {}, {"result": "FAILURE", "timestamp": self.start + 2 * HOUR}))
        self.srv.route(f"/bucket/logs/job/{second}/build-log.txt", obj(b"second\n"))
        self.show([second, self.build], {"o": ([12, 12], ["", ""])})
        self.fetch()
        one, two = frl.run_dirs(self.root)
        self.assertEqual(os.path.relpath(self.bdir(), self.root), f"{one}/job/{self.build}")
        self.assertEqual(os.path.relpath(self.bdir(second), self.root), f"{two}/job/{second}")
        readme = slurp(os.path.join(self.root, two, "README.md"))
        self.assertIn(f"job {second}", readme)
        self.assertNotIn(f"job {self.build}", readme)
        index = slurp(os.path.join(self.root, "INDEX.md"))
        runs = index.split("## Runs in the last")[1].split("\n## ")[0]
        self.assertLess(runs.index(f"]({two}/) | 1 | 0 | ok |"), runs.index(f"]({one}/) | 1 | 0 | ok |"))

    def test_run_folder_names_never_collide_and_sort_in_order(self):
        t = time.time()
        made = [frl.new_run_dir(self.root, t) for _ in range(12)]
        self.assertEqual(made[1][0], made[0][0] + "-2")
        self.assertEqual(frl.run_dirs(self.root), [rel for _, rel in made])

    def old_layout(self, old_id="20261001T120000.000000Z", build=None):
        """An archive as the code before run folders wrote it: logs/<job>/<build>/ and runs/<id>.json."""
        build = build or self.build
        d = os.path.join(self.root, "logs", "job", build)
        os.makedirs(d)
        os.makedirs(os.path.join(self.root, "runs"), exist_ok=True)
        with open(os.path.join(d, "build-log.txt"), "wb") as f:
            f.write(b"log\n")
        frl.write_json(os.path.join(d, "meta.json"), {
            "job": "job", "build": build, "query": "bucket/logs/job", "started": self.start, "created": self.start,
            "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING", "red_cells": []}],
            "result": "FAILURE", "finished": self.start + 600, "log_url": f"{self.srv.url}{self.base}/build-log.txt",
            "log": {"bytes": 4, "md5": hashlib.md5(b"log\n").hexdigest(), "verified": True, "file": "build-log.txt",
                    "file_bytes": 4}, "podinfo": None, "archived_by_run": old_id})
        frl.write_json(os.path.join(self.root, "runs", f"{old_id}.json"),
                       run_record(dt.datetime.strptime(old_id, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=dt.UTC).timestamp(),
                                  []) | {"run": old_id, "outcomes": {"archived": [f"job/{build}"]}})

    def test_the_old_layout_is_moved_into_run_folders(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.old_layout()
        self.assertEqual(self.fetch(), 0)
        self.assertFalse(os.path.exists(os.path.join(self.root, "logs")))
        self.assertEqual(os.path.relpath(self.bdir(), self.root), f"runs/2026-10/2026-10-01T120000Z/job/{self.build}")
        m = self.meta()
        self.assertEqual((m["archived_by_run"], self.read("build-log.txt")), ("2026-10-01T120000Z", b"log\n"))
        moved = frl.read_json(os.path.join(self.root, "runs/2026-10/2026-10-01T120000Z/run.json"))
        self.assertEqual((moved["run"], moved["migrated_from"]), ("2026-10-01T120000Z", "20261001T120000.000000Z"))
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertIn(f"runs/2026-10/2026-10-01T120000Z/job/{self.build}/build-log.txt",
                      slurp(os.path.join(self.root, "INDEX.md")))

    def test_old_layout_left_by_a_late_old_run_joins_its_migrated_runs(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.old_layout()
        self.fetch()
        # an old-code run pushed one more build after the migration (a rebase merges it cleanly)
        other = snowflake(self.start + HOUR)
        self.old_layout(build=other)
        os.remove(os.path.join(self.root, "runs", "20261001T120000.000000Z.json"))  # its report was migrated already
        self.fetch()
        self.assertEqual(os.path.relpath(self.bdir(other), self.root), f"runs/2026-10/2026-10-01T120000Z/job/{other}")
        self.assertEqual(len(frl.run_dirs(self.root)), 3)  # no second folder for the same old run

    def test_a_migration_cut_off_before_the_reports_moved_finishes_in_the_same_folder(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.old_layout()
        real = frl.write_json

        def cut_off(path, data):  # killed after the builds moved, before the run report did
            if path.endswith("run.json") and "migrated_from" in data:
                raise KeyboardInterrupt
            return real(path, data)
        with mock.patch.object(frl, "write_json", side_effect=cut_off), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(self.fetch(), 130)
        self.assertTrue(os.path.exists(os.path.join(self.root, "runs", "20261001T120000.000000Z.json")))
        self.fetch()
        self.assertEqual(os.path.relpath(self.bdir(), self.root), f"runs/2026-10/2026-10-01T120000Z/job/{self.build}")
        report = frl.read_json(os.path.join(self.root, "runs/2026-10/2026-10-01T120000Z/run.json"))
        self.assertEqual(report["migrated_from"], "20261001T120000.000000Z")
        self.assertFalse(os.path.exists(os.path.join(self.root, "runs/2026-10/2026-10-01T120000Z-2")))

    def test_an_old_build_that_cannot_be_placed_stays_and_fails_the_run(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()  # the build is in a run folder already ...
        self.old_layout()  # ... and an old copy of it turns up
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(os.path.exists(os.path.join(self.root, "logs", "job", self.build, "meta.json")))
        self.assertTrue(any("old layout" in w for w in self.runs()[-1]["serious_warnings"]))

    def test_a_cut_off_folder_with_unexpected_files_is_reported_not_deleted(self):
        d = os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", "9")
        os.makedirs(d)
        open(os.path.join(d, "notes.txt"), "w").close()
        self.finished(ago=3 * DAY)
        self.fetch()
        self.assertTrue(os.path.exists(os.path.join(d, "notes.txt")))
        self.assertTrue(any("unexpected files" in w for w in self.runs()[-1]["warnings"]))

    def test_two_copies_of_one_build_are_reported(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        copy_to = os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", self.build)
        shutil.copytree(self.bdir(), copy_to)
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(any("duplicate copies" in e["error"] for e in self.runs()[-1]["errors"]))
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("two copies of one build", out)

    def test_symlinks_in_the_archive_are_not_followed(self):
        outside = tempfile.mkdtemp()
        try:
            victim = os.path.join(outside, ".victim.part")
            open(victim, "w").close()
            os.makedirs(os.path.join(self.root, "runs"))
            os.symlink(outside, os.path.join(self.root, "runs", "2020-01"))
            os.symlink(self.root, os.path.join(self.root, "loop"))  # a loop must not hang the sweep
            self.finished(ago=3 * DAY)
            t0 = time.monotonic()
            self.fetch()
            self.assertLess(time.monotonic() - t0, 30)
            self.assertTrue(os.path.exists(victim))
            self.assertNotIn("runs/2020-01", " ".join(frl.run_dirs(self.root)))
        finally:
            shutil.rmtree(outside)

    def test_a_disk_error_on_rename_is_local_and_not_retried(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        real = os.replace

        def full(src, dst):
            if "build-log" in dst:
                raise OSError(28, "No space left on device")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", side_effect=full):
            self.assertEqual(self.fetch("--start-concurrency", "8", "--max-concurrency", "8"), 1)
        run = self.runs()[-1]
        self.assertIn("LocalIOError", run["errors"][0]["error"])
        self.assertEqual(self.srv.hits(self.base + "/build-log.txt"), 1)
        self.assertEqual(run["concurrency"]["pushbacks"], 0)

    def test_an_oversized_download_is_refused(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"x" * 100))
        with mock.patch.object(frl, "MAX_DOWNLOAD_BYTES", 50):
            self.assertEqual(self.fetch(), 1)
        self.assertIn("byte limit", self.runs()[-1]["errors"][0]["error"])

    def test_unusable_tab_names_fail_only_their_entry(self):
        self.srv.summary("dash", {"t": "FAILING", "bad\ud800": "FAILING", "": "FAILING", "two\nlines": "FAILING",
                                  "x" * 400: "FAILING"})
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"archived": [self.key]})
        self.assertEqual(sum("unusable tab name" in (t.get("error") or "") for t in run["tabs"]), 4)
        json.dumps(run, allow_nan=False).encode("utf-8")  # everything written is valid UTF-8 JSON


class BackfillTest(ArchiveWorld):
    def setUp(self):
        super().setUp()
        now = time.time()
        self.now = now
        # TestGrid now shows one passing build from an hour ago; the last scan was 40 hours ago.
        self.visible = snowflake(now - HOUR)
        self.show([self.visible], {"o": ([1], [""])}, start=int(now - HOUR))
        frl.write_json(os.path.join(self.root, "state.json"),
                       {"tabs": {"dash#t": {"scan": now - 40 * HOUR, "oldest": now - 60 * HOUR}}})
        self.failed, self.passed, self.running, self.too_old, self.crier, self.aborted, self.lower = (
            snowflake(now - h * HOUR) for h in (30, 20, 10, 60, 25, 26, 27))  # too_old: before the search window
        fins = {self.failed: ("FAILURE", False), self.passed: ("SUCCESS", True), self.too_old: ("FAILURE", False),
                self.crier: ("failure", False), self.aborted: ("ABORTED", False), self.lower: ("failure", False)}
        for b, (result, passed) in fins.items():
            self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                           (200, {}, {"result": result, "passed": passed, "timestamp": int(now)}))
        for b in (self.failed, self.passed, self.running, self.too_old, self.aborted, self.lower):
            self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(now)}))
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log of " + b.encode()))
        # self.crier: a pod that never started (no started.json): TestGrid never lists it
        self.listing = [f"logs/job/{b}/finished.json"
                        for b in (self.failed, self.passed, self.running, self.too_old, self.visible, self.crier,
                                  self.aborted, self.lower)]
        self.red = sorted(f"job/{b}" for b in (self.failed, self.aborted, self.lower))

    def test_red_builds_in_a_history_gap_are_taken_from_gcs(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.assertEqual(self.fetch(), 0)  # every build in the gap could be looked up
        run = self.runs()[-1]
        # TestGrid's rule, not the result string
        self.assertEqual(run["outcomes"], {"backfilled": self.red, "watching": [f"job/{self.running}"]})
        self.assertTrue(any("3 red builds from that time were taken from it" in w for w in run["warnings"]))
        self.assertEqual(run["serious_warnings"], [])
        self.assertIn("by TestGrid's rule it was red (it did not pass", self.meta(self.failed)["backfilled"])
        for b in (self.passed, self.running, self.too_old, self.visible, self.crier):
            self.assertIsNone(self.bdir(b), b)
        self.assertIn("taken from GCS: red by TestGrid's rule", slurp(os.path.join(self.root, "INDEX.md")))

    def test_a_build_still_running_in_the_gap_is_judged_once_it_finishes(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.assertIn(f"job/{self.running}", frl.read_json(os.path.join(self.root, "state.json"))["watch"])
        self.assertIsNone(self.bdir(self.running))
        self.fetch()  # still running: still watched
        self.assertEqual(self.runs()[-1]["outcomes"].get("watching"), [f"job/{self.running}"])
        self.srv.route(f"/bucket/logs/job/{self.running}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(time.time())}))
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"].get("backfilled"), [f"job/{self.running}"])
        self.assertIn("by TestGrid's rule it was red", self.meta(self.running)["backfilled"])
        self.assertEqual(frl.read_json(os.path.join(self.root, "state.json"))["watch"], {})

    def test_a_watched_build_that_passes_is_dropped(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.srv.route(f"/bucket/logs/job/{self.running}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(time.time())}))
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"].get("not-red"), [f"job/{self.running}"])
        self.assertIsNone(self.bdir(self.running))
        self.assertEqual(frl.read_json(os.path.join(self.root, "state.json"))["watch"], {})

    def test_a_failed_listing_is_tried_again_next_run(self):
        self.srv.httpd.prefix_routes.clear()  # the listing answers 404
        self.assertEqual(self.fetch(), 1)
        self.assertTrue(any("listing GCS for the history gap failed" in e["error"] for e in self.runs()[-1]["errors"]))
        self.assertLess(frl.read_json(os.path.join(self.root, "state.json"))["tabs"]["dash#t"]["scan"], self.now - 39 * HOUR)
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": self.red, "watching": [f"job/{self.running}"]})


class SelfReviewFixesTest(ArchiveWorld):
    def test_an_object_without_md5_is_checked_by_crc32c(self):
        def composite(data, crc):
            def serve(h):
                send(h, 200, {"x-goog-stored-content-encoding": "identity", "x-goog-stored-content-length": str(len(data)),
                              "x-goog-hash": "crc32c=" + __import__("base64").b64encode(crc.to_bytes(4, "big")).decode(),
                              "Content-Length": str(len(data))}, data)
            return serve
        self.finished(ago=3 * DAY)
        good = b"composite upload\n"
        self.srv.route(self.base + "/build-log.txt", composite(good, frl.crc32c(good) ^ 1))
        self.assertEqual(self.fetch(), 1)
        self.assertIn("crc32c", self.runs()[-1]["errors"][0]["error"])
        self.srv.route(self.base + "/build-log.txt", composite(good, frl.crc32c(good)))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["log"]["verified"])
        self.assertEqual(self.read("build-log.txt"), good)

    def test_an_unreadable_meta_rebuilt_in_place_is_a_repair_with_its_history(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        folder, first = self.bdir(), self.meta()
        with open(os.path.join(folder, "meta.json"), "w") as f:
            f.write("{broken")
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual(self.bdir(), folder)
        m = self.meta()
        self.assertEqual((m["archived_by_run"], m["repaired_by_run"]), (first["archived_by_run"], self.runs()[-1]["run"]))
        self.assertAlmostEqual(m["archived_at"], first["archived_at"], delta=1)

    def test_a_failed_run_is_never_listed_as_ok(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        bad = os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", "9")
        os.makedirs(bad)
        with open(os.path.join(bad, "meta.json"), "w") as f:
            f.write("{broken")
        frl.write_json(os.path.join(self.root, "state.json"),
                       {"tabs": {"dash#gone": {"scan": time.time() - DAY, "oldest": time.time() - 4 * DAY}}})
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        index = slurp(os.path.join(self.root, "INDEX.md"))
        self.assertIn(f"]({run['dir']}/) | 1 | 0 | 2 problems |", index)  # an unreadable meta.json and a lost tab
        readme = slurp(os.path.join(self.root, run["dir"], "README.md"))
        self.assertIn("dash#gone: no longer listed", readme.split("## Problems")[1])

    def test_list_only_shows_warnings(self):
        frl.write_json(os.path.join(self.root, "state.json"),
                       {"tabs": {"dash#gone": {"scan": time.time() - DAY, "oldest": time.time() - 4 * DAY}}})
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url,
                      "--gcs", self.srv.url, "--list-only"])
        self.assertIn("WARNING dash#gone: no longer listed", err.getvalue())


class BackfillMergeTest(BackfillTest):
    def test_one_build_in_two_gap_tabs_is_archived_once_with_both_tabs(self):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        self.srv.table("dash", "t2", table("bucket/logs/job", [self.visible], {"o": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))
        frl.write_json(os.path.join(self.root, "state.json"), {"tabs": {
            "dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR},
            "dash#t2": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}}})
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": self.red, "watching": [f"job/{self.running}"]})
        self.assertEqual(sorted(t["tab"] for t in self.meta(self.failed)["tabs"]), ["t", "t2"])
        self.assertEqual(self.runs()[-1]["backfilled_from_gcs"], 3)

    def test_a_backfilled_build_shown_red_later_gains_its_red_cells(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.assertEqual(self.meta(self.failed)["tabs"][0]["red_cells"], [])
        self.show([self.failed], {"Overall": ([12], ["boom"])}, start=int(self.now - 30 * HOUR))
        self.fetch()
        m = self.meta(self.failed)
        self.assertEqual([c["test"] for c in m["tabs"][0]["red_cells"]], ["Overall"])
        self.assertEqual(m["started"], int(self.now - 30 * HOUR))
        row = next(line for line in slurp(os.path.join(self.root, "INDEX.md")).splitlines() if self.failed in line)
        self.assertNotIn("TestGrid no longer showed it", row)

    def test_a_repair_keeps_the_backfilled_label(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        with open(os.path.join(self.bdir(self.failed), "build-log.txt"), "wb") as f:
            f.write(b"damaged")
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"].get("repaired"), [f"job/{self.failed}"])
        self.assertIn("FAILURE", self.meta(self.failed)["backfilled"])

    def test_a_placeholder_column_in_the_gap_is_looked_up_in_gcs(self):
        # the failed build is still listed, but only as a grey "grid too large" column
        self.show([self.visible, self.failed], {"o": ([1, 6], ["", "grid exceeds maximum size"])},
                  start=int(self.now - HOUR))
        t = table("bucket/logs/job", [self.visible, self.failed], {"o": ([1, 6], ["", "grid exceeds maximum size"])})
        t["timestamps"] = [int((self.now - HOUR) * 1000), int((self.now - 30 * HOUR) * 1000)]
        self.srv.table("dash", "t", t)
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": self.red, "watching": [f"job/{self.running}"]})


class MarginAndTimeoutTest(ArchiveWorld):
    def test_a_tab_keeps_its_margin_when_prowjob_json_cannot_be_read(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "11h", "grace_period": "20m"}}}))
        self.fetch()
        tab = frl.read_json(os.path.join(self.root, "state.json"))["tabs"]["dash#t"]
        self.assertAlmostEqual(tab["current_job_hours"], 11.58, places=2)
        self.srv.route(self.base + "/prowjob.json", (503, {}, b""))
        self.fetch()
        (tab,) = [t for t in self.runs()[-1]["tabs"] if t.get("tab") == "t"]
        self.assertAlmostEqual(tab["max_job_hours"], 11.58, places=2)

    def test_a_scan_that_runs_out_of_time_leaves_archived_builds_alone(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()

        def slow_summary(h):
            time.sleep(1.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                send(h, 200, {}, json.dumps({"t": {"overall_status": "FAILING"}}).encode())
        self.srv.route("/dash/summary", slow_summary)
        self.fetch("--run-timeout-minutes", "0.01")
        run = self.runs()[-1]
        self.assertEqual(frl.read_json(os.path.join(self.root, "state.json"))["unresolved"], {})
        self.assertNotIn(self.key, json.dumps(run["errors"]))
        time.sleep(1.6)


class TestGridRuleTest(unittest.TestCase):
    def test_overall_cell_rule(self):
        now = 1_800_000_000
        st = {"timestamp": now - 3600, "malformed": False}
        old_st = {"timestamp": now - 25 * HOUR, "malformed": False}

        def fin(**kw):
            d = {"result": None, "timestamp": now - 60, "passed": None, "malformed": False} | kw
            return d | {"stamp": kw.get("stamp", d["timestamp"])}
        cases = [
            (fin(passed=False, result="FAILURE"), st, True),
            (fin(passed=True, result="FAILURE"), st, False),  # `passed` decides, not the result string
            (fin(passed=None, result="SUCCESS"), st, False),  # no `passed`: SUCCESS means passed
            (fin(passed=None, result="failure"), st, True),
            (fin(passed=False, result="ABORTED"), st, True),
            (fin(malformed=True, timestamp=None), st, True),  # a malformed artifact is red
            (fin(), {"timestamp": None, "malformed": True}, True),
            (None, st, False),  # running
            (None, old_st, True),  # "did not complete within 24 hours"
            (fin(timestamp=None), old_st, True),
            (fin(passed=False), None, False),  # no started.json: TestGrid never lists it
        ]
        for finished, started, want in cases:
            with self.subTest(finished=finished, started=started):
                self.assertEqual(frl.painted_red(finished, started, now), want)
                raw = None if finished is None else dict(finished, timestamp=finished["stamp"])
                self.assertEqual(cc.testgrid_red(raw, started, now), want)

    def test_a_cut_timeout_binds_until_its_builds_must_have_ended(self):
        t1 = 1_791_000_000
        hours, state = frl.job_margin({}, 18.58, t1, 6)
        self.assertEqual(hours, 18.58)
        t2 = t1 + 12 * HOUR
        hours, state = frl.job_margin(state, 2.25, t2, 6)  # the timeout was cut before this run
        self.assertEqual(hours, 18.58)
        self.assertEqual(frl.job_margin(state, 2.25, t2 + 12 * HOUR, 6)[0], 18.58)  # builds may still run
        self.assertEqual(frl.job_margin(state, 2.25, t2 + 19 * HOUR, 6)[0], 6)
        self.assertEqual(frl.job_margin(state, None, t2 + 2 * HOUR, 6)[0], 18.58)  # unreadable: keep what is known


class GoDecodingTest(unittest.TestCase):
    """TestGrid reads started.json and finished.json with Go's JSON decoder: a field
    it cannot decode fails its read of the build, which it paints TOOL_FAIL (red)."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)

    def judge(self, finished, started, build="1"):
        base = f"{self.srv.url}/bucket/logs/job/{build}"
        if finished is not None:  # None: GCS has no finished.json
            self.srv.route(f"/bucket/logs/job/{build}/finished.json", (200, {}, finished))
        self.srv.route(f"/bucket/logs/job/{build}/started.json", (200, {}, started))
        now = time.time()
        return (frl.painted_red(frl.read_finished(base), frl.read_started(base), now),
                cc.testgrid_red(cc.try_json(base + "/finished.json"), cc.try_json(base + "/started.json"), now))

    def test_a_field_testgrid_cannot_decode_paints_the_build_red(self):
        now = int(time.time())
        st, ok = {"timestamp": now - 3600}, {"timestamp": now - 60, "passed": True, "result": "SUCCESS"}
        self.assertEqual(self.judge(ok, st), (False, False))
        self.assertEqual(self.judge(dict(ok, passed=None, result="SUCCESS"), st), (False, False))  # null is fine
        self.assertEqual(self.judge(dict(ok, metadata={"x": 1}, extra=[1]), st), (False, False))  # other fields
        for finished, started in [(dict(ok, passed="true"), st),
                                  (dict(ok, passed=1), st),
                                  (dict(ok, timestamp=float(now - 60)), st),  # Go cannot put 1.0 in an int64
                                  (dict(ok, timestamp=str(now - 60)), st),
                                  (dict(ok, timestamp=1 << 63), st),  # beyond int64
                                  (dict(ok, result=["SUCCESS"]), st),
                                  (ok, {"timestamp": float(now - 3600)}),
                                  (ok, {"timestamp": True})]:
            with self.subTest(finished=finished, started=started):
                self.assertEqual(self.judge(finished, started), (True, True))

    def test_finish_and_start_times_as_testgrid_reads_them(self):
        now = int(time.time())
        st = {"timestamp": now - 3600}
        cases = [({"timestamp": 0, "passed": True}, st, True),  # not finished to TestGrid; its deadline is past
                 ({"timestamp": -5, "passed": True}, st, True),
                 ({"timestamp": now + 10 * DAY, "passed": True}, st, False),  # finished, however bogus the time
                 ({"timestamp": now + 10 * DAY, "passed": False}, st, True),
                 ({"passed": False}, st, False),  # no finish time: running until 24 hours have passed
                 ({"passed": False}, {"timestamp": now - 25 * HOUR}, True),
                 (None, {"timestamp": now - 25 * HOUR}, True),
                 (None, {"timestamp": -5}, False),  # a start time it cannot date counts as none
                 (None, {"timestamp": now + 10 * DAY}, False),
                 (None, {}, False)]
        for i, (finished, started, want) in enumerate(cases):
            with self.subTest(finished=finished, started=started):
                self.assertEqual(self.judge(finished, started, build=str(100 + i)), (want, want))


class Round3Test(BackfillTest):
    def test_gcs_builds_judged_by_testgrids_full_rule(self):
        overdue, empty, success = (snowflake(self.now - h * HOUR) for h in (32, 33, 34))
        for b in (overdue, empty, success):
            self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 32 * HOUR)}))
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log " + b.encode()))
        self.srv.route(f"/bucket/logs/job/{empty}/finished.json", (200, {"Content-Length": "0"}, b""))
        self.srv.route(f"/bucket/logs/job/{success}/finished.json",
                       (200, {}, {"result": "SUCCESS", "timestamp": int(self.now)}))  # no `passed`
        self.srv.gcs_listing("bucket", self.listing + [f"logs/job/{b}/finished.json" for b in (overdue, empty, success)])
        self.fetch()
        backfilled = self.runs()[-1]["outcomes"]["backfilled"]
        self.assertIn(f"job/{overdue}", backfilled)  # ran 24 hours without finishing
        self.assertIn(f"job/{empty}", backfilled)  # malformed finished.json
        self.assertNotIn(f"job/{success}", backfilled)
        self.assertIn("did not finish within 24 hours", self.meta(overdue)["backfilled"])

    def test_a_watched_build_found_red_but_not_downloadable_is_retried_like_any_red_build(self):
        self.srv.gcs_listing("bucket", self.listing)
        self.fetch()
        self.srv.route(f"/bucket/logs/job/{self.running}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(time.time())}))
        self.srv.route(f"/bucket/logs/job/{self.running}/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        state = frl.read_json(os.path.join(self.root, "state.json"))
        self.assertIn(f"job/{self.running}", state["unresolved"])
        self.assertNotIn(f"job/{self.running}", state["watch"])
        self.assertIn("backfilled", state["unresolved"][f"job/{self.running}"]["rec"])
        waiting = slurp(os.path.join(self.root, "INDEX.md")).split("## Not archived yet")[1].split("\n## ")[0]
        self.assertIn(self.running, waiting)

    def test_a_tab_scanned_for_the_first_time_is_searched_without_a_false_alarm(self):
        frl.write_json(os.path.join(self.root, "state.json"), {"tabs": {}})
        os.makedirs(os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z"))
        frl.write_json(os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "run.json"),
                       run_record(1577836800, []) | {"run": "2020-01-01T000000Z"})
        self.srv.gcs_listing("bucket", self.listing)
        self.assertEqual(self.fetch(), 0)
        # searched back 14 days at most (not to 2020), so the 60-hour-old red build is found too
        self.assertEqual(self.runs()[-1]["outcomes"]["backfilled"], sorted(self.red + [f"job/{self.too_old}"]))
        self.assertEqual(self.runs()[-1]["serious_warnings"], [])


class Round3ArchiveTest(ArchiveWorld):
    def test_an_old_given_up_entry_is_dropped_quietly_unless_still_red(self):
        rec = {"job": "job", "build": "77", "query": "bucket/logs/job", "started": None, "created": frl.created("77"),
               "tabs": [{"dashboard": "dash", "tab": "t", "tab_status": None, "red_cells": []}]}
        still = {"job": "job", "build": self.build, "query": "bucket/logs/job", "started": self.start,
                 "created": self.start, "tabs": rec["tabs"]}
        long_ago = time.time() - 50 * DAY
        frl.write_json(os.path.join(self.root, "state.json"), {"tabs": {}, "given_up": {
            "job/77": {"rec": rec, "first_seen": long_ago, "last_error": "x"},
            self.key: {"rec": still, "first_seen": long_ago, "last_error": "x"}}})
        self.fetch()
        run, state = self.runs()[-1], frl.read_json(os.path.join(self.root, "state.json"))
        self.assertNotIn("job/77", state["given_up"])
        self.assertIn(self.key, state["given_up"])  # TestGrid still shows it red
        self.assertEqual([w for w in run["serious_warnings"] if "given up" in w], [])

    def test_migration_never_follows_a_symlinked_build_folder(self):
        outside = tempfile.mkdtemp()
        try:
            frl.write_json(os.path.join(outside, "meta.json"), {"job": "job", "build": "123", "tabs": [],
                                                                "archived_by_run": "20261001T120000.000000Z"})
            os.makedirs(os.path.join(self.root, "logs", "job"))
            os.symlink(outside, os.path.join(self.root, "logs", "job", "123"))
            self.finished(ago=3 * DAY)
            self.fetch()
            self.assertEqual(frl.read_json(os.path.join(outside, "meta.json"))["archived_by_run"], "20261001T120000.000000Z")
            self.assertTrue(any("not a build folder" in w for w in self.runs()[-1]["warnings"]))
        finally:
            shutil.rmtree(outside)

    def test_an_unreadable_finished_json_on_recheck_keeps_what_was_recorded(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        before = self.meta()
        self.srv.route(self.base + "/finished.json", (200, {}, b"{cut off"))
        self.fetch()
        after = self.meta()
        self.assertEqual((after["result"], after["finished"], after["settled"]),
                         (before["result"], before["finished"], before["settled"]))

    def test_a_copy_that_cannot_be_compared_for_14_days_stops_being_compared(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        m["archived_at"] = time.time() - 15 * DAY
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        self.srv.route(self.base + "/build-log.txt", (500, {}, b""))
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"stopped-comparing": [self.key]})
        self.assertTrue(self.meta()["final"])
        self.assertIn("not compared with GCS", self.meta()["note"])

    def test_duplicates_are_reported_once(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        shutil.copytree(self.bdir(), os.path.join(self.root, "runs", "2020-01", "2020-01-01T000000Z", "job", self.build))
        self.fetch()
        errors = [e["error"] for e in self.runs()[-1]["errors"]]
        self.assertEqual(sum(self.build in e for e in errors), 1, errors)

    def test_cross_check_does_not_blame_testgrid_lag(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        newer = snowflake(time.time() - 1800)  # failed, but TestGrid has not shown it yet
        self.srv.route(f"/bucket/logs/job/{newer}/started.json", (200, {}, {"timestamp": int(time.time() - 1800)}))
        self.srv.route(f"/bucket/logs/job/{newer}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(time.time() - 4000)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/finished.json", f"logs/job/{newer}/finished.json"])
        self.fetch()
        code, out = self.check()
        self.assertNotIn("MISSED", out)


class ProwTimeoutTest(ArchiveWorld):
    def test_go_durations(self):
        for text, want in (("2h0m0s", 7200), ("15m", 900), ("1h30m15.5s", 5415.5), ("500ms", 0.5), ("1.5h", 5400)):
            self.assertAlmostEqual(frl.go_duration(text), want)
        for text in ("", "abc", "1x", "h", "1h 2m", None, 5):
            self.assertIsNone(frl.go_duration(text))

    def test_a_job_that_can_run_long_gets_its_own_margin(self):
        self.srv.route(self.base + "/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "11h0m0s", "grace_period": "20m0s"}}}))
        self.finished(ago=3 * DAY)
        self.fetch()
        (tab,) = [t for t in self.runs()[-1]["tabs"] if t.get("tab") == "t"]
        self.assertAlmostEqual(tab["max_job_hours"], 11 + 20 / 60 + 0.25, places=2)
        self.srv.route(self.base + "/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "1h", "grace_period": "15s"}}}))
        self.fetch()  # builds started under the long timeout may still run: it is honoured ...
        (tab,) = [t for t in self.runs()[-1]["tabs"] if t.get("tab") == "t"]
        self.assertAlmostEqual(tab["max_job_hours"], 11 + 20 / 60 + 0.25, places=2)
        state = frl.read_json(os.path.join(self.root, "state.json"))
        state["tabs"]["dash#t"]["job_hours"] = {h: time.time() - 1 for h in state["tabs"]["dash#t"]["job_hours"]}
        frl.write_json(os.path.join(self.root, "state.json"), state)  # ... until they must have ended
        self.fetch()
        (tab,) = [t for t in self.runs()[-1]["tabs"] if t.get("tab") == "t"]
        self.assertEqual(tab["max_job_hours"], 6)


class RenderingTest(unittest.TestCase):
    def test_markdown_cells_cannot_start_markup(self):
        for s in ("![x](https://evil.example/a.png)", "<img src=x onerror=alert(1)>", "a\r\n# H1", "x y",
                  "[link](javascript:alert(1))", "`code`", "|pipe|", "**bold**", "line\n\n<h1>x</h1>"):
            out = frl.md_cell(s)
            self.assertIsNone(re.search(r"[\r\n ]", out), out)
            self.assertIsNone(re.search(r"(?<!\\)[\[\]<>!`|*]", out), out)
            self.assertEqual(report.md_cell(s), out)
        for s, want in (("# H1", r"\# H1"), ("- item", r"\- item"), ("1. one", r"1\. one"), ("dash-a", "dash-a"),
                        ("gce (alpha)", "gce (alpha)"), ("", ""), ("  ", "")):
            self.assertEqual(frl.md_cell(s), want)

    def test_svg_stays_well_formed_with_control_characters(self):
        with tempfile.TemporaryDirectory() as root:
            now = time.time()
            entries = [{"time": now - 3600, "tabs": ["d#gce\x0bmaster\x00x"]}]
            run = run_record(now, [{"dashboard": "d", "tab": "gce\x0bmaster\x00x", "oldest": now - 20 * DAY}])
            report.write_charts(root, entries, now, run)
            for name in os.listdir(os.path.join(root, "charts")):
                ET.parse(os.path.join(root, "charts", name))

    def test_week_over_week_stays_put_between_runs(self):
        today = report.day_of(time.time())
        now = report.day_start(today) + HOUR  # just after midnight: yesterday is not fully covered yet
        runs = [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY}])]
        entries = [{"time": now - k * DAY - 600, "tabs": ["d#t"]} for k in range(1, 30)]
        p = report.summarize(entries, runs, now)["panels"][0]
        self.assertNotIn(today - dt.timedelta(days=1), p["complete"])
        self.assertEqual(p["week_end"], today - dt.timedelta(days=2))
        self.assertEqual(p["week"], (7, 7))


class CrossCheckRobustnessTest(ArchiveWorld):
    def test_a_listing_failure_keeps_the_other_problems(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        shutil.rmtree(self.bdir())  # now a red build is missing ...
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = (403, {}, b"")  # ... and listing is denied
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("MISSED red build", out)
        self.assertIn("cannot list gs://bucket/logs/job/", out)

    def test_a_long_job_that_finished_inside_the_lag_is_not_missed(self):
        start = int(time.time()) - 9 * HOUR
        build = snowflake(start)
        self.show([build], {"o": ([12], [""])}, start=start)
        self.srv.route(f"/bucket/logs/job/{build}/finished.json",
                       (200, {}, {"result": "FAILURE", "timestamp": int(time.time()) - 600}))
        self.fetch("--list-only")
        os.makedirs(os.path.join(self.root, "runs", "2020-01"), exist_ok=True)
        rel = frl.new_run_dir(self.root, time.time())[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       dict(run_record(time.time(), [{"dashboard": "dash", "tab": "t", "query": "bucket/logs/job",
                                                      "oldest": start}]), run="r", finished=time.time()))
        code, out = self.check()
        self.assertNotIn("MISSED", out)
        self.assertIn("1 not settled at run time", out)

    def test_old_windows_keep_modern_build_ids(self):
        ids = [snowflake(time.time() - HOUR), snowflake(time.time() - 2 * HOUR)]
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in ids])
        self.assertEqual(sorted(cc.list_builds(self.srv.url, "bucket/logs/job", 1_300_000_000)), sorted(ids))

    def test_a_tab_dropped_from_its_dashboard_fails_verification(self):
        self.finished(ago=3 * DAY)
        frl.write_json(os.path.join(self.root, "state.json"),
                       {"tabs": {"dash#gone": {"scan": time.time() - DAY, "oldest": time.time() - 4 * DAY}}})
        self.assertEqual(self.fetch(), 1)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("the run warned: dash#gone: no longer listed", out)

    def test_a_meta_without_times_is_a_problem_not_a_crash(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.fetch()
        m = self.meta()
        del m["started"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), m)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("unreadable", out)
        self.assertNotIn("Traceback", out)


if __name__ == "__main__":
    unittest.main()
