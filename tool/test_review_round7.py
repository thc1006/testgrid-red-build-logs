"""Regression tests for the round-7 review: each failed before its fix."""
import base64
import contextlib
import io
import os
import random
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
from unittest import mock

import urllib3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_review_round4 as r4  # noqa: E402  (module import: its tests are not collected again here)
from test_fetch_red_logs import DAY, FakeServer, send, table  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj, snowflake  # noqa: E402

SUCCEEDED = (200, {}, {"pod": {"status": {"phase": "Succeeded"}}})

LIMIT = 100  # bytes per part, for --max-file-mb
LIMIT_ARGS = ("--max-file-mb", str(LIMIT / (1 << 20)))


class CutOffSplit(ArchiveWorld):
    """A file larger than --max-file-mb is stored in parts. Splitting it used to start from
    the whole file at its final name, so a split cut off part-way (a run timeout, an
    interrupt, a full disk) left a file too large for git there, next to the build's
    meta.json: the commit took it and GitHub rejected the push. A cut-off split also left
    parts meta.json does not list, which were committed."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * HOUR)  # finished, but still compared with GCS on every run
        self.big = random.Random(1).randbytes(350)  # 4 parts of at most 100 bytes

    def stored(self):
        return sorted(n for n in os.listdir(self.bdir()) if n != "meta.json")

    def cut_off_at_the_second_part(self):
        """Run the fetch with the second part's write failing, as a full disk would; return
        the names placed at a final name with their sizes."""
        real, placed = frl.replace, []

        def flaky(src, dst):
            if dst.endswith(".001"):
                raise OSError(28, "No space left on device")
            placed.append((os.path.basename(dst), os.path.getsize(src)))
            return real(src, dst)
        with mock.patch.object(frl, "replace", flaky):
            code = self.fetch(*LIMIT_ARGS)
        return code, placed

    def test_a_refresh_cut_off_part_way_leaves_the_old_copy_and_nothing_else(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"first upload\n", "1"))
        self.assertEqual(self.fetch(*LIMIT_ARGS), 0)
        self.assertEqual(self.stored(), ["build-log.txt"])
        self.srv.route(self.base + "/build-log.txt", obj(self.big, "2"))
        code, placed = self.cut_off_at_the_second_part()
        self.assertEqual(code, 1)
        self.assertTrue(all(size <= LIMIT for name, size in placed if name.startswith("build-log")), placed)
        # the part written before the failure is gone; the old copy is intact and still listed
        self.assertEqual(self.stored(), ["build-log.txt"])
        self.assertTrue(frl.archived_intact(self.bdir(), deep=True))
        self.assertIn("No space left", {e["build"]: e["error"] for e in self.runs()[-1]["errors"]}[self.key])
        # the next run stores the new log in parts
        self.assertEqual(self.fetch(*LIMIT_ARGS), 0)
        self.assertEqual(self.stored(), [f"build-log.txt.{i:03d}" for i in range(4)])
        self.assertTrue(frl.archived_intact(self.bdir(), deep=True))

    def test_a_new_build_cut_off_part_way_leaves_no_folder(self):
        self.srv.route(self.base + "/build-log.txt", obj(self.big, "1"))
        code, placed = self.cut_off_at_the_second_part()
        self.assertEqual(code, 1)
        self.assertTrue(all(size <= LIMIT for name, size in placed if name.startswith("build-log")), placed)
        self.assertIn(("build-log.txt.000", LIMIT), placed)
        self.assertIsNone(self.bdir())  # removed by the run itself, before any commit

    def test_unlisted_files_are_removed_but_listed_ones_and_others_kept(self):
        self.srv.route(self.base + "/build-log.txt", obj(self.big, "1"))
        self.assertEqual(self.fetch(*LIMIT_ARGS), 0)
        d = self.bdir()
        for name in ("build-log.txt", "build-log.txt.004", "build-log.txt.gz.000", "podinfo.json", "notes.txt"):
            with open(os.path.join(d, name), "wb") as f:
                f.write(b"x")
        self.assertEqual(self.fetch(*LIMIT_ARGS), 0)
        self.assertEqual(self.stored(), [f"build-log.txt.{i:03d}" for i in range(4)] + ["notes.txt"])
        warnings = self.runs()[-1]["warnings"]
        self.assertEqual(sum("does not list it" in w for w in warnings), 4, warnings)


class StrayFiles(unittest.TestCase):
    def test_what_meta_json_lists_is_kept(self):
        with tempfile.TemporaryDirectory() as d:
            names = ["build-log.txt", "build-log.txt.gz", "build-log.txt.gz.000", "build-log.txt.gz.001",
                     "podinfo.json", "podinfo.json.000", "meta.json", "notes.txt", ".build-log.txt.x.part"]
            for n in names:
                open(os.path.join(d, n), "w").close()
            parts = {"file": "build-log.txt.gz", "parts": [{"file": "build-log.txt.gz.000"}, {"file": "build-log.txt.gz.001"}]}
            self.assertEqual(frl.stray_files(d, {"log": parts, "podinfo": None}),
                             ["build-log.txt", "build-log.txt.gz", "podinfo.json", "podinfo.json.000"])
            self.assertEqual(frl.stray_files(d, {"log": {"file": "build-log.txt"}, "podinfo": {}}),
                             ["build-log.txt.gz", "build-log.txt.gz.000", "build-log.txt.gz.001", "podinfo.json",
                              "podinfo.json.000"])
            # an older record without "file" means the plain name; an omitted file is not on disk
            self.assertNotIn("podinfo.json", frl.stray_files(d, {"log": None, "podinfo": {"md5": "x"}}))
            self.assertIn("build-log.txt", frl.stray_files(d, {"log": {"omitted": "big", "file": "build-log.txt"}}))


class AbandonWaitsForWrites(unittest.TestCase):
    """abandon() takes the lock replace() and remove() check under: once it returns, no
    worker is half-way through a write, and none can start one."""

    def test_a_worker_cannot_replace_or_remove_once_abandoned(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "src"), os.path.join(d, "dst")
            for p in (src, dst):
                open(p, "w").close()
            ctx.abandon()
            errors = []

            def worker():
                frl.bind_context(ctx)
                for call in (lambda: frl.replace(src, dst), lambda: frl.remove(dst)):
                    try:
                        call()
                    except frl.Abandoned as e:
                        errors.append(e)
            t = threading.Thread(target=worker)
            t.start()
            t.join()
            self.assertEqual(len(errors), 2)
            self.assertTrue(os.path.exists(src) and os.path.exists(dst))

    def test_abandon_waits_for_a_write_in_progress(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        order, inside = [], threading.Event()
        real = os.replace

        def slow_replace(src, dst):
            inside.set()
            time.sleep(0.3)
            order.append("replaced")
            real(src, dst)
        with tempfile.TemporaryDirectory() as d, mock.patch.object(frl.os, "replace", slow_replace):
            src = os.path.join(d, "src")
            open(src, "w").close()

            def worker():
                frl.bind_context(ctx)
                frl.replace(src, os.path.join(d, "dst"))
            t = threading.Thread(target=worker)
            t.start()
            inside.wait(5)
            ctx.abandon()
            order.append("abandoned")
            t.join()
        self.assertEqual(order, ["replaced", "abandoned"])


class GapTabsOfQueuedBuilds(r4.World):
    """A build already queued when a later gap of another tab holds it was not looked at
    for that tab. A watched build (undecided at an earlier gap) was then judged only on its
    first tab: on a tab that hides the job's Overall row a failed build without failing
    tests is not red, so it was dropped, though the other tab painted it red. A red build
    waiting to be archived lost the other tab's sighting."""

    def two_tabs(self, overall_hidden_on_t):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        visible = snowflake(self.now - HOUR)
        rows_t = {"job.Pod": ([1], [""])} if overall_hidden_on_t else {"job.Overall": ([1], [""]), "job.Pod": ([1], [""])}
        self.srv.table("dash", "t", table("bucket/logs/job", [visible], rows_t, start=int((self.now - HOUR) * 1000)))
        self.srv.table("dash", "t2", table("bucket/logs/job", [visible], {"job.Overall": ([1], [""]),
                                                                          "job.Pod": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))
        return visible

    def test_a_watched_build_is_judged_on_the_tab_of_a_later_gap_too(self):
        visible = self.two_tabs(overall_hidden_on_t=True)
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)  # failed, no failing test, no podinfo: only Overall is red
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible)])
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - DAY}})
        self.assertEqual(self.fetch(), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"backfilled": [f"job/{b}"]})
        tabs = self.meta(b)["tabs"]
        self.assertEqual([t["tab"] for t in tabs], ["t2"])
        self.assertEqual([c["test"] for c in tabs[0]["red_cells"]], ["job.Overall"])
        self.assertEqual(self.state()["watch"], {})

    def test_a_red_build_waiting_to_be_archived_gains_the_tab_of_a_later_gap_once(self):
        visible = self.two_tabs(overall_hidden_on_t=False)
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible)])
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        rec = {k: v for k, v in self.watched(b).items() if k != "watch"}
        rec["tabs"] = [{"dashboard": "dash", "tab": "t", "tab_status": "FAILING",
                        "red_cells": [{"test": "job.Overall", "status": "FAIL", "message": "boom"}]}]
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap},
                       unresolved={f"job/{b}": {"rec": rec, "first_seen": self.now - DAY, "last_error": "x"}})
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived": [f"job/{b}"]})
        tabs = self.meta(b)["tabs"]
        self.assertEqual([t["tab"] for t in tabs], ["t", "t2"])  # t once, with the cells TestGrid showed
        self.assertEqual(tabs[0]["red_cells"][0]["message"], "boom")
        self.assertTrue(tabs[1]["judged_from_gcs"])



class RawServer:
    """An HTTP server scripted on a raw socket: script(conn_no, sock, read_request) writes what it likes."""

    def __init__(self, script):
        self.script, self.connections, self.lock = script, 0, threading.Lock()
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(64)
        self.url = f"http://127.0.0.1:{self.sock.getsockname()[1]}"
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                s, _ = self.sock.accept()
            except OSError:
                return
            with self.lock:
                self.connections += 1
                n = self.connections
            threading.Thread(target=self.serve, args=(n, s), daemon=True).start()

    def serve(self, n, s):
        buf = bytearray()

        def read_request():
            while b"\r\n\r\n" not in buf:
                try:
                    data = s.recv(65536)
                except OSError:
                    return None
                if not data:
                    return None
                buf.extend(data)
            head, _, rest = bytes(buf).partition(b"\r\n\r\n")
            buf[:] = rest
            return head.split(b"\r\n", 1)[0].decode("latin-1").split(" ")[1]  # the path
        try:
            self.script(n, s, read_request)
        finally:
            with contextlib.suppress(OSError):
                s.close()

    def close(self):
        self.sock.close()


def response(status, body, headers=b""):
    return f"HTTP/1.1 {status} X\r\nContent-Length: {len(body)}\r\n".encode() + headers + b"\r\n" + body


class HttpHardening(unittest.TestCase):
    def setUp(self):
        frl.BACKOFF = 0
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)
        self.addCleanup(frl.bind_context, frl._DEFAULT_CONTEXT)

    def raw(self, script):
        srv = RawServer(script)
        self.addCleanup(srv.close)
        return srv

    def test_interim_responses_are_skipped_and_never_hand_their_answer_to_the_next_request(self):
        interims = {"/early": b"HTTP/1.1 103 Early Hints\r\nLink: </x>\r\n\r\n",
                    "/processing": b"HTTP/1.1 102 Processing\r\n\r\n",
                    "/chain": b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\nLink: </y>\r\n\r\n"
                              b"HTTP/1.1 199 Misc\r\nX: 1\r\n\r\n"}

        def script(n, s, read_request):
            while (path := read_request()) is not None:
                s.sendall(interims.get(path, b"") + response(200, b"for " + path.encode()))
        srv = self.raw(script)
        for path in ("/early", "/processing", "/chain", "/next", "/early", "/next"):
            self.assertEqual(frl.request(srv.url + path), b"for " + path.encode())
        self.assertEqual(srv.connections, 1)  # one kept-alive connection, each answer to its own request

    def test_101_is_refused_and_endless_interim_responses_are_cut_off(self):
        def script(n, s, read_request):
            while (path := read_request()) is not None:
                if path == "/switch":
                    s.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: x\r\nConnection: Upgrade\r\n\r\n")
                    return
                if path == "/endless":
                    s.sendall(b"HTTP/1.1 103 Early Hints\r\n\r\n" * (frl.MAX_INTERIM + 1) + response(200, b"late"))
                    return
                s.sendall(response(200, b"for " + path.encode()))
        srv = self.raw(script)
        with self.assertRaisesRegex(frl.IntegrityError, "unexpected informational response 101"):
            frl.request(srv.url + "/switch")
        with self.assertRaisesRegex(urllib3.exceptions.ProtocolError, "interim responses") as caught:
            frl.fetch_once(srv.url + "/endless", frl.read_body, None, 10, "GET")
        self.assertIsInstance(caught.exception, frl.TRANSIENT)  # (retried, like any broken response)
        self.assertEqual(frl.request(srv.url + "/next"), b"for /next")

    def test_a_connection_the_server_closed_is_replaced_at_once(self):
        def script(n, s, read_request):
            first = True
            while read_request() is not None:
                if n == 1 and not first:  # the request crossed the server closing the idle connection
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))  # a reset
                    return
                first = False
                s.sendall(response(200, b"ok"))
        srv = self.raw(script)
        self.assertEqual(frl.request(srv.url + "/a"), b"ok")
        frl.BACKOFF = 2.0  # a wait would show
        t0 = time.monotonic()
        self.assertEqual(frl.request(srv.url + "/b"), b"ok")
        self.assertLess(time.monotonic() - t0, 0.9)
        self.assertEqual((self.ctx.stats()["pushbacks"], srv.connections), (0, 2))

    def test_two_location_headers_are_refused(self):
        def script(n, s, read_request):
            while (path := read_request()) is not None:
                if path == "/moved":
                    s.sendall(response(302, b"", b"Location: /a\r\nLocation: /b\r\n"))
                else:
                    s.sendall(response(200, path.encode()))
        srv = self.raw(script)
        with self.assertRaisesRegex(urllib.error.HTTPError, "more than one Location"):
            frl.request(srv.url + "/moved")

    def test_headers_that_do_not_parse_are_integrity_errors_not_crashes(self):
        def headers(**kv):
            return frl.Headers(urllib3.HTTPHeaderDict(kv))
        for value in (chr(0xB2), "1" * 25, "-1", "1e3", ""):
            with self.subTest(value), self.assertRaises(frl.IntegrityError):
                frl.header_int(headers(**{"Content-Length": value}), "Content-Length")
        self.assertIsNone(frl.retry_after(headers(**{"Retry-After": "Mon, 01 Jan 9999999999999999999999 00:00:00 GMT"})))

        class Odd:
            headers = frl.Headers(urllib3.HTTPHeaderDict({"Content-Length": chr(0xB2)}))
        self.assertGreater(frl.deadline_for(Odd), time.monotonic())  # (the body's own read says it is bad)

    def test_proxies_from_the_environment_are_used(self):
        seen = []

        def script(n, s, read_request):
            buf = b""
            while True:
                data = s.recv(65536)
                if not data:
                    return
                buf += data
                while b"\r\n\r\n" in buf:
                    head, _, buf = buf.partition(b"\r\n\r\n")
                    seen.append(head)
                    s.sendall(response(200, b"via proxy: " + head.split(b" ")[1]))
        proxy = self.raw(script)
        host = proxy.url.split("//", 1)[1]
        env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
        with mock.patch.dict(os.environ, env, clear=True):
            for configured in (proxy.url, host, f"http://us%3Aer:p%40ss@{host}"):  # (as urllib takes them)
                with self.subTest(configured), mock.patch.dict(frl._PROXIES, {"http": configured}, clear=True):
                    self.assertEqual(frl.request("http://example.invalid/x"), b"via proxy: http://example.invalid/x")
            fields = dict(line.split(b": ", 1) for line in seen[-1].split(b"\r\n")[1:])
            auth = {k.lower(): v for k, v in fields.items()}[b"proxy-authorization"]
            self.assertEqual(auth, b"Basic " + base64.b64encode(b"us:er:p@ss"))
            with mock.patch.dict(frl._PROXIES, {"http": proxy.url}, clear=True), \
                    mock.patch.dict(os.environ, {"no_proxy": "example.invalid"}):
                self.assertIs(frl.pool_for("http://example.invalid/x"), frl.HTTP)

    def test_reading_nothing_reads_nothing(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        srv.route("/x", (200, {}, b"hello"))
        self.assertEqual(frl.request(srv.url + "/x", consume=lambda r: (r.read(0), r.read1(0), r.read())),
                         (b"", b"", b"hello"))

    def test_the_cap_never_passes_the_ceiling(self):
        limit = frl.AdaptiveLimit(63, 64)
        limit.acquire()
        limit.release(True)  # halved, to 31.5
        for _ in range(3000):
            limit.acquire()
            limit.release(False)
            self.assertLessEqual(limit.cap, 64)

    def test_a_request_waiting_for_a_slot_is_not_sent_once_the_run_is_abandoned(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        srv = FakeServer()
        self.addCleanup(srv.close)
        srv.route("/x", (200, {}, b"x"))
        slot = ctx.limit_for(srv.url + "/x")
        slot.acquire()  # the only slot is taken
        errors = []

        def worker():
            frl.bind_context(ctx)
            try:
                frl.request(srv.url + "/x")
            except frl.Abandoned as e:
                errors.append(e)
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        time.sleep(0.3)  # it waits for the slot
        try:
            ctx.abandon()
        finally:
            slot.release(False)
        t.join(5)
        self.assertEqual((len(errors), srv.hits("/x")), (1, 0))


class Decoders(unittest.TestCase):
    def test_a_mass_failure_keeps_every_red_column_with_its_first_cells(self):
        width, rows = 50, 30
        t = table("bucket/logs/job", [str(1000 + i) for i in range(width)],
                  {f"t{r}": ([12] * width, [f"m{r}"] * width) for r in range(rows)})
        with mock.patch.object(frl, "MAX_RED_CELLS", 1000):  # 1500 red cells
            reds = frl.red_columns(t)
        self.assertEqual(sorted(reds), list(range(width)))
        self.assertEqual((sum(len(c) for c in reds.values()), sum(c.omitted for c in reds.values())), (1000, 500))
        self.assertTrue(all(len(c) + c.omitted == rows for c in reds.values()))
        self.assertEqual(reds[0][0], {"test": "t0", "status": "FAIL", "message": "m0"})

    def test_a_claim_of_a_billion_red_cells_is_bounded(self):
        width = 50_000
        t = {"changelists": [str(10 ** 6 + i) for i in range(width)], "timestamps": [0] * width,
             "tests": [{"name": f"t{r}", "statuses": [{"value": 12, "count": width}]} for r in range(20_000)]}
        t0 = time.monotonic()
        with mock.patch.object(frl, "MAX_RED_CELLS", 10_000):
            reds = frl.red_columns(t)
        self.assertLess(time.monotonic() - t0, 30)
        self.assertEqual((len(reds), sum(len(c) for c in reds.values())), (width, 10_000))

    def test_too_many_columns_is_refused_by_both_decoders(self):
        width = frl.MAX_COLUMNS + 1
        t = {"query": "bucket/logs/job", "changelists": [str(10 ** 6 + i) for i in range(width)],
             "timestamps": [0] * width, "tests": []}
        with self.assertRaisesRegex(ValueError, "columns"):
            frl.table_rows(t)
        with self.assertRaisesRegex(ValueError, "columns"):
            list(cc.columns(t))


class MassFailureArchived(ArchiveWorld):
    def test_the_cells_not_listed_are_counted_in_the_build_record(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.show([self.build], {f"t{r}": ([12], ["boom"]) for r in range(5)})
        with mock.patch.object(frl, "MAX_RED_CELLS", 3):
            self.assertEqual(self.fetch(), 0)
        tab = self.meta()["tabs"][0]
        self.assertEqual((len(tab["red_cells"]), tab["red_cells_omitted"]), (3, 2))


class StateMachine(r4.World):
    def test_a_build_shown_running_long_past_its_time_is_watched_and_judged_once_gone(self):
        b = snowflake(self.now - 25 * HOUR)  # started 25 h ago; its job's limit is 6 h; no finished.json
        start = int(self.now - 25 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": start}))
        self.srv.gcs_listing("bucket", [])
        self.show([b], {"job.Overall": ([4], [""])}, start=start)  # TestGrid still shows it running
        self.assertEqual(self.fetch(), 0)
        key = f"job/{b}"
        self.assertEqual(self.runs()[-1]["outcomes"], {"watching": [key]})
        self.assertEqual(self.fetch(), 0)  # still shown: what TestGrid shows decides, nothing is read
        self.assertEqual(self.runs()[-1]["outcomes"], {"watching": [key]})
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{b}/started.json"), 0)
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))  # it scrolled off
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"].get("backfilled"), [key])
        self.assertIn("it did not finish within 24 hours", self.meta(b)["backfilled"])
        self.assertEqual(self.state()["watch"], {})

    def test_a_watched_build_testgrid_then_shows_settled_and_not_red_is_dropped(self):
        b = snowflake(self.now - 25 * HOUR)
        start = int(self.now - 25 * HOUR)
        self.srv.gcs_listing("bucket", [])
        self.show([b], {"job.Overall": ([4], [""])}, start=start)
        self.fetch()
        self.assertIn(f"job/{b}", self.state()["watch"])
        self.show([b], {"job.Overall": ([1], [""])}, start=start)  # it passed after all, and TestGrid shows that
        for name, body in (("started.json", {"timestamp": start}), ("podinfo.json", {"pod": {"status": {"phase": "Succeeded"}}}),
                           ("finished.json", {"timestamp": start + 20 * HOUR, "passed": True, "result": "SUCCESS"})):
            self.srv.route(f"/bucket/logs/job/{b}/{name}", (200, {}, body))  # (in time: GCS says so too)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{b}"]})
        self.assertEqual(self.state()["watch"], {})

    def test_a_meta_json_that_is_not_an_object_does_not_stop_the_runs(self):
        self.gap_world()
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        folder = os.path.join(self.earlier_run(self.now - 3 * DAY), "job", self.red30h)
        os.makedirs(folder)
        with open(os.path.join(folder, "meta.json"), "w") as f:
            f.write("null")
        self.fetch()  # the gap holds that build: its sighting has no record to go to
        run = self.runs()[-1]
        self.assertTrue(any("unreadable" in e["error"] for e in run["errors"]), run["errors"])
        self.assertGreater(self.state()["tabs"]["dash#t"]["scan"], self.now - HOUR)

    def test_a_build_red_on_one_tab_and_undecided_on_another_is_archived_once(self):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"job.Overall": ([1], [""]), "job.Pod": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.table("dash", "t2", table("bucket/logs/job", [visible], {"job.Pod": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))  # hides Overall
        gap = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        self.put_state(tabs={"dash#t": gap, "dash#t2": gap})
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)
        # failed half an hour ago, no podinfo.json yet: its Pod row still runs. Red where Overall
        # shows; on t2 not red yet, and undecided
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - 1800)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, visible)])
        self.fetch()
        run = self.runs()[-1]
        key = f"job/{b}"
        self.assertEqual(sum(v.count(key) for v in run["outcomes"].values()), 1, run["outcomes"])
        self.assertIn(key, run["outcomes"]["backfilled"])
        self.assertNotIn(key, self.state()["watch"])
        self.assertEqual([t["tab"] for t in self.meta(b)["tabs"]], ["t"])

    def test_a_tab_not_read_now_is_judged_by_the_rows_it_showed(self):
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)  # failed: red in Overall only
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", SUCCEEDED)
        self.srv.table("dash", "t", (500, {}, b"down"))
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR,
                                        "rows": {"overall": False, "pod": True}}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - DAY}})
        self.assertEqual(self.fetch(), 1)  # (the tab could not be read)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{b}"]})

    def test_a_sighting_from_a_gap_is_kept_when_the_comparison_with_gcs_fails(self):
        self.gap_world()
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        self.fetch()
        self.assertEqual([t["tab"] for t in self.meta(self.red30h)["tabs"]], ["t"])
        # a second tab of the job, with a gap of its own; GCS cannot be compared with now
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        self.srv.table("dash", "t2", table("bucket/logs/job", [self.visible], {"job.Overall": ([1], [""])},
                                           start=int((self.now - HOUR) * 1000)))
        state = self.state()
        state["tabs"]["dash#t2"] = {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.srv.route(f"/bucket/logs/job/{self.red30h}/build-log.txt", (500, {}, b""))
        self.assertEqual(self.fetch(), 1)
        self.assertEqual(sorted(t["tab"] for t in self.meta(self.red30h)["tabs"]), ["t", "t2"])

    def test_damaged_state_entries_are_reported_not_a_crash(self):
        b = snowflake(self.now - 30 * HOUR)
        rec = {k: v for k, v in self.watched(b).items() if k != "watch"}
        self.put_state(tabs={"dash#t": {"scan": "yesterday"}},
                       given_up={f"job/{b}": {"rec": rec, "first_seen": self.now - 20 * DAY}})  # no last_error
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertTrue(any("dropped a malformed tabs entry" in w for w in run["serious_warnings"]), run)
        with open(os.path.join(self.root, "INDEX.md"), encoding="utf-8") as f:
            self.assertIn("given up: unknown error", f.read())


class CrossCheckRound7(r4.World):
    def check(self, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, *extra])
        return code, out.getvalue()

    def test_a_build_gone_from_testgrid_is_judged_as_of_the_run_and_one_unreadable_is_a_problem(self):
        self.earlier_run(self.now - 3 * DAY)
        visible, gone, unreadable = (snowflake(self.now - h * HOUR) for h in (1, 30, 31))
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        for b in (gone, unreadable):
            self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
            self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", SUCCEEDED)
        self.srv.route(f"/bucket/logs/job/{gone}/finished.json",
                       (200, {}, {"timestamp": frl.created(gone) + 600, "passed": True}))
        self.srv.route(f"/bucket/logs/job/{unreadable}/finished.json", (503, {}, b""))
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (visible, gone, unreadable)])
        self.fetch()
        started = self.runs()[-1]["started"]
        seen = []
        real = frl.gcs_column

        def gcs_column(*args, **kwargs):
            seen.append(kwargs.get("now"))
            return real(*args, **kwargs)
        with mock.patch.object(frl, "gcs_column", gcs_column):
            code, out = self.check("--hours", "48")
        self.assertEqual(code, 1)
        self.assertIn(f"cannot check job/{unreadable} by TestGrid's rule", out)
        self.assertEqual(set(seen), {started})

    def test_older_builds_are_compared_for_every_day_since_the_run_before(self):
        d = self.earlier_run(self.now - 3 * DAY)  # (done with its sevenths: it compared them all, and so did its verification)
        frl.write_json(os.path.join(d, "run.json"), dict(frl.read_json(os.path.join(d, "run.json")), gcs_compared=True))
        frl.write_json(os.path.join(d, "verification.json"), {"outcome": "ok", "scope": {"differ": 0}})
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [])
        self.fetch()
        code, out = self.check()
        self.assertIn("and 3/7 of the rest", out)

    def test_a_stray_or_oversized_file_next_to_a_build_is_a_problem(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.check()[0], 0)
        with open(os.path.join(self.bdir(), "build-log.txt.000"), "wb") as f:
            f.write(b"x")
        with open(os.path.join(self.bdir(), "notes"), "wb") as f:
            f.truncate(cc.GITHUB_MAX_BYTES + 1)  # (sparse)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("build-log.txt.000: not listed in its meta.json", out)
        self.assertIn("notes: 104857601 bytes, more than GitHub accepts", out)

    def test_an_odd_record_does_not_stop_the_inventory(self):
        self.assertIn("1 builds: 1 without files", cc.inventory({("j", "1"): ("x", {"log": ["x"], "podinfo": 5})}, self.root))



class SecondWave(r4.World):
    """The second review of round 7's changes."""

    def junit_unreadable(self, b):
        """A build that passed, whose junit files cannot be listed now (only they could make it red)."""
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": frl.created(b) + 1800}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", SUCCEEDED)
        real = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def listing(h):
            if f"logs/job/{b}/" in urllib.parse.unquote(h.path) and "delimiter" not in h.path:
                send(h, 403, {}, b"denied")
            else:
                real(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing

    def test_a_build_whose_junit_files_cannot_be_read_is_an_error_until_it_is_dropped(self):
        b = snowflake(self.now - 5 * HOUR)
        self.srv.gcs_listing("bucket", [])
        self.junit_unreadable(b)
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.assertEqual(self.fetch(), 1)
        errors = {e["build"]: e["error"] for e in self.runs()[-1]["errors"]}
        self.assertIn("cannot be judged by TestGrid's rule: its junit files cannot be read", errors[f"job/{b}"])
        self.assertIn(f"job/{b}", self.state()["watch"])

    def test_a_port_crash_is_not_taken_for_unreadable_junit_files(self):
        b = snowflake(self.now - 5 * HOUR)
        self.srv.gcs_listing("bucket", [])
        self.junit_unreadable(b)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = lambda h: send(h, 200, {}, b'{"items": []}')
        real, calls = frl.tr.column, []

        def column(*args, **kwargs):
            calls.append(1)
            if len(calls) == 2:  # the column with the junit files
                raise IndexError("a bug")
            return real(*args, **kwargs)
        with mock.patch.object(frl.tr, "column", column), self.assertRaises(IndexError):
            frl.gcs_column(self.srv.url, "bucket/logs/job", b)

    def test_cross_check_cannot_check_a_build_whose_junit_files_cannot_be_read(self):
        self.earlier_run(self.now - 3 * DAY)
        visible, gone = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (visible, gone)])
        self.fetch()
        self.junit_unreadable(gone)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48"])
        self.assertEqual(code, 1)
        self.assertIn(f"cannot check job/{gone} by TestGrid's rule: ValueError: its junit files cannot be read",
                      out.getvalue())

    def test_the_gap_floor_allows_for_the_longer_limit_of_the_last_scan(self):
        # the last scan, 12 h ago, had an 8.5 h limit; now 6 h: a build that started 7 h before it
        # was not overdue then, and must be searched for now
        b = snowflake(self.now - 19 * HOUR)
        self.failed_in_gcs(b, 19 * HOUR)
        oldest = snowflake(self.now - 18.5 * HOUR)
        self.show([oldest], {"job.Overall": ([1], [""])}, start=int(self.now - 18.5 * HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (b, oldest)])
        self.put_state(tabs={"dash#t": {"scan": self.now - 12 * HOUR, "oldest": self.now - 30 * HOUR, "margin": 8.5}})
        self.fetch()
        self.assertIn(f"job/{b}", self.runs()[-1]["outcomes"].get("backfilled", []))

    def test_a_table_without_results_keeps_the_rows_last_seen(self):
        b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(b, 30 * HOUR)  # red in Overall only
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", SUCCEEDED)
        self.srv.table("dash", "t", table("bucket/logs/job", [], {}))  # no results now
        self.srv.gcs_listing("bucket", [])
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 60 * HOUR,
                                        "rows": {"overall": False, "pod": True}}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - DAY}})
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{b}"]})
        self.assertEqual(self.state()["tabs"]["dash#t"]["rows"], {"overall": False, "pod": True})


class SecondWaveStorage(ArchiveWorld):
    def files(self):
        out = {}
        for n in os.listdir(self.bdir()):
            with open(os.path.join(self.bdir(), n), "rb") as f:
                out[n] = f.read()
        return out

    def test_a_refresh_of_a_copy_in_parts_cut_off_while_staged_leaves_the_old_copy(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(random.Random(3).randbytes(350), "1"))
        self.assertEqual(self.fetch(*LIMIT_ARGS), 0)
        before = self.files()
        self.srv.route(self.base + "/build-log.txt", obj(random.Random(4).randbytes(450), "2"))
        real = frl.temp_path

        def full(path):
            if path.endswith(".002"):
                raise OSError(28, "No space left on device")
            return real(path)
        with mock.patch.object(frl, "temp_path", full):
            self.assertEqual(self.fetch(*LIMIT_ARGS), 1)
        after = self.files()
        self.assertEqual(sorted(after), sorted(before))
        self.assertEqual({n: v for n, v in after.items() if n != "meta.json"},
                         {n: v for n, v in before.items() if n != "meta.json"})
        self.assertTrue(frl.archived_intact(self.bdir(), deep=True))

    def test_the_main_thread_merges_red_sightings_only_and_says_what_the_run_did(self):
        target = os.path.join(self.root, "x")
        os.makedirs(target)
        t = {"dashboard": "d", "tab": "t", "red_cells": [{"test": "job.Overall", "status": "FAIL", "message": None}]}
        frl.write_json(os.path.join(target, "meta.json"), {"tabs": [t], "archived_by_run": "R1", "log": {"md5": "x"}})
        rec = {"tabs": [t, {"dashboard": "d", "tab": "u", "red_cells": []}, dict(t, tab="v", judged_from_gcs=True)]}
        self.assertEqual(frl.add_sightings(target, rec, "R1"), "archived")
        self.assertEqual([x["tab"] for x in frl.read_json(os.path.join(target, "meta.json"))["tabs"]], ["t", "v"])
        self.assertEqual(frl.add_sightings(target, rec, "R2"), "already-archived")

    def test_the_cleanup_goes_on_past_files_that_vanish(self):
        cut = os.path.join(self.root, "runs", "2026-10", "2026-10-01T000000Z", "job", "7")
        os.makedirs(cut)
        for n in ("build-log.txt.000", ".build-log.txt.abc.part"):
            open(os.path.join(cut, n), "w").close()
        real = os.listdir

        def listdir(path):
            names = real(path)
            if path == cut and ".build-log.txt.abc.part" in names:  # a worker cut off removes its temp file now
                os.remove(os.path.join(cut, ".build-log.txt.abc.part"))
            return names
        with mock.patch.object(frl.os, "listdir", listdir):
            frl.clean_partial_builds(self.root, [])
        self.assertFalse(os.path.exists(cut))



class ThirdWave(r4.World):
    """The third review: of the second review's fixes."""

    def test_a_run_remembers_its_margin_and_a_failed_gap_search_keeps_it(self):
        oldest = snowflake(self.now - HOUR)
        self.show([oldest], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{oldest}/finished.json"])
        self.fetch("--max-job-hours", "8.5")
        self.assertEqual(self.state()["tabs"]["dash#t"]["margin"], 8.5)
        # a later run with a gap whose GCS listing fails keeps the old scan, and its margin
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR, "margin": 7.5}})
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = lambda h: send(h, 503, {}, b"")
        self.assertEqual(self.fetch(), 1)
        self.assertEqual(self.state()["tabs"]["dash#t"]["margin"], 7.5)

    def unreadable_junit(self, b, finished_ago):
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - finished_ago)}))
        real = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def listing(h):
            if f"logs/job/{b}/" in urllib.parse.unquote(h.path) and "delimiter" not in h.path:
                send(h, 403, {}, b"denied")
            else:
                real(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing

    def test_a_red_build_whose_junit_files_cannot_be_read_says_so_on_both_paths(self):
        self.srv.gcs_listing("bucket", [])
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        gap, watched = snowflake(self.now - 30 * HOUR), snowflake(self.now - 20 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (gap, visible)])
        for b in (gap, watched):
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log\n"))
            self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", SUCCEEDED)
        self.unreadable_junit(gap, 29 * HOUR)
        self.unreadable_junit(watched, 19 * HOUR)
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       watch={f"job/{watched}": {"rec": self.watched(watched), "first_seen": self.now - DAY}})
        self.fetch()
        self.assertEqual(sorted(self.runs()[-1]["outcomes"]["backfilled"]), sorted([f"job/{gap}", f"job/{watched}"]))
        for b in (gap, watched):
            self.assertIn("its junit files cannot be read", self.meta(b)["backfilled"])

    def test_a_running_build_whose_junit_files_cannot_be_read_is_only_watched(self):
        b = snowflake(self.now - 5 * HOUR)  # no finished.json: running for TestGrid
        self.srv.gcs_listing("bucket", [])
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        real = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = \
            lambda h: send(h, 403, {}, b"") if f"logs/job/{b}/" in urllib.parse.unquote(h.path) else real(h)
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"watching": [f"job/{b}"]})

    def test_a_watched_build_is_not_settled_in_a_run_that_could_not_read_every_tab(self):
        self.srv.summary("dash", {"t": "FAILING", "t2": "FAILING"})
        b = snowflake(self.now - 25 * HOUR)
        start = int(self.now - 25 * HOUR)
        self.srv.gcs_listing("bucket", [])
        self.show([b], {"job.Overall": ([4], [""])}, start=start)
        self.srv.table("dash", "t2", table("bucket/logs/job", [b], {"job.Overall": ([4], [""])}, start=start * 1000))
        self.fetch()
        self.assertIn(f"job/{b}", self.state()["watch"])
        self.show([b], {"job.Pod": ([1], [""])}, start=start)  # t (which hides Overall) shows it settled ...
        self.srv.table("dash", "t2", (500, {}, b""))  # ... while t2, which might show it red, cannot be read
        self.fetch()
        self.assertEqual(self.runs()[-1]["outcomes"].get("watching"), [f"job/{b}"])
        self.assertIn(f"job/{b}", self.state()["watch"])


class ThirdWaveCrossCheck(r4.World):
    def test_a_build_red_without_its_unreadable_junit_files_is_still_judged_red(self):
        self.earlier_run(self.now - 3 * DAY)
        visible, gone = snowflake(self.now - HOUR), snowflake(self.now - 30 * HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{visible}/finished.json"])  # the fetch does not see it
        self.fetch()
        self.srv.route(f"/bucket/logs/job/{gone}/started.json", (200, {}, {"timestamp": frl.created(gone)}))
        self.srv.route(f"/bucket/logs/job/{gone}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": frl.created(gone) + 1800}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (visible, gone)])
        listing = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = \
            lambda h: send(h, 403, {}, b"") if f"logs/job/{gone}/" in urllib.parse.unquote(h.path) else listing(h)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48"])
        self.assertEqual(code, 1)
        self.assertIn(f"MISSED red build job/{gone} (dash#t)", out.getvalue())


class ThirdWaveUnits(unittest.TestCase):
    def test_parts_replace_the_old_copy_all_or_none_when_the_run_is_abandoned(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        with tempfile.TemporaryDirectory() as d:
            final = os.path.join(d, "build-log.txt")
            for i in range(3):
                with open(f"{final}.{i:03d}", "wb") as f:
                    f.write(b"O" * 10)
            tmp = os.path.join(d, ".dl.part")
            with open(tmp, "wb") as f:
                f.write(b"N" * 30)
            real, started = frl.replace, threading.Event()

            def slow_replace(src, dst):
                out = real(src, dst)
                if dst.endswith(".000"):
                    started.set()
                    time.sleep(0.3)  # abandon() is called now, from another thread
                return out
            result = {}

            def worker():
                frl.bind_context(ctx)
                try:
                    result["parts"] = frl.place(tmp, final, 10)
                except Exception as e:  # noqa: BLE001
                    result["error"] = e
            with mock.patch.object(frl, "replace", slow_replace):
                t = threading.Thread(target=worker, daemon=True)
                t.start()
                started.wait(5)
                ctx.abandon()
                t.join(5)
            contents = set()
            for n in sorted(os.listdir(d)):
                if not n.startswith("."):
                    with open(os.path.join(d, n), "rb") as f:
                        contents.add(f.read())
            self.assertEqual(contents, {b"N" * 10}, result)

    def test_the_main_thread_says_what_this_run_did_to_a_build(self):
        with tempfile.TemporaryDirectory() as target:
            def meta(**kw):
                frl.write_json(os.path.join(target, "meta.json"), dict({"tabs": [], "archived_by_run": "R0"}, **kw))
            cases = [(dict(archived_by_run="R1", log={"md5": "x"}, backfilled="note"), "backfilled"),
                     (dict(archived_by_run="R1", log=None), "archived-without-log"),
                     (dict(archived_by_run="R1", log={"md5": "x"}, repaired_by_run="R1"), "repaired"),
                     (dict(repaired_by_run="R1"), "repaired"), (dict(log_recovered_by_run="R1"), "log-recovered"),
                     (dict(refreshed_by_run="R1"), "refreshed"), (dict(refreshed_by_run="R0"), "already-archived")]
            for fields, want in cases:
                with self.subTest(fields):
                    meta(**fields)
                    self.assertEqual(frl.add_sightings(target, {"tabs": []}, "R1"), want)
            meta()
            omitted = {"dashboard": "d", "tab": "m", "red_cells": [], "red_cells_omitted": 7}
            frl.add_sightings(target, {"tabs": [omitted]}, "R1")
            self.assertEqual([t["tab"] for t in frl.read_json(os.path.join(target, "meta.json"))["tabs"]], ["m"])

    def test_an_index_entry_lists_a_limited_number_of_red_tests(self):
        cells = [{"test": f"t{i:03d}", "status": "FAIL", "message": None} for i in range(150)]
        meta = {"started": None, "created": time.time() - 3600, "query": "b/logs/job", "log": None, "podinfo": None,
                "tabs": [{"dashboard": "d", "tab": "t", "red_cells": cells}]}
        entry = frl.index_entry("job", "1", meta, time.time())
        self.assertEqual((len(entry["red_tests"]), entry["red_test_count"]), (frl.INDEX_TESTS, 150))
        self.assertIn("t000 (+149)", "".join(frl.build_rows([dict(entry, status="x")], files=False)))

    def test_proxy_credentials_need_a_user_and_a_password(self):
        for proxy, sent in (("http://u:p@proxy.invalid:3128", True), ("http://u@proxy.invalid:3128", False),
                            ("http://:p@proxy.invalid:3128", False)):
            with self.subTest(proxy), mock.patch.dict(frl._PROXIES, {"http": proxy}, clear=True), \
                    mock.patch.dict(os.environ, {k: v for k, v in os.environ.items() if "proxy" not in k.lower()},
                                    clear=True):
                frl._PROXY_POOLS.pop(proxy, None)
                pool = frl.pool_for("http://example.invalid/x")
                self.assertEqual("proxy-authorization" in {k.lower() for k in pool.proxy_headers}, sent)
                self.assertNotIn("@", pool.proxy.host or "")



class FourthWave(r4.World):
    """The fourth review: of the third review's fixes."""

    def deny_junit_listing(self, b):
        real = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = \
            lambda h: send(h, 403, {}, b"") if f"logs/job/{b}/" in urllib.parse.unquote(h.path) else real(h)

    def test_cross_check_notes_a_running_build_with_unreadable_junit_files(self):
        self.earlier_run(self.now - 3 * DAY)
        visible, running = snowflake(self.now - HOUR), snowflake(self.now - 5 * HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.srv.gcs_listing("bucket", [f"logs/job/{visible}/finished.json"])
        self.fetch()
        self.srv.route(f"/bucket/logs/job/{running}/started.json", (200, {}, {"timestamp": frl.created(running)}))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/started.json" for x in (visible, running)])
        self.deny_junit_listing(running)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48"])
        self.assertNotIn(f"cannot check job/{running}", out.getvalue())
        self.assertIn(f"job/{running}: TestGrid no longer lists it and it is not decided yet", out.getvalue())
        self.assertEqual(code, 0, out.getvalue())

    def test_a_finished_build_waiting_for_podinfo_with_unreadable_junit_is_watched(self):
        b = snowflake(self.now - 2 * HOUR)
        self.srv.gcs_listing("bucket", [])
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 600)}))
        self.deny_junit_listing(b)
        self.show([snowflake(self.now)], {"job.Overall": ([1], [""])}, start=int(self.now))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        self.assertEqual(self.fetch(), 0, self.runs()[-1]["errors"])
        self.assertEqual(self.runs()[-1]["outcomes"], {"watching": [f"job/{b}"]})

    def watched_and_shown_settled(self):
        b = snowflake(self.now - 10 * HOUR)
        self.show([b], {"job.Overall": ([1], [""])}, start=int(self.now - 10 * HOUR))  # settled, not red, on dash#t
        self.srv.gcs_listing("bucket", [])
        self.in_gcs_passed(b, self.now - 10 * HOUR, self.now - 9 * HOUR)  # (and so it was all along)
        return b

    def in_gcs_passed(self, b, started, finished):
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(started)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"timestamp": int(finished), "passed": True, "result": "SUCCESS"}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))

    def test_a_failed_summary_does_not_keep_a_watched_build_from_being_judged(self):
        # (it is judged from GCS on its tabs, whichever tabs could be read now)
        b = self.watched_and_shown_settled()
        self.srv.route("/dash2/summary", (500, {}, b""))
        self.put_state(watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            frl.main(["--archive", self.root, "--dashboard", "dash", "--dashboard", "dash2", "--testgrid",
                      self.srv.url, "--gcs", self.srv.url])
        self.assertEqual(self.runs()[-1]["outcomes"].get("not-red"), [f"job/{b}"])

    def test_a_failed_tab_does_not_keep_it_watched_as_it_is_judged_from_gcs(self):
        b = self.watched_and_shown_settled()
        self.srv.summary("dash", {"t": "FAILING", "other": "FAILING"})
        self.srv.table("dash", "other", (500, {}, b""))
        # (judged from GCS on its tabs, whatever tab could not be read now)
        for other_query, want in (("bucket/logs/job2", {"not-red": [f"job/{b}"]}),
                                  ("bucket/logs/job", {"not-red": [f"job/{b}"]})):
            with self.subTest(other_query):
                self.put_state(tabs={"dash#other": {"scan": self.now - HOUR, "oldest": self.now - DAY,
                                                    "query": other_query}},
                               watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - HOUR}})
                self.fetch()
                self.assertEqual(self.runs()[-1]["outcomes"], want)

    def test_since_hours_does_not_drop_a_watched_build_testgrid_now_shows_red(self):
        b = snowflake(self.now - 30 * HOUR)
        start = int(self.now - 30 * HOUR)
        self.show([b], {"job.Overall": ([12], ["did not finish within 24 hours"])}, start=start)  # red now
        self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log\n"))
        self.srv.gcs_listing("bucket", [])
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 40 * HOUR}},
                       watch={f"job/{b}": {"rec": dict(self.watched(b), started=start), "first_seen": self.now - DAY}})
        self.fetch("--since-hours", "6")
        self.assertEqual(self.runs()[-1]["outcomes"].get("archived"), [f"job/{b}"])


class FourthWaveUnits(unittest.TestCase):
    def test_a_capped_index_entry_keeps_its_job_rows(self):
        cells = [{"test": f"t{i:03d}", "status": "FAIL", "message": None} for i in range(150)]
        cells += [{"test": "job.Overall", "status": "FAIL", "message": "x"},
                  {"test": "job.Pod", "status": "FAIL", "message": "y"}]
        meta = {"started": None, "created": time.time() - 3600, "query": "b/logs/job", "log": None, "podinfo": None,
                "tabs": [{"dashboard": "d", "tab": "t", "red_cells": cells}]}
        entry = frl.index_entry("job", "1", meta, time.time())
        self.assertEqual((len(entry["red_tests"]), entry["red_test_count"]), (frl.INDEX_TESTS, 152))
        self.assertEqual(entry["red_tests"][-2:], ["job.Pod", "job.Overall"])

    def test_the_index_row_counts_the_other_red_tests(self):
        def row(n):
            return "".join(frl.build_rows([{"time": time.time(), "job": "j", "build": "1", "tabs": ["d#t"],
                                            "red_tests": [f"t{i}" for i in range(min(n, 3))], "red_test_count": n,
                                            "prow_url": "u", "status": "s"}], files=False))
        self.assertIn("t0 |", row(1))
        self.assertIn("t0 (+1)", row(2))
        self.assertIn("t0 (+99)", row(100))

    def test_proxy_credentials_go_out_in_utf8(self):
        proxy = "http://u:%C3%A9@proxy.invalid:3128"
        with mock.patch.dict(frl._PROXIES, {"http": proxy}, clear=True), \
                mock.patch.dict(os.environ, {k: v for k, v in os.environ.items() if "proxy" not in k.lower()},
                                clear=True):
            frl._PROXY_POOLS.pop(proxy, None)
            headers = {k.lower(): v for k, v in frl.pool_for("http://example.invalid/x").proxy_headers.items()}
        self.assertEqual(headers["proxy-authorization"], "Basic " + base64.b64encode("u:é".encode()).decode())


if __name__ == "__main__":
    unittest.main()
