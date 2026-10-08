"""Tests for fetch_red_logs.py against a local fake TestGrid + GCS server."""
import base64
import contextlib
import io
import gzip
import hashlib
import http.server
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402

frl.BACKOFF = 0
DAY = 86400
NOW_MS = int(time.time() * 1000)
OLD = NOW_MS - 30 * DAY * 1000  # a month ago


def rle(*cells):
    out = []
    for v in cells:
        if out and out[-1]["value"] == v:
            out[-1]["count"] += 1
        else:
            out.append({"value": v, "count": 1})
    return out


def table(query, builds, rows, start=OLD):
    """rows: {name: (cells, messages)}; builds are listed newest first, an hour apart."""
    return {
        "query": query,
        "changelists": builds,
        "timestamps": [start - i * 3600 * 1000 for i in range(len(builds))],
        "tests": [{"name": n, "statuses": rle(*cells), "messages": msgs} for n, (cells, msgs) in rows.items()],
    }


def md5_b64(data):
    return base64.b64encode(hashlib.md5(data).digest()).decode()


def gcs_object(data, md5=None, stored_len=True):
    headers = {"x-goog-stored-content-encoding": "identity", "Content-Length": str(len(data)),
               "x-goog-hash": f"crc32c=AAAAAA==,md5={md5 or md5_b64(data)}"}
    if stored_len:
        headers["x-goog-stored-content-length"] = str(len(data))
    return 200, headers, data


def send(h, status, headers, body):
    h.send_response(status)
    for k, v in headers.items():
        h.send_header(k, v)
    if "Content-Length" not in headers:
        h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    if not getattr(h, "head", False):
        h.wfile.write(body)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_HEAD(self):
        self.head = True
        self.do_GET()

    def do_GET(self):
        self.server.hits[self.path] = self.server.hits.get(self.path, 0) + 1
        route = self.server.routes.get(self.path)
        if route is None:
            route = next((v for k, v in self.server.prefix_routes.items() if self.path.startswith(k)), None)
        if route is None:
            send(self, 404, {}, b"")
        elif callable(route):
            route(self)
        else:
            status, headers, body = route
            send(self, status, headers, json.dumps(body).encode() if isinstance(body, (dict, list)) or body is None else body)


def gzip_stored(content, stored=None):
    stored = stored if stored is not None else gzip.compress(content)

    def serve(h):
        headers = {"x-goog-stored-content-encoding": "gzip", "x-goog-stored-content-length": str(len(stored)),
                   "x-goog-hash": f"md5={md5_b64(stored)}"}
        if "gzip" in h.headers.get("Accept-Encoding", ""):
            body = stored
            headers["Content-Encoding"] = "gzip"
        else:  # decompressive transcoding
            body = content
        send(h, 200, headers, body)
    return serve


def truncated(h):
    h.send_response(200)
    h.send_header("Content-Length", "100")
    h.end_headers()
    h.wfile.write(b"only ten b")


class FakeServer:
    def __init__(self):
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.routes, self.httpd.hits, self.httpd.prefix_routes = {}, {}, {}
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def route(self, path, value):
        self.httpd.routes[path] = value

    def gcs_listing(self, bucket, objects):
        """Serve the GCS JSON list API for `bucket`, honouring prefix/delimiter like GCS does."""
        def serve(h):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            prefix = q["prefix"][0]
            names = sorted({prefix + o[len(prefix):].split("/", 1)[0] + "/" for o in objects
                            if o.startswith(prefix) and "/" in o[len(prefix):]})
            send(h, 200, {"Content-Type": "application/json"}, json.dumps({"prefixes": names}).encode())
        self.httpd.prefix_routes[f"/storage/v1/b/{bucket}/o?"] = serve

    def hits(self, path):
        return self.httpd.hits.get(path, 0)

    def summary(self, dashboard, tabs):
        self.route(f"/{dashboard}/summary", (200, {}, {t: ({"overall_status": s} if s else None) for t, s in tabs.items()}))

    def table(self, dashboard, tab, value):
        self.route(frl.table_url("", dashboard, tab), value if isinstance(value, tuple) else (200, {}, value))

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


# Statuses: 0 none, 1 pass, 4 running, 6 unknown (grey), 9 timed out, 10 categorized fail,
# 11 build fail (black), 12 fail, 13 flaky (purple), 14 tool fail.
PURPLE = table("bucket/logs/job-a",
               ["1006", "1005", "1004", "1003", "1002", "1001", "1000", "999"],
               {"job-a.Overall": ([4, 1, 12, 1, 1, 1, 11, 6], ["R", "", "fail", "", "", "", "build", "?"]),
                "test-x": ([0, 1, 12, 0, 9, 1, 1, 1], ["", "boom", "slow", "", "", ""]),
                "test-y": ([0, 13, 1, 1, 1, 14, 1, 1], ["flaky", "", "", "", "tool", "", ""])})
GREEN = table("bucket/logs/job-b", ["2002", "2001", "2000"], {"job-b.Overall": ([1, 12, 1], ["", "", ""])},
              start=NOW_MS)  # 2001 started an hour ago and has uploaded nothing yet
BAD = table("bucket/logs/job-c", ["3001", "3000"], {"job-c.Overall": ([12, 12], ["", ""])})
FIN = (200, {}, {"result": "FAILURE", "timestamp": 1})


class RedColumnsTest(unittest.TestCase):
    def test_only_cells_painted_red(self):
        reds = frl.red_columns(PURPLE)
        self.assertEqual(sorted(reds), [2, 4, 5])  # builds 1004, 1002, 1001
        self.assertEqual([c["test"] for c in reds[2]], ["job-a.Overall", "test-x"])
        self.assertEqual([c["status"] for c in reds[4]], ["TIMED_OUT"])
        self.assertEqual([c["status"] for c in reds[5]], ["TOOL_FAIL"])

    def test_messages_skip_no_result_cells(self):
        reds = frl.red_columns(PURPLE)
        self.assertEqual(reds[2][1]["message"], "boom")
        self.assertEqual(reds[4][0]["message"], "slow")
        self.assertEqual(reds[5][0]["message"], "tool")

    def test_misaligned_messages_are_dropped_not_guessed(self):
        t = table("b/logs/j", ["2", "1"], {"r": ([12, 0], ["a", "extra"])})
        self.assertIsNone(frl.red_columns(t)[0][0]["message"])

    def test_rejects_rows_that_do_not_span_the_table(self):
        t = table("b/logs/j", ["2", "1"], {"r": ([12], ["a"])})
        with self.assertRaises(ValueError):
            frl.red_columns(t)


class GunzipTest(unittest.TestCase):
    def test_multi_member(self):
        g = frl.Gunzip()
        data = gzip.compress(b"first half\n") + gzip.compress(b"SECOND HALF\n")
        out = b"".join(g.feed(data[i:i + 7]) for i in range(0, len(data), 7)) + g.finish()
        self.assertEqual(out, b"first half\nSECOND HALF\n")

    def test_truncated_and_garbage(self):
        g = frl.Gunzip()
        g.feed(gzip.compress(b"x" * 1000)[:-6])
        with self.assertRaises(frl.IntegrityError):
            g.finish()
        with self.assertRaises(frl.IntegrityError):
            frl.Gunzip().feed(b"not gzip at all")


class EndToEndTest(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer()
        self.root = tempfile.mkdtemp()
        s = self.srv
        s.summary("dash", {"purple": "FLAKY", "green": "PASSING", "bad": "FLAKY", "broken": "FLAKY"})
        s.table("dash", "purple", PURPLE)
        s.table("dash", "green", GREEN)
        s.table("dash", "bad", BAD)
        s.table("dash", "broken", (500, {}, b"oops"))
        for b in ("1004", "1002", "1001"):
            s.route(f"/bucket/logs/job-a/{b}/finished.json", FIN)
        s.route("/bucket/logs/job-a/1004/build-log.txt", gcs_object(b"log 1004\n"))
        s.route("/bucket/logs/job-a/1002/build-log.txt", gzip_stored(b"log 1002\n" * 1000))
        s.route("/bucket/logs/job-a/1001/podinfo.json", gcs_object(b'{"pod": "never started"}'))
        # 1001 finished without a log; job-b/2001 has nothing uploaded yet.
        s.route("/bucket/logs/job-c/3001/finished.json", FIN)
        s.route("/bucket/logs/job-c/3001/build-log.txt", gcs_object(b"log 3001\n", md5=md5_b64(b"other")))
        s.route("/bucket/logs/job-c/3000/finished.json", FIN)
        s.route("/bucket/logs/job-c/3000/build-log.txt", truncated)

    def tearDown(self):
        self.srv.close()
        shutil.rmtree(self.root)

    def heal(self):
        """Make every route succeed so a run can finish clean."""
        s = self.srv
        s.table("dash", "broken", table("bucket/logs/job-d", ["4000"], {"o": ([1], [""])}))
        s.route("/bucket/logs/job-c/3001/build-log.txt", gcs_object(b"log 3001\n"))
        s.route("/bucket/logs/job-c/3000/build-log.txt", gcs_object(b"log 3000\n"))
        s.route("/bucket/logs/job-b/2001/build-log.txt", gcs_object(b"log 2001\n"))

    def run_tool(self, *extra):
        return frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url,
                         "--gcs", self.srv.url, *extra])

    def path(self, *parts):
        return os.path.join(self.root, *parts)

    def runs(self):
        return [frl.read_json(self.path("runs", n)) for n in sorted(os.listdir(self.path("runs")))]

    def last_run(self):
        return self.runs()[-1]

    def state(self):
        return frl.read_json(self.path("state.json"))

    def indexed(self):
        out = []
        for w in frl.read_json(self.path("index.json")):
            out += frl.read_json(self.path(w["data"]))
        return out

    def week_pages(self):
        text = ""
        for w in frl.read_json(self.path("index.json")):
            with open(self.path(w["page"])) as f:
                text += f.read()
        return text

    def test_first_run(self):
        self.assertEqual(self.run_tool(), 1)  # tab "broken" and two bad downloads fail
        run = self.last_run()
        self.assertEqual(run["outcomes"], {"archived": ["job-a/1002", "job-a/1004"],
                                           "archived-without-log": ["job-a/1001"],
                                           "pending": ["job-b/2001"]})
        self.assertEqual(sorted(e["build"] for e in run["errors"]), ["job-c/3000", "job-c/3001"])
        self.assertEqual([t["tab"] for t in run["tabs"] if t.get("error")], ["broken"])

        with open(self.path("logs/job-a/1004/build-log.txt"), "rb") as f:
            self.assertEqual(f.read(), b"log 1004\n")
        with open(self.path("logs/job-a/1002/build-log.txt"), "rb") as f:
            self.assertEqual(f.read(), b"log 1002\n" * 1000)  # gzip-stored object saved as raw text
        meta = frl.read_json(self.path("logs/job-a/1004/meta.json"))
        self.assertTrue(meta["log"]["verified"])
        self.assertEqual(meta["log"]["md5"], hashlib.md5(b"log 1004\n").hexdigest())
        self.assertEqual(meta["tabs"][0]["tab_status"], "FLAKY")
        no_log = frl.read_json(self.path("logs/job-a/1001/meta.json"))
        self.assertIsNone(no_log["log"])
        self.assertTrue(no_log["podinfo"]["verified"])

        # Not red: running, passing, flaky (purple), build fail (black), unknown (grey).
        for b in ("1006", "1005", "1003", "1000", "999"):
            self.assertFalse(os.path.exists(self.path("logs/job-a", b)), b)
        # Failed downloads leave nothing behind and were retried.
        for b in ("job-c/3001", "job-c/3000", "job-b/2001"):
            self.assertFalse(os.path.exists(self.path("logs", b)), b)
        self.assertEqual(self.srv.hits("/bucket/logs/job-c/3001/build-log.txt"), frl.RETRIES)
        self.assertEqual(self.srv.hits("/bucket/logs/job-c/3000/build-log.txt"), frl.RETRIES)
        # Per-tab state: the failed tab is not marked scanned; unfinished builds are remembered.
        state = self.state()
        self.assertEqual(sorted(state["tabs"]), ["dash#bad", "dash#green", "dash#purple"])
        self.assertEqual(sorted(state["unresolved"]), ["job-b/2001", "job-c/3000", "job-c/3001"])
        self.assertEqual(sorted(e["build"] for e in self.indexed()), ["1001", "1002", "1004"])
        with open(self.path("INDEX.md")) as f:
            md = f.read()
        self.assertIn("charts/trend-light.svg", md)
        self.assertIn("## Not archived yet", md)  # job-b/2001 is pending
        self.assertIn("[build-log.txt](../logs/job-a/1004/build-log.txt)", self.week_pages())
        week = frl.week_of(OLD // 1000)
        self.assertIn(f"- [{week}](weeks/{week}.md)", md)
        for name in ("INDEX.md", "index.json", f"weeks/{week}.md", "logs/job-a/1004/build-log.txt"):
            self.assertEqual(os.stat(self.path(name)).st_mode & 0o777, 0o666 & ~frl.UMASK, name)
        for name in ("trend-light", "trend-dark", "tabs-light", "tabs-dark"):
            self.assertTrue(os.path.getsize(self.path("charts", f"{name}.svg")) > 500, name)

    def test_second_run_only_downloads_new_builds(self):
        self.run_tool()
        self.heal()
        before = self.srv.hits("/bucket/logs/job-a/1004/build-log.txt")
        self.assertEqual(self.run_tool(), 0)
        run = self.last_run()
        self.assertEqual(run["outcomes"], {"already-archived": ["job-a/1001", "job-a/1002", "job-a/1004"],
                                           "archived": ["job-b/2001", "job-c/3000", "job-c/3001"]})
        self.assertEqual(self.srv.hits("/bucket/logs/job-a/1004/build-log.txt"), before)
        self.assertEqual(self.state()["unresolved"], {})
        self.assertEqual(len(self.runs()), 2)  # run IDs never collide

    def test_failed_build_is_retried_after_it_leaves_testgrid(self):
        self.run_tool()
        self.heal()
        self.srv.table("dash", "bad", table("bucket/logs/job-c", ["3002"], {"o": ([1], [""])}))
        self.assertEqual(self.run_tool(), 0)
        run = self.last_run()
        self.assertEqual(run["retried_from_state"], 2)
        self.assertIn("job-c/3001", run["outcomes"]["archived"])
        self.assertIn("job-c/3000", run["outcomes"]["archived"])

    def test_since_hours_limits_backfill(self):
        self.heal()
        self.run_tool("--since-hours", "24")  # only job-b/2001 is recent
        self.assertEqual(self.last_run()["outcomes"], {"archived": ["job-b/2001"]})

    def test_since_hours_still_catches_builds_running_at_the_last_scan(self):
        self.heal()
        recent = table("bucket/logs/job-e", ["5001"], {"o": ([12], [""])}, start=NOW_MS - 13 * 3600 * 1000)
        self.srv.table("dash", "green", recent)
        self.srv.route("/bucket/logs/job-e/5001/build-log.txt", gcs_object(b"log 5001\n"))
        frl.write_json(self.path("state.json"), {"tabs": {"dash#green": time.time() - 12 * 3600}})
        self.run_tool("--since-hours", "12")
        self.assertIn("job-e/5001", self.last_run()["outcomes"]["archived"])

    def test_pending_too_long_becomes_an_error(self):
        self.srv.table("dash", "green", table("bucket/logs/job-b", ["2001"], {"o": ([12], [""])}))
        self.run_tool()
        errors = {e["build"]: e["error"] for e in self.last_run()["errors"]}
        self.assertIn("nothing in GCS", errors["job-b/2001"])

    def test_log_that_appears_later_is_recovered(self):
        self.run_tool()
        self.srv.route("/bucket/logs/job-a/1001/build-log.txt", gcs_object(b"late log\n"))
        self.run_tool()
        self.assertEqual(self.last_run()["outcomes"]["log-recovered"], ["job-a/1001"])
        with open(self.path("logs/job-a/1001/build-log.txt"), "rb") as f:
            self.assertEqual(f.read(), b"late log\n")

    def test_missing_or_damaged_files_are_repaired(self):
        self.run_tool()
        os.remove(self.path("logs/job-a/1004/build-log.txt"))
        with open(self.path("logs/job-a/1002/build-log.txt"), "w") as f:
            f.write("garbage")
        self.run_tool()
        self.assertEqual(self.last_run()["outcomes"]["repaired"], ["job-a/1002", "job-a/1004"])
        with open(self.path("logs/job-a/1004/build-log.txt"), "rb") as f:
            self.assertEqual(f.read(), b"log 1004\n")

    def test_stray_files_and_corrupt_meta_do_not_break_runs(self):
        self.heal()
        os.makedirs(self.path("logs/job-z/1"))
        for p in ("logs/.gitkeep", "logs/job-a.gitkeep", "logs/job-z/.gitkeep"):
            open(self.path(p), "w").close()
        with open(self.path("logs/job-z/1/meta.json"), "w") as f:
            f.write("{not json")
        with open(self.path("state.json"), "w") as f:
            f.write("{corrupt")
        self.assertEqual(self.run_tool(), 1)
        run = self.last_run()
        self.assertEqual(len(run["outcomes"]["archived"]), 5)
        self.assertEqual(run["outcomes"]["archived-without-log"], ["job-a/1001"])
        self.assertEqual([e["error"][:28] for e in run["errors"]], ["unreadable logs/job-z/1/meta"])
        self.assertIn("state.json is corrupt", run["warnings"][0])

    def test_odd_summary_entries_do_not_stop_the_run(self):
        self.srv.summary("dash", {"purple": "FLAKY", "weird": None})
        self.run_tool()
        run = self.last_run()
        self.assertIn("job-a/1004", run["outcomes"]["archived"])
        self.assertTrue(next(t for t in run["tabs"] if t["tab"] == "weird")["error"])

    def test_empty_summary_is_an_error(self):
        self.srv.summary("dash", {})
        self.assertEqual(self.run_tool(), 1)
        self.assertIn("summary lists no tabs", self.last_run()["tabs"][0]["error"])
        self.assertEqual(self.state()["tabs"], {})

    def test_gap_warning_is_per_tab(self):
        frl.write_json(self.path("state.json"), {"tabs": {"dash#purple": time.time() - 3600,
                                                          "dash#green": time.time() - 3 * DAY}})
        self.run_tool()
        warnings = self.last_run()["warnings"]
        self.assertEqual(len(warnings), 1)
        self.assertIn("dash#green", warnings[0])  # GREEN only shows the last 3 hours

    def test_a_young_tab_is_not_a_gap(self):
        # GREEN's whole history is 3 hours old; the last scan saw the same oldest column.
        frl.write_json(self.path("state.json"), {"tabs": {"dash#green": {"scan": time.time() - 600,
                                                                         "oldest": (NOW_MS - 2 * 3600 * 1000) // 1000}}})
        self.run_tool()
        self.assertEqual([w for w in self.last_run()["warnings"] if "may be missing" in w], [])

    def test_trickling_response_times_out(self):
        def trickle(h):
            h.send_response(200)
            h.send_header("Content-Length", "50")
            h.end_headers()
            for _ in range(50):
                h.wfile.write(b"x")
                h.wfile.flush()
                time.sleep(0.05)

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", trickle)
        old = frl.BASE_DEADLINE, frl.RETRIES
        frl.BASE_DEADLINE, frl.RETRIES = 0.3, 2
        try:
            self.run_tool()
        finally:
            frl.BASE_DEADLINE, frl.RETRIES = old
        errors = {e["build"]: e["error"] for e in self.last_run()["errors"]}
        self.assertIn("too slow", errors["job-a/1004"])

    def test_bad_gzip_body_is_retried(self):
        calls = []
        good = gzip_stored(b"log 1004\n")

        def garbled_once(h):
            calls.append(1)
            if len(calls) == 1:
                stored = gzip.compress(b"log 1004\n")
                send(h, 200, {"Content-Encoding": "gzip", "x-goog-stored-content-encoding": "gzip"},
                     b"\x1f\x8b" + b"\x00" * (len(stored) - 2))
            else:
                good(h)

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", garbled_once)
        self.run_tool()
        self.assertEqual(len(calls), 2)
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["archived"])

    def test_multi_member_gzip_object(self):
        content = b"first half\n" * 100 + b"SECOND HALF\n" * 300
        stored = gzip.compress(b"first half\n" * 100) + gzip.compress(b"SECOND HALF\n" * 300)
        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", gzip_stored(content, stored))
        self.run_tool()
        with open(self.path("logs/job-a/1004/build-log.txt"), "rb") as f:
            self.assertEqual(f.read(), content)

    def test_unsupported_encoding_is_rejected(self):
        self.srv.route("/bucket/logs/job-a/1004/build-log.txt",
                       (200, {"Content-Encoding": "br", "x-goog-stored-content-encoding": "br"}, b"brotli?"))
        self.run_tool()
        errors = {e["build"]: e["error"] for e in self.last_run()["errors"]}
        self.assertIn("unsupported Content-Encoding", errors["job-a/1004"])

    def test_multi_prefix_query_is_rejected(self):
        self.srv.table("dash", "broken", table("bucket/logs/job-d,other/logs/job-d", ["4000"], {"o": ([12], [""])}))
        self.run_tool()
        tab = next(t for t in self.last_run()["tabs"] if t["tab"] == "broken")
        self.assertIn("unsupported GCS query", tab["error"])

    def test_gzip_storage_keeps_the_raw_log_verifiable(self):
        self.run_tool("--gzip")
        meta = frl.read_json(self.path("logs/job-a/1002/meta.json"))
        self.assertEqual(meta["log"]["file"], "build-log.txt.gz")
        with gzip.open(self.path("logs/job-a/1002/build-log.txt.gz"), "rb") as f:
            raw = f.read()
        self.assertEqual(raw, b"log 1002\n" * 1000)
        self.assertEqual(hashlib.md5(raw).hexdigest(), meta["log"]["md5"])
        self.assertEqual(os.path.getsize(self.path("logs/job-a/1002/build-log.txt.gz")), meta["log"]["file_bytes"])
        self.assertFalse(os.path.exists(self.path("logs/job-a/1002/build-log.txt")))
        self.assertIn("logs/job-a/1002/build-log.txt.gz", [f for e in self.indexed() for f in e["files"]])
        # a second run sees it intact; deleting it gets it repaired, still gzipped
        self.run_tool("--gzip")
        self.assertIn("job-a/1002", self.last_run()["outcomes"]["already-archived"])
        os.remove(self.path("logs/job-a/1002/build-log.txt.gz"))
        self.run_tool("--gzip")
        self.assertEqual(self.last_run()["outcomes"]["repaired"], ["job-a/1002"])
        # switching an archive from raw to --gzip replaces the raw copy when it is re-downloaded
        os.remove(self.path("logs/job-a/1002/build-log.txt.gz"))
        self.run_tool()
        self.assertTrue(os.path.exists(self.path("logs/job-a/1002/build-log.txt")))
        os.remove(self.path("logs/job-a/1002/build-log.txt"))
        self.run_tool("--gzip")
        self.assertFalse(os.path.exists(self.path("logs/job-a/1002/build-log.txt")))

    def test_implausible_timestamps_fail_only_their_tab(self):
        huge = table("bucket/logs/job-f", ["6001"], {"o": ([12], [""])})
        huge["timestamps"] = [10 ** 20]
        self.srv.table("dash", "broken", huge)
        odd = table("bucket/logs/job-b", ["2002", "2001", "2000"], {"o": ([1, 12, 1], ["", "", ""])}, start=NOW_MS)
        odd["timestamps"][2] = None  # a non-red column
        self.srv.table("dash", "green", odd)
        self.run_tool()
        run = self.last_run()
        errors = {t["tab"]: t.get("error", "") for t in run["tabs"]}
        self.assertIn("implausible timestamp", errors["broken"])
        self.assertIn("implausible timestamp", errors["green"])
        self.assertIn("job-a/1004", run["outcomes"]["archived"])

    def test_poisoned_meta_does_not_crash_later_runs(self):
        os.makedirs(self.path("logs/job-z/9"))
        frl.write_json(self.path("logs/job-z/9/meta.json"),
                       {"job": "job-z", "build": "9", "query": "b/logs/job-z", "started": 1e17, "created": 1,
                        "tabs": [], "log": None, "podinfo": None, "prow_url": "x", "result": None})
        self.assertEqual(self.run_tool(), 1)
        self.assertTrue(any("implausible time" in str(e["error"]) for e in self.last_run()["errors"]))
        self.assertTrue(os.path.exists(self.path("INDEX.md")))

    def test_build_red_on_a_second_tab_later_is_added_to_meta(self):
        both = table("bucket/logs/job-a", ["1004"], {"x": ([12], ["boom"])})
        self.srv.summary("dash", {"purple": "FLAKY", "other": "FLAKY"})
        self.srv.table("dash", "other", (503, {}, b""))
        self.run_tool()
        self.assertEqual([t["tab"] for t in frl.read_json(self.path("logs/job-a/1004/meta.json"))["tabs"]], ["purple"])
        self.srv.table("dash", "other", both)
        self.run_tool()
        meta = frl.read_json(self.path("logs/job-a/1004/meta.json"))
        self.assertEqual([t["tab"] for t in meta["tabs"]], ["purple", "other"])
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["already-archived"])

    def test_repair_keeps_the_record_when_gcs_lost_the_log(self):
        self.run_tool()
        before = frl.read_json(self.path("logs/job-a/1004/meta.json"))
        os.remove(self.path("logs/job-a/1004/build-log.txt"))
        del self.srv.httpd.routes["/bucket/logs/job-a/1004/build-log.txt"]
        self.run_tool()
        errors = {e["build"]: e["error"] for e in self.last_run()["errors"]}
        self.assertIn("GCS no longer has it", errors["job-a/1004"])
        self.assertEqual(frl.read_json(self.path("logs/job-a/1004/meta.json")), before)

    def test_given_up_builds_stay_given_up_and_stay_visible(self):
        self.srv.table("dash", "green", table("bucket/logs/job-b", ["2001"], {"o": ([12], [""])}))
        rec = {"job": "job-b", "build": "2001", "query": "bucket/logs/job-b", "started": OLD // 1000,
               "created": frl.created("2001"), "tabs": [{"dashboard": "dash", "tab": "green", "tab_status": "PASSING",
                                                         "red_cells": [{"test": "o", "status": "FAIL", "message": ""}]}]}
        frl.write_json(self.path("state.json"), {"tabs": {}, "unresolved": {
            "job-b/2001": {"rec": rec, "first_seen": time.time() - 15 * DAY, "last_error": "x"}}})
        self.run_tool()
        self.assertIn("giving up", {e["build"]: e["error"] for e in self.last_run()["errors"]}["job-b/2001"])
        self.assertIn("job-b/2001", self.state()["given_up"])
        hits = self.srv.hits("/bucket/logs/job-b/2001/finished.json")
        self.run_tool()
        self.assertEqual(self.srv.hits("/bucket/logs/job-b/2001/finished.json"), hits)  # not retried
        with open(self.path("INDEX.md")) as f:
            self.assertIn("given up", f.read())

    def test_run_timeout_keeps_partial_results_and_late_workers_write_nothing(self):
        def slow(h):
            time.sleep(2)
            send(h, *gcs_object(b"log 1004\n"))

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", slow)
        self.assertEqual(self.run_tool("--run-timeout-minutes", "0.01"), 1)
        run = self.last_run()
        self.assertIn("run timeout", {e["build"]: e["error"] for e in run["errors"]}["job-a/1004"])
        self.assertIn("job-a/1004", self.state()["unresolved"])
        self.assertIn("job-a/1002", run["outcomes"]["archived"])
        time.sleep(2.5)  # let the abandoned worker finish: it must not write
        self.assertFalse(os.path.exists(self.path("logs/job-a/1004/meta.json")))
        self.assertFalse(os.path.exists(self.path("logs/job-a/1004/build-log.txt")))

    def test_truncated_json_is_retried(self):
        calls = []
        good = json.dumps(PURPLE).encode()

        def cut_once(h):
            calls.append(1)
            send(h, 200, {"Content-Length": str(len(good))} if len(calls) > 1 else {"Connection": "close"},
                 good if len(calls) > 1 else good[:100])

        self.srv.route(frl.table_url("", "dash", "purple"), cut_once)
        self.run_tool()
        self.assertEqual(len(calls), 2)
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["archived"])

    def test_plain_server_errors_do_not_shrink_concurrency(self):
        self.run_tool("--start-concurrency", "8", "--max-concurrency", "8")  # tab "broken" answers 500
        self.assertEqual(self.last_run()["concurrency"]["final"], 8)

    def test_pipes_in_names_are_escaped_in_the_index(self):
        self.srv.summary("dash", {"gce | alpha": "FLAKY"})
        self.srv.table("dash", "gce | alpha", PURPLE)
        self.run_tool()
        self.assertIn("gce \\| alpha", self.week_pages())

    def test_a_failed_repair_is_listed_as_waiting_but_counted_once(self):
        self.heal()
        self.run_tool()  # job-b/2001 (started an hour ago) is archived
        with open(self.path("logs/job-b/2001/build-log.txt"), "w") as f:
            f.write("x")  # damaged
        self.srv.route("/bucket/logs/job-b/2001/build-log.txt", (503, {}, b""))
        self.run_tool()
        self.assertIn("job-b/2001", self.state()["unresolved"])
        with open(self.path("INDEX.md")) as f:
            md = f.read()
        waiting = md.split("## Not archived yet")[1].split("\n## ")[0]
        self.assertIn("job-b 2001", waiting)
        today = frl.iso(time.time())[:10]
        row = next(line for line in md.splitlines() if line.startswith(f"| {today} |"))
        self.assertEqual(row, f"| {today} | 1 (today so far) |")

    def report_rows(self):
        entries, _ = frl.load_index(self.root, time.time())
        return frl.report.summarize(entries, frl.report.load_runs(self.root), OLD / 1000 + 3 * DAY)["rows"]

    def test_timeout_does_not_blame_builds_already_on_disk(self):
        self.run_tool()

        def slow(h):
            time.sleep(2)
            send(h, *gcs_object(b"x"))

        self.srv.route("/bucket/logs/job-a/1004/finished.json", slow)
        os.remove(self.path("logs/job-a/1004/meta.json"))  # forces 1004 to be fetched again, slowly
        self.run_tool("--run-timeout-minutes", "0.01", "--max-concurrency", "1", "--start-concurrency", "1")
        run = self.last_run()
        self.assertEqual([e["build"] for e in run["errors"] if "run timeout" in e["error"]], ["job-a/1004"])
        time.sleep(2.5)

    def test_restored_given_up_build_leaves_given_up(self):
        self.run_tool()
        rec = {"job": "job-a", "build": "1004", "query": "bucket/logs/job-a", "started": OLD // 1000,
               "created": frl.created("1004"), "tabs": []}
        frl.write_json(self.path("state.json"), {"tabs": {}, "unresolved": {}, "given_up": {
            "job-a/1004": {"rec": rec, "first_seen": time.time() - 20 * DAY, "last_error": "x"}}})
        self.run_tool()
        self.assertNotIn("job-a/1004", self.state()["given_up"])

    def test_run_timeout_bounds_a_hanging_table_and_summary(self):
        def hang(h):
            time.sleep(3)
            send(h, 200, {}, json.dumps(PURPLE).encode())

        self.srv.route(frl.table_url("", "dash", "purple"), hang)
        t0 = time.monotonic()
        self.assertEqual(self.run_tool("--run-timeout-minutes", "0.01"), 1)
        self.assertLess(time.monotonic() - t0, 2.5)
        self.assertIn("run timeout", next(t for t in self.last_run()["tabs"] if t["tab"] == "purple")["error"])
        self.srv.route("/dash/summary", hang)
        t0 = time.monotonic()
        self.assertEqual(self.run_tool("--run-timeout-minutes", "0.01"), 1)
        self.assertLess(time.monotonic() - t0, 2.5)
        time.sleep(3.5)

    def test_a_later_run_does_not_rearm_abandoned_workers(self):
        def slow(h):
            time.sleep(1.5)
            send(h, *gcs_object(b"log 1004\n"))

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", slow)
        self.run_tool("--run-timeout-minutes", "0.01")
        self.run_tool("--list-only")  # a second main() in the same process
        time.sleep(2.5)
        self.assertFalse(os.path.exists(self.path("logs/job-a/1004/meta.json")))

    def test_stale_part_files_are_swept(self):
        os.makedirs(self.path("logs/job-a/1004"))
        stale = self.path("logs/job-a/1004/.build-log.txt.abc.part")
        open(stale, "w").close()
        self.run_tool()
        self.assertFalse(os.path.exists(stale))

    def test_same_size_damage_is_repaired(self):
        self.run_tool()
        path = self.path("logs/job-a/1004/build-log.txt")
        with open(path, "wb") as f:
            f.write(b"\0" * len(b"log 1004\n"))
        self.run_tool()
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["repaired"])
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"log 1004\n")

    def test_local_disk_errors_are_not_retried_or_pushback(self):
        class Full(gzip.GzipFile):
            def write(self, data):
                raise OSError(28, "No space left on device")

        real = frl.gzip.GzipFile
        frl.gzip.GzipFile = Full
        try:
            self.run_tool("--gzip", "--start-concurrency", "8", "--max-concurrency", "8")
        finally:
            frl.gzip.GzipFile = real
        run = self.last_run()
        self.assertEqual(self.srv.hits("/bucket/logs/job-a/1004/build-log.txt"), 1)
        self.assertIn("LocalIOError", {e["build"]: e["error"] for e in run["errors"]}["job-a/1004"])
        self.assertEqual(run["concurrency"]["final"], 8)

    def test_non_finite_arguments_are_rejected(self):
        for flag in ("--since-hours", "--max-job-hours", "--run-timeout-minutes"):
            for value in ("nan", "inf", "0", "-1"):
                with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
                    self.run_tool(flag, value)

    def test_malformed_state_entries_are_dropped(self):
        evil = {"job": "job-a", "build": "../../x", "query": "bucket/logs/job-a", "started": None,
                "created": 1, "tabs": []}
        frl.write_json(self.path("state.json"), {"tabs": {"dash#purple": "yesterday"}, "unresolved": {
            "job-a/../../x": {"rec": evil, "first_seen": time.time()},
            "job-a/1": {"rec": dict(evil, build="1"), "first_seen": "soon"}}})
        self.assertEqual(self.run_tool(), 1)  # the dropped entries are reported
        warnings = self.last_run()["warnings"]
        self.assertEqual(sum("dropped a malformed unresolved entry" in w for w in warnings), 2)
        self.assertFalse(os.path.exists(os.path.join(self.root, "..", "x")))

    def test_abandoned_workers_leave_no_temp_files(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        ctx.abandoned.set()
        target = self.path("meta.json")
        errors = []

        def worker():
            frl.bind_context(ctx)
            try:
                frl.write_json(target, {"x": 1})
            except frl.Abandoned as e:
                errors.append(e)

        t = threading.Thread(target=worker)
        t.start()
        t.join()
        self.assertEqual(len(errors), 1)
        self.assertEqual(os.listdir(self.root), [])

    def test_sweep_removes_both_temp_styles_and_stays_inside(self):
        root = os.path.join(self.root, "arch[1]")
        os.makedirs(os.path.join(root, "logs", "j", "1"))
        os.makedirs(os.path.join(root, "charts"))
        outside = os.path.join(self.root, "arch1", "sub")
        os.makedirs(outside)
        for p in (os.path.join(root, "logs", "j", "1", ".build-log.txt.x.part"), os.path.join(root, "charts", "a.svg.part"),
                  os.path.join(outside, ".notes.part")):
            open(p, "w").close()
        self.assertEqual(frl.sweep_parts(root), 2)
        self.assertTrue(os.path.exists(os.path.join(outside, ".notes.part")))

    def test_timeout_during_a_repair_is_not_reported_as_intact(self):
        self.run_tool()
        path = self.path("logs/job-a/1004/build-log.txt")
        with open(path, "wb") as f:
            f.write(b"\0" * len(b"log 1004\n"))  # same size, wrong content

        def slow(h):
            time.sleep(2)
            send(h, *gcs_object(b"log 1004\n"))

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", slow)
        self.run_tool("--run-timeout-minutes", "0.01")
        self.assertIn("run timeout", {e["build"]: e["error"] for e in self.last_run()["errors"]}["job-a/1004"])
        self.assertIn("job-a/1004", self.state()["unresolved"])
        time.sleep(2.5)

    def test_gzip_bytes_are_stable(self):
        self.run_tool("--gzip")
        path = self.path("logs/job-a/1002/build-log.txt.gz")
        with open(path, "rb") as f:
            first = f.read()
        os.remove(path)
        self.run_tool("--gzip")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), first)
        self.assertNotIn(b".part", first)

    def test_oversized_files_are_verified_but_not_stored(self):
        self.run_tool("--max-file-mb", "0.000001")
        meta = frl.read_json(self.path("logs/job-a/1004/meta.json"))
        self.assertTrue(meta["log"]["verified"])
        self.assertIsNone(meta["log"]["file"])
        self.assertIn("over the", meta["log"]["omitted"])
        self.assertFalse(os.path.exists(self.path("logs/job-a/1004/build-log.txt")))
        self.assertIn("[build-log.txt in GCS (too large to store)](", self.week_pages())
        self.run_tool("--max-file-mb", "0.000001")
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["already-archived"])

    def test_a_tab_that_drops_off_the_summary_is_remembered(self):
        frl.write_json(self.path("state.json"), {"tabs": {"dash#gone": {"scan": time.time() - 3 * DAY,
                                                                        "oldest": time.time() - 4 * DAY}}})
        self.run_tool()
        self.assertTrue(any("dash#gone: no longer listed" in w for w in self.last_run()["warnings"]))
        self.assertIn("missing_since", self.state()["tabs"]["dash#gone"])
        self.srv.summary("dash", {"gone": "FLAKY"})
        self.srv.table("dash", "gone", table("bucket/logs/job-g", ["7001"], {"o": ([1], [""])}, start=NOW_MS))
        self.run_tool()
        self.assertTrue(any("dash#gone" in w and "may be missing" in w for w in self.last_run()["warnings"]))
        self.assertNotIn("missing_since", self.state()["tabs"]["dash#gone"])

    def test_week_pages_without_builds_are_removed(self):
        os.makedirs(self.path("weeks"))
        for name in ("2020-W01.md", "2020-W01.json", "notes.md"):
            open(self.path("weeks", name), "w").close()
        self.run_tool()
        self.assertEqual(sorted(n for n in os.listdir(self.path("weeks")) if n.startswith("20") or n == "notes.md"),
                         sorted([f"{frl.week_of(OLD // 1000)}.json", f"{frl.week_of(OLD // 1000)}.md", "notes.md"]))

    def test_huge_numbers_in_state_and_run_reports_do_not_crash(self):
        frl.write_json(self.path("state.json"), {"tabs": {"dash#purple": 10 ** 400}})
        os.makedirs(self.path("runs"))
        bad_tabs = [{"tab": "t"}, {"dashboard": ["x"], "tab": "t"}, {"dashboard": "d", "tab": "t", "oldest": 10 ** 400}]
        frl.write_json(self.path("runs", "0.json"), run_record(time.time(), bad_tabs))
        self.run_tool()
        self.assertTrue(os.path.exists(self.path("INDEX.md")))

    def test_a_damaged_given_up_build_stays_given_up(self):
        self.run_tool()
        path = self.path("logs/job-a/1004/build-log.txt")
        with open(path, "wb") as f:
            f.write(b"\0" * len(b"log 1004\n"))
        rec = frl.read_json(self.path("logs/job-a/1004/meta.json"))
        rec = {k: rec[k] for k in ("job", "build", "query", "started", "created", "tabs")}
        frl.write_json(self.path("state.json"), {"tabs": {}, "given_up": {
            "job-a/1004": {"rec": rec, "first_seen": time.time() - 20 * DAY, "last_error": "x"}}})
        self.run_tool()
        self.assertIn("job-a/1004", self.state()["given_up"])
        with open(self.path("INDEX.md")) as f:
            waiting = f.read().split("## Not archived yet")[1].split("\n## ")[0]
        self.assertIn("job-a 1004", waiting)  # shown as waiting ...
        self.assertEqual([e["build"] for e in self.indexed()].count("1004"), 1)  # ... but counted once

    def test_a_log_that_appears_after_testgrid_drops_the_build_is_recovered(self):
        self.run_tool()
        self.assertIn("job-a/1001", self.state()["no_log"])
        self.srv.table("dash", "purple", table("bucket/logs/job-a", ["1006"], {"o": ([1], [""])}))
        self.srv.route("/bucket/logs/job-a/1001/build-log.txt", gcs_object(b"late log\n"))
        self.run_tool()
        self.assertEqual(self.last_run()["outcomes"]["log-recovered"], ["job-a/1001"])
        self.assertNotIn("job-a/1001", self.state()["no_log"])

    def test_a_timeout_keeps_tracking_a_logless_build(self):
        self.run_tool()
        self.assertIn("job-a/1001", self.state()["no_log"])

        def slow(h):
            time.sleep(2)
            send(h, 404, {}, b"")

        self.srv.route("/bucket/logs/job-a/1001/build-log.txt", slow)
        self.run_tool("--run-timeout-minutes", "0.01")
        self.assertIn("job-a/1001", self.state()["no_log"])
        time.sleep(2.5)

    def test_omitted_podinfo_links_its_own_gcs_copy(self):
        meta = {"started": OLD // 1000, "created": 1, "tabs": [], "query": "b/logs/j",
                "log_url": "https://gcs/b/logs/j/9/build-log.txt", "log": None,
                "podinfo": {"bytes": 9, "md5": "x", "verified": True, "file": None, "omitted": "big"}}
        entry = frl.index_entry("j", "9", meta, time.time())
        self.assertEqual(entry["remote"], [("podinfo.json", "https://gcs/b/logs/j/9/podinfo.json")])
        self.assertEqual(entry["files"], [])

    def test_sweep_leaves_files_that_are_not_temp_files(self):
        os.makedirs(self.path("logs/x.part/2"))
        keep = self.path("logs/x.part/2/meta.json")
        open(keep, "w").close()
        frl.sweep_parts(self.root)
        self.assertTrue(os.path.exists(keep))

    def test_sigint_stops_the_run_at_once_without_writing(self):
        def trickle(h):
            h.send_response(200)
            h.send_header("Content-Length", "1000000")
            h.end_headers()
            for _ in range(200):
                h.wfile.write(b"x")
                h.wfile.flush()
                time.sleep(0.05)

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", trickle)
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fetch_red_logs.py")
        p = subprocess.Popen([sys.executable, script, "--archive", self.root, "--dashboard", "dash",
                              "--testgrid", self.srv.url, "--gcs", self.srv.url],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(3)  # it is now stuck in the slow download
        t0 = time.monotonic()
        p.send_signal(signal.SIGINT)
        out, err = p.communicate(timeout=10)
        self.assertLess(time.monotonic() - t0, 3)
        self.assertEqual(p.returncode, 130)
        self.assertIn("interrupted", err)
        self.assertFalse(os.path.exists(self.path("logs/job-a/1004/meta.json")))
        time.sleep(10)  # let the server finish the abandoned response

    def test_second_concurrent_run_is_refused(self):
        held = frl.lock_archive(self.root)
        try:
            self.assertEqual(self.run_tool(), 2)
            self.assertFalse(os.path.exists(self.path("runs")))
        finally:
            held.close()

    def test_backs_off_when_throttled(self):
        calls = []

        def throttle_once(h):
            calls.append(1)
            if len(calls) == 1:
                send(h, 429, {}, b"")
            else:
                send(h, *gcs_object(b"log 1004\n"))

        self.srv.route("/bucket/logs/job-a/1004/build-log.txt", throttle_once)
        self.run_tool("--start-concurrency", "4", "--max-concurrency", "4")
        self.assertEqual(len(calls), 2)
        self.assertIn("job-a/1004", self.last_run()["outcomes"]["archived"])
        self.assertGreaterEqual(self.last_run()["concurrency"]["pushbacks"], 1)

    def test_rejects_unsafe_build_ids(self):
        self.srv.table("dash", "broken", table("bucket/logs/job-d", ["../../etc"], {"o": ([12], [""])}))
        self.run_tool()
        tab = next(t for t in self.last_run()["tabs"] if t["tab"] == "broken")
        self.assertIn("unexpected build id", tab["error"])
        self.assertFalse(os.path.exists(os.path.join(self.root, "..", "etc", "meta.json")))


class AdaptiveLimitTest(unittest.TestCase):
    def test_grows_to_ceiling_and_halves_on_pushback(self):
        limit = frl.AdaptiveLimit(2, 5)
        for _ in range(100):
            limit.acquire()
            limit.release(pushback=False)
        self.assertEqual(int(limit.cap), 5)
        limit.acquire()
        limit.release(pushback=True)
        self.assertEqual(int(limit.cap), 2)
        self.assertEqual(limit.peak, 5)

    def test_never_exceeds_cap(self):
        limit, active, peak, lock = frl.AdaptiveLimit(3, 3), [0], [0], threading.Lock()

        def work():
            limit.acquire()
            with lock:
                active[0] += 1
                peak[0] = max(peak[0], active[0])
            time.sleep(0.01)
            with lock:
                active[0] -= 1
            limit.release(pushback=False)

        threads = [threading.Thread(target=work) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(peak[0], 3)


class GunzipPaddingTest(unittest.TestCase):
    def test_zero_padding_after_last_member(self):
        g = frl.Gunzip()
        out = g.feed(gzip.compress(b"abc") + b"\0" * 16) + g.feed(b"\0" * 4) + g.finish()
        self.assertEqual(out, b"abc")
        with self.assertRaises(frl.IntegrityError):
            frl.Gunzip().feed(gzip.compress(b"abc") + b"\0\0" + gzip.compress(b"def"))


class AdaptiveLimitBurstTest(unittest.TestCase):
    def test_a_burst_of_pushbacks_halves_once(self):
        limit = frl.AdaptiveLimit(16, 16)
        for _ in range(4):
            limit.acquire()
            limit.release(pushback=True)
        self.assertEqual(int(limit.cap), 8)
        self.assertEqual(limit.pushbacks, 4)


def run_record(started, tabs, since_hours=None, max_job_hours=6):
    return {"run": str(started), "started": started, "since_hours": since_hours, "max_job_hours": max_job_hours,
            "tabs": tabs}


class ReportTest(unittest.TestCase):
    import report

    def entries(self, now, per_day=2, days=30, tab="d#t"):
        return [{"time": now - k * DAY - 3600 * j, "tabs": [tab]} for k in range(days) for j in range(per_day)]

    def test_a_gap_between_scans_marks_its_days_partial(self):
        now = time.time()
        runs = [run_record(now - 10 * DAY, [{"dashboard": "d", "tab": "t", "oldest": now - 25 * DAY}]),
                run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 3 * DAY}])]
        s = self.report.summarize(self.entries(now), runs, now)
        p = s["panels"][0]
        gap = [d for d in s["days"] if now - 9 * DAY < self.report.day_start(d) < now - 4 * DAY]
        self.assertTrue(gap and not any(d in p["complete"] for d in gap))
        self.assertIsNone(p["week"])  # the gap sits inside the last 14 days

    def test_steady_rate_reads_steady(self):
        now = time.time()
        runs = [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY}])]
        p = self.report.summarize(self.entries(now), runs, now)["panels"][0]
        self.assertEqual(p["week"], (14, 14))
        self.assertAlmostEqual(p["avg"][max(p["avg"])], 2.0)

    def test_a_tab_that_never_loads_keeps_days_partial(self):
        now = time.time()
        runs = [run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 40 * DAY},
                                 {"dashboard": "d", "tab": "broken", "error": "HTTPError 500"}])]
        p = self.report.summarize(self.entries(now), runs, now)["panels"][0]
        self.assertEqual(p["complete"], set())
        self.assertIsNone(p["week"])

    def test_week_text_never_says_no_change_for_a_change(self):
        self.assertEqual(self.report.week_text((201, 200))[1], "last 7 days 201 · +<1% vs previous 7 (200)")
        self.assertEqual(self.report.week_text((199, 200))[1], "last 7 days 199 · -<1% vs previous 7 (200)")
        self.assertEqual(self.report.week_text((200, 200)), (None, "last 7 days 200 · no change vs previous 7 (200)"))
        self.assertEqual(self.report.week_text((3, 0))[1], "last 7 days 3 · new vs previous 7 (0)")
        self.assertEqual(self.report.week_text((10, 20))[1], "last 7 days 10 · -50% vs previous 7 (20)")
        self.assertEqual(self.report.week_text((1, 200))[1], "last 7 days 1 · -99.5% vs previous 7 (200)")
        self.assertEqual(self.report.week_text((0, 200))[1], "last 7 days 0 · -100% vs previous 7 (200)")

    def test_one_band_per_stretch_of_partial_days(self):
        now = time.time()
        runs = [run_record(now - 20 * DAY, [{"dashboard": "d", "tab": "t", "oldest": now - 25 * DAY}]),
                run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 10 * DAY}])]
        s = self.report.summarize(self.entries(now), runs, now)
        svg = self.report.render_trend(s, "light")
        flags = "".join("C" if d in s["panels"][0]["complete"] else "." for d in s["days"][:-1])
        stretches = len([x for x in flags.split("C") if x])
        self.assertEqual(svg.count('fill="#f0efec"/>'), stretches)

    def test_malformed_run_reports_are_ignored(self):
        root = tempfile.mkdtemp()
        try:
            os.makedirs(os.path.join(root, "runs"))
            for i, bad in enumerate([{"since_hours": "12"}, {"max_job_hours": float("nan")}, {"started": None}]):
                frl.write_json(os.path.join(root, "runs", f"{i}.json"),
                               dict(run_record(time.time(), [{"dashboard": "d", "tab": "t", "oldest": 1}]), run=str(i), **bad))
            self.assertEqual(self.report.load_runs(root), [])
        finally:
            shutil.rmtree(root)

    def test_first_run_heatmap_shows_numbers(self):
        root = tempfile.mkdtemp()
        try:
            now = time.time()
            current = run_record(now, [{"dashboard": "d", "tab": "t", "oldest": now - 20 * DAY}])
            self.report.write_charts(root, self.entries(now, days=20), now, current)
            with open(os.path.join(root, "charts", "tabs-light.svg")) as f:
                svg = f.read()
            self.assertGreaterEqual(svg.count(">2</text>"), 13)
            self.assertEqual(svg.count('fill="none" stroke="#e1e0d9"'), 1)  # only the legend's "no history yet"
        finally:
            shutil.rmtree(root)


class CrossCheckTest(unittest.TestCase):
    def setUp(self):
        import cross_check
        self.cc = cross_check
        self.srv = FakeServer()
        self.root = tempfile.mkdtemp()

    def tearDown(self):
        self.srv.close()
        shutil.rmtree(self.root)

    def check(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url])
        return code, out.getvalue()

    def fetch(self, *extra):
        return frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url,
                         "--gcs", self.srv.url, *extra])

    def test_reports_a_red_build_with_nothing_in_gcs(self):
        start = NOW_MS - 20 * 3600 * 1000
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", table("bucket/logs/job", [str((int(start) - frl.PROW_EPOCH_MS) << 22)],
                                          {"o": ([12], [""])}, start=start))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 1)
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertIn("MISSED red build", out)
        self.assertIn("the run reported an error", out)

    def test_a_gap_warning_fails_the_check(self):
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", table("bucket/logs/job", ["1"], {"o": ([1], [""])}, start=NOW_MS))
        self.srv.gcs_listing("bucket", [])
        frl.write_json(os.path.join(self.root, "state.json"), {"tabs": {"dash#t": time.time() - 5 * DAY}})
        self.assertEqual(self.fetch(), 1)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("the run warned", out)

    def test_red_build_without_a_start_time_is_still_checked(self):
        build = str((NOW_MS - 20 * 3600 * 1000 - frl.PROW_EPOCH_MS) << 22)
        t = table("bucket/logs/job", [build], {"o": ([12], [""])})
        t["timestamps"] = [0]
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", t)
        self.srv.route(f"/bucket/logs/job/{build}/finished.json", FIN)
        self.srv.route(f"/bucket/logs/job/{build}/build-log.txt", gcs_object(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{build}/finished.json"])
        self.assertEqual(self.fetch(), 0)
        shutil.rmtree(os.path.join(self.root, "logs", "job", build))
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("MISSED red build", out)

    def test_verification_has_a_time_limit(self):
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", table("bucket/logs/job", ["1"], {"o": ([1], [""])}, start=NOW_MS))
        self.srv.gcs_listing("bucket", [])
        self.assertEqual(self.fetch(), 0)

        def hang(h):
            time.sleep(5)
            send(h, 200, {}, b"{}")

        self.srv.table("dash", "t", hang)
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cross_check.py")
        t0 = time.monotonic()
        out = subprocess.run([sys.executable, script, "--archive", self.root, "--testgrid", self.srv.url,
                              "--gcs", self.srv.url, "--timeout-minutes", "0.01"], capture_output=True, text=True)
        self.assertEqual(out.returncode, 1)
        self.assertIn("did not finish within", out.stdout)
        self.assertLess(time.monotonic() - t0, 4)
        time.sleep(4)

    def archived_build(self):
        start = NOW_MS - 20 * 3600 * 1000
        build = str((int(start) - frl.PROW_EPOCH_MS) << 22)
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", table("bucket/logs/job", [build], {"o": ([12], [""])}, start=start))
        self.srv.route(f"/bucket/logs/job/{build}/finished.json", FIN)
        self.srv.gcs_listing("bucket", [f"logs/job/{build}/finished.json"])
        return build

    def test_an_omitted_log_is_still_compared_with_gcs(self):
        build = self.archived_build()
        self.srv.route(f"/bucket/logs/job/{build}/build-log.txt", gcs_object(b"x" * 4096))
        self.assertEqual(self.fetch("--max-file-mb", "0.001"), 0)
        self.assertEqual(self.check()[0], 0)
        self.srv.route(f"/bucket/logs/job/{build}/build-log.txt", gcs_object(b"y" * 4096))
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("GCS md5", out)

    def test_a_log_appearing_for_a_logless_build_is_reported(self):
        build = self.archived_build()
        self.assertEqual(self.fetch(), 0)  # finished, no log, no podinfo
        self.assertEqual(self.check()[0], 0)
        self.srv.route(f"/bucket/logs/job/{build}/build-log.txt", gcs_object(b"late\n"))
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("not archived", out)

    def test_a_stale_run_report_fails_the_check(self):
        self.archived_build()
        self.assertEqual(self.fetch(), 0)
        runs = sorted(os.listdir(os.path.join(self.root, "runs")))
        path = os.path.join(self.root, "runs", runs[-1])
        report_ = frl.read_json(path)
        report_["finished"] -= 3 * 3600
        frl.write_json(path, report_)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("the last fetch did not finish", out)

    def test_no_run_report_is_a_problem_not_a_crash(self):
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("no run report", out)

    def test_missing_local_log_is_a_problem_not_a_crash(self):
        start = NOW_MS - 20 * 3600 * 1000
        build = str((int(start) - frl.PROW_EPOCH_MS) << 22)
        self.srv.summary("dash", {"t": "FLAKY"})
        self.srv.table("dash", "t", table("bucket/logs/job", [build], {"o": ([12], [""])}, start=start))
        self.srv.route(f"/bucket/logs/job/{build}/finished.json", FIN)
        self.srv.route(f"/bucket/logs/job/{build}/build-log.txt", gcs_object(b"log\n"))
        self.srv.gcs_listing("bucket", [f"logs/job/{build}/finished.json"])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.check()[0], 0)
        os.remove(os.path.join(self.root, "logs", "job", build, "build-log.txt"))
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn("cannot verify", out)


if __name__ == "__main__":
    unittest.main()
