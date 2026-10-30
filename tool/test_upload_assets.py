"""upload_assets.py, against a fake GitHub: the releases of a month, and each case of an asset there already.

Run: cd tool && python -m unittest test_upload_assets -v
"""
import ast
import hashlib
import http.server
import io
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import upload_assets as ua  # noqa: E402  # fails: no upload_assets

REPO = "owner/repo"
TOKEN = "t0ken"
ENV = {"GITHUB_TOKEN": TOKEN, "GITHUB_REPOSITORY": REPO, "GITHUB_REF_NAME": "main"}


class FakeGitHub:
    """The parts of GitHub's REST API the script uses, on one local server for both of its hosts. Each release is
    {"id", "tag_name", "prerelease", ...}, its assets {name: asset}. upload_faults: the status each next upload
    answers instead (a 502 leaves a "starter" asset, as GitHub's may); wrong_digest: the next upload's answer says
    another digest; wrong_name, another name. faults: (status, headers, message) each next request answers instead
    (a rate limit, say); hide_digest: how many next answers of an asset (an upload's or a read's) say no digest;
    reread_digest: the digest a read of an asset says. An asset's browser_download_url is on `download` (GitHub's
    host, or this server's, which serves its bytes, from data, through a redirect as GitHub's does)."""

    wrong_name, hide_digest, reread_digest, download = False, 0, None, "https://github.com"

    def __init__(self, default_branch="main"):
        self.default_branch, self.releases, self.assets, self.next_id = default_branch, {}, {}, 100
        self.upload_faults, self.wrong_digest, self.requests, self.lock = [], False, [], threading.Lock()
        self.faults, self.delete_faults, self.data = [], [], {}
        fake = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def answer(self, status, body=None, headers=()):
                data = json.dumps(body).encode() if body is not None else b""
                self.send_response(status)
                for k, v in dict(headers).items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def handle_any(self):
                if self.command == "GET" and (self.path.startswith(f"/{REPO}/releases/download/")
                                              or self.path.startswith("/blob/")):
                    self.downloaded()
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                with fake.lock:
                    fake.requests.append((self.command, self.path, self.headers.get("Authorization")))
                    if fake.faults:
                        status, headers, message = fake.faults.pop(0)
                        self.answer(status, {"message": message}, headers)
                        return
                    status, out = fake.serve(self.command, self.path, body, self.headers)
                self.answer(status, out)

            def downloaded(self):
                with fake.lock:
                    fake.requests.append((self.command, self.path, self.headers.get("Authorization")))
                    tag, _, name = self.path.removeprefix(f"/{REPO}/releases/download/").partition("/")
                    found = [a for rid, assets in fake.assets.items() for a in assets.values()
                             if fake.releases[rid]["tag_name"] == tag and a["name"] == name]
                    data = fake.data.get(self.path.removeprefix("/blob/"))
                if found:
                    self.answer(302, headers={"Location": f"/blob/{found[0]['id']}"})
                    return
                if data is None:
                    self.answer(404, {"message": "Not Found"})
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = do_POST = do_DELETE = handle_any

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def new_id(self):
        self.next_id += 1
        return self.next_id

    def release(self, tag, assets=(), prerelease=False):
        """Add a release, with assets [(name, data)]."""
        rid = self.new_id()
        self.releases[rid] = {"id": rid, "tag_name": tag, "draft": False, "prerelease": prerelease,
                              "make_latest": "false", "target_commitish": "main"}
        self.assets[rid] = {}
        for name, data in assets:
            self.add(rid, name, data)
        return rid

    def add(self, rid, name, data, state="uploaded", digest=True):
        tag = self.releases[rid]["tag_name"]
        asset = {"id": self.new_id(), "name": name, "size": len(data), "state": state,
                 "digest": "sha256:" + hashlib.sha256(data).hexdigest() if digest else None,
                 "updated_at": "2026-10-30T00:00:00Z",
                 "browser_download_url": f"{self.download}/{REPO}/releases/download/{tag}/{name}"}
        self.assets[rid][name], self.data[str(asset["id"])] = asset, data
        return asset

    def find(self, aid):
        return next(((rid, n) for rid, assets in self.assets.items() for n, a in assets.items() if a["id"] == aid),
                    (None, None))

    def serve(self, method, path, body, headers):
        if headers.get("Authorization") != f"Bearer {TOKEN}":
            return 401, {"message": "Bad credentials"}
        url = urllib.parse.urlsplit(path)
        q = urllib.parse.parse_qs(url.query)
        parts = url.path.strip("/").split("/")
        if parts[:3] != ["repos", *REPO.split("/")]:
            return 404, {"message": "Not Found"}
        rest = parts[3:]
        page = int(q.get("page", ["1"])[0])
        per = int(q.get("per_page", ["30"])[0])

        def paged(items):
            return 200, items[(page - 1) * per:page * per]
        if method == "GET" and rest == []:
            return 200, {"default_branch": self.default_branch}
        if rest[:1] != ["releases"]:
            return 404, {"message": "Not Found"}
        rest = rest[1:]
        if method == "GET" and rest == []:
            return paged(sorted(self.releases.values(), key=lambda r: -r["id"]))
        if method == "POST" and rest == []:
            spec = json.loads(body)
            if any(r["tag_name"] == spec["tag_name"] for r in self.releases.values()):
                return 422, {"message": "Validation Failed", "errors": [{"code": "already_exists"}]}
            rid = self.release(spec["tag_name"], prerelease=spec["prerelease"])
            self.releases[rid].update({k: spec[k] for k in ("draft", "make_latest", "target_commitish", "body")})
            return 201, self.releases[rid]
        if method == "GET" and rest[:1] == ["tags"] and len(rest) == 2:
            tag = urllib.parse.unquote(rest[1])
            found = [r for r in self.releases.values() if r["tag_name"] == tag]
            return (200, found[0]) if found else (404, {"message": "Not Found"})
        if rest[:1] == ["assets"] and len(rest) == 2:
            rid, name = self.find(int(rest[1]))
            if rid is None:
                return 404, {"message": "Not Found"}
            if method == "DELETE":
                if self.delete_faults:
                    return self.delete_faults.pop(0), {"message": "fault"}
                del self.assets[rid][name]
                return 204, None
            return 200, self.hidden(dict(self.assets[rid][name], **(
                {"digest": self.reread_digest} if self.reread_digest else {})))
        if len(rest) == 2 and rest[1] == "assets" and int(rest[0]) in self.releases:
            rid = int(rest[0])
            if method == "GET":
                return paged(sorted(self.assets[rid].values(), key=lambda a: a["id"]))
            name = q["name"][0]
            if headers.get("Content-Type") != "application/x-xz":
                return 400, {"message": "no Content-Type"}
            if name in self.assets[rid]:
                return 422, {"message": "Validation Failed", "errors": [{"code": "already_exists"}]}
            if self.upload_faults:
                status = self.upload_faults.pop(0)
                if status == 502:
                    self.add(rid, name, b"", state="starter", digest=False)
                return status, {"message": "fault"}
            asset = self.add(rid, name, body)
            if self.wrong_digest:
                self.wrong_digest = False
                return 201, dict(asset, digest="sha256:" + "0" * 64)
            if self.wrong_name:
                self.wrong_name = False
                return 201, dict(asset, name="job.1.tar.xz")
            return 201, self.hidden(asset)
        return 404, {"message": "Not Found"}

    def hidden(self, asset):
        if self.hide_digest:
            self.hide_digest -= 1
            return dict(asset, digest=None)
        return asset

    def tags(self):
        return sorted(r["tag_name"] for r in self.releases.values())

    def held(self, tag):
        rid = next(r["id"] for r in self.releases.values() if r["tag_name"] == tag)
        return {n: a["digest"] for n, a in self.assets[rid].items()}


def sha(data):
    return hashlib.sha256(data).hexdigest()


class Upload(unittest.TestCase):
    """A manifest of assets the fetch packed, uploaded by upload_assets.main."""

    def setUp(self):
        self.gh = FakeGitHub()
        self.addCleanup(self.gh.close)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        for name in ("BACKOFF", "GAP"):
            patcher = mock.patch.object(ua, name, 0)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.entries = []

    def asset(self, name, data, month="2026-10", **extra):
        with open(os.path.join(self.dir, name), "wb") as f:
            f.write(data)
        self.entries.append(dict({"name": name, "month": month, "bytes": len(data), "sha256": sha(data)}, **extra))
        return self.entries[-1]

    def upload(self, branch="main", entries=None, delete=None):
        with open(os.path.join(self.dir, "manifest.json"), "w") as f:
            json.dump({"run": "2026-10-30T051234Z", "assets": self.entries if entries is None else entries,
                       **({"delete": delete} if delete is not None else {})}, f)
        env = dict(ENV, GITHUB_REF_NAME=branch)
        with mock.patch("sys.stdout") as out, mock.patch("sys.stderr") as err:
            code = ua.main([self.dir, "--api", self.gh.url, "--uploads", self.gh.url], env)
        self.printed = "".join(c.args[0] for c in out.write.call_args_list)
        self.errors = "".join(c.args[0] for c in err.write.call_args_list)
        return code

    def uploaded(self, key="uploaded"):
        with open(os.path.join(self.dir, "uploaded.json")) as f:
            return json.load(f).get(key)

    def releases(self):
        with open(os.path.join(self.dir, "releases.json")) as f:
            return json.load(f)["releases"]

    def test_a_release_of_the_month_made_once(self):
        a = self.asset("ci-node-crio-conformance_2115810275973140480.tar.xz", b"one")
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.tags(), ["node-logs-2026-10"])
        release = next(iter(self.gh.releases.values()))
        self.assertEqual({k: release[k] for k in ("draft", "prerelease", "make_latest", "target_commitish")},
                         {"draft": False, "prerelease": False, "make_latest": "false", "target_commitish": "main"})
        got = self.uploaded()[a["name"]]
        self.assertEqual((got["release"], got["size"], got["digest"]), ("node-logs-2026-10", 3, "sha256:" + a["sha256"]))
        self.assertEqual(got["url"], f"https://github.com/{REPO}/releases/download/node-logs-2026-10/{a['name']}")
        b = self.asset("ci-node-crio-dra_2103308210961125376.tar.xz", b"two")
        self.entries = [b]
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.tags(), ["node-logs-2026-10"])  # (found by its tag)
        self.assertEqual(sorted(self.gh.held("node-logs-2026-10")), sorted([a["name"], b["name"]]))
        self.assertEqual([x["name"] for x in self.releases()["node-logs-2026-10"]["assets"]], sorted([a["name"], b["name"]]))
        self.assertTrue(all(auth == f"Bearer {TOKEN}" for _, _, auth in self.gh.requests))

    def test_a_full_release_continues_in_the_next(self):
        with mock.patch.object(ua, "MAX_ASSETS", 2):
            for i in range(5):
                self.asset(f"job_{i}.tar.xz", bytes([i]))
            self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.tags(), ["node-logs-2026-10", "node-logs-2026-10-2", "node-logs-2026-10-3"])
        self.assertEqual([self.uploaded()[f"job_{i}.tar.xz"]["release"] for i in range(5)],
                         ["node-logs-2026-10"] * 2 + ["node-logs-2026-10-2"] * 2 + ["node-logs-2026-10-3"])
        self.assertEqual(sorted(self.releases()), self.gh.tags())

    def test_nothing_goes_into_a_draft(self):
        self.asset("job_1.tar.xz", b"x")
        rid = self.gh.release("node-logs-2026-10")
        self.gh.releases[rid]["draft"] = True  # (as when its tag is deleted)
        self.assertEqual(self.upload(), 1)
        self.assertIn("node-logs-2026-10 is a draft", self.errors)
        self.assertEqual((self.uploaded(), self.gh.tags()), ({}, ["node-logs-2026-10"]))
        self.assertEqual(self.gh.assets[rid], {})

    def test_another_branch_has_its_own_prereleases(self):
        a = self.asset("job_1.tar.xz", b"x")
        self.gh.release("node-logs-2026-10")
        self.assertEqual(self.upload(branch="Fault/Inject_1"), 0, self.errors)
        tag = f"node-logs-fault-inject-1-{sha(b'Fault/Inject_1')[:8]}-2026-10"
        self.assertEqual(self.uploaded()[a["name"]]["release"], tag)
        made = next(r for r in self.gh.releases.values() if r["tag_name"] == tag)
        self.assertEqual((made["prerelease"], made["target_commitish"]), (True, "main"))
        self.assertEqual(list(self.releases()), [tag])  # (only the branch's)
        with open(os.path.join(self.dir, "releases.json")) as f:
            self.assertEqual(json.load(f)["prefix"], tag.removesuffix("2026-10"))
        # (no two branches share their releases)
        self.assertEqual(self.upload(branch="fault-inject-1"), 0, self.errors)
        self.assertNotEqual(self.uploaded()[a["name"]]["release"], tag)
        self.assertEqual(ua.branch_prefix("main", "main"), "node-logs-")
        self.assertRegex(ua.branch_prefix("--", "main"), "^node-logs-[0-9a-f]{8}-$")

    def test_one_there_already_with_its_digest_is_kept(self):
        # (a run uploaded it, and its commit was lost: the rebuild gave the same bytes)
        a = self.asset("job_1.tar.xz", b"same")
        rid = self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"same")])
        before = self.gh.assets[rid]["job_1.tar.xz"]["id"]
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.uploaded()[a["name"]]["id"], before)
        self.assertNotIn("POST", [m for m, p, _ in self.gh.requests if "/assets" in p])

    def test_one_there_of_a_pending_record_is_replaced(self):
        a = self.asset("job_1.tar.xz", b"new")
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"old")])
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {a["name"]: "sha256:" + a["sha256"]})

    def test_a_starter_asset_is_deleted(self):
        a = self.asset("job_1.tar.xz", b"data")
        rid = self.gh.release("node-logs-2026-10")
        self.gh.add(rid, "job_1.tar.xz", b"", state="starter", digest=False)
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {a["name"]: "sha256:" + a["sha256"]})

    def test_a_5xx_is_tried_again_after_deleting_what_it_left(self):
        a = self.asset("job_1.tar.xz", b"data")
        self.gh.upload_faults = [502, 503]
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {a["name"]: "sha256:" + a["sha256"]})
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "POST" and "/assets?" in p), 3)

    def test_but_only_so_often(self):
        a = self.asset("job_1.tar.xz", b"data")
        b = self.asset("job_2.tar.xz", b"more")
        self.gh.upload_faults = [502] * ua.RETRIES
        self.assertEqual(self.upload(), 1)
        self.assertIn("job_1.tar.xz: POST", self.errors)
        self.assertEqual(list(self.uploaded()), [b["name"]])  # (the next one is uploaded all the same)
        self.assertEqual(self.gh.held("node-logs-2026-10")[a["name"]], None)  # (the last try's starter: the next run's)

    def test_a_wrong_digest_in_githubs_answer_deletes_the_asset(self):
        a = self.asset("job_1.tar.xz", b"data")
        self.gh.wrong_digest = True
        self.assertEqual(self.upload(), 1)
        self.assertIn(f"job_1.tar.xz: GitHub took 4 bytes of digest sha256:{'0' * 64}", self.errors)
        self.gh.wrong_name = True  # (GitHub renames an asset whose name has special characters)
        self.assertEqual(self.upload(), 1)
        self.assertIn("job_1.tar.xz: GitHub took 4 bytes of digest", self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})
        self.assertEqual(self.uploaded(), {})
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(list(self.uploaded()), [a["name"]])

    def test_a_release_deleted_by_hand_ends_no_month(self):
        # (the month's releases are those listed, not <tag>, <tag>-2, ... up to the first missing)
        with mock.patch.object(ua, "MAX_ASSETS", 1):
            self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"1")])
            self.gh.release("node-logs-2026-10-3")
            self.asset("job_2.tar.xz", b"2")
            self.assertEqual(self.upload(), 0, self.errors)
            self.assertEqual(self.uploaded()["job_2.tar.xz"]["release"], "node-logs-2026-10-3")
            self.entries = [self.asset("job_3.tar.xz", b"3")]
            self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.uploaded()["job_3.tar.xz"]["release"], "node-logs-2026-10-4")
        self.assertEqual(self.gh.tags(), ["node-logs-2026-10", "node-logs-2026-10-3", "node-logs-2026-10-4"])

    def sleeps(self):
        sleeps = []
        patcher = mock.patch.object(ua.time, "sleep", side_effect=sleeps.append)
        patcher.start()
        self.addCleanup(patcher.stop)
        return sleeps

    def test_a_rate_limit_waits_as_github_asks(self):
        # (docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api)
        self.asset("job_1.tar.xz", b"x")
        sleeps = self.sleeps()
        reset = str(int(time.time()) + 100)
        for fault, waits in (((403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset}, "API rate limit exceeded"),
                              lambda w: 99 <= w[0] <= 102),
                             ((403, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": reset}, "Forbidden"),
                              lambda w: 99 <= w[0] <= 102),
                             ((429, {"Retry-After": "30"}, "slow down"), lambda w: w == [30]),
                             ((403, {}, "You have exceeded a secondary rate limit"), lambda w: w == [60]),
                             ((403, {"Retry-After": "7"}, "Forbidden"), lambda w: w == [7]),
                             ((503, {"Retry-After": "5"}, "Unavailable"), lambda w: w == [5]),
                             ((429, {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "9" * 5000}, "limited"),
                              lambda w: w == [60]),  # (a reset of more digits than int() takes: none said)
                             ((429, {"Retry-After": "9" * 13}, "limited"), lambda w: w == [60])):
            with self.subTest(fault):
                sleeps.clear()
                self.gh.faults = [fault]
                self.assertEqual(self.upload(), 0, self.errors)
                self.assertTrue(waits(sleeps), sleeps)
        self.gh.faults = [(403, {}, "API rate limit exceeded")] * 2
        sleeps.clear()
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(sleeps, [60, 120])  # (more each time)
        self.gh.faults = [(403, {}, "Resource not accessible by integration")]
        self.assertEqual(self.upload(), 1)  # (no rate limit: not tried again)

    def test_a_wait_past_the_steps_time_stops_the_run(self):
        self.asset("job_1.tar.xz", b"1")
        self.asset("job_2.tar.xz", b"2")
        self.asset("job_3.tar.xz", b"3")
        sleeps, put = self.sleeps(), ua.put

        def then_limited(gh, releases, entry):
            if entry["name"] == "job_2.tar.xz":
                self.gh.faults = [(429, {"Retry-After": str(ua.SECONDS)}, "slow down")]
            return put(gh, releases, entry)
        with mock.patch.object(ua, "put", side_effect=then_limited):
            self.assertEqual(self.upload(), 1)
        self.assertIn(f"a wait of {ua.SECONDS} s would pass the step's time", self.errors)
        self.assertEqual((sleeps, list(self.uploaded())), ([], ["job_1.tar.xz"]))
        self.assertIn("job_2.tar.xz", self.gh.requests[-1][1])  # (the one limited: none after it)
        self.assertNotIn("job_3.tar.xz", str(self.gh.requests))
        self.assertFalse(os.path.exists(os.path.join(self.dir, "releases.json")))

    def test_a_rate_limit_that_outlasts_a_requests_tries_stops_the_run(self):
        self.asset("job_1.tar.xz", b"1")
        self.asset("job_2.tar.xz", b"2")
        sleeps, serve = self.sleeps(), self.gh.serve

        def limited(method, path, body, headers):
            if method == "POST" and "/assets?" in path:
                return 403, {"message": "You have exceeded a secondary rate limit"}
            return serve(method, path, body, headers)
        with mock.patch.object(self.gh, "serve", side_effect=limited):
            self.assertEqual(self.upload(), 1)
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "POST" and "/assets?" in p), ua.RETRIES)
        self.assertEqual(sleeps, [60, 120, 240])
        self.assertNotIn("job_2.tar.xz", str(self.gh.requests))  # (not tried, nor waited for, again)
        self.assertFalse(os.path.exists(os.path.join(self.dir, "releases.json")))

    def test_as_does_one_of_another_request(self):
        self.asset("job_1.tar.xz", b"1")
        self.asset("job_2.tar.xz", b"2")
        rid, serve = self.gh.release("node-logs-2026-10"), self.gh.serve
        sleeps = self.sleeps()

        def limited(method, path, body, headers):  # (the listing of the release's assets)
            if f"/releases/{rid}/assets?" in path:
                return 403, {"message": "You have exceeded a secondary rate limit"}
            return serve(method, path, body, headers)
        with mock.patch.object(self.gh, "serve", side_effect=limited):
            self.assertEqual(self.upload(), 1)
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if f"/releases/{rid}/assets?" in p), ua.RETRIES)
        self.assertEqual(sleeps, [60, 120, 240])

    def test_an_upload_that_could_outlast_the_step_is_left_for_the_next_run(self):
        small = self.asset("job_1.tar.xz", b"y")
        big = self.asset("job_2.tar.xz", b"x" * 300_000)  # (60 + 3 seconds at 100 kB/s)
        once, timeouts = ua.GitHub.once, []

        def timed(gh, method, url, data=None, headers=None, timeout=60):
            timeouts.append(timeout)
            return once(gh, method, url, data, headers, timeout)
        with mock.patch.object(ua, "SECONDS", 62), mock.patch.object(ua, "RATE", 100_000), \
                mock.patch.object(ua.GitHub, "once", timed):
            self.assertEqual(self.upload(), 0, self.errors)
            self.assertIn(f"{big['name']}: its upload could outlast the step's time", self.printed)
            self.assertEqual(list(self.uploaded()), [small["name"]])
            self.assertEqual(self.upload(), 0, self.errors)  # (the run's first upload is started all the same)
            self.assertEqual(sorted(self.uploaded()), [small["name"], big["name"]])
            self.assertEqual(set(timeouts), {60})  # (of each read and write)
            third = self.asset("job_3.tar.xz", b"z" * 300_000)
            self.entries, self.gh.upload_faults = [third], [502]
            self.assertEqual(self.upload(), 1)  # (but not a retry that could outlast the step)
        self.assertIn(f"{third['name']}: POST", self.errors)
        self.assertIn("HTTP 502; no time left to try again", self.errors)
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "POST" and "job_3" in p), 1)

    def test_one_left_for_the_next_run_keeps_what_is_there(self):
        self.asset("job_1.tar.xz", b"y")
        self.asset("job_2.tar.xz", b"x" * 300_000)
        self.gh.release("node-logs-2026-10", [("job_2.tar.xz", b"an older pack")])
        with mock.patch.object(ua, "SECONDS", 62), mock.patch.object(ua, "RATE", 100_000):
            self.assertEqual(self.upload(), 0, self.errors)
        self.assertIn("job_2.tar.xz: its upload could outlast the step's time", self.printed)
        self.assertEqual(self.gh.held("node-logs-2026-10")["job_2.tar.xz"], "sha256:" + sha(b"an older pack"))

    def test_a_failed_upload_was_the_runs_first(self):
        self.asset("job_1.tar.xz", b"y")
        big = self.asset("job_2.tar.xz", b"x" * 300_000)
        self.gh.upload_faults = [422]
        with mock.patch.object(ua, "SECONDS", 62), mock.patch.object(ua, "RATE", 100_000):
            self.assertEqual(self.upload(), 1)
        self.assertIn(f"{big['name']}: its upload could outlast the step's time", self.printed)
        self.assertEqual(self.uploaded(), {})

    def test_a_backoff_past_the_steps_time_too(self):
        # (a 5xx asks for none: its backoff would pass the step's time)
        self.gh.faults = [(500, {}, "fault")]
        gh = ua.GitHub(self.gh.url, self.gh.url, REPO, TOKEN, time.monotonic() + 0.5)
        with mock.patch.object(ua, "BACKOFF", 2.0), self.assertRaisesRegex(ua.OutOfTime, "^a wait of [1-3] s would pass"):
            gh.call("GET", f"/repos/{REPO}")

    def test_nothing_once_the_steps_time_is_up(self):
        gh = ua.GitHub(self.gh.url, self.gh.url, REPO, TOKEN, time.monotonic() - 1)
        with self.assertRaisesRegex(ua.OutOfTime, "the step's time is up"):  # (not even a request a second after the last)
            gh.call("DELETE", f"/repos/{REPO}/releases/assets/1", ok=(204,))
        self.asset("job_1.tar.xz", b"x")
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])  # (kept: it would change nothing)
        with mock.patch.object(ua, "SECONDS", -1):
            self.assertEqual(self.upload(), 1)
        self.assertIn("the step's time is up", self.errors)
        self.assertEqual((self.uploaded(), [m for m, _, _ in self.gh.requests]), ({}, ["GET", "GET"]))  # (the repo, the releases)
        with open(os.path.join(self.dir, "uploaded.json")) as f:
            self.assertEqual(json.load(f)["run"], "2026-10-30T051234Z")  # (the manifest's)

    def test_an_error_whose_body_cannot_be_read(self):
        class Unreadable(io.RawIOBase):
            def readinto(self, b):
                raise ConnectionResetError("reset")

        class Opener:
            def open(self, req, timeout):
                raise urllib.error.HTTPError(req.full_url, 502, "Bad Gateway", {}, Unreadable())
        gh = ua.GitHub(self.gh.url, self.gh.url, REPO, TOKEN, math.inf)
        gh.opener = Opener()
        with self.assertRaisesRegex(ua.Retry, "HTTP 502"):
            gh.once("GET", gh.api + f"/repos/{REPO}")

    def test_requests_that_change_something_are_a_second_apart(self):
        self.asset("job_1.tar.xz", b"1")
        self.asset("job_2.tar.xz", b"2")
        sleeps = self.sleeps()
        with mock.patch.object(ua, "GAP", 1.0):
            self.assertEqual(self.upload(), 0, self.errors)
        changes = [m for m, _, _ in self.gh.requests if m != "GET"]
        self.assertEqual(changes, ["POST"] * 3)  # (the release, and the two assets)
        self.assertEqual(len(sleeps), 2)
        self.assertTrue(all(0 < s <= 1 for s in sleeps), sleeps)  # (a second less the time since the last)

    def test_an_answer_without_its_digest(self):
        a = self.asset("job_1.tar.xz", b"data")
        self.gh.hide_digest = 1  # (the upload's answer: a read says it)
        self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.uploaded()[a["name"]]["digest"], "sha256:" + a["sha256"])
        self.assertNotIn("note", self.uploaded()[a["name"]])
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "GET" and "/releases/assets/" in p), 1)
        self.gh.assets[next(iter(self.gh.assets))].clear()
        self.gh.hide_digest, sleeps = 1 + ua.DIGEST_READS, self.sleeps()  # (none says it: kept, with the file's)
        with mock.patch.object(ua, "BACKOFF", 2.0):
            self.assertEqual(self.upload(), 0, self.errors)
        self.assertEqual(self.uploaded()[a["name"]]["digest"], "sha256:" + a["sha256"])
        self.assertIn("note", self.uploaded()[a["name"]])
        self.assertEqual(sleeps, [2.0, 4.0, 8.0])  # (each read after a wait, doubled)
        self.assertEqual(list(self.gh.held("node-logs-2026-10")), [a["name"]])
        self.gh.assets[next(iter(self.gh.assets))].clear()
        self.gh.hide_digest, self.gh.reread_digest = 1, "sha256:" + "0" * 64  # (one that differs: deleted)
        self.assertEqual(self.upload(), 1)
        self.assertIn(f"GitHub took 4 bytes of digest sha256:{'0' * 64}", self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})

    def test_what_is_sent_is_the_file_checked(self):
        a = self.asset("job_1.tar.xz", b"data")
        path, checked = os.path.join(self.dir, a["name"]), ua.checked

        def then_changed(entry, folder, seen, how):
            got = checked(entry, folder, seen)
            if how == "in place":
                with open(path, "r+b") as f:
                    f.write(b"DATA")
            else:
                with open(path + ".new", "wb") as f:
                    f.write(b"DATA")
                os.replace(path + ".new", path)
            return got
        self.gh.hide_digest = 99  # (GitHub says no digest: only the bytes sent tell)
        with mock.patch.object(ua, "checked", side_effect=lambda *x: then_changed(*x, "in place")):
            self.assertEqual(self.upload(), 1)
        self.assertIn(f"(sent sha256:{sha(b'DATA')})", self.errors)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})
        with open(path, "wb") as f:
            f.write(b"data")
        with mock.patch.object(ua, "checked", side_effect=lambda *x: then_changed(*x, "replaced")):
            self.assertEqual(self.upload(), 0, self.errors)
        rid = next(iter(self.gh.assets))
        self.assertEqual(self.gh.assets[rid][a["name"]]["size"], 4)
        self.assertEqual(self.uploaded()[a["name"]]["digest"], "sha256:" + a["sha256"])

    def test_a_folder_named_like_an_asset(self):
        folder = os.path.join(self.dir, "job_3.tar.xz")
        os.mkdir(folder)
        with open(os.path.join(folder, "x"), "wb"):
            pass
        size = os.stat(folder).st_size
        if not size:
            self.skipTest("a folder of no size here")
        self.entries = [{"name": "job_3.tar.xz", "month": "2026-10", "bytes": size, "sha256": sha(b"")}]
        self.assertEqual(self.upload(), 1)
        self.assertIn("job_3.tar.xz: not a file", self.errors)

    def test_answers_of_another_shape(self):
        self.asset("job_1.tar.xz", b"x")
        self.gh.release("node-logs-2026-10")
        serve = self.gh.serve
        for method, path, answer in (("GET", f"/repos/{REPO}", []),
                                     ("GET", f"/repos/{REPO}/releases?", [{"id": "1", "tag_name": "node-logs-2026-10"}]),
                                     ("GET", f"/repos/{REPO}/releases?", [{"id": True, "tag_name": "node-logs-2026-10"}]),
                                     ("GET", "/assets?per_page", [{"name": "job_1.tar.xz"}]),
                                     ("POST", "/assets?name=", {"id": 1})):
            def odd(m, p, body, headers, method=method, path=path, answer=answer):
                if m == method and (p == path or "?" in path and path in p):
                    return 201 if m == "POST" else 200, answer
                return serve(m, p, body, headers)
            with self.subTest(path), mock.patch.object(self.gh, "serve", side_effect=odd):
                self.assertEqual(self.upload(), 1)
                self.assertIn("GitHub answered", self.errors)

    def test_entries_it_does_not_upload(self):
        good = self.asset("job_9.tar.xz", b"fine")
        self.asset("job_1.tar.xz", b"x")
        os.symlink(os.path.join(self.dir, "job_1.tar.xz"), os.path.join(self.dir, "job_2.tar.xz"))
        os.mkdir(os.path.join(self.dir, "job_3.tar.xz"))
        for name in ("job_1.tar.gz", "job_x.tar.xz", "-job_1.tar.xz"):  # (files the names would take)
            with open(os.path.join(self.dir, name), "wb") as f:
                f.write(b"x")
        os.mkdir(os.path.join(self.dir, "sub"))
        base = {"month": "2026-10", "bytes": 1, "sha256": sha(b"x")}
        bad = [dict(base, name="job_2.tar.xz"),  # (a link)
               dict(base, name="job_3.tar.xz"),  # (a folder)
               dict(base, name="../job_1.tar.xz"), dict(base, name="sub/../job_1.tar.xz"), dict(base, name="job_1.tar.gz"),
               dict(base, name="job_x.tar.xz"),
               dict(base, name="-job_1.tar.xz"), dict(base, name="job_1.tar.xz", month="2026-13"),
               dict(base, name="job_1.tar.xz", month="../../x"), dict(base, name="job_1.tar.xz", bytes=2),
               dict(base, name="job_1.tar.xz", bytes=True), dict(base, name="job_1.tar.xz", bytes=0),
               dict(base, name="job_1.tar.xz", sha256=sha(b"y")), dict(base, name="job_1.tar.xz", sha256="X" * 64),
               dict(base, name="j" * 247 + "_1.tar.xz"), "job_1.tar.xz", None]
        with mock.patch.object(ua, "MAX_BYTES", 4):
            self.assertEqual(self.upload(entries=bad + [good, good, dict(base, name="job_1.tar.xz", bytes=5)]), 1)
        self.assertEqual(list(self.uploaded()), [good["name"]])
        self.assertEqual(self.gh.held("node-logs-2026-10"), {good["name"]: "sha256:" + good["sha256"]})
        self.assertEqual(self.errors.count("ERROR"), len(bad) + 2)
        self.assertIn("job_9.tar.xz: listed twice", self.errors)
        self.assertIn("job_1.tar.xz: not a size GitHub takes: 'True'", self.errors)
        self.assertIn("not an asset's name: 'jjjj", self.errors)  # (256 characters: more than GitHub's 255)
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "POST" and "/assets?" in p), 1)  # (none tried)

    def test_the_asset_of_a_build_taken_out_is_deleted(self):
        # (whatever its digest, from every branch's releases: a build's asset has its name)
        self.gh.release("node-logs-2026-09", [("job_1.tar.xz", b"x")])
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"packed by another liblzma"), ("job_2.tar.xz", b"y")])
        self.gh.release("node-logs-test-0123abcd-2026-10", [("job_1.tar.xz", b"x")], prerelease=True)
        self.gh.release("v1.0", [("job_1.tar.xz", b"x")])  # (not one of node logs)
        self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_3.tar.xz"]), 0, self.errors)
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted", "job_3.tar.xz": "absent"})  # fails: None
        self.assertEqual([list(self.gh.held(t)) for t in ("node-logs-2026-09", "node-logs-2026-10",
                                                         "node-logs-test-0123abcd-2026-10", "v1.0")],
                         [[], ["job_2.tar.xz"], [], ["job_1.tar.xz"]])
        self.assertEqual(self.releases()["node-logs-2026-09"]["assets"], [])

    def test_on_another_branch_only_from_its_own_releases(self):
        mine = ua.branch_prefix("fault-inject", "main") + "2026-10"
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        self.gh.release(ua.branch_prefix("other", "main") + "2026-10", [("job_1.tar.xz", b"x")], prerelease=True)
        self.gh.release(mine, [("job_1.tar.xz", b"x")], prerelease=True)
        self.assertEqual(self.upload(branch="fault-inject", delete=["job_1.tar.xz"]), 0, self.errors)
        self.assertEqual(self.gh.held(mine), {})  # fails: (kept) job_1.tar.xz
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})
        self.assertEqual([list(self.gh.held(t)) for t in self.gh.tags() if t != mine], [["job_1.tar.xz"]] * 2)

    def test_absent_only_if_a_second_listing_lacks_it_too(self):
        # (a listing paged by offset: another run deletes an asset of page 1 while page 2 is asked for)
        names = [f"a_{i}.tar.xz" for i in range(100)] + ["job_1.tar.xz"] + [f"b_{i}.tar.xz" for i in range(49)]
        rid, serve = self.gh.release("node-logs-2026-10", [(n, b"x") for n in names]), self.gh.serve

        def racing(method, path, body, headers):
            got = serve(method, path, body, headers)
            if method == "GET" and f"/releases/{rid}/assets?" in path and "&page=1" in path:
                self.gh.assets[rid].pop("a_0.tar.xz", None)
            return got
        with mock.patch.object(self.gh, "serve", side_effect=racing):
            self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 0, self.errors)
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})  # fails: None
        self.assertNotIn("job_1.tar.xz", self.gh.held("node-logs-2026-10"))

    def test_every_copy_though_one_was_found(self):
        # (main's, and one in a branch's prerelease that a delete by another run hides from its paged listing)
        names = [f"a_{i}.tar.xz" for i in range(100)] + ["job_1.tar.xz"]
        rid, serve = self.gh.release(ua.branch_prefix("b", "main") + "2026-10", [(n, b"x") for n in names], True), self.gh.serve
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])

        def racing(method, path, body, headers):
            got = serve(method, path, body, headers)
            if method == "GET" and f"/releases/{rid}/assets?" in path and "&page=1" in path:
                self.gh.assets[rid].pop("a_0.tar.xz", None)
            return got
        with mock.patch.object(self.gh, "serve", side_effect=racing):
            self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 0, self.errors)
        self.assertEqual([t for t in self.gh.tags() if "job_1.tar.xz" in self.gh.held(t)], [])  # fails: (kept) both

    def test_two_listings_for_all_the_names(self):
        for month in range(1, 7):
            self.gh.release(f"node-logs-2026-0{month}", [(f"a{month}_{i}.tar.xz", b"x") for i in range(250)])
        names = [f"job_{i}.tar.xz" for i in range(20)]
        self.assertEqual(self.upload(delete=names), 0, self.errors)
        self.assertEqual(self.uploaded("deleted"), dict.fromkeys(names, "absent"))  # fails: None
        # (the repo, then two listings, each of the releases and their assets, 3 pages each: the first, and the last)
        self.assertEqual(sum(1 for m, _, _ in self.gh.requests if m == "GET"), 1 + 2 * (1 + 6 * 3))
        self.gh.requests.clear()
        self.assertEqual(self.upload(delete=[]), 0, self.errors)  # (none to delete: only the last lists the assets)
        self.assertEqual(sum(1 for m, _, _ in self.gh.requests if m == "GET"), 1 + 1 + (1 + 6 * 3))

    def test_none_once_the_steps_time_is_up(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        with mock.patch.object(ua, "SECONDS", -1):
            self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 1)  # fails: 0
        self.assertIn("the step's time is up", self.errors)
        self.assertEqual([m for m, _, _ in self.gh.requests], ["GET", "GET"])  # (the repo, the releases)

    def test_those_deleted_before_the_step_was_cut_off(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        self.asset("job_2.tar.xz", b"y")
        with mock.patch.object(ua, "put", side_effect=ua.OutOfTime("the step's time is up")):
            self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_3.tar.xz"]), 1)
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})  # fails: None
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})

    def test_those_deleted_before_the_step_was_cut_off_in_a_listing(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x"), ("job_2.tar.xz", b"y")])
        delete = ua.delete

        def deleting(gh, asset):  # (a rate limit on the second)
            if asset["name"] == "job_2.tar.xz":
                raise ua.OutOfTime("a rate limit outlasted the tries")
            return delete(gh, asset)
        with mock.patch.object(ua, "delete", side_effect=deleting):
            self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_2.tar.xz"]), 1)  # fails: 0
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})
        self.assertEqual(list(self.gh.held("node-logs-2026-10")), ["job_2.tar.xz"])

    def test_and_in_the_last_listing(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x"), ("job_2.tar.xz", b"y")])
        delete, calls = ua.delete, []

        def deleting(gh, asset):  # (both fail in the first listing; in the last, a rate limit on the second)
            calls.append(asset["name"])
            if len(calls) <= 2:
                raise ua.Failed("HTTP 500")
            if len(calls) == 4:
                raise ua.OutOfTime("a rate limit outlasted the tries")
            return delete(gh, asset)
        with mock.patch.object(ua, "delete", side_effect=deleting):
            self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_2.tar.xz"]), 1)  # fails: 0
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted", "job_2.tar.xz": "failed"})
        self.assertEqual(list(self.gh.held("node-logs-2026-10")), ["job_2.tar.xz"])

    def test_absent_only_once_every_release_was_listed(self):
        self.gh.release("node-logs-2026-09")
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        rid, serve = self.gh.release("node-logs-2026-11"), self.gh.serve

        def failing(method, path, body, headers):  # (the listing of one release's assets)
            return (500, {"message": "fault"}) if f"/releases/{rid}/assets" in path else serve(method, path, body, headers)
        with mock.patch.object(self.gh, "serve", side_effect=failing):
            self.assertEqual(self.upload(delete=["job_3.tar.xz"]), 1)
        self.assertEqual(self.uploaded("deleted"), {})  # fails: None

    def test_a_release_not_listed_stops_none_of_the_others(self):
        rid, serve = self.gh.release("node-logs-2026-01"), self.gh.serve
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])

        def failing(method, path, body, headers):  # (the assets of the release sorted first)
            if f"/releases/{rid}/assets" in path:
                return 403, {"message": "Resource not accessible by integration"}
            return serve(method, path, body, headers)
        with mock.patch.object(self.gh, "serve", side_effect=failing):
            self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_3.tar.xz"]), 1)
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})  # fails: (kept) job_1.tar.xz
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})  # (job_3: not "absent")

    def test_a_delete_that_fails_says_so(self):
        # (the archive keeps asking for it: see fetch_red_logs.record_uploads)
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        self.gh.delete_faults = [500] * ua.RETRIES * 2  # (tried in both listings)
        self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 1)  # fails: 0
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "failed"})
        self.assertIn("ERROR job_1.tar.xz: DELETE", self.errors)
        self.assertEqual(list(self.gh.held("node-logs-2026-10")), ["job_1.tar.xz"])

    def test_one_deleted_when_tried_again_is_deleted(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x")])
        self.gh.delete_faults = [500] * ua.RETRIES  # (the run's listing's tries: the new listing's delete passes)
        self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 0, self.errors)
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted"})  # fails: None
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})

    def test_nor_stops_the_others(self):
        # (one copy of job_1 refused for good: its other copy and job_2 go, and job_1 has failed)
        rid, serve = self.gh.release("node-logs-2026-09", [("job_1.tar.xz", b"x")]), self.gh.serve
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x"), ("job_2.tar.xz", b"y")])
        refused = self.gh.assets[rid]["job_1.tar.xz"]["id"]

        def refusing(method, path, body, headers):
            if method == "DELETE" and path.endswith(f"/assets/{refused}"):
                return 403, {"message": "Resource not accessible by integration"}
            return serve(method, path, body, headers)
        with mock.patch.object(self.gh, "serve", side_effect=refusing):
            self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_2.tar.xz"]), 1)  # fails: 0
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "failed", "job_2.tar.xz": "deleted"})
        self.assertEqual(self.errors.count("ERROR"), 1)
        self.assertEqual([self.gh.held(t) for t in self.gh.tags()], [{"job_1.tar.xz": "sha256:" + sha(b"x")}, {}])

    def test_every_name_held(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x"), ("job_2.tar.xz", b"y")])
        self.assertEqual(self.upload(delete=["job_1.tar.xz", "job_2.tar.xz"]), 0, self.errors)
        self.assertEqual(self.uploaded("deleted"), {"job_1.tar.xz": "deleted", "job_2.tar.xz": "deleted"})  # fails: None
        self.assertEqual(self.gh.held("node-logs-2026-10"), {})

    def test_a_deleted_one_leaves_room(self):
        self.gh.release("node-logs-2026-10", [("job_1.tar.xz", b"x"), ("job_2.tar.xz", b"y")])
        self.asset("job_3.tar.xz", b"z")
        with mock.patch.object(ua, "MAX_ASSETS", 2):
            self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 0, self.errors)
            self.assertEqual(self.uploaded()["job_3.tar.xz"]["release"], "node-logs-2026-10")  # fails: node-logs-2026-10-2
            self.entries, self.gh.delete_faults = [self.asset("job_4.tar.xz", b"w")], [422] * 2
            self.assertEqual(self.upload(delete=["job_2.tar.xz"]), 1)
        self.assertEqual(self.uploaded()["job_4.tar.xz"]["release"], "node-logs-2026-10-2")  # (one not deleted leaves none)

    def test_entries_to_delete_it_does_not_take(self):
        for delete in (["../x_1.tar.xz"], [{"name": "job_1.tar.xz"}], [None], {"job_1.tar.xz": sha(b"x")}):
            with self.subTest(delete):
                self.assertEqual(self.upload(delete=delete), 1)  # fails: 0
                self.assertIn("ERROR", self.errors)

    def test_nor_is_one_uploaded(self):
        self.asset("job_1.tar.xz", b"x")
        self.assertEqual(self.upload(delete=["job_1.tar.xz"]), 1)  # fails: 0
        self.assertIn("job_1.tar.xz: of a build taken out; not uploaded", self.errors)
        self.assertEqual((self.gh.tags(), self.uploaded()), ([], {}))

    def test_a_manifest_it_cannot_read(self):
        for name in ("releases.json", "uploaded.json"):  # (not this run's: the record step must not read them)
            with open(os.path.join(self.dir, name), "w") as f:
                json.dump({"uploaded": {"job_1.tar.xz": {}}}, f)
        with open(os.path.join(self.dir, "manifest.json"), "w") as f:
            f.write("{")
        env = ENV
        with mock.patch("sys.stderr"):
            self.assertEqual(ua.main([self.dir, "--api", self.gh.url, "--uploads", self.gh.url], env), 1)
        self.assertEqual(self.gh.requests, [])
        self.assertEqual((self.uploaded(), os.path.exists(os.path.join(self.dir, "releases.json"))), ({}, False))

    def test_none_without_the_fetchs_folder(self):
        # (a fetch that failed before it made it: the upload step says so, and the fetch fails the run)
        env = ENV
        with mock.patch("sys.stdout"):
            self.assertEqual(ua.main([os.path.join(self.dir, "none"), "--api", self.gh.url], env), 0)
        self.assertEqual(self.gh.requests, [])

    def test_none_without_the_token_and_the_run(self):
        for missing in ENV:
            env = {k: v for k, v in ENV.items() if k != missing}
            with self.subTest(missing), mock.patch("sys.stderr"):
                self.assertEqual(ua.main([self.dir, "--api", self.gh.url], env), 1)
        self.assertEqual(self.gh.requests, [])

    def test_a_redirect_is_not_followed(self):
        # (urllib would send the token on to where it leads)
        elsewhere = FakeGitHub()
        self.addCleanup(elsewhere.close)
        moved = []

        class Moved(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                moved.append(self.path)
                self.send_response(301)
                self.send_header("Location", elsewhere.url + self.path)
                self.send_header("Content-Length", "0")
                self.end_headers()
        httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Moved)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        gh = ua.GitHub(f"http://127.0.0.1:{httpd.server_address[1]}", "", REPO, TOKEN, math.inf)
        with self.assertRaises(ua.Failed):
            gh.call("GET", f"/repos/{REPO}")
        self.assertEqual((moved, elsewhere.requests), ([f"/repos/{REPO}"], []))


class Isolation(unittest.TestCase):
    def test_it_imports_only_the_standard_library(self):
        with open(os.path.join(HERE, "upload_assets.py")) as f:
            tree = ast.parse(f.read())
        names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        names |= {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
        self.assertEqual(names - sys.stdlib_module_names, set())
        with open(os.path.join(HERE, "upload_assets.py")) as f:
            self.assertNotIn("sys.path", f.read())

    def test_it_runs_as_the_workflow_runs_it(self):
        # (python3 -I -S, no site module: --help gets that far)
        done = subprocess.run([sys.executable, "-I", "-S", os.path.join(HERE, "upload_assets.py"), "--help"],
                              capture_output=True, text=True, timeout=60, env={})
        self.assertEqual(done.returncode, 0, done.stderr)


if __name__ == "__main__":
    unittest.main()
