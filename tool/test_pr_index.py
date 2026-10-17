"""Pull request jobs' builds. TestGrid's group of such a job reads Prow's index of its builds,
gs://<bucket>/pr-logs/directory/<job>/: an entry there per build, <build>.txt, links the folder that
holds the build's files, pr-logs/pull/[<org>_<repo>/]<pull>/<job>/<build>/ (or pr-logs/pull/batch/
<job>/<build>/). The archive reads them where the link says, as TestGrid's util/gcs/read.go does
(readLink, ListBuilds), and took them for folders under the index before.

Run: cd tool && python -m unittest test_pr_index -v
"""
import glob
import json
import os
import shutil
import sys
import threading
import time
import unittest
import urllib.parse
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import test_review_round4 as r4  # noqa: E402  (a module import: its tests are not collected again here)
from test_fetch_red_logs import FakeServer, send, table  # noqa: E402
from test_integrity import DAY, HOUR, obj, snowflake  # noqa: E402

INDEX = "pr-logs/directory/pull-job"


def entry_path(build):
    return f"/storage/v1/b/bucket/o/{urllib.parse.quote(f'{INDEX}/{build}.txt', safe='')}?fields=metadata"


def listing(body):
    """A GCS listing route that sends body, without what the request's fields leave out (as GCS does)."""
    def serve(h):
        fields = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query).get("fields", [""])[0]
        send(h, 200, {"Content-Type": "application/json"},
             json.dumps({k: v for k, v in body.items() if not fields or k in fields}).encode())
    return serve


class Links(unittest.TestCase):
    """An entry's link, read as TestGrid reads it."""

    def test_its_x_goog_meta_link_then_its_link(self):
        self.assertEqual(frl.link_folder({"x-goog-meta-link": "gs://b/p/1", "link": "gs://b/q/2"}), ("1", "b/p/1"))
        self.assertEqual(frl.link_folder({"link": "gs://b/q/2"}), ("2", "b/q/2"))

    def test_an_empty_x_goog_meta_link_is_no_link_whatever_its_link(self):
        # (readLink returns it, and ListBuilds takes "" for an entry that links nothing)
        self.assertIsNone(frl.link_folder({"x-goog-meta-link": "", "link": "gs://b/q/2"}))

    def test_none(self):
        for metadata in (None, {}, {"other": "gs://b/p/1"}, [], "gs://b/p/1"):
            self.assertIsNone(frl.link_folder(metadata), metadata)

    def test_as_testgrid_reads_it(self):
        """The build TestGrid's ListBuilds takes from a link (util/gcs/read.go, f15bfa4808a4) and where
        it reads its files, as a Go program of its code and gcs.go's Path.SetURL, built with Go 1.26.5,
        reads them: "fails" where its listing fails; (build, None) for one TestGrid reads outside GCS,
        and one with a %XX, or a part that is empty, "." or "..", or not a letter, a digit or "_"
        followed by letters, digits, ".", "_" and "-" (this archive reads none of them); else (build,
        folder), where TestGrid reads folder/started.json."""
        for link, taken in (
                ("gs://kubernetes-ci-logs/pr-logs/pull/140698/pull-kubernetes-linter-hints/2078641291377250304",
                 ("2078641291377250304", "kubernetes-ci-logs/pr-logs/pull/140698/pull-kubernetes-linter-hints/"
                                         "2078641291377250304")),
                (" gs://b/pr-logs/pull/1/j/2/ \n", ("2", "b/pr-logs/pull/1/j/2")),  # (bootstrap.py's end in a space)
                ("\u00a0gs://b/p/1\u3000", ("1", "b/p/1")), ("GS://b/p/1", ("1", "b/p/1")),
                ("gs://b/p/1?", ("1", "b/p/1")), ("gs://b/p/1#", ("1", "b/p/1")),
                ("  ", (".", None)), ("gs://b", (".", None)), ("gs://b/", (".", None)), ("gs:", (".", None)),
                ("gs:///p/1", ("1", None)), ("file:///p/1", ("1", None)), ("/p/1", ("1", None)), ("p/1", ("1", None)),
                ("//b/p/1", ("1", None)),
                ("gs://b/p/1//", ("1", None)), ("gs://b//p/1", ("1", None)), ("gs://b/p//1", ("1", None)),  # (b/p/1//...)
                ("gs://b/p/./1", ("1", None)), ("gs://b/p/../1", ("1", None)),  # (b/p/1/..., b/1/...)
                ("gs://b/p/1%2F", ("1", None)), ("gs://b/p/%41/1", ("1", None)),  # (b/p/..., b/p/A/1/...)
                ("gs://b/p/é/1", ("1", None)), ("gs://b/p/1\u200b", ("1\u200b", None)), ("gs://b/p/-1", ("-1", None)),
                ("gs://b/p/kubernetes~x/9/j/1", ("1", None)),
                ("https://b/p/1", "fails"), ("gs:b/p/1", "fails"), ("gs://b:80/p/1", "fails"), ("gs://u@b/p/1", "fails"),
                ("gs://b/p/1?x=1", "fails"), ("gs://b/p/1#f", "fails"), ("gs://[::1]/p/1", "fails"),
                ("gs://[x]/p/1", "fails"), ("gs://b/p/1\x1c", "fails"), ("gs://b/p/1\x7f", "fails"),
                ("gs://b/p/%zz/1", "fails"), (":gs://b/p/1", "fails"), ("a:b/1", "fails"), ("1a:b/1", "fails"),
                ("gs://b c/p/1", "fails"), ("gs://b%41/p/1", "fails"), ("gs://@b/p/1", "fails"),
                ("///", ("/", None)), ("gs://b//", (".", None)),
                ("\u200b", ("\udc80\udc8b", None))):  # (TestGrid's Object() drops the first byte of a path)
            with self.subTest(link=link):
                if taken == "fails":
                    with self.assertRaises(frl.IntegrityError):
                        frl.link_folder({"x-goog-meta-link": link})
                else:
                    self.assertEqual(frl.link_folder({"x-goog-meta-link": link}), taken)

    def test_one_that_is_not_text_is_one_testgrid_cannot_list(self):
        with self.assertRaises(frl.IntegrityError):
            frl.link_folder({"x-goog-meta-link": 7})

    def test_a_lone_surrogate_escape_as_go_reads_it(self):
        # (json.loads keeps one as it is; Go's encoding/json, in GCS's client, reads U+FFFD)
        link = json.loads('"gs://b/p/1/2' + chr(92) + 'ud800"')
        self.assertEqual(frl.link_folder({"x-goog-meta-link": link}), ("2" + chr(0xFFFD), None))

    def test_which_queries_are_an_index(self):
        for query, index in (("kubernetes-ci-logs/pr-logs/directory/pull-kubernetes-linter-hints", True),
                             ("b/pr-logs/directory/j", True), ("b/x/pr-logs/directory/j", True), ("b/logs/j", False),
                             ("b/pr-logs/directory", False), ("b/pr-logs/directory/j/k", False),
                             ("b/pr-logs/pull/1/j", False), ("b/xpr-logs/directory/j", False)):
            self.assertEqual(frl.indexed(query), index, query)


class Folders(unittest.TestCase):
    """Where a build keeps its files (build_folder), and the builds a listing of the index holds."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        self.addCleanup(frl.bind_context, frl.context())
        frl.bind_context(frl.RunContext(frl.AdaptiveLimit(8, 8)))  # (a folder cache of its own, not one a run left)

    def entry(self, build, metadata):
        self.srv.route(entry_path(build), (200, {}, {"metadata": metadata}))

    def test_an_entry_is_no_folder(self):
        # (GCS lists the entry 5.txt under the prefix "5" too: only "5/" is a folder)
        self.entry("5", {})
        self.srv.gcs_listing("bucket", [f"{INDEX}/5.txt"])
        with self.assertRaises(frl.NoSuchBuild):
            frl.build_folder(self.srv.url, f"bucket/{INDEX}", "5")

    def test_one_object_is_enough_to_tell_a_folder(self):
        asked = []

        def serve(h):
            asked.append(urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query))
            listing({"items": [{"name": f"{INDEX}/5/started.json"}]})(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        self.assertEqual(frl.build_folder(self.srv.url, f"bucket/{INDEX}", "5"), f"bucket/{INDEX}/5")
        self.assertEqual((asked[0]["prefix"], asked[0]["maxResults"]), ([f"{INDEX}/5/"], ["1"]))

    def test_a_listing_that_is_not_one_tells_nothing(self):
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing({"items": {"name": f"{INDEX}/5/x"}})
        with self.assertRaises(frl.IntegrityError) as raised:
            frl.build_folder(self.srv.url, f"bucket/{INDEX}", "5")
        self.assertNotIsInstance(raised.exception, frl.NoSuchBuild)
        self.assertIn("unexpected GCS listing", str(raised.exception))

    def test_a_pull_request_job_s_prowjob_json_where_its_entry_links(self):
        # (its timeouts: how long a build of the job can run)
        self.entry("5", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job/5"})
        self.srv.route("/bucket/pr-logs/pull/9/pull-job/5/prowjob.json",
                       (200, {}, {"spec": {"decoration_config": {"timeout": "2h"}}}))
        self.assertEqual(frl.prow_job_hours(self.srv.url, f"bucket/{INDEX}", "5"), 2.25)

    def test_a_record_s_folder_has_two_parts_or_more(self):
        self.assertEqual(frl.stored_folder({"build": "5", "gcs_path": "bucket/5"}), "bucket/5")
        for path in ("5", "bucket/6", "bucket//5", "bucket/../5", 5):
            with self.subTest(path=path):
                self.assertIsNone(frl.stored_folder({"build": "5", "gcs_path": path}))

    def test_each_read_of_a_build_this_archive_does_not_read_fails(self):
        # (one whose entry links another build: a second read fails too, and does not wait for the first)
        self.entry("5", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job/7"})
        timeline, ctx, got = frl.Timeline(self.srv.url, f"bucket/{INDEX}"), frl.context(), []

        def read():
            frl.bind_context(ctx)
            try:
                timeline.files("5")
                got.append("read")
            except frl.IntegrityError:
                got.append("failed")
        for _ in range(2):
            t = threading.Thread(target=read, daemon=True)
            t.start()
            t.join(10)
        self.assertEqual(got, ["failed", "failed"])

    def test_a_build_listed_twice_is_one(self):
        # (its entry on one page, and a folder of it under the index on the next, as "5.txt" sorts before
        # "5/": the first says where it is, for the fetch and cross_check alike)
        now = time.time()
        b = str(snowflake(now - HOUR))
        pages = [{"items": [{"name": f"{INDEX}/{b}.txt",
                             "metadata": {"x-goog-meta-link": f"gs://bucket/pr-logs/pull/9/pull-job/{b}"}}],
                  "nextPageToken": "2"},
                 {"prefixes": [f"{INDEX}/{b}/"]}]

        def serve(h):
            listing(pages[1 if "pageToken" in urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query) else 0])(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        for name, ids in (("fetch", lambda: frl.list_build_ids(self.srv.url, f"bucket/{INDEX}", now - DAY, now)),
                          ("cross_check", lambda: cc.list_builds(self.srv.url, f"bucket/{INDEX}", int(now - DAY)))):
            with self.subTest(name):
                frl.bind_context(frl.RunContext(frl.AdaptiveLimit(8, 8)))
                self.assertEqual(ids(), [b])
                self.assertEqual(frl.build_folder(self.srv.url, f"bucket/{INDEX}", b),
                                 f"bucket/pr-logs/pull/9/pull-job/{b}")

    def test_a_periodic_job_s_under_its_query_without_a_request(self):
        self.assertEqual(frl.build_folder(self.srv.url, "bucket/logs/job", "5"), "bucket/logs/job/5")
        self.assertEqual(self.srv.httpd.hits, {})

    def test_a_pull_request_job_s_where_its_entry_links_once_a_run(self):
        self.entry("5", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job/5"})
        for _ in range(2):
            self.assertEqual(frl.build_folder(self.srv.url, f"bucket/{INDEX}", "5"), "bucket/pr-logs/pull/9/pull-job/5")
        self.assertEqual(self.srv.hits(entry_path("5")), 1)

    def test_under_the_index_without_an_entry_or_with_one_that_links_nothing(self):
        # (TestGrid lists a folder under the index as a build too)
        self.entry("6", {})
        self.srv.gcs_listing("bucket", [f"{INDEX}/5/started.json", f"{INDEX}/6/started.json"])
        for build in ("5", "6"):
            with self.subTest(build=build):
                self.assertEqual(frl.build_folder(self.srv.url, f"bucket/{INDEX}", build), f"bucket/{INDEX}/{build}")

    def test_not_one_with_neither_an_entry_nor_a_folder(self):
        # (TestGrid takes such a build from another entry's link, as x.txt's, which only a listing finds)
        self.entry("6", {})
        self.srv.gcs_listing("bucket", [f"{INDEX}/x.txt"])
        for build in ("5", "6"):
            with self.subTest(build=build), self.assertRaises(frl.IntegrityError):
                frl.build_folder(self.srv.url, f"bucket/{INDEX}", build)
        del self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]  # (nor in a bucket GCS lists nothing of)
        with self.assertRaises(frl.IntegrityError):
            frl.build_folder(self.srv.url, f"bucket/{INDEX}", "7")

    def test_not_one_that_links_another_build(self):
        # (TestGrid takes the build from the link: that column is not this build's)
        self.entry("5", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job/7"})
        with self.assertRaises(frl.IntegrityError):
            frl.build_folder(self.srv.url, f"bucket/{INDEX}", "5")

    def test_not_one_this_archive_does_not_read_nor_one_testgrid_cannot_list_once_a_run(self):
        self.entry("5", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job//5"})
        self.entry("6", {"x-goog-meta-link": "gs://bucket:1/pr-logs/pull/9/pull-job/6"})
        for build in ("5", "6", "5", "6"):
            with self.subTest(build=build), self.assertRaises(frl.IntegrityError):
                frl.build_folder(self.srv.url, f"bucket/{INDEX}", build)
        self.assertEqual((self.srv.hits(entry_path("5")), self.srv.hits(entry_path("6"))), (1, 1))

    def test_a_listing_of_the_index(self):
        now = time.time()
        a, b, c, d = (str(snowflake(now - h * HOUR)) for h in (4, 3, 2, 1))
        page = {"prefixes": [f"{INDEX}/{c}/"],  # (a folder under the index is a build too, as for TestGrid)
                "items": [{"name": f"{INDEX}/{a}.txt", "metadata": {"x-goog-meta-link": f"gs://bucket/pr-logs/pull/9/pull-job/{a}"}},
                          {"name": f"{INDEX}/{b}.txt", "metadata": {}},  # (links nothing: not a build)
                          {"name": f"{INDEX}/x.txt", "metadata": {"link": f"gs://bucket/pr-logs/pull/batch/pull-job/{b}"}},
                          {"name": f"{INDEX}/{d}.txt", "metadata": {"link": f"gs://bucket/pr-logs/pull/9/pull-job//{d}"}}]}
        self.assertEqual(frl.listed_builds(page, f"{INDEX}/", f"bucket/{INDEX}"),
                         {c: f"bucket/{INDEX}/{c}", a: f"bucket/pr-logs/pull/9/pull-job/{a}",
                          b: f"bucket/pr-logs/pull/batch/pull-job/{b}", d: None})  # (d: a folder not read here)
        self.assertEqual(frl.listed_builds(page, "logs/job/", "bucket/logs/job"), {})  # (not an index: folders only)

    def test_the_index_listed_for_a_gap(self):
        now = time.time()
        a, b, c = (str(snowflake(now - h * HOUR)) for h in (3, 2, 1))
        listed = {"prefixes": [f"{INDEX}/{b}/"],
                  "items": [{"name": f"{INDEX}/{a}.txt", "metadata": {"x-goog-meta-link": f"gs://bucket/pr-logs/pull/9/pull-job/{a}"}},
                            {"name": f"{INDEX}/{c}.txt", "metadata": {"x-goog-meta-link": f"gs://bucket/pr-logs/pull/9//{c}"}}]}
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing(listed)
        self.assertEqual(frl.list_build_ids(self.srv.url, f"bucket/{INDEX}", now - DAY, now), [b, a, c])
        self.assertEqual([frl.build_folder(self.srv.url, f"bucket/{INDEX}", x) for x in (a, b)],
                         [f"bucket/pr-logs/pull/9/pull-job/{a}", f"bucket/{INDEX}/{b}"])
        with self.assertRaises(frl.IntegrityError):  # (listed, as by TestGrid: an error of that build)
            frl.build_folder(self.srv.url, f"bucket/{INDEX}", c)
        self.assertEqual([self.srv.hits(entry_path(x)) for x in (a, b, c)], [0, 0, 0])  # (the listing said where)

    def test_a_listing_with_a_link_testgrid_cannot_list_fails(self):  # (TestGrid's listing fails on it too)
        listed = {"items": [{"name": f"{INDEX}/1.txt", "metadata": {"x-goog-meta-link": "https://bucket/1"}}]}
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing(listed)
        with self.assertRaises(frl.IntegrityError):
            frl.list_build_ids(self.srv.url, f"bucket/{INDEX}", 0, time.time())

    def test_a_build_s_files_are_read_where_its_entry_links(self):
        # (for the reads of the job's other builds and of the build itself, its junit files too)
        b = str(snowflake(time.time() - HOUR))
        folder = f"pr-logs/pull/9/pull-job/{b}"
        self.entry(b, {"x-goog-meta-link": f"gs://bucket/{folder}"})
        self.srv.route(f"/bucket/{folder}/started.json", (200, {}, {"timestamp": int(time.time() - HOUR)}))
        self.srv.route(f"/bucket/{folder}/artifacts/junit_01.xml", (200, {}, b"<testsuite/>"))
        self.srv.gcs_listing("bucket", [f"{folder}/artifacts/junit_01.xml"])
        started = frl.Timeline(self.srv.url, f"bucket/{INDEX}").files(b)[0]
        self.assertIsNotNone(started)  # fails: None, read from the index's path
        read = frl.read_build(self.srv.url, f"bucket/{INDEX}", b, 0, time.time())
        self.assertEqual((read["started"], [j[4] for j in read["junits"]]), (started, [f"{folder}/artifacts/junit_01.xml"]))

    def test_control_a_build_under_the_index_is_read_there(self):
        # (as before: a folder under the index without an entry, for the reads of the job's other builds and of
        # the build itself)
        b = str(snowflake(time.time() - HOUR))
        self.srv.route(f"/bucket/{INDEX}/{b}/started.json", (200, {}, {"timestamp": 1}))
        self.srv.gcs_listing("bucket", [])  # (its junit files)
        self.assertEqual(frl.Timeline(self.srv.url, f"bucket/{INDEX}").files(b)[0], b'{"timestamp": 1}')
        read = frl.read_build(self.srv.url, f"bucket/{INDEX}", b, 0, time.time())
        self.assertEqual((read["started"], read["finished"], read["podinfo"]), (b'{"timestamp": 1}', None, None))

    def test_cross_check_reads_finished_json_where_the_entry_links(self):
        self.entry("6", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job/6"})
        self.srv.route("/bucket/pr-logs/pull/9/pull-job/6/finished.json", (200, {}, {"result": "FAILURE"}))
        self.assertEqual(cc.finished_json(self.srv.url, f"bucket/{INDEX}", "6"), {"result": "FAILURE"})
        self.entry("7", {"x-goog-meta-link": "gs://bucket/pr-logs/pull/9/pull-job//7"})
        self.assertIsInstance(cc.finished_json(self.srv.url, f"bucket/{INDEX}", "7"), cc.Unreachable)


class PullRequestJob(r4.World):
    """Tab t is TestGrid's group of a pull request job, pull-job: Prow's index lists its builds, each
    linking its folder under pr-logs/pull/9/."""

    def setUp(self):
        super().setUp()
        self.key = f"pull-job/{self.build}"
        self.folder = self.of(self.build)
        self.base = "/" + self.folder
        self.show([self.build], {"pull-job.Overall": ([12], ["boom"])})
        self.entry(self.build)

    def of(self, build):
        return f"bucket/pr-logs/pull/9/pull-job/{build}"

    def show(self, builds, rows, start=None):
        self.srv.table("dash", "t", table(f"bucket/{INDEX}", builds, rows, start=(start or self.start) * 1000))

    def entry(self, build):
        self.srv.route(entry_path(build), (200, {}, {"metadata": {"x-goog-meta-link": f"gs://{self.of(build)}"}}))

    def index_listing(self, builds, objects=()):
        """GCS's listing of the bucket: Prow's index of pull-job with an entry per build of `builds`
        (by name, from startOffset to endOffset, as GCS lists), any other prefix as gcs_listing does."""
        self.srv.gcs_listing("bucket", list(objects))
        other = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def serve(h):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            if q.get("prefix") != [f"{INDEX}/"]:
                return other(h)
            lo, hi = q.get("startOffset", [""])[0], q.get("endOffset", [None])[0]
            names = sorted(f"{INDEX}/{b}.txt" for b in builds)
            items = [{"name": n, "metadata": {"x-goog-meta-link": f"gs://{self.of(n[len(INDEX) + 1:-4])}"}}
                     for n in names if n >= lo and (hi is None or n < hi)]
            listing({"items": items})(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve

    def bdir(self, build=None):
        found = glob.glob(os.path.join(self.root, "runs", "*", "*", "pull-job", build or self.build))
        return found[0] if found else None

    def failed_in_gcs(self, b, ago):
        self.srv.route(f"/{self.of(b)}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now - ago + HOUR)}))
        self.srv.route(f"/{self.of(b)}/started.json", (200, {}, {"timestamp": int(self.now - ago)}))
        self.srv.route(f"/{self.of(b)}/build-log.txt", obj(b"log of " + b.encode()))
        self.entry(b)

    def test_a_red_build_is_archived_from_the_folder_its_entry_links(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived": [self.key]})  # fails: archived-without-log
        m = self.meta()
        self.assertEqual((m["gcs_path"], m["prow_url"], m["log_url"], self.read("build-log.txt")),
                         (self.folder, f"https://prow.k8s.io/view/gs/{self.folder}",
                          f"{self.srv.url}/{self.folder}/build-log.txt", b"log\n"))
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_later_runs_read_it_where_its_record_says(self):
        # (even once the index has no entry for it: its log, replaced in GCS, is fetched again from there)
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("build-log.txt"), b"log\n")  # fails: no log (read from the index's path)
        del self.srv.httpd.routes[entry_path(self.build)]
        self.srv.route(self.base + "/build-log.txt", obj(b"log, replaced\n", "2"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.read("build-log.txt")),
                         ({"refreshed": [self.key]}, b"log, replaced\n"))

    def test_a_damaged_copy_is_fetched_again_where_its_record_says(self):
        # (even once the index has no entry for it)
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("build-log.txt"), b"log\n")  # fails: no log (read from the index's path)
        del self.srv.httpd.routes[entry_path(self.build)]
        with open(os.path.join(self.bdir(), "build-log.txt"), "wb") as f:
            f.write(b"lo")  # (no longer the size meta.json records)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.read("build-log.txt")), ({"repaired": [self.key]}, b"log\n"))

    def archived_from_under_the_index(self):
        """As code before archived a pull request job's red builds: from a folder under the index (here
        one with only a started.json), the index not read."""
        del self.srv.httpd.routes[entry_path(self.build)]
        self.srv.route(f"/bucket/{INDEX}/{self.build}/started.json", (200, {}, {"timestamp": self.start}))
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived-without-log": [self.key]})
        self.assertEqual(self.meta()["log_url"], f"{self.srv.url}/bucket/{INDEX}/{self.build}/build-log.txt")
        self.assertTrue(self.meta()["note"].startswith("GCS had neither finished.json nor build-log.txt "))

    def test_one_whose_entry_cannot_be_read_is_one_not_compared(self):
        # (its copy is kept, and the run says it did not compare all it was due to)
        self.archived_from_under_the_index()
        self.srv.route(entry_path(self.build), (403, {}, b""))
        self.assertEqual(self.fetch(), 1)  # fails: 0, compared under the index
        run = self.runs()[-1]
        self.assertEqual((run["errors"], run["gcs_compared"]),
                         ([{"build": self.key, "error": "comparing the archived copy with GCS failed: HTTPError: "
                                                         "HTTP Error 403: Forbidden"}], False))
        self.assertNotIn(self.key, self.state().get("unresolved", {}))
        self.assertIsNone(self.meta()["log"])

    def test_a_record_s_folder_that_is_not_the_build_s_is_not_read(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("build-log.txt"), b"log\n")  # fails: no log (read from the index's path)
        for i, path in enumerate((f"bucket/pr-logs/pull/9/pull-job/{int(self.build) + 1}", "bucket/../x/" + self.build, 7,
                                  f"bucket/pr-logs/pull/9//{self.build}")):
            with self.subTest(path=path):
                frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), gcs_path=path))
                # (its log, replaced in GCS where the index links it, is fetched again from there)
                self.srv.route(self.base + "/build-log.txt", obj(b"log %d\n" % i, str(i + 2)))
                self.assertEqual(self.fetch(), 0)
                self.assertEqual((self.runs()[-1]["outcomes"], self.read("build-log.txt")),
                                 ({"refreshed": [self.key]}, b"log %d\n" % i))

    def test_one_in_another_bucket_is_read_there_by_later_runs(self):
        other = f"other/pr-logs/pull/9/pull-job/{self.build}"
        self.srv.route(entry_path(self.build), (200, {}, {"metadata": {"x-goog-meta-link": f"gs://{other}"}}))
        self.srv.route(f"/{other}/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(self.now - 3 * HOUR)}))
        self.srv.route(f"/{other}/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("gcs_path"), other)  # fails: None, read from the index's path
        del self.srv.httpd.routes[entry_path(self.build)]
        self.srv.route(f"/{other}/build-log.txt", obj(b"log, replaced\n", "2"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.read("build-log.txt")),
                         ({"refreshed": [self.key]}, b"log, replaced\n"))

    def test_its_folder_and_links_are_kept_when_the_comparison_then_fails(self):
        self.archived_from_under_the_index()
        self.entry(self.build)
        self.srv.route(self.base + "/finished.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 1)  # fails: 0, compared under the index
        self.assertIn("comparing the archived copy with GCS failed", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual((self.meta().get("gcs_path"), self.meta()["log_url"]),
                         (self.folder, f"{self.srv.url}/{self.folder}/build-log.txt"))

    def test_one_archived_from_under_the_index_is_read_where_its_entry_links_once_there_is_one(self):
        # (once its entry is read, meta.json says where its files are, and so do its links)
        self.archived_from_under_the_index()
        self.entry(self.build)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"log-recovered": [self.key]})  # fails: already-archived
        m = self.meta()
        self.assertEqual((m.get("gcs_path"), m["prow_url"], m["log_url"]),
                         (self.folder, f"https://prow.k8s.io/view/gs/{self.folder}",
                          f"{self.srv.url}/{self.folder}/build-log.txt"))
        self.assertNotIn("note", m)  # (it said GCS had nothing in the folder under the index)
        self.srv.route(self.base + "/build-log.txt", obj(b"log, replaced\n", "2"))  # (compared where it is)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"{self.key} build-log.txt: GCS changed since it was archived", out)

    def test_control_one_read_under_the_index_that_gcs_no_longer_has_is_compared_as_gone(self):
        # (as one of a periodic job's: GCS deletes old builds; the copy stays, and the run compares all
        # it was due to, every run after)
        del self.srv.httpd.routes[entry_path(self.build)]
        under = f"/bucket/{INDEX}/{self.build}"
        self.srv.route(under + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(self.now - 3 * HOUR)}))
        self.srv.route(under + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(("gcs_path" in self.meta(), self.read("build-log.txt")), (False, b"log\n"))
        for path in [p for p in self.srv.httpd.routes if p.startswith(under + "/")]:
            del self.srv.httpd.routes[path]
        for _ in range(2):
            self.assertEqual(self.fetch(), 0)
            run = self.runs()[-1]
            self.assertEqual((run["outcomes"], run["errors"], run["gcs_compared"]),
                             ({"already-archived": [self.key]}, [], True))
        self.assertEqual(self.read("build-log.txt"), b"log\n")

    def test_one_whose_entry_then_links_a_folder_this_archive_does_not_read_is_not_compared(self):
        # (not one GCS no longer has: the run says it did not compare all it was due to, every time)
        del self.srv.httpd.routes[entry_path(self.build)]
        under = f"/bucket/{INDEX}/{self.build}"
        self.srv.route(under + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(self.now - 3 * DAY)}))
        self.srv.route(under + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.srv.route(entry_path(self.build),
                       (200, {}, {"metadata": {"x-goog-meta-link": f"gs://bucket/pr-logs/pull/9//pull-job/{self.build}"}}))
        with mock.patch.object(frl, "gcs_sevenths", return_value=set(range(7))):  # (final: compared in its seventh)
            self.assertEqual(self.fetch(), 1)  # fails: 0
        run = self.runs()[-1]
        self.assertEqual(run["gcs_compared"], False)
        self.assertIn("links a folder this archive does not read", run["errors"][0]["error"])

    def test_a_watched_one_found_red_is_read_where_its_entry_links(self):
        # (still running when a run searched GCS for a gap, then found red by the next run)
        self.earlier_run(self.now - 20 * DAY)
        self.put_state(tabs={})
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"pull-job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        b = snowflake(self.now - 3 * HOUR)
        self.failed_in_gcs(b, 3 * HOUR)
        finished = self.srv.httpd.routes.pop(f"/{self.of(b)}/finished.json")  # (still running)
        self.srv.route(f"/{self.of(visible)}/finished.json",
                       (200, {}, {"result": "SUCCESS", "timestamp": int(self.now - HOUR)}))
        self.entry(visible)
        self.index_listing([b, visible], [f"pr-logs/pull/9/pull-job/{x}/started.json" for x in (b, visible)])
        self.assertEqual(self.fetch(), 0)
        self.assertIn(f"pull-job/{b}", self.state()["watch"])
        self.srv.route(f"/{self.of(b)}/finished.json", finished)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"].get("backfilled"), [f"pull-job/{b}"])
        m = self.meta(b)
        self.assertEqual((m.get("gcs_path"), m["prow_url"], m["log_url"]),
                         (self.of(b), f"https://prow.k8s.io/view/gs/{self.of(b)}",
                          f"{self.srv.url}/{self.of(b)}/build-log.txt"))

    def test_a_red_build_in_a_history_gap_is_taken_from_gcs(self):
        # (TestGrid shows one passing build from an hour ago; the index links builds that failed 30 h
        # and 5 days ago, judged by TestGrid's rule from the folders it links)
        self.earlier_run(self.now - 20 * DAY)
        self.put_state(tabs={})
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"pull-job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        red30h, red5d = snowflake(self.now - 30 * HOUR), snowflake(self.now - 5 * DAY)
        for b, ago in ((red30h, 30 * HOUR), (red5d, 5 * DAY)):
            self.failed_in_gcs(b, ago)
        self.srv.route(f"/{self.of(visible)}/finished.json", (200, {}, {"result": "SUCCESS", "timestamp": int(self.now - HOUR)}))
        self.entry(visible)
        self.index_listing([red30h, red5d, visible],
                           [f"pr-logs/pull/9/pull-job/{b}/finished.json" for b in (red30h, red5d, visible)])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(sorted(self.runs()[-1]["outcomes"]["backfilled"]),
                         sorted([f"pull-job/{red30h}", f"pull-job/{red5d}"]))  # fails: none
        self.assertEqual(self.meta(red30h)["gcs_path"], self.of(red30h))
        self.srv.httpd.hits.clear()
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertEqual([p for p in self.srv.httpd.hits if p.startswith("/storage/v1/b/bucket/o/")], [])  # (as listed)

    def test_cross_check_lists_the_index_for_ones_testgrid_no_longer_shows(self):
        # (one taken from GCS, then lost: cross_check.py finds it in the index, red by TestGrid's rule)
        self.test_a_red_build_in_a_history_gap_is_taken_from_gcs()
        lost = sorted(self.runs()[-1]["outcomes"]["backfilled"])[0].split("/")[1]
        shutil.rmtree(self.bdir(lost))
        code, out = self.check()
        self.assertEqual(code, 1, out)
        self.assertTrue(any(f"pull-job/{lost}" in line for line in out.splitlines() if "PROBLEM" in line), out)

    def test_cross_check_reads_when_a_lost_one_finished_where_its_entry_links(self):
        # (one that finished after the run's settle time is not one the run missed)
        self.test_a_red_build_in_a_history_gap_is_taken_from_gcs()
        lost = sorted(self.runs()[-1]["outcomes"]["backfilled"])[0].split("/")[1]
        shutil.rmtree(self.bdir(lost))
        self.srv.route(f"/{self.of(lost)}/finished.json",
                       (200, {}, {"result": "FAILURE", "passed": False, "timestamp": int(self.now + HOUR)}))
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_cross_check_reads_a_missed_one_where_its_entry_links(self):
        # (one TestGrid shows red, not archived: finished.json is read from its folder)
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        shutil.rmtree(self.bdir())
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"MISSED red build {self.key} (dash#t), finished", out)  # fails: finished.json missing


class PeriodicJob(r4.World):
    def test_control_its_builds_are_read_under_its_query(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        for _ in range(2):  # (archived, then compared with GCS)
            self.assertEqual(self.fetch(), 0)
        m = self.meta()
        self.assertEqual(("gcs_path" in m, m["log_url"], m["prow_url"]),
                         (False, f"{self.srv.url}/bucket/logs/job/{self.build}/build-log.txt",
                          f"https://prow.k8s.io/view/gs/bucket/logs/job/{self.build}"))
        self.assertEqual([p for p in self.srv.httpd.hits if p.startswith("/storage/v1/b/bucket/o/")], [])


if __name__ == "__main__":
    unittest.main()
