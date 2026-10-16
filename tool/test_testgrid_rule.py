"""testgrid_rule against TestGrid's own code: testdata/testgrid_rule_golden.jsonl.gz holds
what TestGrid's updater (tool/oracle, built from the commit TestGrid's production manifests
deploy) made of 590 builds, edge cases and random ones; each is replayed here. Then the
helpers, and gcs_column reading a build over HTTP as TestGrid's GCS client does."""
import base64
import gzip
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import FakeServer  # noqa: E402

GOLDEN = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "testgrid_rule_golden.jsonl.gz")
FOLDER = "logs/job/1/"  # where the oracle put each build


def b64(s):
    return None if s is None else base64.b64decode(s)


def replay(case):
    """The case as the oracle saw it: objects under FOLDER, junit read as TestGrid reads it."""
    objects = {}
    for name in ("started", "finished", "podinfo"):
        if case[name] is not None:
            data = b64(case[name])
            objects[FOLDER + name + ".json"] = (len(data), data)
    for a in case["artifacts"]:
        data = b64(a["data"]) or b""
        objects[FOLDER + a["name"]] = (a["size"] if a["size"] is not None else len(data), data)
    junits = []
    for listed, opened in tr.junit_reads(sorted(objects)):
        size, data = objects.get(opened, (None, None))
        junits.append((opened, size, data, listed))
    stop = case["stop"] + case.get("stop_ns", 0) // 1953125 / 512  # (fractions of 1/512 s: exact)
    options = tgc.cell_options(case["group"])[0] if case.get("group") is not None else None
    return tr.column("job", b64(case["started"]), b64(case["finished"]), b64(case["podinfo"]), junits,
                     stop, case["now"], folder=FOLDER, options=options)


class GoldenTest(unittest.TestCase):
    def test_every_build_as_testgrid_judged_it(self):
        with gzip.open(GOLDEN, "rt") as f:
            cases = [json.loads(line) for line in f]
        self.assertGreater(len(cases), 500)
        for case in cases:
            with self.subTest(case=case["id"]):
                col = replay(case)
                want = case["want"]
                self.assertEqual(col.errors, want["errors"])
                self.assertEqual(sorted(col.results), want["results"])
                self.assertEqual(len(col.malformed), want["malformed"])
                self.assertEqual(sorted(m for m in col.malformed if m.endswith(".json") and ":" not in m),
                                 want["malformed_json"])
                either = col.either  # (rows two splits name alike: Go's map order picks one)
                cells = {row: col.cells.get(row) if status in either.get(row, ()) else status
                         for row, status in (want["cells"] or {}).items()}
                self.assertEqual(col.cells or None, cells or None)

    def test_the_golden_cases_reach_every_outcome(self):
        """So a port that always answers the same could not pass the replay."""
        with gzip.open(GOLDEN, "rt") as f:
            cases = [json.loads(line) for line in f]
        results = {r for c in cases for r in c["want"]["results"]}
        self.assertTrue({"PASS", "FAIL", "RUNNING", "UNKNOWN", "TOOL_FAIL", "FLAKY"} <= results, results)
        cells = {s for c in cases for s in (c["want"]["cells"] or {}).values()}
        self.assertTrue({"PASS", "FAIL", "RUNNING", "FLAKY", "PASS_WITH_SKIPS"} <= cells, cells)
        self.assertTrue(any(c["want"]["malformed"] for c in cases))
        self.assertTrue(any("nostart" in c["want"]["errors"] for c in cases))
        self.assertTrue(any("ancient" in c["want"]["errors"] for c in cases))
        self.assertTrue(any(c["want"]["cells"] and c["want"]["cells"].get("job.Pod") == "FAIL" for c in cases))


class StrconvTest(unittest.TestCase):
    def test_parse_int(self):
        for s, want in ((b"0", 0), (b"-0", 0), (b"+5", 5), (b"9223372036854775807", (1 << 63) - 1),
                        (b"-9223372036854775808", -(1 << 63)), (b"9223372036854775808", None),
                        (b"18446744073709551616", None), (b"", None), (b"+", None), (b"1_0", None), (b"0x1", None),
                        (b" 1", None), ("١".encode(), None)):
            with self.subTest(s=s):
                self.assertEqual(tr.go_parse_int(s), want)
        self.assertEqual(tr.go_parse_int(b"2147483647", 32), 2147483647)
        self.assertIsNone(tr.go_parse_int(b"2147483648", 32))

    def test_parse_float(self):
        ok = [b"1", b"-1.5", b".5", b"1.", b"1e5", b"1E-5", b"inf", b"+Inf", b"-infinity", b"nan", b"NaN",
              b"0x1p3", b"0X1.8P1", b"0x.8p1", b"1_000.5", b"0x1_0p1", b"1e308", b"1e-400",
              b"1.7976931348623157e308", b"0x1.fffffffffffffp1023"]
        bad = [b"", b".", b"+", b"-", b"1e", b"1e+", b"e5", b"1,5", b"+nan", b"-nan", b"infin", b"infinityx",
               b"0x1", b"0x", b"0xp1", b"1__0", b"_1", b"1_", b"1_e5", b"1e400", b"2e308", b"0x1p99999",
               b"1.5.5", b" 1"]
        for s in ok:
            with self.subTest(s=s):
                self.assertTrue(tr.go_parse_float_ok(s))
        for s in bad:
            with self.subTest(s=s):
                self.assertFalse(tr.go_parse_float_ok(s))

    def test_trim_space(self):
        self.assertEqual(tr.go_trim_space(" \t1 　".encode()), b"1")
        self.assertEqual(tr.go_trim_space(b"\x1c1\x1f"), b"\x1c1\x1f")  # not space to Go
        self.assertEqual(tr.go_trim_space(b"\xe2 1"), b"\xe2 1")  # a bad sequence is not space


class TimeTest(unittest.TestCase):
    def test_rfc3339_as_time_parse_takes_it(self):
        ok = ["2026-10-11T01:02:03Z", "2026-10-11T01:02:03.5+08:00", "2024-02-29T00:00:00Z", "0000-01-01T00:00:00Z",
              # time.Parse's general parser takes these too (go.dev/issue/54580)
              "2026-10-11T1:02:03Z", "2026-10-11T01:02:03,5Z", "2026-10-11T01:02:03+24:00", "2026-10-11T01:02:03+23:60"]
        bad = ["2026-02-29T00:00:00Z", "2026-13-01T00:00:00Z", "2026-10-11 01:02:03Z", "2026-10-11T01:02:03z",
               "2026-10-11T24:00:00Z", "2026-10-11T01:02:60Z", "2026-10-11T01:02:03", "2026-10-11T01:02:03+25:00",
               "2026-10-11T01:02:03+23:61", "2026-10-11T01:02:03Zx", "", "1900-02-29T00:00:00Z"]
        for s in ok:
            with self.subTest(s=s):
                self.assertTrue(tr.go_rfc3339_ok(s))
        for s in bad:
            with self.subTest(s=s):
                self.assertFalse(tr.go_rfc3339_ok(s))


class QuantityTest(unittest.TestCase):
    def test_parse_quantity(self):
        # (a missing number is 0: "Ki" and "e5" are quantities, ".Pi" and "Ei" are not, as their
        # binary suffix leaves no precision for the fast path and the slow one needs a digit)
        ok = [b"1", b"0", b"100m", b"1Gi", b"1.5Gi", b"1Pi", b".Ki", b"3Ki", b"1e3", b"+5", b"-000.5", b".", b"-.",
              b"1.", b"123456789012345678901234567890", b"0.0001n", b"1e-10", b"7Ei", b"Ki", b"e5"]
        bad = [b"", b".Pi", b"Ei", b"-Pi", b"1Kix", b"1.5.5", b".e-10", b"\\u0031", b"1 Gi", b"x",
               b"--1", b"1Mi1", b"true"]
        for s in ok:
            with self.subTest(s=s):
                self.assertTrue(tr.quantity_ok(s))
        for s in bad:
            with self.subTest(s=s):
                self.assertFalse(tr.quantity_ok(s))


class JunitNamesTest(unittest.TestCase):
    def test_the_objects_testgrid_parses_and_the_one_it_opens(self):
        names = ["logs/job/1/junit.xml", "logs/job/1/artifacts/junit_01.xml", "logs/job/1/artifacts/junit/a.xml",
                 "logs/job/1/artifacts/test.xml", "logs/job/1/artifacts/testXxml", "logs/job/1/artifacts/./junit_2.xml",
                 "logs/job/1/artifacts/x/../junit_3.xml", "logs/job/1/artifacts/junit.XML",
                 "logs/job/1/artifacts/notjunit.xml", "logs/job/1/build-log.txt", "logs/job/1/junitx.xml"]
        self.assertEqual(tr.junit_reads(names), [
            ("logs/job/1/junit.xml", "logs/job/1/junit.xml"),
            ("logs/job/1/artifacts/junit_01.xml", "logs/job/1/artifacts/junit_01.xml"),
            ("logs/job/1/artifacts/junit/a.xml", "logs/job/1/artifacts/junit/a.xml"),
            ("logs/job/1/artifacts/test.xml", "logs/job/1/artifacts/test.xml"),
            ("logs/job/1/artifacts/testXxml", "logs/job/1/artifacts/testXxml"),
            ("logs/job/1/artifacts/./junit_2.xml", "logs/job/1/artifacts/junit_2.xml"),
            ("logs/job/1/artifacts/x/../junit_3.xml", "logs/job/1/artifacts/junit_3.xml"),
            ("logs/job/1/junitx.xml", "logs/job/1/junitx.xml")])

    def test_resolve(self):
        for name, want in (("a/b", "a/b"), ("a/./b", "a/b"), ("a/../b", "b"), ("../../a", "a"), ("a//b", "a//b"),
                           ("a/b/..", "a/"), ("a/.", "a/"), ("a//..", "a/")):
            with self.subTest(name=name):
                self.assertEqual(tr.resolve(name), want)


class VerdictTest(unittest.TestCase):
    """What a tab shows of a column: the rows it hides, and which columns are not decided."""

    def column(self, finished=None, started=b'{"timestamp": %d}', podinfo=None, junit=None, now=None):
        now = now or int(time.time())
        started = started % (now - 3600) if started and b"%d" in started else started
        junits = [("logs/job/1/junit.xml", len(junit), junit)] if junit is not None else []
        col = tr.column("job", started, finished, podinfo, junits, now - 15 * 86400, now)
        col.has_finished = finished is not None
        return col

    def test_the_rows_a_tab_hides_do_not_count(self):
        failed = b'{"timestamp": %d, "passed": false}' % (int(time.time()) - 7200)
        col = self.column(failed)
        self.assertEqual(frl.verdict(col, "job"), ("red", [{"test": "job.Overall", "status": "FAIL",
                                                           "message": "it did not pass; Prow result none"}]))
        self.assertEqual(frl.verdict(col, "job", {"overall": False, "pod": True})[0], "not-red")
        junit = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'
        col = self.column(failed, junit=junit)
        state, cells = frl.verdict(col, "job", {"overall": False, "pod": True})
        self.assertEqual((state, [c["test"] for c in cells]), ("red", ["t"]))

    def test_a_passed_build_can_be_red(self):
        passed = b'{"timestamp": %d, "passed": true}' % (int(time.time()) - 7200)
        self.assertEqual(frl.verdict(self.column(passed), "job")[0], "not-red")
        junit = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'
        self.assertEqual(frl.verdict(self.column(passed, junit=junit), "job")[0], "red")
        flaky = b'<testsuite><testcase name="t"><failure/></testcase><testcase name="t"/></testsuite>'
        self.assertEqual(frl.verdict(self.column(passed, junit=flaky), "job")[0], "not-red")  # FLAKY
        pod = json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [
            {"name": "sidecar", "state": {"terminated": {"exitCode": 2}}}]}}}).encode()
        state, cells = frl.verdict(self.column(passed, podinfo=pod), "job")
        self.assertEqual((state, cells[0]["test"], cells[0]["message"]), ("red", "job.Pod", "sidecar exited 2: "))
        self.assertEqual(frl.verdict(self.column(passed, podinfo=pod), "job", {"overall": True, "pod": False})[0],
                         "not-red")

    def test_undecided_until_testgrid_stops_reading_it(self):
        self.assertEqual(frl.verdict(self.column(None), "job")[0], "undecided")  # running
        # finished a minute ago: the Pod row waits (RUNNING) for podinfo.json up to an hour, and
        # TestGrid reads the column again until then, unless it is red already
        passed = b'{"timestamp": %d, "passed": true}' % (int(time.time()) - 60)
        self.assertEqual(frl.verdict(self.column(passed), "job")[0], "undecided")
        succeeded = json.dumps({"pod": {"status": {"phase": "Succeeded"}}}).encode()
        self.assertEqual(frl.verdict(self.column(passed, podinfo=succeeded), "job")[0], "not-red")
        failed_now = b'{"timestamp": %d, "passed": false}' % (int(time.time()) - 60)
        self.assertEqual(frl.verdict(self.column(failed_now), "job")[0], "red")
        self.assertEqual(frl.verdict(self.column(failed_now), "job", {"overall": False, "pod": True})[0], "undecided")
        self.assertEqual(frl.verdict(self.column(None, started=None), "job")[0], "undecided")  # not started
        failed = b'{"timestamp": %d, "passed": false}' % (int(time.time()) - 60)
        self.assertEqual(frl.verdict(self.column(failed, started=None), "job")[0], "not-red")  # never started
        self.assertEqual(frl.verdict(self.column(failed, started=b'{"timestamp": 5}'), "job")[0], "not-red")  # too old
        self.assertEqual(frl.verdict(self.column(b"{", started=None), "job")[0], "red")  # races to TOOL_FAIL

    def test_tab_rows_hide_only_what_a_table_shows_hidden(self):
        def table(*names):
            return {"tests": [{"name": n} for n in names]}
        self.assertEqual(frl.tab_rows(table("job.Overall", "job.Pod", "t"), "job"), {"overall": True, "pod": True})
        self.assertEqual(frl.tab_rows(table("job.Pod", "t"), "job"), {"overall": False, "pod": True})
        self.assertEqual(frl.tab_rows(table("job.Overall", "t"), "job"), {"overall": True, "pod": False})
        self.assertEqual(frl.tab_rows(table("t"), "job"), {"overall": True, "pod": True})  # no evidence either way
        self.assertEqual(frl.tab_rows(table(), "job"), {"overall": True, "pod": True})
        # columns with results, and neither row: the tab hides both (it shows only some tests)
        self.assertEqual(frl.tab_rows(table("t"), "job", results=True), {"overall": False, "pod": False})
        self.assertEqual(frl.tab_rows(table("job.Pod", "t"), "job", results=True), {"overall": False, "pod": True})

    def test_tab_days(self):
        now = 2_000_000_000
        self.assertEqual(frl.tab_days(now - 3 * 86400, now), 15)
        self.assertEqual(frl.tab_days(now - 59.5 * 86400, now), 60)
        self.assertEqual(frl.tab_days(None, now), 15)
        self.assertEqual(frl.tab_days(True, now), 15)


class GcsColumnTest(unittest.TestCase):
    """gcs_column reads what TestGrid's GCS client reads: decompressed objects, every
    junit file listed under the build, a plain one over 100e6 bytes as too large unread."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        frl.BACKOFF = 0
        self.build = "2109000000000000000"
        now = int(time.time())
        self.put("started.json", json.dumps({"timestamp": frl.created(self.build)}).encode())
        self.put("finished.json", json.dumps({"timestamp": now - 60, "passed": True}).encode())

    def put(self, name, body, headers=None):
        self.srv.route(f"/bucket/logs/job/{self.build}/{name}", (200, headers or {}, body))

    def column(self):
        self.srv.gcs_listing("bucket", [])
        return frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)

    def test_a_failing_test_in_a_listed_junit_file(self):
        self.put("artifacts/junit_01.xml", b'<testsuite><testcase name="t"><failure/></testcase></testsuite>')
        col = self.column()
        self.assertEqual((col.errors, col.red_rows()), ([], ["t"]))

    def test_a_gzipped_response_is_read_decompressed(self):
        self.put("artifacts/junit_01.xml", gzip.compress(b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'),
                 {"Content-Encoding": "gzip"})
        self.assertEqual(self.column().red_rows(), ["t"])

    def test_a_malformed_junit_file_fails_the_build(self):
        self.put("artifacts/junit_01.xml", b"<testsuite><testcase")
        col = self.column()
        self.assertEqual(col.results, {"FAIL"})
        self.assertTrue(col.notes["job.Overall"].startswith("malformed artifacts: artifacts/junit_01.xml: parse"),
                        col.notes)

    def test_a_junit_file_testgrid_cannot_open(self):
        # listed as artifacts/./junit_2.xml: TestGrid opens artifacts/junit_2.xml, which is not there
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/artifacts/./junit_2.xml"])
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual(col.results, {"FAIL"})

    def test_a_listing_of_several_pages_is_read_to_its_end(self):
        names = [f"logs/job/{self.build}/artifacts/a{i}.txt" for i in range(5)]
        self.put("artifacts/zz/junit_01.xml", b'<testsuite><testcase name="t"><failure/></testcase></testsuite>')
        self.srv.gcs_listing("bucket", names, page=2)  # the junit file is on the last of four pages
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual(col.red_rows(), ["t"])

    def test_sizes_as_testgrid_takes_them(self):
        failing = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'
        big = f"logs/job/{self.build}/artifacts/junit_01.xml"
        # stored gzipped: read whatever its stored size, as TestGrid reads it decompressed, of unknown length
        self.put("artifacts/junit_01.xml", gzip.compress(failing), {"Content-Encoding": "gzip"})
        self.srv.gcs_listing("bucket", [], sizes={big: tr.JUNIT_MAX_SIZE + 1})
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual((col.malformed, col.red_rows()), ([], ["t"]))
        # stored plainly and too large: malformed, and not read
        self.put("artifacts/junit_01.xml", failing)
        self.srv.gcs_listing("bucket", [], sizes={big: tr.JUNIT_MAX_SIZE + 1})
        hits = self.srv.hits(f"/bucket/{big}")
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual((col.malformed, col.results), (["artifacts/junit_01.xml: too large"], {"FAIL"}))
        self.assertEqual(self.srv.hits(f"/bucket/{big}"), hits)

    def test_an_unreadable_junit_file_holds_back_only_a_column_not_red_without_it(self):
        self.put("artifacts/junit_01.xml", b"")
        self.srv.route(f"/bucket/logs/job/{self.build}/artifacts/junit_01.xml", (403, {}, b"denied"))
        self.srv.gcs_listing("bucket", [f"logs/job/{self.build}/artifacts/junit_01.xml"])
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)  # passed an hour ago
        self.assertIn("HTTPError", col.partial)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")  # a failing test could make it red
        self.put("finished.json", json.dumps({"timestamp": int(time.time()) - 7200, "passed": False}).encode())
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # red without it
        self.assertEqual(frl.verdict(col, "job", {"overall": False, "pod": True})[0], "undecided")

    def test_unreadable_files_raise(self):
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json", (403, {}, b""))
        self.srv.gcs_listing("bucket", [])
        with self.assertRaises(frl.urllib.error.HTTPError):  # not taken as "no podinfo.json"
            frl.gcs_column(self.srv.url, "bucket/logs/job", self.build)


if __name__ == "__main__":
    unittest.main()
