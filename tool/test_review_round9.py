"""Regression tests for the round-9 review: TestGrid's reads of a build's column follow its
updater (a column is read again while it runs, while its build is less than 20 minutes
old, and while an older column of its job runs; the build is red if a column of any read
is), and the review's mutation-testing gaps. The tests of what changed failed before it;
the others (an older build that finished, one that started later, ...) pin its bounds."""
import concurrent.futures as cf
import contextlib
import email.utils
import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import urllib.parse
from unittest import mock

import urllib3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import goregexp  # noqa: E402
import test_review_round4 as r4  # noqa: E402  (module imports: their tests are not collected again here)
import test_review_round7 as r7  # noqa: E402
import test_review_round8 as r8  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, send  # noqa: E402
from test_fetch_red_logs import table as ttc_table  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

MINUTE, GAP, DEADLINE = 60, frl.READ_GAP, frl.DEADLINE
POD_OK = {"pod": {"status": {"phase": "Succeeded"}}}
POD_FAILED = {"pod": {"status": {"phase": "Failed",  # (a failed sidecar fails the Pod row)
                                 "containerStatuses": [{"name": "sidecar", "state": {"terminated": {"exitCode": 1}}}]}}}
FAILING = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'
PASSING = b'<testsuite><testcase name="t"/></testsuite>'
NO_OVERALL = lambda row: row != "job.Overall"  # noqa: E731  (a tab that hides the job's Overall row)


def written(t):
    return {"Last-Modified": email.utils.formatdate(t, usegmt=True)}


class JobWorld(unittest.TestCase):
    """A job's builds in GCS, each file with when it was written."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        frl.BACKOFF = 0
        self.now = time.time()
        self.objects, self.times = [], {}
        self.srv.gcs_listing("bucket", [])

    def build(self, made_ago, started=MINUTE, finished=None, passed=True, podinfo=None, pod=POD_OK, junits=()):
        """A build made made_ago seconds ago; started, finished, podinfo: when its file was
        written, in seconds after it was made (None: there is none; started.json says when it
        was written, finished.json too); junits: (name, seconds after it was made, xml)."""
        b = snowflake(self.now - made_ago)
        made = frl.created(b)
        base = f"/bucket/logs/job/{b}"
        if started is not None:
            self.srv.route(f"{base}/started.json", (200, {}, {"timestamp": int(made + started)}))
        if finished is not None:
            self.srv.route(f"{base}/finished.json", (200, written(made + finished), {
                "timestamp": int(made + finished), "passed": passed, "result": "SUCCESS" if passed else "FAILURE"}))
        if podinfo is not None:
            self.srv.route(f"{base}/podinfo.json", (200, written(made + podinfo), pod))
        for name, after, xml in junits:
            path = f"logs/job/{b}/artifacts/{name}"
            self.srv.route(f"/bucket/{path}", (200, {}, xml))
            self.times[path] = made + after
        self.objects.append(f"logs/job/{b}/prowjob.json")  # (so the job's listing has the build)
        self.srv.gcs_listing("bucket", self.objects, written=self.times)
        return b

    def column(self, b, days=15):
        return frl.gcs_column(self.srv.url, "bucket/logs/job", b, days, self.now)

    def judged(self, b, days=15, shows=None):
        return frl.verdict(self.column(b, days), "job", shows=shows)[0]

    def reads(self, b, days=15):
        col = self.column(b, days)
        return (col, *col.alternatives)


class ReadAgainWhileThePodRowWaits(JobWorld):
    """A build that finished soon after its 24-hour deadline: TestGrid's first read after the
    deadline may come after finished.json, while its Pod row waits for podinfo.json; it then
    reads the column again until the Pod row stops waiting. The port took that read as the
    last, undecided for good (review W3a, N087)."""

    def test_a_podinfo_written_after_the_reads_is_not_in_them(self):
        b = self.build(40 * HOUR, finished=25 * HOUR, podinfo=30 * HOUR, pod=POD_FAILED)
        col = self.column(b)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job", shows=NO_OVERALL)[0], "not-red")
        self.assertTrue(any(c.finished_seen for c in (col, *col.alternatives)))

    def test_the_reads_end_one_gap_after_the_pod_row_stops_waiting(self):
        # (it stops waiting an hour after finished.json's time: 26 h after the build was made)
        for after, want in ((26 * HOUR + GAP - 5 * MINUTE, "red"), (26 * HOUR + GAP + 5 * MINUTE, "not-red")):
            b = self.build(40 * HOUR, finished=25 * HOUR, podinfo=after, pod=POD_FAILED)
            self.assertEqual(self.judged(b, shows=NO_OVERALL), want, after / HOUR)


class StillRead(JobWorld):
    """A build TestGrid may still read after now (its reads of it are not over) is not decided
    unless red: an object written before that read is in it. The port decided a finished build
    from GCS as it was."""

    def test_a_build_read_less_than_a_gap_ago_is_undecided(self):
        b = self.build(3 * HOUR, finished=2 * HOUR, podinfo=2 * HOUR + MINUTE)
        col = self.column(b)
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")

    def test_a_build_read_for_the_last_time_more_than_a_gap_ago_is_decided(self):
        b = self.build(6 * HOUR, finished=2 * HOUR, podinfo=2 * HOUR + MINUTE)
        self.assertFalse(self.column(b).open)
        self.assertEqual(self.judged(b), "not-red")

    def test_red_is_decided_at_once(self):
        b = self.build(3 * HOUR, finished=2 * HOUR, passed=False, podinfo=2 * HOUR + MINUTE)
        col = self.column(b)
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "red")

    def test_a_build_too_old_to_show_is_not_read_again(self):
        b = self.build(25 * HOUR, finished=24 * HOUR, podinfo=24 * HOUR + MINUTE)
        self.assertTrue(self.column(b, days=15).open)
        self.assertFalse(self.column(b, days=1).open)  # (too old 24 h 20 min after it started)


class ReadAgainWhileNew(JobWorld):
    """An update that begins less than 20 minutes after a build started reads its column again
    (InflateGrid's reprocess time), within the update's 20 minutes; a short build's reads go on
    until a gap after its column stops running."""

    def test_a_junit_file_written_while_a_short_build_is_still_read_is_in_it(self):
        # (its Pod row waits for podinfo.json until 6 min after it was made: the read after that
        # may come a gap later)
        for after, want in ((6 * MINUTE + GAP - 5 * MINUTE, "red"), (6 * MINUTE + GAP + 5 * MINUTE, "not-red")):
            b = self.build(8 * HOUR, finished=5 * MINUTE, podinfo=6 * MINUTE, junits=[("junit_01.xml", after, FAILING)])
            self.assertEqual(self.judged(b), want, after / MINUTE)

    def test_the_reprocess_reads_end_an_update_after_its_reprocess_time(self):
        # (its files there from the start, saying it started and finished 3 h after it was made: its
        # column never runs, and its first read may come a gap after it was made, but an update that
        # begins less than 20 min after its start reads it again, within its span: until 40 min
        # after its start; review W4a)
        start = 3 * HOUR
        for after, want in ((start + 40 * MINUTE - 2 * MINUTE, "red"), (start + 40 * MINUTE + 2 * MINUTE, "not-red")):
            b = self.build(10 * HOUR, junits=[("junit_01.xml", after, FAILING)])
            made = frl.created(b)
            for name, body in (("started.json", {"timestamp": int(made + start)}), ("podinfo.json", POD_OK),
                               ("finished.json", {"timestamp": int(made + start), "passed": True, "result": "SUCCESS"})):
                self.srv.route(f"/bucket/logs/job/{b}/{name}", (200, {}, body))
            self.assertEqual(self.judged(b), want, after / MINUTE)


class ReadAgainWhileAnOlderColumnRuns(JobWorld):
    """An update reads again every column from the oldest that runs (truncateRunning): while an
    older build of the job runs, a newer one's reads go on, and a junit file written then is in
    them."""

    def late_junit(self, ago):
        """A build made 28 h ago that finished in an hour, with a failing junit file written `ago`."""
        made = frl.created(snowflake(self.now - 28 * HOUR))
        return self.build(28 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                          junits=[("junit_01.xml", self.now - ago - made, FAILING)])

    def test_a_hung_older_build_keeps_a_newer_ones_column_read(self):
        self.build(30 * HOUR)  # never finished: running until 24 h after it started (6 h ago)
        b = self.late_junit(7 * HOUR)
        self.assertEqual(self.judged(b), "red")

    def test_without_it_the_newer_ones_reads_end_with_its_own(self):
        b = self.late_junit(7 * HOUR)
        self.assertEqual(self.judged(b), "not-red")

    def test_an_older_build_that_finished_holds_nothing(self):
        self.build(30 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        b = self.late_junit(7 * HOUR)
        self.assertEqual(self.judged(b), "not-red")

    def test_the_reads_end_one_gap_after_the_older_one_stops_running(self):
        self.build(30 * HOUR)  # running until 24 h after it started, a minute after it was made
        stops = frl.created(snowflake(self.now - 30 * HOUR)) + MINUTE + DEADLINE
        for ago, want in ((self.now - stops - GAP + 5 * MINUTE, "red"), (self.now - stops - GAP - 5 * MINUTE, "not-red")):
            b = self.late_junit(ago)
            self.assertEqual(self.judged(b), want, ago / HOUR)

    def test_older_is_by_start_not_by_id(self):
        # made before the judged build, but started after it (its pod pending for hours): newer
        self.build(29 * HOUR, started=3 * HOUR)
        b = self.late_junit(7 * HOUR)
        self.assertEqual(self.judged(b), "not-red")

    def test_one_that_started_at_the_same_time_with_a_lower_id_is_newer(self):
        # (an update reads builds by ID and sorts their columns by start, stably: of the same
        # start, the lower ID's column is the newer, so it holds nothing; review W4a)
        made = frl.created(snowflake(self.now - 28 * HOUR))
        self.build(29 * HOUR, started=made + MINUTE - frl.created(snowflake(self.now - 29 * HOUR)))
        b = self.late_junit(7 * HOUR)
        self.assertEqual(self.judged(b), "not-red")

    def test_a_still_running_older_build_leaves_it_open(self):
        self.build(10 * HOUR)  # running until 14 h from now
        b = self.build(8 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        col = self.column(b)
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")


class ReadAgainAfterABuildWithoutAStart(JobWorld):
    """A build without started.json has a running column TestGrid puts right after the column it
    read before it in the same update: while older columns are read again with it, it holds the
    newer columns' reads one update longer."""

    def setUp(self):
        super().setUp()
        self.older = self.build(30 * HOUR, finished=HOUR, podinfo=2 * HOUR)  # running (its Pod row) until 2 h
        self.made = frl.created(self.older)

    def judged_with_junit(self, ago, after):
        """A build made `ago` (after the older one), its failing junit file written `after` the older one was made."""
        made = frl.created(snowflake(self.now - ago))
        b = self.build(ago, finished=10 * MINUTE, podinfo=11 * MINUTE,
                       junits=[("junit_01.xml", self.made + after - made, FAILING)])
        return self.judged(b)

    def test_it_holds_the_reads_of_newer_builds(self):
        self.build(29 * HOUR, started=None, finished=MINUTE)  # appeared while the older one was read again
        self.assertEqual(self.judged_with_junit(28 * HOUR, 2 * HOUR + 2 * GAP - 10 * MINUTE), "red")
        self.assertEqual(self.judged_with_junit(28 * HOUR, 2 * HOUR + 2 * GAP + 10 * MINUTE), "not-red")

    def test_without_it_they_end_a_gap_earlier(self):
        self.assertEqual(self.judged_with_junit(28 * HOUR, 2 * HOUR + 2 * GAP - 10 * MINUTE), "not-red")

    def test_one_that_appeared_before_the_older_ones_last_read_holds_newer_reads(self):
        # (made 2.5 h after the older one, half an hour after its column stopped running in GCS:
        # TestGrid may read the older one done only a gap later, in the update that reads this one
        # first; review W4a. The judged build, made 3 h after the older one, is read until a gap
        # after 3 h 11 min on its own, but until 2 h + 2 gaps as this one holds it)
        self.build(27 * HOUR + 30 * MINUTE, started=None, finished=MINUTE)
        self.assertEqual(self.judged_with_junit(27 * HOUR, 2 * HOUR + 2 * GAP - 10 * MINUTE), "red")

    def test_one_that_appeared_after_the_older_ones_last_read_holds_nothing(self):
        # (the older one's last read may come a gap after its column stopped running, 2 h after it
        # was made; one made after that is read first in its update, at the epoch; the judged
        # build, made later still, is read until a gap after its column stopped running)
        self.build(30 * HOUR - 2 * HOUR - GAP - 10 * MINUTE, started=None, finished=MINUTE)
        ago = 30 * HOUR - 2 * HOUR - GAP - 20 * MINUTE
        own = frl.created(snowflake(self.now - ago)) - self.made + 11 * MINUTE + GAP  # (its own reads' end)
        self.assertEqual(self.judged_with_junit(ago, own - 10 * MINUTE), "red")
        self.assertEqual(self.judged_with_junit(ago, own + 10 * MINUTE), "not-red")


class RedAtAnyRead(JobWorld):
    """A column stays on its tabs until the next read of it: the build is red if the column of
    any read is, though a later read is not."""

    def test_a_failure_a_later_junit_file_merges_into_a_flake_was_red(self):
        b = self.build(10 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 30 * MINUTE, FAILING), ("junit_02.xml", 40 * MINUTE, PASSING)])
        col = self.column(b)
        self.assertEqual(col.cells["t"], "FLAKY")
        self.assertEqual(frl.verdict(col, "job"), ("red", [{"test": "t", "status": "FAIL", "message": None}]))

    def test_a_fail_read_again_after_the_build_finished_was_red(self):
        # An older build finished 10 minutes before its first read after its deadline could come:
        # read then, its Pod row waits an hour, and the newer columns are read until a gap after.
        self.build(40 * HOUR, finished=MINUTE + DEADLINE + GAP - 10 * MINUTE)
        # finished after its own reads after its deadline (until 24 h 21 min + GAP after it was
        # made), while the older build's hold its column's reads
        b = self.build(39 * HOUR, finished=27 * HOUR, podinfo=27 * HOUR + MINUTE)
        reads = self.reads(b)
        self.assertTrue(any(c.finished_seen and c.cells["job.Overall"] == "PASS" for c in reads))
        self.assertEqual(self.judged(b), "red")  # (its deadline's FAIL)


class Reads(unittest.TestCase):
    """possible_reads, runs and the junit parse cache."""

    @staticmethod
    def col(running=False, errors=(), results=("PASS",), finished_seen=False):
        c = tr.Column(list(errors), [], set(results), None if errors else {"job.Overall": "RUNNING" if running else "PASS"})
        c.finished_seen = finished_seen
        return c

    def test_one_read_within_a_gap_after_the_build_is_made(self):
        reads, reach, running = frl.possible_reads(lambda t: self.col(), 1000.0, [], [], 1000.0 + 5 * GAP)
        self.assertEqual((reach, running), (1000.0 + GAP, -float("inf")))
        self.assertEqual([t for t, _, _, _ in reads], [1000.0, 1000.0])  # (the moment it is made, then the stretch after)
        self.assertEqual([until for _, until, _, _ in reads], [float("inf")] * 2)  # (not read again: shown from then on)
        reads, _, _ = frl.possible_reads(lambda t: self.col(running=t < 3000.0), 1000.0, [3000.0], [], 1000.0 + 5 * GAP)
        self.assertEqual([(t, until, end) for t, until, _, end in reads],  # (one that runs is replaced within a gap)
                         [(1000.0, 1000.0 + GAP, 1000.0), (1000.0, 3000.0 + GAP, 3000.0), (3000.0, float("inf"), 3000.0),
                          (3000.0, float("inf"), 3000.0 + GAP)])  # (the last within a gap of the last that ran)

    def test_reads_go_on_a_gap_after_the_column_stops_running(self):
        view = lambda t: self.col(running=t < 5000)  # noqa: E731
        _, reach, running = frl.possible_reads(view, 1000.0, [5000.0], [], 1000.0 + 10 * GAP)
        self.assertEqual((reach, running), (5000.0 + GAP, 5000.0))

    def test_a_column_running_now_may_run_on(self):
        _, reach, running = frl.possible_reads(lambda t: self.col(running=True), 1000.0, [], [], 1000.0 + 10 * GAP)
        self.assertGreater(reach, 1000.0 + 10 * GAP)
        self.assertEqual(running, float("inf"))

    def test_a_reason_to_read_again_until_a_time(self):
        _, reach, _ = frl.possible_reads(lambda t: self.col(), 1000.0, [], [(1000.0, 7000.0)], 1000.0 + 10 * GAP)
        self.assertEqual(reach, 7000.0 + GAP)
        # (and a hold that begins later, after the build's own reads: read then, and a gap after it)
        reads, reach, _ = frl.possible_reads(lambda t: self.col(), 1000.0, [], [(20000.0, 21000.0)], 1000.0 + 10 * GAP)
        self.assertEqual(reach, 21000.0 + GAP)
        self.assertFalse([t for t, _, _, _ in reads if 1000.0 + GAP < t < 20000.0])

    def test_no_read_after_now(self):
        seen = []
        frl.possible_reads(lambda t: seen.append(t) or self.col(running=True), 1000.0, [3000.0], [], 2000.0)
        self.assertLessEqual(max(seen), 2000.0)

    def test_what_runs(self):
        self.assertTrue(frl.runs(self.col(running=True)))
        self.assertFalse(frl.runs(self.col()))
        self.assertTrue(frl.runs(self.col(errors=["nostart"], results=["RUNNING"])))
        self.assertFalse(frl.runs(self.col(errors=["nostart"], results=["RUNNING"], finished_seen=True)))
        self.assertFalse(frl.runs(self.col(errors=["ancient"], results=["UNKNOWN"])))
        self.assertFalse(frl.runs(self.col(errors=["podinfo"], results=["TOOL_FAIL"])))

    def test_each_junit_file_is_parsed_once(self):
        world = JobWorld("run")
        world.setUp()
        self.addCleanup(world.doCleanups)
        b = world.build(10 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                        junits=[(f"junit_0{i}.xml", i * MINUTE, FAILING) for i in range(1, 6)])
        with mock.patch.object(tr, "parse_junit", wraps=tr.parse_junit) as parse:
            col = world.column(b)
        self.assertGreater(len(col.alternatives), 2)
        self.assertEqual(parse.call_count, 5)


class TimelineReads(JobWorld):
    """The job's other builds are read once per run, however many of its builds are judged."""

    def test_each_build_is_read_once(self):
        older = [self.build(h * HOUR, finished=HOUR, podinfo=HOUR + MINUTE) for h in (30, 29, 28)]
        judged = [self.build(h * HOUR, finished=HOUR, podinfo=HOUR + MINUTE) for h in (20, 19, 18, 17)]
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        with cf.ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda b: frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now, timeline), judged * 3))
        for b in older:
            self.assertEqual(self.srv.hits(f"/bucket/logs/job/{b}/started.json"), 1, b)

    def test_priming_reads_them_in_the_pool(self):
        older = self.build(30 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        b = self.build(20 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        timelines = frl.Timelines(self.now)
        with cf.ThreadPoolExecutor(4) as pool:
            timelines.prime(pool, self.srv.url, [("bucket/logs/job", b)])
            deadline = time.monotonic() + 30
            while self.srv.hits(f"/bucket/logs/job/{older}/podinfo.json") == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{older}/podinfo.json"), 1)

    def test_builds_that_cannot_be_listed_leave_a_shown_build_open(self):
        b = self.build(20 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        listing = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def serve(h):  # (the build's own folder can be listed; the job's builds cannot)
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            return send(h, 503, {}, b"") if "delimiter" in q else listing(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        self.assertEqual(self.judged(b), "undecided")
        self.assertEqual(self.judged(b, days=0.5), "not-red")  # (too old to show: its reads are over)

    def test_a_job_folder_gcs_has_no_bucket_for_has_no_builds(self):
        b = self.build(20 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        listing = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def serve(h):
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            return send(h, 404, {}, b"") if "delimiter" in q else listing(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        self.assertEqual(self.judged(b), "not-red")


class MismatchRecordOrder(unittest.TestCase):
    def test_a_mismatch_that_closes_before_it_opens_is_dropped(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        now = time.time()
        frl.write_json(os.path.join(d, "state.json"), {"tab_configs": {"dash#t": {"mismatch": {
            "detail": "d", "since": now - HOUR, "until": now - 2 * HOUR}}}})
        warnings = []
        state = frl.load_state(d, warnings)
        self.assertNotIn("dash#t", state["tab_configs"])
        self.assertIn("dropped a malformed config mismatch record", " ".join(warnings))



# ------------------------------------------------------------ the review's mutation-testing gaps (W3a)

class Interim(unittest.TestCase):
    def setUp(self):
        frl.BACKOFF = 0
        self.ctx = frl.RunContext(frl.AdaptiveLimit(4, 4))
        frl.bind_context(self.ctx)
        self.addCleanup(frl.bind_context, frl._DEFAULT_CONTEXT)

    def test_100_continue_counts_as_an_interim_response(self):  # F07
        def script(n, s, read_request):
            while read_request() is not None:
                s.sendall(b"HTTP/1.1 100 Continue\r\n\r\n" * (frl.MAX_INTERIM + 1) + r7.response(200, b"x"))
        srv = r7.RawServer(script)
        self.addCleanup(srv.close)
        with self.assertRaisesRegex(urllib3.exceptions.ProtocolError, "interim responses"):
            frl.fetch_once(f"{srv.url}/x", frl.read_body, None, 10, "GET")


class Reads40Hours(r8.LastReadWorld):
    """Reads of a build that started 40 hours ago (round 8's world), at their edges."""

    def route_build(self, started_ago):
        start = int(self.now - started_ago)
        b = snowflake(start)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": start}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (404, {}, b""))
        return start, b

    def fin(self, b, timestamp, written, passed=True):
        body = {"timestamp": timestamp, "passed": passed, "result": "SUCCESS" if passed else "FAILURE"}
        if timestamp is None:
            del body["timestamp"]
        self.srv.route(f"/bucket/logs/job/{b}/finished.json", (200, {"Last-Modified": email.utils.formatdate(
            written, usegmt=True)}, body))

    def verdict_of(self, b, days=15, hide_overall=False):
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, days, self.now)
        return frl.verdict(col, "job", shows=NO_OVERALL if hide_overall else None)[0]

    def test_a_podinfo_written_after_the_pod_wait_while_reads_go_on_is_in_them(self):  # N050
        self.finished(2 * HOUR, passed=True)
        self.podinfo(2 * HOUR + 2 * HOUR + 55 * 60, failed=True)
        self.assertEqual(self.judged(15)[0], "red")

    def test_a_finished_json_without_its_time_counts_as_there_from_the_start(self):  # N034
        self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                       (200, {}, {"timestamp": self.start + 30 * HOUR, "passed": True, "result": "SUCCESS"}))
        self.junit(30 * HOUR)  # (written after its reads, which end soon after the start)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertEqual(frl.verdict(col, "job", shows=NO_OVERALL)[0], "not-red")

    def test_finished_at_the_deadline_may_be_painted_failed(self):  # N035, review W4a
        # (an update reads the files, then makes the column up to its 20 minutes later and compares
        # the deadline then: finished.json written in the 20 minutes before the deadline may be
        # missed by a read whose column is made after it)
        for written, want in ((24 * HOUR - 60, "red"), (24 * HOUR - frl.UPDATE_SPAN + 2, "red"),
                              (24 * HOUR - frl.UPDATE_SPAN, "not-red")):
            self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                           (200, {"Last-Modified": email.utils.formatdate(self.start + written, usegmt=True)},
                            {"timestamp": self.start + written - 60, "passed": True, "result": "SUCCESS"}))
            self.podinfo(written)
            self.assertEqual(self.judged(15)[0], want, written)

    def test_a_build_finished_minutes_ago_waits_for_its_podinfo(self):  # N048
        start, b = self.route_build(2 * HOUR)
        self.fin(b, int(self.now - 30 * 60), int(self.now - 30 * 60))
        self.assertEqual(self.verdict_of(b), "undecided")

    def test_a_build_started_long_before_it_was_made_is_too_old(self):  # N047
        made = int(self.now - 30 * HOUR)
        b = snowflake(made)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": made - 2 * 86400}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (404, {}, b""))
        self.assertEqual(self.verdict_of(b, days=1), "not-red")

    def test_finished_after_its_deadline_minutes_ago_still_waits_for_podinfo(self):  # N086, N102
        start, b = self.route_build(25 * HOUR)
        self.fin(b, start + 24 * HOUR + 1800, start + 24 * HOUR + 1800)
        self.assertEqual(self.verdict_of(b, hide_overall=True), "undecided")

    def test_a_finished_json_without_its_timestamp_is_not_a_crash(self):  # N039
        start, b = self.route_build(2 * HOUR)
        self.fin(b, None, start + HOUR, passed=False)
        self.assertEqual(self.verdict_of(b), "undecided")

    def test_a_finished_build_without_podinfo_is_decided_after_the_pod_wait_and_a_gap(self):  # N040
        start, b = self.route_build(5 * HOUR)
        self.fin(b, start + HOUR, start + HOUR)
        self.assertEqual(self.verdict_of(b), "not-red")

    def test_a_podinfo_written_soon_after_finished_json_is_in_the_reads(self):  # N043
        start, b = self.route_build(5 * HOUR)
        self.fin(b, start + HOUR, start + HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(start + HOUR + 600, usegmt=True)}, POD_OK))
        self.assertEqual(self.verdict_of(b), "not-red")

    def test_the_reads_after_the_deadline_end_a_gap_after_it(self):  # N053, N054, N055
        for after, want in ((24 * HOUR + GAP - 10 * 60 + 1, "red"), (24 * HOUR + GAP + 10 * 60 + 1, "not-red")):
            self.junit(after)  # (a never-finished build: its deadline read may come until 24 h + GAP)
            col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
            self.assertEqual(frl.verdict(col, "job", shows=NO_OVERALL)[0], want, after / HOUR)

    def test_a_one_day_tabs_read_before_the_build_was_too_old_was_red(self):  # N069
        # finished.json (failed, 23 h 50 min) written at 23 h 55 min: read then it is FAIL, not too
        # old yet; read after its Pod row stopped waiting it is too old, but the FAIL was shown
        start, b = self.route_build(40 * HOUR)
        self.fin(b, start + 23 * HOUR + 50 * 60, start + 23 * HOUR + 55 * 60, passed=False)
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(start + 24 * HOUR + 40 * 60, usegmt=True)}, POD_OK))
        self.assertEqual(self.verdict_of(b, days=1), "red")

    def bad_podinfo(self, written_after):
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + written_after, usegmt=True)}, b"{"))

    def test_a_podinfo_unreadable_only_after_the_deadline_reads_is_not_in_them(self):  # N370, N374
        self.bad_podinfo(30 * HOUR)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertEqual(frl.verdict(col, "job", shows=NO_OVERALL)[0], "not-red")

    def test_a_junit_written_hours_after_the_last_read_is_not_in_it(self):  # N371
        self.finished(10 * HOUR, passed=True)
        self.bad_podinfo(30 * HOUR)
        self.junit(15 * HOUR)
        self.assertEqual(self.judged(15)[0], "not-red")

    def test_a_build_without_a_start_is_judged_by_its_reads(self):  # N089
        self.srv.route(f"/bucket/logs/job/{self.build}/started.json", (200, {}, {}))
        self.finished(2 * HOUR, passed=True)
        self.podinfo(4 * HOUR)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")

    def test_a_junit_written_after_the_reads_after_it_finished_is_not_in_them(self):  # N041
        self.finished(2 * HOUR, passed=True)  # (podinfo.json without Last-Modified: there from the start)
        self.junit(2 * HOUR + GAP + 10 * 60)
        self.assertEqual(self.judged(15)[0], "not-red")

    def test_a_finished_json_uploaded_late_is_in_the_reads(self):  # N044
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json", (404, {}, b""))
        self.srv.route(f"/bucket/logs/job/{self.build}/finished.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + 3 * HOUR, usegmt=True)},
                        {"timestamp": self.start + HOUR, "passed": True, "result": "SUCCESS"}))
        self.assertEqual(self.judged(15)[0], "not-red")

    def test_a_junit_file_that_cannot_be_read_leaves_the_build_undecided(self):  # N074
        self.finished(2 * HOUR, passed=True)
        one, two = (f"logs/job/{self.build}/artifacts/junit_0{i}.xml" for i in (1, 2))
        self.srv.route("/bucket/" + one, (200, {}, b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'))
        self.srv.route("/bucket/" + two, (503, {}, b""))
        self.srv.gcs_listing("bucket", [one, two])
        self.assertEqual(self.judged(15)[0], "undecided")

    def test_junits_are_listed_when_a_read_could_read_the_build(self):  # N070
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + 30 * HOUR, usegmt=True)}, b"{"))
        self.junit(20 * HOUR)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.build, 15, self.now)
        self.assertEqual(frl.verdict(col, "job", shows=NO_OVERALL)[0], "red")

    def test_a_build_without_a_start_read_after_its_podinfo_could_not_be_is_red(self):  # N033
        self.srv.route(f"/bucket/logs/job/{self.build}/started.json", (200, {}, {}))
        self.finished(3 * HOUR, passed=False)
        self.srv.route(f"/bucket/logs/job/{self.build}/podinfo.json",
                       (200, {"Last-Modified": email.utils.formatdate(self.start + 3 * HOUR + 600, usegmt=True)}, b"{"))
        self.assertEqual(self.judged(15)[0], "red")  # (TestGrid could not read its podinfo.json: TOOL_FAIL)


class EntryTypes(unittest.TestCase):
    def test_each_group_field_of_the_wrong_type_is_refused(self):  # T137a, N35x
        wrong = {"name": 5, "gcs_prefix": 5, "days_of_results": "15", "use_kubernetes_client": "yes",
                 "result_source": [], "disable_merged_status": 1, "disable_prowjob_analysis": "x", "ignore_skip": "yes",
                 "custom_evaluator_rule_set": [], "test_name_config": "x", "build_override_strftime": 1,
                 "build_override_configuration_value": 1, "ignore_old_results": "no"}
        self.assertEqual(set(wrong), set(tgc.FIELD_TYPES))
        for field, value in wrong.items():
            self.assertFalse(tgc.valid_entry(ttc.entry("", **{field: value})), field)

    def test_a_config_whose_commit_time_is_not_a_number_is_a_value_error(self):  # T142
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        for committed in ("x", None, [1]):
            with open(path, "w") as f:
                json.dump({"format": tgc.FORMAT, "after": 1, "runs": [], "group_changes": {}, "failed": {},
                           "configs": {ttc.recipe_of("a" * 40): {"commit": "a" * 40, "committed": committed, "image": "i",
                                                                 "dashboards": {}}}}, f)
            with self.assertRaises(ValueError, msg=committed):
                tgc.load(path)

    def test_an_escape_with_a_sign_or_space_is_what_go_rejects(self):  # T02
        for options in ("include-filter-by-regex=%+2", "include-filter-by-regex=% 2"):
            self.assertIn("cannot parse", tgc.TabRule(ttc.entry(options)).broken or "", options)


class TimeParsing(unittest.TestCase):
    def test_implausible_last_modified_is_none(self):  # N011
        self.assertIsNone(frl.last_modified({"Last-Modified": "Thu, 01 Jan 1970 00:00:00 GMT"}))
        self.assertIsNone(frl.last_modified({"Last-Modified": "Fri, 01 Jan 2999 00:00:00 GMT"}))

    def test_implausible_or_overlong_time_created_is_none(self):  # N017, N019
        self.assertIsNone(frl.rfc3339_time("1970-01-01T00:00:00Z"))
        self.assertIsNone(frl.rfc3339_time("2999-01-01T00:00:00Z"))
        self.assertIsNone(frl.rfc3339_time("2026-10-11T12:34:56." + "1" * 30 + "+00:00"))
        self.assertIsNotNone(frl.rfc3339_time("2026-10-11T12:34:56.789Z"))

    def test_the_listing_asks_for_time_created(self):  # N016
        urls = []
        with mock.patch.object(frl, "get_json", side_effect=lambda url: urls.append(url) or {"items": []}):
            frl.list_objects("http://x", "bucket", "logs/job/1/")
        self.assertIn("timeCreated", urllib.parse.parse_qs(urllib.parse.urlsplit(urls[0]).query)["fields"][0])


class Filters(unittest.TestCase):
    def test_a_matching_search_is_spent_too(self):  # N312
        rule = tgc.TabRule(ttc.entry("include-filter-by-regex=a"))
        rule.budget = tgc.Budget(0)
        with self.assertRaises(tgc.FilterTooSlow):
            rule.shows("a")

    def test_a_doomed_search_stops_within_64_characters(self):  # N307, N308, N309
        r = tgc.go_regexp("a")
        seen = []
        real = goregexp.is_word
        with mock.patch.object(goregexp, "is_word", side_effect=lambda c: seen.append(c) or real(c)), \
                mock.patch.object(goregexp.time, "thread_time", side_effect=iter(range(0, 10_000, 1)).__next__):
            with self.assertRaises(tgc.FilterTooSlow):
                r.search("b" * 1000, tgc.Budget(0.5))
        self.assertLess(len(seen), 200)  # (two per position it looks at)

    def test_the_cache_holds_what_it_counts(self):  # N333
        r = tgc.go_regexp("[a-z].{20}~").matcher
        with mock.patch.object(goregexp, "CACHE_ELEMENTS", 500):
            for i in range(50):
                r.search("abcdefghijklmnopqrstuvwxyz" * 3 + str(i))
                held = sum(len(k[0]) + len(v[1]) + 1 for k, v in r.cache.items())
                self.assertLessEqual(held, 500 + 2 * len(r.states) + 1)

    def test_the_cache_is_emptied_by_entries(self):  # N332, N335
        r = tgc.go_regexp("x").matcher
        with mock.patch.object(goregexp, "CACHE_ENTRIES", 10):
            for i in range(30):
                r.search(chr(0x100 + i) * 3)
                self.assertLessEqual(len(r.cache), 10)


class ConfiguredRun(ttc.ConfigWorld):
    """Runs with TestGrid's config (test_testgrid_config's world)."""

    def two_tabs(self, t_entry, t2_entry, t2_shows=()):
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        builds = [self.visible, *t2_shows]
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", builds,
                                               {"job.Pod": ([1] * len(builds), [""] * len(builds)),
                                                "kubetest.Up": ([1] * len(builds), [""] * len(builds))},
                                               start=int(self.now - HOUR) * 1000))
        return self.config_of({"dash": {"t": t_entry, "t2": t2_entry}})

    def passing_build(self, ago):
        w = snowflake(self.now - ago)
        self.srv.route(f"/bucket/logs/job/{w}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - ago + HOUR)}))
        self.srv.route(f"/bucket/logs/job/{w}/started.json", (200, {}, {"timestamp": int(self.now - ago)}))
        self.srv.route(f"/bucket/logs/job/{w}/podinfo.json", (200, {}, POD_OK))
        return w

    def watch(self, w, tabs):
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": t, "tab_status": "FAILING", "red_cells": []}
                                          for t in tabs])
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#t2": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 2 * HOUR}})

    def test_a_build_shown_on_a_held_tab_is_judged_only_on_the_others(self):  # N145
        w = self.passing_build(5 * HOUR)
        path = self.two_tabs(ttc.entry(""), ttc.entry("", build_override_configuration_value="x"), t2_shows=[w])
        self.watch(w, ["t", "t2"])
        self.fetch_with(path)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{w}"]})

    def test_a_held_tab_after_another_holds_the_build(self):  # N123
        w = self.passing_build(5 * HOUR)
        path = self.two_tabs(ttc.entry("", build_override_configuration_value="x"), ttc.entry(""))
        self.watch(w, ["t2", "t"])
        self.fetch_with(path)
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{w}"]})

    def test_a_tab_with_a_group_problem_still_has_its_mismatch_recorded(self):  # F40a
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))
        self.fetch_with(self.config(self.HIDE, build_override_configuration_value="x"))
        self.assertIn("does not match the tab", " ".join(self.runs()[-1]["serious_warnings"]))
        self.assertIsNone(self.state()["tab_configs"]["dash#t"]["mismatch"]["until"])

    def test_a_tab_configs_record_that_is_not_an_object_is_warned(self):  # N160
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}}, tab_configs=[])
        self.fetch()
        self.assertIn("tab_configs is not an object", " ".join(self.runs()[-1]["warnings"]))

    def test_filters_out_of_time_while_judging_fall_back_to_the_rows(self):  # N106, N107
        w = snowflake(self.now - 5 * DAY)
        self.failed_in_gcs(w, 5 * DAY)
        self.srv.route(f"/bucket/logs/job/{w}/artifacts/junit_01.xml", (200, {}, r8.KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, w)])
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "gone", "tab_status": "FAILING", "red_cells": []}])
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 2 * DAY, "oldest": self.now - 9 * DAY,
                                           "missing_since": self.now - DAY}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 5 * DAY}})
        with mock.patch.object(tgc, "FILTER_SECONDS", 0.0):
            self.fetch_with(self.config_of({"dash": {"t": ttc.entry(""), "gone": ttc.entry(self.HIDE)}}))
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{w}"]})

    def test_a_shown_build_stays_watched_while_another_tab_of_its_job_is_unread(self):  # N148
        w = snowflake(self.now - 2 * HOUR)
        self.srv.route(f"/bucket/logs/job/{w}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - HOUR)}))
        self.srv.route(f"/bucket/logs/job/{w}/started.json", (200, {}, {"timestamp": int(self.now - 2 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{w}/podinfo.json", (200, {}, POD_OK))
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        self.show([self.visible, w], {"job.Pod": ([1, 1], ["", ""]), "kubetest.Up": ([1, 1], ["", ""])},
                  start=int(self.now - HOUR))
        self.srv.table("dash", "t2", (404, {}, b""))
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#t2": {"scan": self.now - HOUR, "oldest": self.now - 3 * HOUR, "query": "bucket/logs/job"}},
                       watch={f"job/{w}": {"rec": self.watched(w), "first_seen": self.now - 2 * HOUR}})
        self.fetch_with(self.config(""))
        self.assertEqual(self.runs()[-1]["outcomes"], {"watching": [f"job/{w}"]})

    def test_each_part_of_a_mismatch_record_is_checked(self):  # N165, N166, N167, N168
        good = {"detail": "d", "since": self.now - DAY, "until": None}
        for bad in (dict(good, extra=1), dict(good, detail=5), dict(good, since="x"), dict(good, until="x")):
            self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                           tab_configs={"dash#t": {"config": ttc.entry(""), "read": self.now - HOUR, "mismatch": bad}})
            self.fetch()
            self.assertIn("dropped a malformed config mismatch record", " ".join(self.runs()[-1]["warnings"]), bad)

    def test_while_a_tab_disagrees_a_build_red_on_a_row_its_config_shows_is_red(self):  # N111
        self.junit(self.b, b'<testsuite name="kubetest"><testcase name="Test"/><testcase name="Up"/></testsuite>')
        self.assertEqual(self.fetch_with(self.config("", gcs_prefix="bucket/logs/other")), 1)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def held(self, ago):
        b = snowflake(self.now - ago)
        self.failed_in_gcs(b, ago)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, b)])
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - ago + HOUR}})
        return b

    def test_held_for_sixty_days(self):  # N120, N121
        for days, outcome in ((59.5, "held"), (60.5, "stopped-holding")):
            b = self.held(days * DAY)
            self.fetch_with(self.config("", build_override_configuration_value="x"))
            self.assertEqual(self.runs()[-1]["outcomes"], {outcome: [f"job/{b}"]}, days)

    def test_a_held_build_still_shown_running_is_held_for_sixty_days(self):  # N128, N130
        for days, outcome in ((20, "held"), (61, "stopped-holding")):
            b = snowflake(self.now - days * DAY)
            start = int(self.now - days * DAY)
            self.srv.gcs_listing("bucket", [])
            self.show([b], {"job.Overall": ([4], [""])}, start=start)  # TestGrid still shows it running
            self.put_state(tabs={"dash#t": {"scan": self.now - HOUR / 4, "oldest": start}},
                           watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - days * DAY + HOUR}})
            self.fetch_with(self.config("", build_override_configuration_value="x"))
            self.assertEqual(self.runs()[-1]["outcomes"], {outcome: [f"job/{b}"]}, days)

    def test_a_closed_mismatch_keeps_the_time_it_closed(self):  # N212
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        path = self.config(self.HIDE)
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))
        self.fetch_with(path)  # (the record opens)
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Up": ([1], [""])}, start=int(self.now - HOUR))
        self.fetch_with(path)  # (it closes)
        # (moved ten hours back, for a build made after it closed whose reads by TestGrid are over)
        state = self.state()
        state["tab_configs"]["dash#t"]["mismatch"].update(since=self.now - 20 * HOUR, until=self.now - 10 * HOUR)
        y = snowflake(self.now - 8 * HOUR)
        self.failed_in_gcs(y, 8 * HOUR)
        self.srv.route(f"/bucket/logs/job/{y}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 7 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{y}/podinfo.json", (200, {}, POD_OK))
        self.srv.route(f"/bucket/logs/job/{y}/artifacts/junit_01.xml", (200, {}, r8.KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, y)])
        state["watch"] = {f"job/{y}": {"rec": self.watched(y), "first_seen": self.now - 7 * HOUR}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.fetch_with(path)  # (it agrees still: the record keeps the time it closed)
        self.assertEqual(self.runs()[-1]["outcomes"], {"not-red": [f"job/{y}"]})

    def test_a_closed_mismatch_is_kept_sixty_one_days(self):  # N215, N217
        for days, kept in ((60.5, True), (62, False)):
            record = {"detail": "d", "since": self.now - 70 * DAY, "until": self.now - days * DAY}
            self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                           tab_configs={"dash#t": {"config": ttc.entry(self.HIDE), "read": self.now - HOUR,
                                                   "mismatch": record}})
            self.fetch_with(self.config(self.HIDE))
            self.assertEqual("mismatch" in self.state()["tab_configs"]["dash#t"], kept, days)

    def gone_tab(self, read_ago, **parts):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 2 * DAY, "oldest": self.now - 9 * DAY,
                                           "missing_since": self.now - DAY}},
                       tab_configs={"dash#gone": {"config": ttc.entry(""), "read": self.now - read_ago}}, **parts)

    def test_an_old_config_of_a_tab_no_build_involves_is_not_stale(self):  # N240
        self.gone_tab(10 * DAY)
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("was last read", " ".join(self.runs()[-1]["warnings"]))

    def test_an_old_config_of_an_unlisted_tab_judging_a_build_is_stale(self):  # N241
        # (when no config is read now; one read now without the tab says the tab is gone)
        w = snowflake(self.now - 5 * DAY)
        self.failed_in_gcs(w, 5 * DAY)
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "gone", "tab_status": "FAILING", "red_cells": []}])
        self.gone_tab(10 * DAY, watch={f"job/{w}": {"rec": rec, "first_seen": self.now - 5 * DAY}})
        self.fetch_with(os.path.join(self.dir, "missing.json"))
        self.assertIn("was last read", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_a_config_without_a_read_time_is_stale(self):  # N242
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       tab_configs={"dash#t": {"config": ttc.entry(self.HIDE)}})
        self.fetch()
        self.assertIn("was last read at an unknown time", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_never_read_names_only_listed_tabs(self):  # N248
        w = snowflake(self.now - 2 * DAY)
        self.failed_in_gcs(w, 2 * DAY)
        rec = dict(self.watched(w), tabs=[{"dashboard": "dash", "tab": "other", "tab_status": "FAILING", "red_cells": []}])
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       watch={f"job/{w}": {"rec": rec, "first_seen": self.now - DAY}})
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("never read", " ".join(self.runs()[-1]["warnings"]))

    def stalled(self):
        b = snowflake(self.now - 25 * HOUR)
        start = int(self.now - 25 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": start}))
        self.srv.gcs_listing("bucket", [])
        self.put_state(tabs={})
        self.show([b], {"job.Overall": ([4], [""])}, start=start)  # TestGrid still shows it running
        return b

    def test_a_stalled_builds_tab_without_config_is_warned(self):  # N261
        self.stalled()
        self.fetch()
        self.assertIn("no TestGrid config known for 1 tabs (dash#t)", " ".join(self.runs()[-1]["warnings"]))

    def test_a_still_shown_watched_builds_tab_without_config_is_warned(self):  # N259
        self.stalled()
        self.fetch()
        self.fetch()
        self.assertIn("no TestGrid config known for 1 tabs (dash#t)", " ".join(self.runs()[-1]["warnings"]))

    def test_a_red_build_retried_is_not_judged_without_config(self):  # N262
        b = snowflake(self.now - 2 * HOUR)
        rec = {k: v for k, v in self.watched(b).items() if k != "watch"}
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR / 4, "oldest": self.now - HOUR / 2}},  # (no gap)
                       unresolved={f"job/{b}": {"rec": rec, "first_seen": self.now - HOUR, "last_error": None}})
        self.fetch()
        self.assertNotIn("no TestGrid config known", " ".join(self.runs()[-1]["warnings"]))

    def test_the_config_of_a_tab_no_build_involves_is_not_warned(self):  # N249
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 2 * DAY, "oldest": self.now - 9 * DAY,
                                           "missing_since": self.now - DAY}},
                       tab_configs={"dash#gone": {"config": ttc.entry("", build_override_configuration_value="x"),
                                                  "read": self.now - HOUR}})
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("dash#gone", " ".join(self.runs()[-1]["warnings"]))

    def test_list_only_warns_about_the_config(self):  # N255
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       tab_configs={"dash#t": {"config": ttc.entry(self.HIDE), "read": self.now - 10 * DAY}})
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url, "--gcs", self.srv.url,
                      "--list-only"])
        self.assertIn("TestGrid's config was last read", err.getvalue())

    def forgotten(self, missing_days, **parts):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR},
                             "dash#gone": {"scan": self.now - 40 * DAY, "oldest": self.now - 45 * DAY,
                                           "missing_since": self.now - missing_days * DAY}},
                       tab_configs={"dash#gone": {"config": ttc.entry(""), "read": self.now - HOUR}}, **parts)

    def test_a_forgotten_tabs_config_is_dropped(self):  # N194
        self.forgotten(31)
        self.fetch_with(self.config(self.HIDE))
        self.assertNotIn("dash#gone", self.state()["tab_configs"])

    def test_a_forgotten_tabs_config_is_kept_for_a_build_still_tried(self):  # N193, N191
        b = snowflake(self.now - 2 * HOUR)
        rec = dict({k: v for k, v in self.watched(b).items() if k != "watch"},
                   tabs=[{"dashboard": "dash", "tab": "gone", "tab_status": "FAILING", "red_cells": []}])
        for kept, entry in (("unresolved", {"rec": rec, "first_seen": self.now - HOUR, "last_error": None}),
                            ("given_up", {"rec": rec, "first_seen": self.now - 15 * DAY, "last_error": "x"})):
            self.forgotten(31, **{kept: {f"job/{b}": entry}})
            self.fetch_with(self.config(self.HIDE))
            self.assertIn(f"job/{b}", self.state()[kept], kept)
            self.assertIn("dash#gone", self.state()["tab_configs"], kept)


class CrossChecked(ttc.ConfigWorld):
    """cross_check judges builds TestGrid no longer lists as the fetch does."""

    def without_history(self):
        state = self.state()
        for k in ("config_uploads", "config_entries"):
            state.pop(k, None)
        frl.write_json(os.path.join(self.root, "state.json"), state)

    def test_a_malformed_config_in_run_json_is_not_followed(self):  # C08
        self.earlier_run(self.now - 3 * DAY)
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        run = frl.read_json(run_path)
        for t in run["tabs"]:
            t["config"] = dict(t["config"], extra=1)  # (not an entry valid_entry accepts)
        frl.write_json(run_path, run)
        self.without_history()
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_cross_check_takes_the_longest_days_of_a_jobs_tabs(self):  # C05
        self.earlier_run(self.now - 3 * DAY)
        self.srv.route(f"/bucket/logs/job/{self.b}/started.json", (200, {}, {"timestamp": int(self.now - 30 * HOUR - 2 * DAY)}))
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", [self.visible], {"job.Pod": ([1], [""]),
                                                                                  "kubetest.Up": ([1], [""])},
                                               start=int(self.now - HOUR) * 1000))
        self.fetch_with(self.config_of({"dash": {"t": ttc.entry("", days_of_results=1),
                                                 "t2": ttc.entry("", days_of_results=15)}}))
        self.assertIsNotNone(self.bdir(self.b))
        shutil.rmtree(self.bdir(self.b))
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_cross_check_judges_a_tab_whose_filters_cannot_be_applied_by_its_rows(self):  # N386
        self.earlier_run(self.now - 3 * DAY)
        with mock.patch.object(tgc, "FILTER_SECONDS", 0.0):  # (its filters run out of time)
            self.fetch_with(self.config(self.HIDE))
            self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})
            shutil.rmtree(self.bdir(self.b))
            code, out = self.check()
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_a_tab_with_options_not_known_here_is_judged_by_its_filters(self):
        self.earlier_run(self.now - 3 * DAY)
        self.fetch_with(self.config(self.HIDE + "&mystery-option=1"))
        run_ = self.runs()[-1]
        self.assertNotIn("backfilled", run_["outcomes"])  # (kubetest.Test hidden by its filter)
        self.assertIn("sets base options not known here (mystery-option)", " ".join(run_["warnings"]))
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)

    def test_cross_check_judges_a_build_made_after_the_mismatch_closed_by_the_config(self):  # N384
        self.earlier_run(self.now - 3 * DAY)
        y = snowflake(self.now - 6 * HOUR)  # made after the tab agreed with its config again
        self.failed_in_gcs(y, 6 * HOUR)
        self.srv.route(f"/bucket/logs/job/{y}/artifacts/junit_01.xml", (200, {}, r8.KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b, y)])
        state = {"tabs": {"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                 "tab_configs": {"dash#t": {"config": ttc.entry(self.HIDE), "read": self.now - HOUR,
                                            "mismatch": {"detail": "d", "since": self.now - 50 * HOUR,
                                                         "until": self.now - 7 * HOUR}}}}
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.fetch_with(self.config(self.HIDE))
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})  # (b: made before it agreed)
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{y}", out)

    def test_an_unreadable_build_on_a_held_tab_cannot_be_checked(self):  # C07a
        self.earlier_run(self.now - 3 * DAY)
        self.show([self.visible], {"job.Overall": ([1], [""]), "job.Pod": ([1], [""])}, start=int(self.now - HOUR))
        self.fetch_with(self.config("", build_override_configuration_value="x"))
        saved = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def listing(h):  # (the build's own folder cannot be listed: its junit files cannot be read)
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            if q["prefix"][0] == f"logs/job/{self.b}/":
                return r4.send(h, 503, {}, b"")
            return saved(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = listing
        code, out = self.check()
        self.assertIn(f"cannot check job/{self.b} by TestGrid's rule", out)


if __name__ == "__main__":
    unittest.main()
