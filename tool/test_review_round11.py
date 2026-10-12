"""Review round 11 (W4a): the timing model of TestGrid's reads (fetch_red_logs.possible_reads,
gcs_column, Timeline.older_end) against TestGrid's updater at the pinned commit. Each test
failed on 76191a2: each asserts what TestGrid's Go code does (cited per test).

Schedules named in the tests are valid under the updater's code with production's flags
(cluster/prod/updater.yaml: --wait=2h --group-timeout=20m --group-concurrency=10, pubsub on
kubernetes-ci-logs). "U1 at +1h55m" means an update of the job's test group whose
InflateDropAppend starts then; it reads its columns within 20 minutes.

Run: cd tool && python -m unittest test_w4a_findings -v
"""
import email.utils
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round9 as r9  # noqa: E402  (module import: its tests are not collected here)
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import FakeServer  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

MINUTE = 60
FAILING, POD_FAILED = r9.FAILING, r9.POD_FAILED


class World(r9.JobWorld):
    """JobWorld with builds placed by absolute time (seconds since the epoch)."""

    def at(self, made, **kw):
        """A build made at `made` (absolute); junits: [(name, absolute time written, xml)]."""
        b = snowflake(made)
        real = frl.created(b)
        kw["junits"] = [(n, t - real, x) for n, t, x in kw.get("junits", ())]
        return self.build(self.now - made, **kw)

    def failing_junit(self, written):
        return [("junit_01.xml", written, FAILING)]


# ---------------------------------------------------------------- F1 (missed red)
class NoStartColumnAfterAGoodColumnReadForAnyReason(World):
    """readColumns (read.go:173-209) places a column of a build without started.json at the
    Started of the good column read just before it in the same update (+0.01 ms); it then has a
    RUNNING Overall cell, so the next update's truncateRunning (updater.go:500-519) cuts at it and
    reads again every build listed after the older column's hint (read.go:58, hintStarted), which
    includes newer builds. That happens whenever the older good build G is read in the same update
    as N, for ANY reason: G new (its first read), or G read again once after it stopped running
    (its column was RUNNING in the grid). older_end() only counts G running (running_until) or in
    its 20-minute reprocess time when N was made (fetch_red_logs.py:1710-1714), so it misses
    those, and a newer build's later read."""

    def test_a_good_build_first_read_with_the_no_start_build_holds_newer_reads(self):
        a = self.now - 20 * HOUR
        self.at(a, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)            # G (quick)
        self.at(a + 30 * MINUTE, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE)  # N (crier wrote it)
        b = self.at(a + 40 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(a + 3 * HOUR + 30 * MINUTE))
        # TestGrid: update U1 at a+1h55m (2 h after one at a-5min) reads G, N, B, all new, in that
        # order: N is placed right after G and persists, RUNNING. U2 at a+3h55m cuts at N
        # (truncateRunning), lists every build after G's hint, reads N (now first: at the epoch)
        # and B again, with its failing junit file (written at a+3h30m): B is red from U2 on.
        # The port: B's reads end at a+3h21m (its reprocess time + 2h20m); G "held" only until
        # a+21m < N's ID time, so N holds nothing.
        self.assertEqual(self.judged(b), "red")

    def test_a_good_build_read_again_after_it_ran_holds_newer_reads(self):
        a = self.now - 20 * HOUR
        self.at(a, started=MINUTE, finished=40 * MINUTE, podinfo=41 * MINUTE)            # G: runs 40 min
        self.at(a + 71 * MINUTE, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE)  # N
        b = self.at(a + 81 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(a + 4 * HOUR + 11 * MINUTE))
        # TestGrid: U_a at a+39m reads G RUNNING. U_b at a+2h39m reads G again (it was RUNNING in
        # the grid; now done), N and B (new): N placed after G, persists. U_c at a+4h39m cuts at
        # N and reads B again, with the junit file written at a+4h11m: red.
        # The port: G's running_until is a+41m < N's ID time (a+71m): N holds nothing; B's reads
        # end at a+4h02m.
        self.assertEqual(self.judged(b), "red")


# ---------------------------------------------------------------- F2 (missed red)
class ChainedNoStartColumns(World):
    """Two builds without a start, N1 < G1 < N2 (G1 good), while an older build G0 runs. The update
    after G0 stops running places N1 after G0 and N2 after G1; the next cuts at N1, puts N1 at the
    epoch (read first), reads G1 again and so places N2 after G1 again; only the update after that
    drops N2's hold. So N2 holds newer builds' reads one update longer than N1 (read.go:173-209,
    updater.go:500-519, inflate.go:117). older_end() gives both the same end (held + READ_GAP from
    G0's running_until), so it misses the last read of B."""

    def test_the_second_no_start_build_holds_one_update_longer(self):
        m = self.now - 30 * HOUR
        self.at(m, started=MINUTE, finished=3 * HOUR, podinfo=3 * HOUR + MINUTE)              # G0
        self.at(m + HOUR, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE)               # N1
        self.at(m + 70 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)      # G1
        self.at(m + 80 * MINUTE, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE)        # N2
        b = self.at(m + 90 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(m + 8 * HOUR))
        # TestGrid, an update every 2 h: U_a m+2h59m (G0 RUNNING: cut at G0; N1 after G0, N2 after
        # G1), U_b m+4h59m (G0 read again, done; same placements), U_c m+6h59m (cut at N1: N1 to
        # the epoch, N2 after G1 again), U_d m+8h59m (cut at N2: B read again, with the junit
        # file written at m+8h): red. The port: older_end = m+3h01m + 2h20m; B's reads end at
        # m+7h41m.
        self.assertEqual(self.judged(b), "red")


# ---------------------------------------------------------------- F3 (missed red)
class ConfigChangeBringsBackNoStartColumns(World):
    """After its test group's config changed, an update reads again every build started in the
    last 7 days (reprocessColumn, updater.go:621-628, 765-782): it lists every build after the hint
    of the columns older than that, so builds without a start, long dropped from the grid, are read
    again too, each placed right after the good column read before it, RUNNING. The next update
    cuts at the oldest of them and reads every newer build again (updater.go:500-519). The port's
    seeds cover only the reprocess update itself (gcs_column's `events`), and Timeline ignores
    events, so it misses that next read."""

    def test_the_update_after_the_config_change_reads_newer_builds_again(self):
        g = self.now - 72 * HOUR
        self.at(g, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)                      # G
        self.at(g + 10 * MINUTE, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE)          # N
        e = self.now - 24 * HOUR  # the update that saw the changed config began then
        b = self.at(g + 20 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(e + HOUR))
        # TestGrid: the reprocess update at e reads G, N (after G: persists) and B; the next, at
        # e+2h, cuts at N and reads B again with its failing junit file (written at e+1h): red.
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now, events=[(e, e, "changed")])
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ---------------------------------------------------------------- F4 (missed red / decided too early)
class ReadGapLongerThanTwoHoursTwenty(World):
    """READ_GAP (2 h 20 min, fetch_red_logs.py:1558) is not the longest gap between two reads of a
    column TestGrid reads again.
    (a) Queue.Send (util/queue/queue.go:250-258) sets a group's next update to `now + freq` when it
    hands the group out; if an update of that group is still running, updateTestGroup drops it
    ("Already updating...", updater.go:374-380). A pubsub notification (production subscribes to
    kubernetes-ci-logs; pubsub.go:167-190) or the last-updated fixer (updater.go:172-204) arriving
    during a 20-minute update thus pushes the next one to 2 h after the END of the current one:
    a read early in an update can be followed by the next read 20 + 120 + 20 = 160 minutes later.
    (b) A failed update (a group timeout, or GCS error) is retried freq/4..freq/2 later
    (updater.go:404-418): one failure allows 120 + 20 + 60 + 20 = 220 minutes."""

    def pod_row_waiting_then_failed_podinfo(self, podinfo_after_wait):
        # finished at +1h (passed); its Pod row waits for podinfo.json until +2h (RUNNING);
        # podinfo.json with a failed pod is written `podinfo_after_wait` after that.
        return self.build(10 * HOUR, finished=HOUR, podinfo=2 * HOUR + podinfo_after_wait, pod=POD_FAILED)

    def test_a_dropped_update_lets_the_next_read_come_2h40m_later(self):
        b = self.pod_row_waiting_then_failed_podinfo(2 * HOUR + 30 * MINUTE)
        # U1 starts at +1h59m and reads B (Pod RUNNING); a notification at +2h18m (U1 still
        # running) hands the group out again: dropped, next = +4h18m; U2 reads B at +4h32m
        # (after podinfo.json, written at +4h30m): Pod FAIL. The port stops at +4h20m.
        self.assertEqual(self.judged(b), "red")

    def test_a_failed_update_is_not_followed(self):
        b = self.pod_row_waiting_then_failed_podinfo(3 * HOUR + 30 * MINUTE)
        # U1 at +1h59m reads B (Pod RUNNING); U2 at +3h59m times out at +4h19m; retried at
        # +4h19m + [30, 60) min, e.g. +5h15m, it reads B at +5h32m (podinfo.json at +5h30m): red.
        # The port follows TestGrid's updates as they go when none fails: whether one failed is in
        # TestGrid's own records, which are not public (README, Limits).
        self.assertEqual(self.judged(b), "not-red")


# ---------------------------------------------------------------- F5 (missed red; Python)
class TooOldComparedOnWholeSeconds(unittest.TestCase):
    """tr.column checks "too old" as `_unix(ts) < _unix(stop)` (testgrid_rule.py:1512). _wrap()
    adds 2**63 (testgrid_rule.py:208-209), so for a float `stop` the sum is a float rounded to a
    multiple of 2048 s. gcs_column's view(at) passes stop = at - UPDATE_SPAN - span, a float for
    every stretch midpoint and Last-Modified mark (fetch_red_logs.py:1801-1805). Go compares
    time.Unix(ts, 0).Before(stop) exactly (read.go:467)."""

    C = tr._UNIX_TO_INTERNAL

    def start_with(self, r, near):
        """A start time s near `near` with (s + C) mod 2048 == r."""
        return near - ((near + self.C - r) % 2048)

    def test_the_rule_compares_a_fractional_stop_exactly(self):
        s = self.start_with(1900, 1_791_600_000)
        started = json.dumps({"timestamp": s}).encode()
        # stop 650 s before the start: not too old in Go (the 24-hour FAIL is shown)
        self.assertEqual(tr.column("job", started, None, None, [], s - 650.0, s + 86950.0).errors, [])
        s = self.start_with(500, 1_791_600_000)
        started = json.dumps({"timestamp": s}).encode()
        # stop 0.4 s after the start: too old in Go
        self.assertEqual(tr.column("job", started, None, None, [], s + 0.4, s + 86950.0).errors, ["ancient"])

    def test_a_one_day_tabs_deadline_fail_is_not_lost_to_rounding(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        frl.BACKOFF = 0
        now = time.time()
        srv.gcs_listing("bucket", [])
        s = self.start_with(1900, int(now - 40 * HOUR))
        b = snowflake(s)
        base = f"/bucket/logs/job/{b}"
        srv.route(f"{base}/started.json", (200, {}, {"timestamp": s}))
        srv.route(f"{base}/podinfo.json", (200, {}, r9.POD_OK))
        # finished (passed) 18 min 20 s after its 24-hour deadline, finished.json written then
        srv.route(f"{base}/finished.json", (200, {"Last-Modified": email.utils.formatdate(s + 87500, usegmt=True)},
                                            {"timestamp": s + 87500, "passed": True, "result": "SUCCESS"}))
        # On a tab of one day, an update that began at s+24h (stop = s: not too old) and read the
        # build between its deadline and s+24h18m20s painted it FAIL ("did not complete within 24
        # hours"), as fetch_red_logs's own model says (README "Limits"). The port evaluates that
        # stretch at its midpoint with stop = s - 650.0, which rounds to s + 148: "too old".
        col = frl.gcs_column(srv.url, "bucket/logs/job", b, 1, now)
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ---------------------------------------------------------------- F6 (missed red)
class DeadlineComparedWhenTheColumnIsMade(unittest.TestCase):
    """readResult reads started.json, finished.json, podinfo.json and the junit files at once
    (read.go:420-540); overallCell then compares the 24-hour deadline with time.Now() when the
    column is made (gcs.go:517, called from read.go:204 after resp.res() returns). The junit
    listing and downloads, the shared build pool (--build-concurrency=10 for every group,
    read.go:78-139) and the previous build's conversion can put minutes between the finished.json
    read and that comparison (both within the update's 20 minutes). The port's view(at) takes GCS
    and the clock at the same instant, so a build whose finished.json was written in the minutes
    before its deadline never gets the FAIL TestGrid may paint.
    test_review_round9.Reads40Hours.test_finished_exactly_at_the_deadline_is_in_time pins this."""

    def test_finished_json_read_just_before_and_the_column_made_just_after_the_deadline(self):
        srv = FakeServer()
        self.addCleanup(srv.close)
        frl.BACKOFF = 0
        now = time.time()
        srv.gcs_listing("bucket", [])
        s = int(now - 40 * HOUR)
        b = snowflake(s)
        base = f"/bucket/logs/job/{b}"
        srv.route(f"{base}/started.json", (200, {}, {"timestamp": s}))
        srv.route(f"{base}/podinfo.json", (200, {}, r9.POD_OK))
        srv.route(f"{base}/finished.json", (200, {"Last-Modified": email.utils.formatdate(s + 86400 - 30, usegmt=True)},
                                            {"timestamp": s + 86400 - 60, "passed": True, "result": "SUCCESS"}))
        # An update reads finished.json at s+24h-45s (not there yet), downloads the build's junit
        # files, and makes the column at s+24h+15s: "Build did not complete within 24 hours", FAIL.
        col = frl.gcs_column(srv.url, "bucket/logs/job", b, 15, now)
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ---------------------------------------------------------------- F7 (false red)
class SameStartOrderInTheGrid(World):
    """Two builds with the same start: readColumns reads them in build-ID order (read.go:225) and
    SortStarted is stable (updater.go:560-564), so the lower ID A sorts first ("newer"). When A runs
    and B (higher ID) no longer does, truncateRunning returns the columns after A (updater.go:513),
    B among them: B is NOT read again, and `since` (B's hint) drops A from the listing, so A's
    column vanishes (util/gcs/read.go:267-275). older_end() counts a same-start build as older
    (`r.started <= started`, fetch_red_logs.py:1702) and keeps B read while A runs; Timeline does
    not model the vanishing at all. test_review_round9.ReadAgainWhileAnOlderColumnRuns
    .test_one_that_started_at_the_same_time_is_older pins the opposite. (Since review W7a-2 the
    port also reads a build that may vanish as if it never did: B's column may never be appended.)"""

    def setUp(self):
        super().setUp()
        made = frl.created(snowflake(self.now - 28 * HOUR))
        self.a = self.build(29 * HOUR, started=made + MINUTE - frl.created(snowflake(self.now - 29 * HOUR)))
        late = self.now - 7 * HOUR - made
        self.b = self.build(28 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                            junits=[("junit_01.xml", late, FAILING)])

    def test_a_same_start_lower_id_running_build_does_not_keep_the_other_read(self):
        self.assertEqual(self.judged(self.b), "not-red")

    def test_the_running_lower_id_build_may_not_vanish_before_its_deadline_fail(self):
        # TestGrid does not show A's 24-hour FAIL if its column is gone ~2 updates after B finished,
        # which takes an update that appends B's column; one whose reads all come after its grace
        # appends only the first, A's (review W7a-2): read so too, A is red at its deadline.
        self.assertEqual(self.judged(self.a), "red")


if __name__ == "__main__":
    unittest.main()
