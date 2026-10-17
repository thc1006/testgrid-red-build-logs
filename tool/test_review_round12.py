"""Review wave W5a: the reworked read model of fetch_red_logs (commit 05793d9: READ_GAP, the
reprocess seed, made_columns, Timeline.older_end's ID-ordered holds, Timeline.vanishes, the
per-read display windows, run()'s watched builds) against TestGrid's Go code at the commit
TestGrid's production manifests deploy (testgrid@v0.0.177-0.20260902201319-f15bfa4808a4; paths
below are relative to it). Each test asserts what TestGrid does and fails on 05793d9 for the
reason its docstring gives; nothing here changes fetch_red_logs.

Schedules named in the tests are valid under the updater's code with production's flags
(cluster/prod/updater.yaml: --wait=2h --group-timeout=20m, pubsub on kubernetes-ci-logs):
"an update at X" means an update of the job's test group that begins at X and reads its
builds then. Each scenario was also replayed in a simulator of the updater's grid
(InflateGrid, reprocessColumn, SortStarted, truncateRunning, hintStarted, listBuilds,
readColumns), under every schedule of updates every 2 h and 2 h 20 min at 7-minute phases.

Run: cd tool && python -m unittest test_w5a_findings -v
"""
import concurrent.futures as cf
import email.utils
import math
import os
import sys
import threading
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round9 as r9  # noqa: E402  (module imports: their tests are not collected here)
import test_review_round11 as r11  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

MINUTE = 60
FAILING, PASSING = r9.FAILING, r9.PASSING


def written(t):
    return {"Last-Modified": email.utils.formatdate(t, usegmt=True)}


# ------------------------------------------------------------------ W5a-1 (missed red)
class VanishedTwinIsListedAgainByAConfigChange(r11.World):
    """Timeline.vanishes() + gcs_column (fetch_red_logs.py:1777-1799, 1912-1919): of two builds
    with the same start, the lower ID A vanishes from the grid when the higher ID T no longer
    runs (truncateRunning cuts at A, updater.go:500-519, and hintStarted, read.go:40-49, makes
    listBuilds start after T's ID, util/gcs/read.go:265-275). gcs_column then drops every read
    of A after `gone`, including the reads of the update after a change to the test group's
    config (the `events` seeds, fetch_red_logs.py:1908-1911).

    But that update re-lists A: reprocessColumn adds a RUNNING column at now - 7 days
    (updater.go:621-626, 764-782), truncateRunning cuts there (its floor, updater.go:505-509),
    so oldCols are only the columns that started more than 7 days before, and listBuilds lists
    every build after their highest hint, A among them (read.go:58-63). A is read again: still
    not finished 24 hours after its start, overallCell paints it FAIL ("Build did not complete
    within 24 hours", gcs.go:517-520), and A is back in the grid, red, not running, for good."""

    def test_the_update_after_a_config_change_shows_the_vanished_twin_failed(self):
        m = self.now - 49 * HOUR  # T's ID time
        t = self.at(m, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)  # T: done in 6 minutes
        start = frl.created(t) + MINUTE
        a_made = m - HOUR
        a = self.at(a_made, started=start - frl.created(snowflake(a_made)))  # A: lower ID, same start, never ends
        e = start + 39 * HOUR  # (the upload job's run that changed the group: 10 hours ago)
        # TestGrid, every 2 h: A and T read together while T runs; the update after T is read done
        # cuts at A and lists after T: A is gone (~2 h after the start, never shown FAIL; round 11
        # pins that without the change). The update after the change (within 7 days of A's start)
        # lists A again and reads it 39 h after its start: FAIL. The port: every read of A after
        # gone (T done + 2 h 40 min) is dropped, the reprocess update's too: not red.
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", a, 15, self.now, events=[(e, e, "changed")])
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ------------------------------------------------------------------ W5a-1b (false red)
class VanishingIsNotOnlyForTheSameStart(r11.World):
    """Timeline.vanishes (fetch_red_logs.py:1789-1790) looks only for builds of the SAME start
    and a higher ID. The cut that makes them vanish applies to any column of a higher ID that
    sorts older: SortStarted orders by start (updater.go:560-564), truncateRunning returns the
    columns older than the oldest running one (updater.go:507-515), hintStarted takes the
    highest hint among them (read.go:40-49) and listBuilds lists only the builds after it
    (util/gcs/read.go:265-275). So a build A that starts after a build C of a higher ID (A's pod
    pending longer) vanishes as soon as A runs, C does not and nothing older does; InflateGrid
    also drops A's column while it started less than 20 minutes before (inflate.go:113-116),
    and C's hint keeps it from being listed again. (Not seen on the two dashboards in the 22
    days to 2026-10-12: by when GCS says each started.json was written, no build of their 46
    jobs started after one of a higher ID, and none two in the same second. Since review W7a-2 the
    port also reads such a build as if it never vanished: C's column may never be appended.)"""

    def test_a_build_that_starts_after_a_newer_one_vanishes_before_its_deadline(self):
        m = self.now - 40 * HOUR
        a = self.at(m, started=2 * HOUR)  # A: starts 2 h after it was made, never finishes
        self.at(m + 30 * MINUTE, started=MINUTE, finished=10 * MINUTE, podinfo=11 * MINUTE)  # C: higher ID, starts first
        # TestGrid: C is done (and kept) from ~m+1h on, once an update appends C's column; A sorts
        # newer than C: every update cuts at A (running) or drops it (started less than 20 min
        # before) and lists after C's ID. A is gone within hours and never shown FAIL at its
        # deadline (m+26h) while C keeps it out; no schedule of the simulator shows it red. But an
        # update whose reads all come after its grace appends only its first column, A's (review
        # W7a-2): read so too, A is FAIL after m+26h, red, with a build after C or not.
        self.assertEqual(self.judged(a), "red")
        self.at(m + 3 * HOUR, started=MINUTE, finished=10 * MINUTE, podinfo=11 * MINUTE)
        self.assertEqual(self.judged(a), "red")
        # (and when it vanishes: once C, settled a gap after its read done, pins it out)
        tl = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        self.assertIsNotNone(tl.grid(a, frl.created(a) + 2 * HOUR, 15)[1])


# ------------------------------------------------------------------ W5a-1c (false red)
class VanishedTwinHoldsNothing(r11.World):
    """Timeline.older_end (fetch_red_logs.py:1755-1762, 1775) holds a build's reads while any
    good column that started before it runs (BuildRuns.running_until), but BuildRuns never
    applies Timeline.vanishes: a lower-ID twin that vanished from the grid (see round 11's
    SameStartOrderInTheGrid and gcs_column's cut) still counts as running there until its 24-hour
    deadline. Once it is gone, truncateRunning (updater.go:500-519) has no running column to cut
    at: a newer build's reads end with its own. (Since review W7a-2 the port also reads and holds
    as if it never vanished: T's column may never be appended.)"""

    def test_a_newer_build_is_not_read_again_while_a_vanished_twin_would_run(self):
        m = self.now - 40 * HOUR
        t = self.at(m, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)  # T: done in 6 minutes
        start = frl.created(t) + MINUTE
        a = self.at(m - HOUR, started=start - frl.created(snowflake(m - HOUR)))  # A: lower ID, same start, never ends
        b = self.at(m + 2 * HOUR, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(start + 20 * HOUR))
        # TestGrid: A is gone ~2-3 h after its start, once an update appends T's column; B (made m+2h,
        # done m+2h06m) is then read for the last time by m+5h06m, without its junit file written at
        # m+20h (the simulator: never red). But an update whose reads all come after its grace
        # appends only its first column, A's (review W7a-2): read so too, A runs until its deadline,
        # FAIL, and holds B, read again until a gap after that, its junit file in it: both red.
        self.assertEqual((self.judged(a), self.judged(b)), ("red", "red"))


# ------------------------------------------------------------------ W5a-2 (false red)
class ErroredColumnPlacesNoStartBuildAtTheEpoch(r11.World):
    """Timeline.older_end (fetch_red_logs.py:1763-1772) takes every build with a usable start as
    a good column (BuildRuns.started is set unless the column's errors are "ancient" or "started",
    fetch_red_logs.py:1667), so a no-start build N is held by the reads of a lower-ID build whose
    podinfo.json (or finished.json) is not JSON. In readColumns (read.go:176-209) such a build is
    an errored column (readResult returns the "podinfo: ..." error, read.go:440-455 and 526-534):
    it is placed at `started + 0.01*failures` and does NOT set `started`; only a good column does
    (read.go:199-209). An update that reads it and N, but no good column before them, puts N at
    started=0 + 0.02 ms, the epoch: the next InflateGrid drops N as too old (inflate.go:117-119),
    and nothing is held. So the reads of a newer build B end with its own."""

    def test_a_build_with_a_malformed_podinfo_holds_no_newer_reads(self):
        a = self.now - 20 * HOUR
        e = self.at(a, started=MINUTE, finished=2 * MINUTE)  # E: done in 2 minutes ...
        self.srv.route(f"/bucket/logs/job/{e}/podinfo.json",  # ... its podinfo.json, not JSON, written at +3 min
                       (200, written(frl.created(e) + 3 * MINUTE), b"not json"))
        self.at(a + 10 * MINUTE, started=None, finished=5 * MINUTE, podinfo=5 * MINUTE)  # N: never started
        b = self.at(a + 20 * MINUTE, started=MINUTE, finished=2 * MINUTE, podinfo=3 * MINUTE,
                    junits=self.failing_junit(a + 4 * HOUR))
        # TestGrid: any update that lists N reads E errored (TOOL_FAIL) before it: N at the epoch,
        # dropped by the next update. B is read for the last time by a+3h (made a+20m, its column
        # done at a+23m): its failing junit file, written at a+4h, is never read (the simulator:
        # no schedule shows B red). The port takes E as good: N "held" until E's last read
        # (a+2h43m), B read again until a+5h23m, with the junit file: red.
        self.assertEqual(self.judged(b), "not-red")


# ------------------------------------------------------------------ W5a-2b (false red)
class AncientColumnsHoldNothingOnAOneDayTab(r11.World):
    """Timeline.older_end (fetch_red_logs.py:1767-1770) takes every good lower-ID column as read
    again by the update after a change to the test group (until hi + UPDATE_SPAN), so a build
    without a start after it is "held" and every newer build is read again until a gap after
    that: older_end is a single end time, and gcs_column (possible_reads' `again`) lets those
    reads come at any time from the build's own reads on. On a tab of 1 day of results
    (sig-release-master-blocking#ci-kubernetes-unit has days_of_results: 1), the reprocess update
    reads the builds older than a day as ancient (read.go:467-471, stop = its start - 1 day,
    updater.go:595-602): an ancient column does not set `started` (read.go:182-198), so the build
    without a start after it goes to the epoch, and nothing is held; and any read of the newer
    build after the change is ancient as well. Its reads end with its own, days before."""

    def test_a_change_a_day_later_holds_nothing_on_a_one_day_tab(self):
        a = self.now - 40 * HOUR
        self.at(a, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE)  # G
        self.at(a + 10 * MINUTE, started=None, finished=5 * MINUTE, podinfo=5 * MINUTE)  # N: never started
        b = self.at(a + 20 * MINUTE, started=MINUTE, finished=5 * MINUTE, podinfo=6 * MINUTE,
                    junits=self.failing_junit(a + 7 * HOUR))
        events = [(a + 30 * HOUR, a + 31 * HOUR, "changed")]
        # TestGrid (1 day of results): B is read for the last time by a+6h06m (N holds it until a gap
        # after G's reads); the update after the change (a+30h..31h20m) reads G, B (and N) ancient:
        # no hold, and B's column there is not red. Its junit file, written at a+7h, is never read
        # (the simulator: no schedule shows B red with days 1; with days 15 the reprocess update
        # reads it, red, as the port says). The port: N "held" until a+31h20m, B read again at any
        # time until a+34h, so with its junit file before it is too old (a+24h41m): red.
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 1, self.now, events=events)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")


# ------------------------------------------------------------------ W5a-3 (missed red)
class NotUpdatedConfigStillShowsTheGridItHas(r9.JobWorld):
    """tab_verdict (fetch_red_logs.py:2046-2052) skips a config era whose test group TestGrid
    does not update (TabRule.not_updated: no use_kubernetes_client nor GCS result source), as
    if the tab showed nothing then. TestGrid's updater does not rewrite that group's grid
    (cmd/updater/main.go:152-163 fails the update; updater.go:100-103), but its tabulator still
    tabulates the grid it has, by the tab's config of the time: mapTasks takes the tab and
    group from the current config (pkg/tabulator/tabstate.go:66-81), read() downloads the
    group's grid (tabstate.go:229-252), and tabulate() filters its rows by that tab's
    base_options (tabstate.go:372-376); the queue hands every group out at least hourly
    (--wait=1h, cluster/prod/tabulator.yaml). So the columns read before the change stay on
    the tab, filtered by the new config."""

    def test_a_read_from_before_is_shown_by_the_config_of_a_group_not_updated(self):
        b = self.build(30 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 50 * MINUTE, FAILING)])  # red only on its row "t"
        hides_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$"))
        no_source = tgc.TabRule(ttc.entry("", use_kubernetes_client=False))  # (no source of results)
        self.assertTrue(no_source.not_updated and no_source.shows("t") and not hides_t.shows("t"))
        change = self.now - 10 * HOUR  # (B's last read was ~25 h ago)
        rule = {"eras": [(-math.inf, change, hides_t), (change - 30 * MINUTE, math.inf, no_source)]}
        col = self.column(b)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # (by every row)
        # TestGrid: from the change on, the tab shows the grid it had, B's column in it, by the new
        # base options, which show "t": red. The port skips that era: not red.
        self.assertEqual(frl.tab_verdict(col, "job", rule, frl.created(b))[0], "red")


# ------------------------------------------------------------------ W5a-4 (performance)
class EventWidenedWindowIsReadOneBuildAtATime(r9.JobWorld):
    """Performance. With a change to the test group's config (`events`), Timeline.older_end
    widens the window of builds it reads to made - lo + 7 days (15 for a new group) + 6 h, up to
    MAX_LOOKBACK = 21 days (fetch_red_logs.py:1741-1752), and the adaptive loop by READ_GAP per
    build without a start. Timelines.prime (fetch_red_logs.py:1801-1808, 1823-1833; called by
    run() and backfill_gaps) reads only [made - OLDER, made + PENDING], so the rest is read by
    `[self.runs(b) for b in ...]` in older_end: one build after another (each 3 GETs), and every
    other thread judging a build of the job waits on the same futures in the same order. A
    build judged after an older one also lists the job again (Timeline.builds reuses a listing
    only if it covers the whole window; with the 21-day cap the windows of two builds never
    nest), and every listing stays in Timeline._windows. Measured against GCS for one
    ci-kubernetes-build-fast build (~11 builds an hour): 453 builds to read (1359 GETs) without
    events, 2208 (6624 GETs) with a "changed" event a day before it, 4032 (12096 GETs) with a
    "new" one after it (every run of the upload job whose config is not built yet, a running
    one included, is a "new" event: ConfigHistory.events); the 1530 requests of the first,
    unprimed, took 678 s from this machine (one at a time, ~0.44 s each)."""

    def test_after_prime_judging_a_build_reads_no_build_one_at_a_time(self):
        for k in range(7 * 24):  # a build every hour for the 7 days before the judged one, each done in 5 minutes
            self.build(30 * HOUR + (7 * 24 - k) * HOUR, finished=5 * MINUTE, podinfo=6 * MINUTE)
        b = self.build(30 * HOUR, finished=5 * MINUTE, podinfo=6 * MINUTE)
        made = frl.created(b)
        events = [(made - 2 * 24 * HOUR, made - 2 * 24 * HOUR + HOUR, "changed")]  # (2 days before it)
        timelines = frl.Timelines(self.now)
        with cf.ThreadPoolExecutor(8) as pool:
            timelines.prime(pool, self.srv.url, [("bucket/logs/job", b)])  # (as run() and backfill_gaps do)
        main, late = threading.get_ident(), []
        real = frl.BuildRuns.__init__

        def init(runs, timeline, build):
            if threading.get_ident() == main:
                late.append(build)
            real(runs, timeline, build)
        with mock.patch.object(frl.BuildRuns, "__init__", init):
            frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now,
                           timeline=timelines.get(self.srv.url, "bucket/logs/job"), events=events)
        # (fails: the builds of the 5.5 days before the window prime() read, one at a time)
        self.assertEqual(late, [])


if __name__ == "__main__":
    unittest.main()
