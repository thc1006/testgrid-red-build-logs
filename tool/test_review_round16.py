"""Review wave W6a: the read model of fetch_red_logs after e5df0fb / 35c5616 / 398eb0d / 3395a24
(possible_reads with hold spans, BuildRuns, HoldModel, Timeline, Timelines.prime, gcs_column,
tab_verdict's `since`) against TestGrid's Go code at the commit TestGrid's production manifests
deploy (testgrid@v0.0.177-0.20260902201319-f15bfa4808a4; paths below are relative to it). Each
test asserts what TestGrid does (or, for performance, what a judged build should cost) and fails
on 14ff6b1 for the reason its docstring gives; nothing here changes fetch_red_logs.

"An update at X" means an update of the job's test group that begins at X and reads its builds
then (production: --wait=2h --group-timeout=20m, pubsub on kubernetes-ci-logs). Each scenario of a
missed or false red was replayed in review W6a's simulator of the updater's grid (InflateGrid,
reprocessColumn, SortStarted, truncateRunning, hintStarted, listBuilds, readColumns; a build's
folder listed from Prow's pending prowjob.json on, read without a start until started.json).

Run: cd tool && python -m unittest test_review_round16 -v
"""
import email.utils
import math
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round9 as r9  # noqa: E402  (module imports: their tests are not collected here)
import test_review_round11 as r11  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_integrity import HOUR  # noqa: E402

MINUTE = 60
DAY = 24 * HOUR


def written(t):
    return {"Last-Modified": email.utils.formatdate(t, usegmt=True)}


# ------------------------------------------------------------------ W6a-1 (missed red)
class ErroredColumnIsNotSettledAtItsStart(r11.World):
    """HoldModel.vanishing (fetch_red_logs.py:1834-1858) takes every build with a start
    (self.good) as a column that sorts at that start, and counts one of a higher ID that sorts
    older than the build as "settled in the grid" once it no longer runs (run_end). But a column
    whose finished.json or podinfo.json is not JSON is an errored column from then on
    (BuildRuns.good_until): readResult returns the decode error (read.go:440-455), and
    readColumns places it at `started + 0.01*failures`, the start of the last GOOD column read
    before it in that update (read.go:176-198), not at its own start. So when B (a higher ID
    than A, its pod started before A's) is errored by the time it is done, B sits right after A
    (A is read first, in ID order), newer than A, or at the epoch while A has no start yet, where
    the next update drops it: truncateRunning cuts at A, which runs (updater.go:500-519), keeps
    no column of a higher ID than A, and A is listed and read again at every update
    (hintStarted, read.go:40-49) until its 24-hour deadline paints it FAIL (gcs.go:517-520).
    gcs_column drops every read of A after B's `gone` instead (fetch_red_logs.py:2139-2151)."""

    def test_a_build_is_not_dropped_past_an_errored_column_that_started_before_it(self):
        m = self.now - 40 * HOUR
        self.at(m - 3 * HOUR, started=MINUTE, finished=20 * MINUTE, podinfo=20 * MINUTE + 1)  # P: done before
        a = self.at(m, started=20 * MINUTE)  # A: its pod starts at m+20m, never finishes
        b = self.at(m + 5 * MINUTE, started=MINUTE, finished=10 * MINUTE)  # B: starts at m+6m, done at m+15m ...
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json",  # ... its podinfo.json, not JSON, a second later
                       (200, written(frl.created(b) + 10 * MINUTE + 1), b"not json"))
        # TestGrid, an update every 2 h from m-5m: from m+1h55m on each update reads A (RUNNING, at
        # its start) and B (errored: at A's start + 0.01 ms), and the next cuts at A, keeps P only,
        # and lists both again; the first update after m+20m+24h paints A FAIL: red. (Before A's
        # pod starts, A and B are both at the epoch, dropped by the next update.) The port: A
        # "vanishes" at B's `gone` (~m+2h55m; B taken as settled at its start), so its reads end
        # long before the deadline.
        self.assertEqual(self.judged(a), "red")

    def test_and_the_build_still_holds_the_newer_ones(self):
        """The same mistake costs the neighbours their holds: the HoldModel's second pass takes A as
        gone (it runs at `gone`), so A, which TestGrid keeps re-reading for 24 hours as the oldest
        running column, holds nothing after `gone`. C, newer than A, is read again at every update
        while A runs; its junit file written 12 hours after it finished is read: red (the
        simulator: 157 of 157 schedules)."""
        m = self.now - 40 * HOUR
        self.at(m - 3 * HOUR, started=MINUTE, finished=20 * MINUTE, podinfo=20 * MINUTE + 1)
        self.at(m, started=20 * MINUTE)
        b = self.at(m + 5 * MINUTE, started=MINUTE, finished=10 * MINUTE)
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json",
                       (200, written(frl.created(b) + 10 * MINUTE + 1), b"not json"))
        c = self.at(m + 3 * HOUR, started=30, finished=20 * MINUTE, podinfo=20 * MINUTE + 1,
                    junits=self.failing_junit(m + 15 * HOUR))  # (C done at m+3h20m; its junit file at m+15h)
        self.assertEqual(self.judged(c), "red")


# ------------------------------------------------------------------ W6a-2 (missed red)
class VanishedBuildIsListedAgainWhenTheOlderColumnsAgeOut(r11.World):
    """HoldModel.vanishing / gcs_column (fetch_red_logs.py:1834-1858, 2131-2151) take a build A that
    vanished from the grid (a column B of a higher ID sorts older than A, settled there, so
    hintStarted lists only the builds after B: read.go:40-63) to be listed again only by an
    update after a config change. But the hint is that of the columns the update KEEPS: once B is
    older than the tab's days of results, InflateGrid drops it (inflate.go:117-119; it keeps an
    old column only when it would keep none), and when no column of a higher ID than A is kept
    (here every newer one is at or after the oldest running column, which truncateRunning cuts
    at, updater.go:500-519), listBuilds lists A again (util/gcs/read.go:265-275) and A is read:
    on a tab of 1 day of results (sig-release-master-blocking#ci-kubernetes-unit), an update that
    began less than a day after A's start and makes A's column after its 24-hour deadline
    paints it FAIL."""

    def test_a_vanished_build_is_read_again_once_the_higher_column_is_too_old(self):
        m = self.now - 40 * HOUR
        a = self.at(m, started=2 * HOUR + 24 * MINUTE)  # A: its pod starts at m+2h24m, never finishes
        self.at(m + 2 * MINUTE, started=MINUTE, finished=3 * HOUR + 30 * MINUTE,
                podinfo=3 * HOUR + 30 * MINUTE + 1)  # B: higher ID, starts at m+3m, done at m+3h32m
        self.at(m + 4 * HOUR + 42 * MINUTE, started=MINUTE, finished=23 * HOUR + 40 * MINUTE,
                podinfo=23 * HOUR + 40 * MINUTE + 1)  # C: runs until m+28h23m
        # TestGrid (1 day of results), an update every 2 h 20 min from m+26 min: A vanishes once B
        # is done (~m+5h). The update at m+26h06m drops B (started before its stop, m+2h06m), cuts
        # at C (running), keeps nothing, lists every build: A (started m+2h24m, not too old) is
        # read and its column made after m+26h24m (its deadline): FAIL, red, kept in the grid.
        # The port: A's reads end ~m+6h12m (B's `gone`); no change to the group: not red.
        self.assertEqual(self.judged(a, days=1), "red")


# ------------------------------------------------------------------ W6a-3 (false red)
class VanishingOnlyWaitsForTheHoldToEnd(r11.World):
    """HoldModel.vanishing (fetch_red_logs.py:1851-1852) skips a column B of a higher ID that sorts
    older than the build A whenever ANY hold on B (an older column that runs) ends after B is
    done: "the grid is cut there, and the build listed". It is, but only while that older column
    runs: once its update reads it done, B is settled, and the next update that finds A running
    cuts at A and lists only the builds after B's ID (updater.go:500-519, read.go:40-63): A
    vanishes, a hold later. The port keeps reading A for as long as it runs, and takes its column
    at its 24-hour deadline as shown: red, where TestGrid shows A running until it drops it. (Since
    review W7a-2 it does so again, on purpose: B's column may never be appended; vanishing still
    waits for the hold to end.)"""

    def test_a_build_vanishes_once_the_older_running_column_is_done(self):
        m = self.now - 40 * HOUR
        self.at(m - HOUR, started=30, finished=11 * HOUR, podinfo=11 * HOUR + 1)  # H0: runs until m+10h
        a = self.at(m, started=30 * MINUTE)  # A: its pod starts at m+30m, never finishes
        self.at(m + 10 * MINUTE, started=MINUTE, finished=10 * MINUTE, podinfo=10 * MINUTE + 1)  # B: before A
        # TestGrid: every update cuts at H0 (older than B) and lists A and B again until H0 is read
        # done (by m+12h40m); the next one finds A running, cuts at A, keeps B (a higher ID than
        # A's), and lists only builds after B: A is gone by m+15h20m, never read at its deadline
        # (m+24h30m) while B keeps it out (nothing older than B runs, no config change); the
        # simulator: no schedule shows A red. The port: no vanishing, A FAIL at its deadline: red.
        # But that takes an update that appends B's column, and one whose reads all come after its
        # grace appends only its first, A's (review W7a-2): read so too, A is FAIL at its deadline.
        self.assertEqual(self.judged(a), "red")
        # (where it vanishes: once H0 is read done and B settles, not while H0 runs)
        tl = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        done, gone = tl.grid(a, frl.created(a) + 30 * MINUTE, 15)[1][:2]
        self.assertGreater(done, m + 10 * HOUR)
        self.assertLess(gone, m + 24 * HOUR)


# ------------------------------------------------------------------ W6a-8 (false red, in ID order)
class AHoldEndsWhenTheOlderColumnIsTooOld(r11.World):
    """possible_reads (fetch_red_logs.py:1642) takes a hold (from, to) as a seed until to +
    READ_GAP: the update after the last running read still finds the older column RUNNING in the
    grid and cuts there. HoldModel.run_end (1771-1774) caps `to` at the older build's start + days
    of results + UPDATE_SPAN, but the READ_GAP after it is not capped: an update that begins after
    the older build's start + days of results drops its column before it cuts (InflateGrid's
    `earliest`, inflate.go:117-119, runs before truncateRunning, updater.go:618-628), so nothing
    holds the newer build any more. On a tab of 1 day of results (ci-kubernetes-unit) a build that
    never finishes holds the newer ones until its own deadline, which is also when it is too old;
    the port lets them be read for 2 h 40 min more."""

    def test_a_too_old_running_column_holds_nothing(self):
        m = self.now - 30 * HOUR
        self.at(m, started=MINUTE)  # R: never finishes (FAIL, and too old, at m+24h1m)
        t = self.at(m + HOUR, started=MINUTE, finished=20 * MINUTE, podinfo=20 * MINUTE + 1,
                    junits=self.failing_junit(m + 25 * HOUR))  # T: done at m+1h21m, its junit file at m+25h
        # TestGrid (1 day of results): T is read again while R's column runs in the grid, by an
        # update that begins by m+24h1m at the latest (later ones drop R as too old first), so by
        # m+24h21m; its junit file, written at m+25h, is never read: no schedule shows T red
        # (the simulator: 0 of 157). The port: T may be read until m+27h01m, with the file: red.
        self.assertEqual(self.judged(t, days=1), "not-red")


# ------------------------------------------------------------------ W6a-4 (missed red)
class GroupReadingAnotherJobStillShowsItsOldColumns(r9.JobWorld):
    """tab_eras (fetch_red_logs.py:2300-2314, from e5df0fb) takes an era whose test group reads
    another job's builds (r.prefix != query) as one where TestGrid had no such tab, and the README
    says so ("a config whose test group read another job's builds shows none of this job's").
    But when the tab's own test group changes its GCS prefix, its grid keeps this job's columns
    that started more than 7 days before the update that took the change: reprocessColumn puts
    its RUNNING column at now - 7 days (updater.go:621-628, 764-782), truncateRunning keeps every
    column older than that floor (updater.go:500-519), InflateGrid keeps them until they are
    older than the days of results (inflate.go:113-119), and listBuilds lists the other prefix's
    builds after their highest hint (read.go:58-63): the tab shows them, by the new config. Here
    the change also stopped hiding row "t", which the old config hid: TestGrid showed the build
    red until it scrolled off; the port skips the era."""

    def test_a_column_older_than_7_days_stays_on_the_tab_after_its_group_reads_another_job(self):
        b = self.build(16 * DAY, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 50 * MINUTE, r9.FAILING)])  # red only on its row "t"
        hides_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$"))
        moved = tgc.TabRule(ttc.entry("", gcs_prefix="bucket/logs/other"))  # (same group "job", another job)
        self.assertEqual((hides_t.prefix, moved.prefix, moved.entry["test_group"]),
                         ("bucket/logs/job", "bucket/logs/other", "job"))
        change = self.now - 2 * DAY  # (b started 14 days before: its column is kept, shown until ~1 day ago)
        rule = {"eras": [(-math.inf, change, hides_t), (change - 30 * MINUTE, math.inf, moved)]}
        col = self.column(b)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # (by every row)
        self.assertEqual(frl.tab_verdict(col, "job", rule, frl.created(b), query="bucket/logs/job")[0], "red")

    def test_and_from_the_last_scan_on(self):
        """The same with `since` (the changed-since search of 35c5616/3395a24): the last scan, in
        the old config, saw b not red; from the change on the tab showed it red."""
        b = self.build(16 * DAY, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 50 * MINUTE, r9.FAILING)])
        hides_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$"))
        moved = tgc.TabRule(ttc.entry("", gcs_prefix="bucket/logs/other"))
        change = self.now - 2 * DAY
        rule = {"eras": [(-math.inf, change, hides_t), (change - 30 * MINUTE, math.inf, moved)]}
        self.assertEqual(frl.tab_verdict(self.column(b), "job", rule, frl.created(b), query="bucket/logs/job",
                                         since=change - 6 * HOUR)[0], "red")

    def test_a_build_the_group_never_read_is_not_shown_when_it_reads_the_job_again(self):
        """(false red) The other way round: tab_verdict's made_with (fetch_red_logs.py:2348-2349)
        takes the reads of an era whose group read another job as made "by each options", and
        judges them under the later eras. But no read of this job's build was made then: the
        group listed the other prefix (read.go:51-73). When the group reads this job again (a
        config change: the update reads again the builds that started in the 7 days before,
        listing those after the hint of the columns older than that, updater.go:621-628 and
        read.go:58-63), a build made while it read the other job and older than 7 days then is
        never listed (the other job's columns kept then were made after it: their hint is higher):
        TestGrid never showed it on the tab (review W6a's simulator: never in the grid)."""
        b = self.build(10 * DAY, finished=HOUR, passed=False, podinfo=HOUR + MINUTE)  # Overall FAIL
        here = tgc.TabRule(ttc.entry(""))
        moved = tgc.TabRule(ttc.entry("", gcs_prefix="bucket/logs/other"))
        made = frl.created(b)
        away, back = made - DAY, made + 8 * DAY + 2 * HOUR  # (b is 8 days old when the group reads this job again)
        rule = {"eras": [(-math.inf, away, here), (away - 30 * MINUTE, back, moved),
                         (back - 30 * MINUTE, math.inf, here)]}
        self.assertEqual(frl.tab_verdict(self.column(b), "job", rule, made, query="bucket/logs/job")[0], "not-red")


# ------------------------------------------------------------------ W6a-6 (missed red / decided too early)
class ReadsMadeUnderAnUnknownConfigStillCount(r9.JobWorld):
    """tab_verdict (fetch_red_logs.py:2345-2375) judges each config era on the reads made under
    each config (made_with: TabRule eras and None eras only). The reads made while a config the
    archive does not know was in force (an UNKNOWN era: an upload-job run whose config is not
    built) are in no made_with span; without `since` the UNKNOWN era itself makes the build
    undecided, but with `since` after that era (35c5616's changed-since search; config_changed
    looks only at era boundaries after `since`) the era is skipped, and those reads are dropped:
    a column TestGrid made then and kept showing (it is never read again) is not judged by the
    later configs. Here it is red on row "t", which the config from `change` on shows: TestGrid
    showed it red; the port decides "not-red" (it should be red, or at least undecided)."""

    def test_the_last_read_made_under_an_unknown_config_is_judged_after_since(self):
        t1 = self.now - 12 * HOUR
        b = self.build(11 * HOUR, finished=5 * MINUTE, podinfo=5 * MINUTE + 1,
                       junits=[("junit_01.xml", 4 * MINUTE, r9.FAILING)])  # made at t1+1h, read until ~t1+4h
        show_all = tgc.TabRule(ttc.entry(""))
        hides_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$"))
        t2, change = t1 + 6 * HOUR, t1 + 9 * HOUR
        since = t2 + HOUR  # (the last scan, by the config that hides "t": b not red)
        rule = {"eras": [(-math.inf, t1, show_all), (t1 - 30 * MINUTE, t2, frl.UNKNOWN),
                         (t2 - 30 * MINUTE, change, hides_t), (change - 30 * MINUTE, math.inf, show_all)]}
        col = self.column(b)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # (its only column, by every row)
        # (red, by the later config's filters; or undecided while the options of the reads made under
        # the unknown config are not known, as for the era itself without `since`)
        self.assertIn(frl.tab_verdict(col, "job", rule, frl.created(b), since=since)[0],
                      ("red", "undecided"))  # fails: not-red
        # (and from a scan after TestGrid's tabulator may have shown the unknown config, an hour after it)
        self.assertIn(frl.tab_verdict(col, "job", rule, frl.created(b), since=since + 2 * HOUR)[0],
                      ("red", "undecided"))


# ------------------------------------------------------------------ W6a-7 (missed red, order-dependent)
class WindowShorterThanTheDaysANewGroupReads(r11.World):
    """Timeline.back (fetch_red_logs.py:1949-1959) widens a build's window for a "new" (or
    "unknown", 3395a24) event to made - lo + the days of results + PENDING, but caps it at
    MAX_LOOKBACK = 21 days. The update of a new group reads every build of its days of results
    (no grid: no hint, read.go:58-63), 60 days for ci-kubernetes-e2e-{gce,ec2}-scale-performance-5000
    and 30 for gce-scale-correctness, so a build without a start N0 older than 21 days is put at
    the start of the good column G0 before it (read.go:176-198), RUNNING, and the next update cuts
    at it (updater.go:500-519) and reads B again (B started after G0, its ID is higher than N0's).
    Judged alone, B's window does not reach N0: no hold, B's reads end with the new group's first
    update. Judged after G0 or N0 in the same run, the cached HoldModel (Timeline.grid keys it by
    days and events, 1985-1991, and widens it to the union of the windows) has N0, and B is red:
    the verdict depends on what the run judged before."""

    def judge(self, b, e, timeline=None):
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 60, self.now,
                             timeline=timeline, events=[(e, e, "new")])
        return frl.verdict(col, "job")[0]

    def test_a_build_without_a_start_older_than_21_days_holds_the_build(self):
        e = self.now - 5 * DAY  # the update after the change (the group new to TestGrid) began then
        g0 = self.at(self.now - 40 * DAY, started=30, finished=10 * MINUTE, podinfo=10 * MINUTE + 1)
        n0 = self.at(self.now - 40 * DAY + 10 * MINUTE, started=None, finished=2 * MINUTE, podinfo=2 * MINUTE + 1)
        b = self.at(self.now - 15 * DAY, started=30, finished=10 * MINUTE, podinfo=10 * MINUTE + 1,
                    junits=self.failing_junit(e + HOUR))  # (written after the new group's first update)
        # TestGrid: the update at e reads G0, N0 (put at G0's start, RUNNING) and B (no junit file
        # yet); the next, at e+2h, cuts at N0 and reads B again, with its failing junit file: red.
        shared = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        for x in (g0, n0):
            self.judge(x, e, shared)
        self.assertEqual(self.judge(b, e, shared), "red")  # (after G0 and N0: the model has N0)
        self.assertEqual(self.judge(b, e), "red")  # fails: not-red, judged alone


# ------------------------------------------------------------------ W6a-5 (performance)
class EachBuildOfTheWindowIsDecodedOnce(r9.JobWorld):
    """Performance. Judging one build reads every build of its job in the window Timeline.window
    gives (fetch_red_logs.py:1961-1971) as a BuildRuns (1691-1712), whose possible_reads paints the
    column at every mark and stretch, twice (made_columns): ~29 calls of testgrid_rule.column, each
    decoding started.json, finished.json and podinfo.json again with the Go-faithful pure-Python
    decoder (testgrid_rule.read_json). Prow's podinfo.json holds the whole pod (85-120 KB on the
    dashboards' jobs, 2026-10-12): ~8 ms a decode here, 166 ms of CPU per build (perf_bench.py in
    review W6a: 450 builds primed in 75 s of CPU, one core). ci-kubernetes-build-fast (262 builds a
    day) has ~400 builds in a window without events (~70 s), ~2400 with a "changed" event 2 days
    before the build (~7 min), ~4000 with a "new" or "unknown" one (~11 min; an "unknown" event
    is any upload-job run whose config is not built yet, for every tab), per run and per job, on a
    GitHub runner with 50 minutes for the whole fetch; all of it in the pool's threads under the
    GIL. Nothing in a build's files changes between those calls."""

    def test_a_builds_podinfo_json_is_decoded_once_for_its_runs(self):
        pod = {"pod": {"metadata": {"name": "x"}, "spec": {"containers": [
            {"name": "test", "env": [{"name": f"VAR_{i}", "value": "v" * 48} for i in range(1200)]}]},
            "status": {"phase": "Succeeded"}}}
        b = self.build(30 * HOUR, finished=40 * MINUTE, podinfo=40 * MINUTE + 3, pod=pod)
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        timeline.files(b)  # (read once, as prime() does)
        decodes, real = [], frl.tr.read_json

        def counting(data, root):
            if root == "podinfo" and data is not None:
                decodes.append(len(data))
            return real(data, root)
        with mock.patch.object(frl.tr, "read_json", counting):
            timeline.runs(b)
        self.assertGreater(min(decodes), 80_000)
        self.assertLessEqual(len(decodes), 2)  # fails: 17 decodes of the same 90 KB


if __name__ == "__main__":
    unittest.main()
