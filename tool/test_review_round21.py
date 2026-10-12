"""Review wave W7a: the read model of fetch_red_logs against TestGrid's own updater, run update after
update (pkg/updater at testgrid@v0.0.177-0.20260902201319-f15bfa4808a4, unmodified, under a fake
clock with testing/synctest, against a fake GCS whose objects appear when written: review W7a's
tg/cmd/w7aoracle). Each test asserts what TestGrid does and fails on 593f10e for the reason its
docstring gives (the last one asserts what the port does instead, and why). Paths below are
relative to the TestGrid module.

"An update at X" means an update of the job's test group that begins at X (production:
--wait=2h --group-timeout=20m --build-concurrency=10, pubsub on kubernetes-ci-logs). Each
scenario was replayed in the Go oracle (sim/repro7.py names it), which shows the build red in the
schedule the docstring gives.

Run: cd tool && python -m unittest test_review_round21 -v
"""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round9 as r9  # noqa: E402  (module imports: their tests are not collected here)
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_integrity import HOUR  # noqa: E402

MINUTE = 60
DAY = 24 * HOUR


# ------------------------------------------------------------------ W7a-1 (missed red, in ID order)
class AColumnCutByTheGraceIsReadByTheFollowUp(r9.JobWorld):
    """InflateDropAppend appends the columns after the first only until its grace, half of the group
    timeout (updater.go:576-580 and 680-699: 10 minutes in production); the first column alone may
    come until the timeout. When an update's reads wait (for the result reader lock that one group
    holds at a time while it reads more than 2 builds, read.go:221-223, or for the pool of 10
    readers every group shares), its first column comes after the grace and the others are not
    appended: the update writes its grid and returns "unprocessed", and Update re-queues the group
    at once (updater.go:420-423); that update reads them, up to 20 minutes later again. READ_GAP
    (2 h + 2 x 20 min) is the longest time from an update's start to the end of the next one (a
    hand-out dropped at the end of a 20-minute update, the next 2 h later); the follow-up adds a
    third 20 minutes. Here (sim/repro7.py grace_first_read, every step as the updater's code
    allows): U0 lists R1, R2, R3 at its start and its reads wait 9 minutes for the lock; A and B
    are made after U0's listing, each done within a minute; C's started.json, written 8 minutes
    into U0, makes pubsub hand the group out 30 s later (pubsub.go:204-209) while U0 runs: dropped
    (updater.go:376-380), and the next hand-out comes 2 h after it (util/queue/queue.go:256). U1
    lists A, B and C, its reads wait 19m58s: A's column (the first) is appended, B's and C's are
    not; the follow-up's reads wait 19m58s too and read B 2h46m after it was made, with its failing
    junit file, written at 2h44m: red, kept in the grid (W7a Go oracle). The port reads B until
    2h41m (its last running read, 61 s in, + READ_GAP; R1-R3 and A were done before B was made, C
    is newer: nothing holds it): not red, decided."""

    def test_a_build_whose_column_the_grace_cut_is_read_by_the_follow_up_update(self):
        for k in range(3):  # R1-R3 (U0 reads them)
            self.build(30 * HOUR + (30 - 5 * k) * MINUTE, started=10, finished=40, podinfo=41)
        self.build(30 * HOUR, started=10, finished=40, podinfo=41)  # A
        b = self.build(30 * HOUR - 2 * MINUTE, started=20, finished=60, podinfo=61,
                       junits=[("junit_01.xml", 2 * HOUR + 44 * MINUTE, r9.FAILING)])  # B
        self.build(30 * HOUR - 6 * MINUTE - 30, started=MINUTE, finished=MINUTE + 40, podinfo=MINUTE + 41)  # C
        col = self.column(b)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ------------------------------------------------------------------ W7a-2 (missed red, out of ID order)
class TheHigherColumnNeverSettlesWhileEveryUpdateIsSlow(r9.JobWorld):
    """HoldModel.vanishing takes a column of a higher ID that sorts older than the build (it started
    first) to be settled in the grid once its update read it done, and the build to drop out of the
    listing then (hintStarted, read.go:40-49). That column is in the grid only if an update appended
    it: an update appends the columns after the first only until its grace (updater.go:680-699).
    While A (pending 4 h, then running past its 24-hour deadline) is listed, it is the first column
    of every update (the lowest ID listed: readColumns reads in ID order, read.go:225-236), and B
    (a higher ID, started first, done in 20 minutes) the second; when every update's reads are slow
    (each read 9 minutes: B's column comes after the grace; the follow-ups as slow), B's column is
    never appended, never pins A out, and A, re-read at every update, is painted FAIL at its
    deadline (gcs.go:517-520) and kept: red (W7a oracle, repro7.py outoforder_slow; with fast reads
    it vanishes and is never red). The port drops A's reads after B settles (1169263), or reads it
    again only once B is too old (0734e94): not red. (Out-of-order starts were not seen on the
    dashboards' jobs, and the updater must stay this slow for a day: the least likely finding.)"""

    def test_a_build_does_not_vanish_while_the_higher_column_is_never_appended(self):
        a = self.build(17 * DAY, started=4 * HOUR, finished=4 * HOUR + 24 * HOUR + 40 * MINUTE,
                       podinfo=4 * HOUR + 24 * HOUR + 40 * MINUTE + 1)  # A: its pod starts 4 h after its ID
        self.build(17 * DAY - 10 * MINUTE, started=MINUTE, finished=20 * MINUTE, podinfo=20 * MINUTE + 1)  # B
        col = self.column(a)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "red")


# ------------------------------------------------------------------ W7a-3 (missed red, tab configs)
class AGroupThatReadsAnotherJobKeepsTheRecentColumns(r9.JobWorld):
    """A change to a test group's config never makes TestGrid read its columns again:
    InflateDropAppend uploads the bytes shrinkGridInline marshalled before it sets `grid.Config = tg`
    (updater.go:718-735), so no stored grid holds a Config, and reprocessColumn returns nil at once
    (`old.Config == nil`, updater.go:765-767): no RUNNING column at now - 7 days, no truncation at
    it, the hint unchanged (W7a oracle: config_probe_test.go TestConfigNotPersisted; the uploaded
    grid has no Config, and a changed group lists nothing again; the same grid with a Config stored
    in it does). So when the tab's own test group switches its GCS prefix to another job, its grid
    keeps EVERY column of this job (but the ones from its oldest running column up,
    truncateRunning), and lists the other prefix's builds after their highest hint; the tab shows
    those columns by the new config until they are too old (W7a oracle, repro7.py elsewhere_recent:
    B's column, 5 days old at the switch, in every grid after it). tab_verdict skips the Elsewhere
    era for a build made less than REPROCESS_DAYS before the switch (`made >= away - 7 days:
    continue`, W6a-4's fix, which took reprocessColumn to fire). Here the new config stops hiding
    row "t": TestGrid showed B red; the port says not-red."""

    def test_a_column_younger_than_7_days_stays_on_the_tab_after_its_group_reads_another_job(self):
        b = self.build(7 * DAY, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 50 * MINUTE, r9.FAILING)])  # red only on its row "t"
        hides_t = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^t$"))
        moved = tgc.TabRule(ttc.entry("", gcs_prefix="bucket/logs/other"))  # (same group "job", another job)
        change = self.now - 2 * DAY  # (b 5 days old then: its column is kept, shown for 10 days more)
        rule = {"eras": [(-math.inf, change, hides_t), (change - 30 * MINUTE, math.inf, moved)]}
        col = self.column(b)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # (by every row)
        self.assertEqual(frl.verdict(col, "job", shows=hides_t.shows)[0], "not-red")  # (by the old filters)
        self.assertEqual(frl.tab_verdict(col, "job", rule, frl.created(b), query="bucket/logs/job")[0],
                         "red")


# ------------------------------------------------------------------ W7a-4 (false red, kept)
class AChangedGroupIsReadAgainAllTheSame(r9.JobWorld):
    """The same reason as W7a-3: TestGrid never reads a changed group's columns again
    (reprocessColumn never fires: no stored grid holds a Config). relevant_events/gcs_column's seeds
    (and HoldModel's event spans, vanishing's cuts, Timeline.back) take a "changed" event as an
    update that reads again every build that started in the 7 days before it. Here B was done and
    passing at every read; its failing junit file was written 10 hours after its reads ended, and
    the group's config changed 10 hours later: TestGrid never reads B again (W7a oracle, repro7.py
    reprocess_dead: never red in its grid; the update after the change lists nothing). The port
    reads it again all the same, and archives it: TestGrid's code means to read those columns again,
    and does once a grid it stores holds its config; the port does not depend on its not doing so,
    at the price of a build TestGrid did not show red (README)."""

    def test_a_changed_group_is_read_again_though_testgrid_does_not_yet(self):
        b = self.build(30 * HOUR, started=MINUTE, finished=10 * MINUTE, podinfo=10 * MINUTE + 1,
                       junits=[("junit_01.xml", 20 * HOUR, r9.FAILING)])
        e = self.now - 10 * HOUR
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", b, 15, self.now, events=[(e, e + HOUR, "changed")])
        self.assertEqual(frl.verdict(self.column(b), "job")[0], "not-red")  # (without the event)
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # fails: red


if __name__ == "__main__":
    unittest.main()
