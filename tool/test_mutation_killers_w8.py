"""Mutation-testing gaps of review W8a on b638092 (snap-fix5): the read model where a vanished
build is read, and holds, as if it never vanished too (HoldModel: run_end, _pass, `out` in
vanishing's model without the build), gcs_column's reads of a build that vanishes, and the
Elsewhere skip of tab_verdict. Each test is tagged with the W8a mutants it kills that the suite
did not."""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r8 as k8  # noqa: E402  (module imports: their tests are not collected again here)
import test_mutation_killers_r9 as k9  # noqa: E402
import test_mutation_killers_w7 as w7  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
M = k9.M
runs = w7.runs
FAILING = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'


# ------------------------------------------------------------ HoldModel: the model without the build

class TheModelWithoutIt(unittest.TestCase):
    """X (never finishes) vanishes once C1, a column of a higher ID that started before it, is
    settled (5 h after X was made); C2, of a higher ID too, started after X and done early, pins it
    out from the update after X's last read in the grid (a tab of one day)."""

    def setUp(self):
        self.c1 = runs(M + 1, started=29, running_until=M + 2 * HOUR)
        self.c2 = runs(M + 2, started=118, running_until=M + HOUR)

    def back(self, *others, x=None):
        x = x or self.x
        return frl.HoldModel([x, self.c1, *others], DAY).vanishing(x.build, x.started)[3]

    def test_PB2_it_is_not_read_there_after_gone(self):
        # (N, without a start, is put at X's start at X's reads: in the grid without X, there are none
        # after gone, so N holds C2 no longer than X does, and C2 pins X out until it is too old)
        self.x = runs(M, started=60, running_until=INF, reach=INF)
        n = runs(M + 0.5, nostart=True)
        self.assertEqual(self.back(self.c2, n), self.c2.started + DAY)

    def test_VO7_a_build_that_vanishes_itself_pins_nothing_out(self):
        # (X runs until 6 h after it was made; Y, newer than X and of a higher ID, runs until 10 h and
        # vanishes too, at 9 h, once C3 (started before Y, after X) is settled past X's hold: C3 pins X out
        # until it is too old; Y, settled at 13 h if it never vanished, pins nothing out)
        self.x = runs(M, started=60, running_until=M + 6 * HOUR)
        y = runs(M + 3, started=87, running_until=M + 10 * HOUR)
        c3 = runs(M + 4, started=76, running_until=M + 3 * HOUR)
        model = frl.HoldModel([self.x, self.c1, y, c3], DAY)
        self.assertIn(y.build, model.gone)
        self.assertEqual(model.vanishing(self.x.build, self.x.started)[3], c3.started + DAY)


class ListedAgainOnlyWhileNotTooOld(unittest.TestCase):
    """X (runs until 6 h after it was made) vanishes when C1 settles, 5 h after; C2, started a
    minute after X, pins it out until a minute after X is too old for a tab of one day, so no update
    lists X again; N, without a start, put at X's start at its reads, so holds no newer column D
    then."""

    def test_PB6_PB7_no_hold_from_a_listing_once_it_is_too_old(self):
        x = runs(M, started=60, running_until=M + 6 * HOUR)
        c1 = runs(M + 1, started=29, running_until=M + 2 * HOUR)
        c2 = runs(M + 2, started=118, running_until=M + HOUR)
        n = runs(M + 0.5, nostart=True)
        d = runs(M + 3, started=3 * HOUR, running_until=INF)  # (running on: it pins nothing out)
        model = frl.HoldModel([x, c1, c2, n, d], DAY)
        self.assertEqual(model.gone[x.build][3], c2.started + DAY)  # (back: past X's start + a day)
        self.assertEqual([(lo, hi) for lo, hi in model.holds(d.build, d.started) if lo >= x.started + DAY], [])


# ------------------------------------------------------------ gcs_column: when a build vanishes

class VanishesOrNot(k8.Reads):
    """gcs_column: a build vanishes, and is listed again (and read then: here a failing junit file
    written two days after it was made, after its own reads), if every read of it from `done` to
    `gone` finds it running (C1, a higher ID that started first, pins it out from `gone`)."""

    def judge(self, b, c1_started, c1_until=-INF, days=15):
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=c1_started - 0.5, running_until=c1_until), now=self.now)
        return jobs.grid(b, frl.created(b) + 60, days)[1], self.judged(b, days, timeline=jobs)

    def test_GC2_one_read_running_from_done_to_gone_is_listed_again(self):
        # (X pending 5 h, running without a start; C1, started 4 h after X was made, never seen running,
        # settles 40 minutes later: nothing else X's column may change at falls from `done` to `gone`)
        b = self.build(15 * DAY + 10 * HOUR, started=5 * HOUR, finished=10 * HOUR, podinfo=10 * HOUR,
                       junits=[("junit_01.xml", 2 * DAY, FAILING)])
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=4 * HOUR - 0.5), now=self.now)
        _, gone, _, back, _ = jobs.grid(b, made + 5 * HOUR, 15)[1]
        self.assertEqual((gone, back), (made + 4 * HOUR + 40 * MINUTE, made + 4 * HOUR + 15 * DAY))
        self.assertEqual(self.judged(b, timeline=jobs), "red")

    def test_GC5_one_done_before_gone_does_not_vanish(self):
        b = self.build(15 * DAY + 4 * HOUR, finished=4 * HOUR, podinfo=4 * HOUR, junits=[("junit_01.xml", 2 * DAY, FAILING)])
        made = frl.created(b)
        (_, gone, _, _, _), state = self.judge(b, 30, c1_until=made + 2 * HOUR)
        self.assertEqual((gone, state), (made + 5 * HOUR, "not-red"))

    def test_GC6_nor_one_not_read_from_done_to_gone(self):
        # (X, done 5 minutes after it was made, is read until a gap after; C1, made 5 h after X but with a
        # start before X's, never seen running, settles a gap after it was made: no read of X then)
        b = self.build(15 * DAY + 4 * HOUR, finished=5 * MINUTE, podinfo=5 * MINUTE,
                       junits=[("junit_01.xml", 2 * DAY, FAILING)])
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 5 * HOUR, started=-(5 * HOUR - 30)), now=self.now)
        self.assertEqual(jobs.grid(b, made + 60, 15)[1][1], made + 5 * HOUR + GAP)
        self.assertEqual(self.judged(b, timeline=jobs), "not-red")

    def test_GC4_nor_one_done_as_it_would_vanish(self):
        b = self.build(15 * DAY + 4 * HOUR, finished=5 * HOUR, podinfo=5 * HOUR, junits=[("junit_01.xml", 2 * DAY, FAILING)])
        made = frl.created(b)
        (_, gone, _, _, _), state = self.judge(b, 30, c1_until=made + 2 * HOUR)
        self.assertEqual((gone, state), (made + 5 * HOUR, "not-red"))


# ------------------------------------------------------------ tab_verdict: a group that reads another job

class ElsewhereSkip(unittest.TestCase):
    """While a tab's group reads another job, the tab keeps the columns of this one made before
    it left: not one made as it left."""

    def test_TE1_TE4_a_build_made_as_its_group_left_is_not_one_of_its_columns(self):
        away = 8 * DAY - 30 * MINUTE
        eras = [(-INF, 8 * DAY, w7.HERE_HIDES), (away, INF, w7.AWAY_SHOWS)]
        self.assertEqual(w7.judged(eras, away, made=away), "not-red")
        self.assertEqual(w7.judged(eras, away - MINUTE, made=away - MINUTE), "red")


if __name__ == "__main__":
    unittest.main()
