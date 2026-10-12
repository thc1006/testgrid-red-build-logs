"""Mutation-testing gaps of review W7c in what 0734e94 changed (a vanished build read again once
nothing pins it out: listed_again, HoldModel.vanishing's `back`, the listed-again holds of every
ID, the instant holds, and gcs_column's vanish block): each test is tagged with the W7c mutants of
0734e94 (NMF: mechanical; NSF: hand-made) it kills that the suite did not. The tests follow the
code as of 0734e94 (they fail on 1169263, which lacks it)."""
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r8 as k8  # noqa: E402  (module imports: their tests are not collected again here)
import test_mutation_killers_r9 as k9  # noqa: E402
import test_mutation_killers_w7 as w7  # noqa: E402
from test_integrity import snowflake  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
M = k9.M
runs = w7.runs


class ListedAgain(unittest.TestCase):
    """listed_again(pinned, back, old, runs_at): the update after `back` lists the build again, and
    reads it, within READ_GAP; if it runs then, back in the grid, it vanishes again while an older
    column pins it, and is listed again after each such pin; never once it is too old (`old`)."""

    PINNED = [(10, 20), (30, 40), (50, 60), (70, 80)]

    def test_within_a_gap_after_back(self):  # NMF0010, NMF0012, NSF003, NSF008
        self.assertEqual(frl.listed_again(self.PINNED, 20, 1000, lambda t: False), [(20, 20 + GAP)])

    def test_after_each_later_pin_if_it_runs(self):  # NMF0007, NMF0010, NMF0013-NMF0022, NSF003, NSF005-NSF007, NSF009
        got = frl.listed_again(self.PINNED, 40, 75, lambda t: t == 40)
        self.assertEqual(got, [(40, 40 + GAP), (60, 60 + GAP)])  # (not the pin ending at back, nor after old)
        self.assertEqual(frl.listed_again(self.PINNED, 40, 80, lambda t: True), [(40, 40 + GAP), (60, 60 + GAP)])
        self.assertEqual(frl.listed_again(self.PINNED, 40, 81, lambda t: True), [(40, 40 + GAP), (60, 60 + GAP),
                                                                               (80, 80 + GAP)])

    def test_never_once_too_old(self):
        self.assertEqual(frl.listed_again(self.PINNED, 40, 40, lambda t: True), [])
        self.assertEqual(frl.listed_again(self.PINNED, 40, 39, lambda t: True), [])


class BackWhenNoHigherColumnPins(unittest.TestCase):
    """HoldModel: out of the grid, a column of a higher ID that sorts newer (C2, made an hour after X,
    done quickly) keeps X out too once settled: on a tab of 15 days X is listed again only when C2
    is too old (`back`), which is after X itself is."""

    def test_back_is_when_the_newer_column_is_too_old(self):  # NMF0026, NMF0076, NSF010
        x = runs(M, running_until=M + 20 * HOUR)
        c1 = runs(M + 0.5, started=30, running_until=M + 5 * HOUR)
        c2 = runs(M + HOUR, started=MINUTE, running_until=M + HOUR + 5 * MINUTE)
        gone = frl.HoldModel([x, c1, c2], 15 * DAY).gone[x.build]
        back = c2.started + 15 * DAY
        self.assertEqual((gone[2], gone[3], gone[4]), ([(M + 5 * HOUR + GAP, c1.started + 15 * DAY)], back, [(back, INF)]))


class ListedAgainReadsAndHolds(unittest.TestCase):
    """A vanished build X listed again (C, a higher ID that started first, too old on a tab of one
    day): the update after reads it (HoldModel.reads); if it runs then, it holds the newer columns of
    every ID (the update lists every build after the highest ID kept, now lower than X's), and a
    read less than a gap before it is too old holds them for an instant (the next update may begin
    in time)."""

    def test_read_again_once_listed_though_done(self):  # NMF0010, NMF0031, NMF0035-NMF0038, NSF003, NSF011
        x = runs(M, started=4 * HOUR, running_until=M + 20 * HOUR)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        reads = frl.HoldModel([x, c], DAY).reads[x.build]
        self.assertEqual(reads[-1], (M + 25 * HOUR, M + 25 * HOUR + GAP))

    def test_a_lower_id_that_started_after_it_is_held(self):
        # NMF0026, NMF0031, NMF0041, NMF0042, NMF0045, NMF0046, NSF013
        until = M + 25 * HOUR + 10 * MINUTE
        x = runs(M, started=4 * HOUR, running_until=until)
        x.runs_at = lambda t, every=True: t <= (until - SPAN if every else until)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        low = snowflake(M - HOUR)  # (made before X, started after it)
        holds = k9.Jobs(x, c).grid(low, M + 6 * HOUR, 1)[0]
        self.assertEqual(holds, [(M - HOUR, until)])  # (as if X never vanished, review W7a-2; and listed again)
        self.assertIn((M + 25 * HOUR, until, x.started, -1), frl.HoldModel([x, c], DAY).entries)

    def test_listed_again_too_late_for_a_whole_hold(self):  # NMF0026, NMF0031, NMF0041, NMF0042, NSF014
        x = runs(M, started=4 * HOUR, running_until=M + 27 * HOUR)
        c = runs(M + 1, started=2 * HOUR + 20 * MINUTE, running_until=M + 3 * HOUR)
        back = c.started + DAY  # (more than a gap after X's start + 1 day + 20 minutes - a gap)
        b = snowflake(M + 6 * HOUR)
        # (as if X never vanished, review W7a-2: until a gap before it is too old; and listed again later)
        self.assertEqual(k9.Jobs(x, c).grid(b, M + 6 * HOUR + MINUTE, 1)[0],
                         [(M + 6 * HOUR, x.started + DAY + SPAN - GAP), (back, back)])

    def test_a_no_start_build_put_at_a_late_read_holds_for_an_instant(self):  # NSF015, NSF016
        # (G, too old for a tab of 1 day at M+1h21m, is read again by the update after a change by M; N,
        # put at its start then, holds B for an instant: the next update may begin before G is too old)
        g = runs(M - DAY + HOUR, started=MINUTE, reach=M - DAY + HOUR + GAP)
        n = runs(M - 2 * HOUR, nostart=True)
        changed = [(M - 30 * MINUTE, M - 20 * MINUTE, "changed")]
        holds = k9.Jobs(g, n).grid(snowflake(M - HOUR), M - HOUR + MINUTE, 1, changed)[0]
        self.assertEqual(holds, [(M - 30 * MINUTE, M - 30 * MINUTE)])


class ReadsOnceListedAgain(k8.Reads):
    """gcs_column of a vanished build listed again: its reads then are those of being listed again
    (a gap after `back`) and its own (an update after a change, here a new group's first one)."""

    def test_an_update_after_a_change_reads_it_too(self):  # NMF0130, NMF0138, NMF0143, NSF027, NSF029
        # (X pending 5 h, failed 8 h after it was made, out of the grid then: listed again when C, of a
        # higher ID, started 10 minutes after X was made, is too old for 15 days of results; a new group
        # 3 h later reads every build, X included)
        b = self.build(15 * DAY + 6 * HOUR, started=5 * HOUR, finished=8 * HOUR, passed=False, podinfo=8 * HOUR + 1)
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=10 * MINUTE - 0.5, running_until=made + HOUR), now=self.now)
        new = [(made + 15 * DAY + 3 * HOUR, made + 15 * DAY + 4 * HOUR, "new")]
        col = self.column(b, days=15, timeline=jobs, events=new)
        back = jobs.grid(b, made + 5 * HOUR, 15, new)[1][3]
        starts = {lo for _, windows in col.reads for lo, _, _ in windows}
        self.assertEqual((back, back in starts, new[0][0] in starts), (made + 10 * MINUTE + 15 * DAY, True, True))
        self.assertEqual(frl.verdict(col, "job")[0], "red")


if __name__ == "__main__":
    unittest.main()
