"""Review wave W7c, third pass, on 7caed27: the hold model's vanished builds and the holds of a
build without a start. Each test fails on b886a86 for the reason its docstring gives.

Run: cd tool && python -m unittest test_review_round20 -v
"""
import itertools
import math
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r9 as k9  # noqa: E402  (module imports: their tests are not collected here)
import test_mutation_killers_w7 as w7  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
M = k9.M
runs = w7.runs


class VanishedBuildsTakenTogether(unittest.TestCase):
    """HoldModel worked out the builds that vanish one after another, each from the model as the
    ones before had left it: the first did not see the later ones gone, so a build that vanished
    too (Y) still held, there, the column that pins X out (C2), and X's `back` depended on which
    of them the model took first, and differed from what vanishing() gave gcs_column for the same
    model. Now each is worked out from the same model, again with them gone until nothing changes,
    and gcs_column takes what the model took. (W7c's VN18: X, which never finishes, vanishes once
    C1 is settled; Y, of the lowest ID, which never finishes either, once C0 and C1 are. The second
    test holds on b886a86 too: the model takes the builds in an order of its own.)"""

    def builds(self):
        x = runs(M, started=60, running_until=INF)
        c2 = runs(M + 2, started=118, running_until=M + HOUR)
        c1 = runs(M + 1, started=29, running_until=M + 30 + DAY - (GAP + 30 * MINUTE) - GAP)
        y = runs(M - 10, started=110, running_until=INF)
        c0 = runs(M - 5, started=50, running_until=M + HOUR)
        return x, [x, c1, c2, y, c0], c0

    def test_the_model_and_gcs_column_take_one_back(self):
        # (Y may never vanish either, review W7a-2: as it would, it holds C2 until it is too old, and X
        # comes back once C1 is; with Y gone, once C0 is)
        x, builds, c0 = self.builds()
        model = frl.HoldModel(builds, DAY)
        c1 = builds[1]
        self.assertEqual(model.gone[x.build][3], c1.started + DAY)
        self.assertEqual(model.vanishing(x.build, x.started), model.gone[x.build])
        self.assertEqual(model.vanish(x.build, x.started), model.gone[x.build])

    def test_whatever_order_the_builds_come_in(self):
        x, builds, _ = self.builds()
        first = frl.HoldModel(builds, DAY).gone
        for order in itertools.permutations(builds):
            self.assertEqual(frl.HoldModel(list(order), DAY).gone, first)


class NoStartHeldUntilTheHolderIsTooOld(unittest.TestCase):
    """A build without a start (N) is read again while a column of a lower ID runs (R), as any
    newer column is: but not by an update that begins once R is too old for the tab, which drops
    R before it cuts the grid (InflateGrid). Its hold ended a gap later than that of a build with
    a start (held_by), so N was read again for a gap more than TestGrid may (W7c's SF057)."""

    def test_a_hold_by_a_column_that_runs_until_it_is_too_old(self):
        r = runs(M, started=60, running_until=INF)
        n = runs(M + 10 * MINUTE, nostart=True)
        holds = frl.HoldModel([r, n], DAY).holds(n.build, None)
        self.assertEqual(holds, [(frl.created(n.build), r.started + DAY + SPAN - GAP)])


if __name__ == "__main__":
    unittest.main()
