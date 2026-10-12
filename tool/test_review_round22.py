"""Review wave W8a, on b638092 (deployed as e6f70b7): a missed red when the column that pins a
build out vanishes itself. Each test fails on b638092 for the reason its docstring gives, but
the first, which shows the build red without the second column.

Run: cd tool && python -m unittest test_review_round22 -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r8 as k8  # noqa: E402
import test_mutation_killers_r9 as k9  # noqa: E402
import test_mutation_killers_w7 as w7  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
FAILING = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'


class APinnerThatMayNeverVanish(k8.Reads):
    """HoldModel.vanishing() took the pins of the higher-ID columns that sort older from pins(self,
    c), which gives nothing for a column that vanishes itself (`c.build in model.gone`). But a
    column that vanishes may also never vanish (an update appends the columns after its first only
    until its grace): then it stays in the grid, settles, and pins the build out until it is too
    old. X (pending 5 h, finished passing 12 h after it was made) is pinned out by C (made after X,
    started an hour before it, runs until 6 h). A failing junit file of X is written 15 days 3 h 30
    min after X was made: only a read when X is listed again, when C is too old (15 days 4 h after),
    sees it. With C alone, the port lists X again then, and finds it red. Add D (a higher ID than C,
    started before C, done in an hour): it settles at 4 h, pins C out (C runs then: C vanishes in the
    model) and pins X out too, until D is too old, 15 days and 10 s after X was made. Where C never
    vanishes, C still pins X out until it is too old, so X is listed again only then, and read: red.
    The port, taking C as pinning nothing, listed X again when D is too old, read it before the junit
    file was written, and decided not red. Now X may be listed again at the end of each pin."""

    def setUp(self):
        super().setUp()
        self.b = self.build(15 * DAY + 8 * HOUR, started=5 * HOUR, finished=12 * HOUR, podinfo=12 * HOUR,
                            junits=[("junit_01.xml", 15 * DAY + 3 * HOUR + 30 * MINUTE, FAILING)])
        made = frl.created(self.b)
        self.c = k9.buildruns(made + 1, started=4 * HOUR - 1, running_until=made + 6 * HOUR)
        self.d = k9.buildruns(made + 2, started=8, running_until=made + HOUR)

    def judged_with(self, *others):
        return self.judged(self.b, timeline=k9.Jobs(*others, now=self.now))

    def test_with_c_alone_x_is_red(self):
        self.assertEqual(self.judged_with(self.c), "red")

    def test_d_which_makes_c_vanish_does_not_make_x_not_red(self):
        self.assertNotEqual(self.judged_with(self.c, self.d), "not-red")



class NotWhileAColumnThatDoesNotVanishPinsItOut(unittest.TestCase):
    """(review W7a on the fix above) A time a column that vanishes itself stops pinning the build
    out is a time it may be listed again only if no column that does not vanish pins it out then:
    that one keeps the build out in every world. X (pending 5 h, runs on) vanishes once D1 settles;
    D2 pins it out until a day after D2 started (`back`); C, which D2 pins out of the grid, would
    pin X until a day after C started, inside the pin of E (a higher ID, made a day after X, done
    in half an hour, so settled from M+1d+3h30m on, a column that sorts newer, out of the grid)."""

    def test_the_end_of_a_pin_inside_one_of_a_column_that_stays_is_no_time_to_list_it(self):
        M, runs, INF = k9.M, w7.runs, float("inf")
        x = runs(M, started=5 * HOUR, running_until=INF)
        d1 = runs(M + 1, started=2 * HOUR - 1, running_until=M + 2.5 * HOUR)
        c = runs(M + 2, started=4 * HOUR - 2, running_until=M + 8 * HOUR)
        d2 = runs(M + 3, started=3 * HOUR - 3, running_until=M + 3.5 * HOUR)
        e = runs(M + DAY, started=MINUTE, running_until=M + DAY + 30 * MINUTE)
        model = frl.HoldModel([x, d1, c, d2, e], DAY)
        self.assertIn(c.build, model.gone)
        _, _, _, back, ends = model.vanishing(x.build, x.started)
        self.assertEqual((back, ends), (d2.started + DAY, [d2.started + DAY, e.started + DAY]))
        self.assertNotIn(c.started + DAY, ends)


if __name__ == "__main__":
    unittest.main()
