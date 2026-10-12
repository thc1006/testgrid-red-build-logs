"""Review wave W7c, on 0734e94: the read model of fetch_red_logs against TestGrid's Go code at
the commit production runs (testgrid@v0.0.177-0.20260902201319-f15bfa4808a4; paths below are
relative to it). Each test asserts what TestGrid does and fails on 0734e94 for the reason its
docstring gives.

Run: cd tool && python -m unittest test_review_round18 -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_fetch_red_logs as tf  # noqa: E402  (module imports: their tests are not collected here)
import test_mutation_killers_w7 as w7  # noqa: E402
import test_review_round11 as r11  # noqa: E402
from test_integrity import HOUR  # noqa: E402
from test_review_round9 import POD_FAILED  # noqa: E402

MINUTE = 60
DAY = 24 * HOUR


# ------------------------------------------------------------------ W7c bug 2 (false red)
class ANewerColumnKeepsAVanishedBuildOut(r11.World):
    """A build X vanishes from the grid while it runs, once C1, a build of a higher ID whose pod
    started first, is settled there (HoldModel.vanishing: the update that cuts the grid at X keeps
    C1 and lists only the builds after C1's hint, updater.go:500-519, read.go:40-58). C2, made
    after X (a higher ID, and it sorts newer), is done and kept in the grid: once C1 is too old
    (InflateGrid, inflate.go:76), the update keeps C2, runs nothing and lists only the builds after
    C2's hint, so X stays out until it is too old itself (`back`: when C2 is). vanishing() found
    `back` from a model in which X itself still held the columns newer than it: first its running
    column, not yet cut at `gone`, then the holds it makes once listed again from that `back`, so
    C2 never pinned it out and X was listed again at C1's end. (X is red all the same: C1's column
    may never be appended, review W7a-2, and read as if it never vanished, X fails. The last two
    tests hold on 0734e94 too, X's column no longer running when C1 is too old.)"""

    def builds(self, m, x_fails):
        x = self.at(m, started=3 * MINUTE, **(dict(finished=8 * HOUR, passed=False, podinfo=8 * HOUR + 1,
                                                    pod=POD_FAILED) if x_fails else {}))
        self.at(m + MINUTE, started=MINUTE, finished=59 * MINUTE, podinfo=59 * MINUTE + 1)  # C1
        c2 = self.at(m + 2 * HOUR, started=MINUTE, finished=30 * MINUTE, podinfo=30 * MINUTE + 1)
        return x, int(frl.created(c2) + MINUTE)  # (and when C2 started)

    def back(self, x, days):
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        return timeline.grid(x, int(frl.created(x) + 3 * MINUTE), days)[1][3]

    def test_a_build_running_past_its_deadline_is_not_listed_again_on_a_tab_of_one_day(self):
        # (X never finishes: FAIL from its 24-hour deadline, m+24h3m; C1 is too old at m+24h2m,
        # C2 only at m+26h1m, after X is too old itself, at m+24h3m)
        x, c2_started = self.builds(self.now - 30 * HOUR, x_fails=False)
        self.assertEqual(self.back(x, 1), c2_started + DAY)
        self.assertEqual(self.judged(x, days=1), "red")

    def test_a_build_that_fails_out_of_the_grid_is_not_listed_again_on_a_tab_of_one_day(self):
        x, c2_started = self.builds(self.now - 30 * HOUR, x_fails=True)  # (it fails at m+8h, out of the grid)
        self.assertEqual(self.back(x, 1), c2_started + DAY)
        self.assertEqual(self.judged(x, days=1), "red")

    def test_a_build_running_past_its_deadline_is_not_listed_again_on_a_tab_of_two_days(self):
        # (C1 too old at m+48h2m, C2 at m+50h1m; X at m+48h3m. A column of a higher ID that sorts
        # newer lets X be listed again only while it is cut: test_review_round17)
        x, c2_started = self.builds(self.now - 49 * HOUR, x_fails=False)
        self.assertEqual(self.back(x, 2), c2_started + 2 * DAY)
        self.assertEqual(self.judged(x, days=2), "red")


# ------------------------------------------------------------------ W7c bug 5 (builds not listed)
class ListingAcrossADigit(unittest.TestCase):
    """list_build_ids listed from the first ID of `lo` (startOffset) even when that ID has fewer
    digits than the last: GCS compares names as strings, and the 19-digit IDs of builds made from
    2018-05-25 on sort before the 18-digit ones made before, so a listing from before that day
    dropped every build made after it."""

    def test_a_listing_from_before_ids_had_19_digits_has_the_builds_after(self):
        srv = tf.FakeServer()
        self.addCleanup(srv.close)
        asked, served = [], []
        before = w7.build_at(1_525_200_000_000)  # (2018-05-01: 18 digits)
        after = w7.build_at(1_527_253_900_000)  # (2018-05-25: 19 digits)
        srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = w7.offsets_listing(
            [f"logs/job/{b}/started.json" for b in (before, after)], asked, served)
        got = frl.list_build_ids(srv.url, "bucket/logs/job", 1_525_100_000, 1_527_254_000)
        self.assertEqual(sorted(got, key=int), [before, after])
        self.assertNotIn("startOffset", asked[0])


if __name__ == "__main__":
    unittest.main()
