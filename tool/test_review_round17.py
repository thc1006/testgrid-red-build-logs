"""Review wave W7 and the checks run with it, on 1169263: the read model of fetch_red_logs against
TestGrid's Go code at the commit TestGrid's production manifests deploy
(testgrid@v0.0.177-0.20260902201319-f15bfa4808a4; paths below are relative to it). Each test
asserts what TestGrid does and fails on 1169263 for the reason its docstring gives.

"An update at X" means an update of the job's test group that begins at X and reads its builds
then (production: --wait=2h --group-timeout=20m, pubsub on kubernetes-ci-logs).

Run: cd tool && python -m unittest test_review_round17 -v
"""
import email.utils
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round11 as r11  # noqa: E402  (module imports: their tests are not collected here)
from test_integrity import HOUR  # noqa: E402
from test_review_round9 import FAILING, PASSING  # noqa: E402

MINUTE = 60


def written(t):
    return {"Last-Modified": email.utils.formatdate(t, usegmt=True)}


# ------------------------------------------------------------------ the W6a simulator, seed 497 (missed red)
class ListedAgainOnceNoColumnPinsItOut(r11.World):
    """A build A whose column sorts newer than B's, a build of a higher ID (A's pod started hours
    after B's), vanishes from the grid once B is settled there (HoldModel.vanishing): the update
    that cuts the grid at A, which runs, keeps B and lists only the builds after B's hint
    (updater.go:500-519, read.go:40-58). But B does not pin A out for good: the first update that
    begins after B is too old drops it (InflateGrid, inflate.go:76), and when every other column of
    a higher ID than A's still runs (truncateRunning cuts them), it keeps no column of a higher ID
    and lists, and reads, A again (hintStarted). gcs_column kept only the reads of A outside the
    times it is pinned out and added none for that one, so a red column A gets then (here errored:
    its podinfo.json is not JSON, read.go:440-455) was never judged. While A is out of the grid, a
    column of a higher ID pins it out whatever its start, so A is listed again only once none
    does."""

    def test_a_vanished_build_is_read_again_when_the_column_that_pinned_it_out_is_too_old(self):
        m = self.now - 53 * HOUR - 25 * MINUTE
        self.at(m, started=0, finished=22 * MINUTE)  # P: done before
        # A: made at m+38m, its pod pending until m+4h10m; fails at m+4h32m, its podinfo.json not JSON
        a = self.at(m + 38 * MINUTE, started=3 * HOUR + 32 * MINUTE, finished=3 * HOUR + 54 * MINUTE, passed=False,
                    junits=[("junit_01.xml", m + 7 * HOUR + 23 * MINUTE, PASSING)])
        self.srv.route(f"/bucket/logs/job/{a}/podinfo.json",
                       (200, written(frl.created(a) + 3 * HOUR + 55 * MINUTE), b"not json"))
        # B: a higher ID, done at m+1h13m, its podinfo.json then too
        self.at(m + 50 * MINUTE, started=MINUTE, finished=23 * MINUTE, podinfo=23 * MINUTE)
        # C and D, of higher IDs, run past m+25h (C until m+27h38m; D starts at m+13h28m)
        self.at(m + 4 * HOUR + 50 * MINUTE, started=MINUTE, finished=22 * HOUR + 48 * MINUTE, passed=False,
                podinfo=22 * HOUR + 48 * MINUTE, junits=[("junit_01.xml", m + 28 * HOUR + 46 * MINUTE, PASSING)])
        self.at(m + 9 * HOUR + 5 * MINUTE, started=4 * HOUR + 23 * MINUTE, finished=27 * HOUR + 10 * MINUTE,
                junits=[("junit_01.xml", m + 36 * HOUR + 15 * MINUTE, FAILING)])
        # TestGrid (a tab of 1 day), an update every 2h20m from m-3h: A is read RUNNING at m+1h40m and
        # m+4h, and is gone from the grid at m+6h20m, B pinning it out. The update at m+25h drops B
        # (too old: it started at m+51m), cuts at C, which runs, keeps nothing, lists every build and
        # reads A: errored, red (TOOL_FAIL) on the tab from then on. The port: A's reads ended at
        # m+6h43m, not red.
        self.assertEqual(self.judged(a, days=1), "red")
        # (on a tab of 2 days, B is too old only at m+48h51m, and C and D, done by then, keep A out
        # until after it is too old itself: never listed again; but an update whose reads all come
        # after its grace appends only its first column, A's, review W7a-2: read as if it never
        # vanished, A is read after its podinfo.json is written, errored, red)
        self.assertEqual(self.judged(a, days=2), "red")


if __name__ == "__main__":
    unittest.main()
