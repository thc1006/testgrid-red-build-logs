"""Review of 2026-10-16: an outside review of 4ace4f3, and what testing it found. Each test
fails on 4ace4f3 for the reason its docstring gives; a test named test_control_* passes there
and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round25 -v
"""
import contextlib
import os
import sys
import time
import unittest
import zlib
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402
from test_integrity import DAY, ArchiveWorld, obj, snowflake  # noqa: E402


# ------------------------------------------------------------------ comparisons with GCS a run's deadline cut off

class CutOffComparisons(ArchiveWorld):
    """Six final builds TestGrid shows red: three in the run's seventh of the archive (compared
    with GCS, final or not), three not. A second run's deadline (1.8 s) comes while GCS has not
    answered for the first three (3.5 s)."""

    def setUp(self):
        super().setUp()
        t0 = int(time.time()) - 5 * DAY
        mine, other, t = [], [], t0
        while len(mine) < 3 or len(other) < 3:
            b = str(snowflake(t))
            (mine if zlib.crc32(f"job/{b}".encode()) % 7 == 0 else other).append(b)
            t -= 600
        self.mine, self.other = mine[:3], other[:3]
        builds = sorted(self.mine + self.other, key=frl.created, reverse=True)
        self.srv.table("dash", "t", {"query": "bucket/logs/job", "changelists": builds,
                                     "timestamps": [frl.created(b) * 1000 for b in builds],
                                     "tests": [{"name": "job.Overall", "statuses": [{"count": 6, "value": 12}],
                                                "messages": ["boom"] * 6}]})
        for b in builds:
            base = f"/bucket/logs/job/{b}"
            self.srv.route(base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": frl.created(b) + 600}))
            self.srv.route(base + "/build-log.txt", obj(b"log " + b.encode()))
        self.sevenths = mock.patch.object(frl, "gcs_sevenths", return_value={0})
        self.sevenths.start()
        self.addCleanup(self.sevenths.stop)
        self.assertEqual(self.fetch(), 0)  # (archives all six: final, as they finished 5 days ago)

    def keys(self, builds):
        return sorted(f"job/{b}" for b in builds)

    def slow(self, b):
        fast = obj(b"log " + b.encode())

        def serve(h):
            time.sleep(3.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                fast(h)
        return serve

    def test_control_comparisons_that_end(self):
        self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"already-archived": self.keys(self.mine + self.other)})
        self.assertEqual(run["serious_warnings"], [])

    def test_a_comparison_the_deadline_cut_off_is_not_checked(self):
        """run() gave a build whose worker the deadline cut off the outcome add_sightings names,
        "already-archived" for one this run did not update (fetch_red_logs.py:4828): a final build
        in the run's sevenths whose comparison with GCS did not end read as one compared and
        found unchanged, the run named no problem and exited 0."""
        for b in self.mine:
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", self.slow(b))
        code = self.fetch("--run-timeout-minutes", "0.03")
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"not-checked": self.keys(self.mine),  # fails: all "already-archived"
                                           "already-archived": self.keys(self.other)})
        self.assertEqual(code, 1)  # fails: 0
        self.assertEqual(run["serious_warnings"], [
            f"3 archived builds were not compared with GCS before the run's deadline ({', '.join(self.keys(self.mine))}): "
            "those not final yet are compared next run, the final ones by the next run that compares their seventh"])
        time.sleep(3.6)  # (the server's slow answers end before it closes)


if __name__ == "__main__":
    unittest.main()
