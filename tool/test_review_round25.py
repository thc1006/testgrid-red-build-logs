"""Review of 2026-10-16: an outside review of 4ace4f3, and what testing it found. Each test
fails on 4ace4f3 for the reason its docstring gives; a test named test_control_* passes there
and shows the scenario works otherwise.

Run: cd tool && uv run --frozen python -m unittest test_review_round25 -v
"""
import contextlib
import io
import os
import shutil
import sys
import tempfile
import time
import unittest
import zlib
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj, snowflake  # noqa: E402


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
            "the next run compares them"])
        time.sleep(3.6)  # (the server's slow answers end before it closes)


class SeventhsOfRunsNotDone(unittest.TestCase):
    """gcs_sevenths: which sevenths of the archive a run compares with GCS again, final or not.
    4ace4f3 took the days since the run before, whatever that run did: a run cut off before it
    compared its sevenths, or whose verification found a problem, counted as having compared them,
    so the next run went on to the next day's seventh and left them for a week."""

    DAY = 20_000

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.root)

    def run_at(self, hours, compared=True, verified="ok"):
        """A run folder started `hours` after 00:00 UTC of day DAY: whether its fetch compared all
        it was due to, and its verification's outcome (None: no verification.json)."""
        started = self.DAY * 86400 + hours * 3600
        rel = frl.new_run_dir(self.root, started)[1]
        frl.write_json(os.path.join(self.root, rel, "run.json"),
                       {"run": rel.rsplit("/", 1)[1], "started": started, "gcs_compared": compared})
        if verified is not None:
            frl.write_json(os.path.join(self.root, rel, "verification.json"), {"outcome": verified})

    def sevenths(self, hours):
        with mock.patch("time.time", return_value=(self.DAY + 30) * 86400):
            return frl.gcs_sevenths(self.root, self.DAY * 86400 + hours * 3600)

    def days(self, *days):
        return {(self.DAY + d) % 7 for d in days}

    def test_control_the_run_before_was_done(self):
        self.run_at(-18)
        self.assertEqual(self.sevenths(6), self.days(0))

    def test_control_the_first_run(self):
        self.assertEqual(self.sevenths(6), self.days(0))  # (nothing archived before it to compare again)

    def test_a_run_cut_off_leaves_its_day_to_the_next(self):
        self.run_at(-18)
        self.run_at(6, compared=False)  # (its deadline came before it compared its seventh)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_so_does_one_whose_verification_found_a_problem(self):
        self.run_at(-18)
        self.run_at(6, verified="problems")  # (such as "GCS changed since it was archived")
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_or_was_not_verified_to_the_end(self):
        self.run_at(-18)
        for i, outcome in enumerate(("timed out", "interrupted", "stopped", "incomplete", None)):
            self.run_at(6 + i, verified=outcome)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_a_run_not_done_on_the_day_of_the_last_done_one(self):
        # (the run done that day compared its seventh, but the later one may have found GCS changed since)
        self.run_at(1)
        self.run_at(5, compared=False)
        self.assertEqual(self.sevenths(30), self.days(0, 1))  # fails: {DAY + 1} only

    def test_no_run_done_in_the_week_before(self):
        self.run_at(-50, compared=False)
        self.run_at(-20, verified="problems")
        self.assertEqual(self.sevenths(6), set(range(7)))  # fails: {DAY - 1, DAY}
        self.run_at(-8 * 24)  # (one done more than a week before: all seven still)
        self.assertEqual(self.sevenths(6), set(range(7)))  # fails: {DAY - 1, DAY}


class ComparedAgainAfterACutOff(ArchiveWorld):
    """Six final builds TestGrid shows red, three in today's seventh of the archive, none in
    tomorrow's. Run 1 compares them and is verified ok. Run 2, an hour later, is cut off while
    GCS has not answered for the three (it compares today's seventh again: the day's), and one of
    them changed in GCS since run 1; its verification finds that. Run 3 is tomorrow. (The clock
    is today's noon, UTC, from setUp on.)"""

    def setUp(self):
        real, start = time.time, time.time()
        self.noon, self.shift = int(real() // 86400) * 86400 + 12 * 3600, 0
        clock = mock.patch("time.time", lambda: self.noon + self.shift + (real() - start))
        clock.start()
        self.addCleanup(clock.stop)
        super().setUp()
        day = self.noon // 86400
        t, mine, other = self.noon - 5 * DAY, [], []
        while len(mine) < 3 or len(other) < 3:
            b = str(snowflake(t))
            seventh = zlib.crc32(f"job/{b}".encode()) % 7
            (mine if seventh == day % 7 else other if seventh != (day + 1) % 7 else []).append(b)
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
        self.slow_on = False

    def keys(self, builds):
        return sorted(f"job/{b}" for b in builds)

    def slow(self, data, generation):
        fast = obj(data, generation)

        def serve(h):
            if self.slow_on:
                time.sleep(3.5)
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                fast(h)
        return serve

    def verify(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", "48",
                            "--record"])
        return code, frl.read_json(os.path.join(self.root, frl.run_dirs(self.root)[-1], "verification.json"))

    def test_a_build_left_by_a_run_cut_off_is_compared_by_the_next(self):
        """The fetch's run 3 compared tomorrow's seventh only (gcs_sevenths took the days since run 2,
        fetch_red_logs.py:3274-3278): the build GCS replaced, which run 2's verification found
        changed, was neither fetched again nor verified again, and run 3's verification passed."""
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.verify()[1]["outcome"], "ok")
        changed = self.mine[0]
        for b in self.mine:
            data, gen = (b"replaced " + b.encode(), "2") if b == changed else (b"log " + b.encode(), "1")
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", self.slow(data, gen))
        self.shift, self.slow_on = HOUR, True
        self.fetch("--run-timeout-minutes", "0.03")
        time.sleep(3.6)  # (the server's slow answers end)
        self.slow_on = False
        code, v = self.verify()
        self.assertEqual((code, v["outcome"]), (1, "problems"))
        self.assertTrue(any(f"job/{changed} build-log.txt: GCS changed since it was archived" in p for p in v["problems"]),
                        v["problems"])
        self.shift = DAY
        self.assertEqual(self.fetch(), 0)
        run3 = self.runs()[-1]
        self.assertEqual(run3["outcomes"], {"refreshed": [f"job/{changed}"],  # fails: all "already-archived"
                                            "already-archived": self.keys(self.mine[1:] + self.other)})
        self.assertEqual(self.read("build-log.txt", changed), b"replaced " + changed.encode())
        code, v = self.verify()
        day = self.noon // 86400
        self.assertEqual((code, v["outcome"], v["scope"]["sevenths"]), (0, "ok", sorted({day % 7, (day + 1) % 7})))


if __name__ == "__main__":
    unittest.main()
