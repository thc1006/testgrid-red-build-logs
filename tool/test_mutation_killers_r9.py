"""Mutation-testing gaps of review W5c (round 9 of the mutation killers), in the code changed
since 76191a2: when a tab may have shown each read of a build's column (gcs_column's .reads,
verdict's and tab_verdict's windows, shown_in, shown_during), TestGrid's reads over time
(made_columns, possible_reads, BuildRuns, Timeline.older_end and .vanishes), TestGrid's config
over time (ConfigHistory, events, the lacking tabs and the warnings of run()), the upload job's
runs (testgrid_config: decode_uploads, plan, merge, load, main, the containers runs ran),
testgrid_config.sh, testgrid_rule._before, oracle/relevant_diff.bazel and oracle/drift.sh. Each
test is tagged with the W5c mutants (run on 05793d9) it kills that the suite did not: MF, MC, MR,
MX and MD ones are mechanical mutants of fetch_red_logs.py, testgrid_config.py, testgrid_rule.py,
cross_check.py and oracle/relevant_diff.py; S ones hand-made (SF and SG fetch_red_logs.py, SC
testgrid_config.py, SR testgrid_rule.py, SX cross_check.py, SD oracle/relevant_diff.py, SS
testgrid_config.sh, ST oracle/drift.sh). The tests follow the code as it is now."""
import contextlib
import io
import json
import math
import os
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected again here)
import test_mutation_killers_r8 as k8  # noqa: E402
import test_review_round9 as r9  # noqa: E402
import test_review_round10 as r10  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_integrity import snowflake  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
T0 = 1_790_000_000  # (a fixed time for columns made here)


def junit(*failing, passing=()):
    """A junit file with these failing and passing tests."""
    cases = "".join(f'<testcase name="{n}"><failure/></testcase>' for n in failing) + \
        "".join(f'<testcase name="{n}"/>' for n in passing)
    return f"<testsuite>{cases}</testsuite>".encode()


def column(*failing, finished=True, open_=False, now=T0 + DAY, reads=None):
    """A column of a build that started at T0 and passed by finished.json (so only its junit rows
    can be red), whose junit file fails `failing` (passes "ok" otherwise); finished=False: it is
    still running. reads: [(other columns or "self", [(from, to)])] as gcs_column's .reads."""
    started = json.dumps({"timestamp": T0}).encode()
    done = json.dumps({"timestamp": T0 + 600, "passed": True, "result": "SUCCESS"}).encode() if finished else None
    pod = json.dumps({"pod": {"status": {"phase": "Succeeded"}}}).encode()
    xml = junit(*failing, passing=("ok",))
    col = tr.column("job", started, done, pod, [("logs/job/1/artifacts/junit_01.xml", len(xml), xml)], T0 - 15 * DAY,
                    T0 + 700)
    col.open, col.now = open_, now
    if reads is not None:  # (a window (from, to) is of a read made at `from`, as gcs_column's (from, to, read by))
        col.reads = [(col if c == "self" else c, [w if len(w) == 3 else (*w, w[0]) for w in windows])
                     for c, windows in reads]
        col.alternatives = [c for c, _ in col.reads[1:]]
    return col


def shown_in(windows, lo, hi):
    """Whether a column read (and not read again) with these windows was shown at some time in
    [lo, hi], as shown_during tells (shown_in, before it was folded into shown_during)."""
    return frl.shown_during(column(reads=[("self", windows)], now=-INF), lo, hi)


def rows(verdict):
    """(state, the red rows' names) of a verdict."""
    return verdict[0], sorted(c["test"] for c in verdict[1])


# ------------------------------------------------------------ when a tab may have shown a read

class ShownIn(unittest.TestCase):
    """shown_during, for a column not read again: whether a column shown in these windows was
    shown at some time in [lo, hi] (closed at both ends)."""

    def test_any_window_that_meets_the_span(self):  # MF094, MF315, SG01
        self.assertTrue(shown_in([(0, 10), (100, 200)], 150, 160))
        self.assertFalse(shown_in([(0, 10), (100, 200)], 20, 30))

    def test_a_window_before_or_after_the_span_does_not(self):  # MF315, SG01
        self.assertFalse(shown_in([(0, 10)], 20, 30))
        self.assertFalse(shown_in([(40, 50)], 20, 30))

    def test_touching_ends_count(self):  # MF451, MF453
        self.assertTrue(shown_in([(0, 20)], 20, 30))
        self.assertTrue(shown_in([(30, 40)], 20, 30))


class ShownDuring(unittest.TestCase):
    """shown_during: a read shown then, or (while TestGrid may still read the column) a read it
    may make after now, if the span reaches past now."""

    def test_no_window_meets_the_span(self):  # MF030, MF191, MF192, MF315, SG01
        col = column(reads=[("self", [(100, 200)])], now=250)
        self.assertFalse(frl.shown_during(col, 300, 400))
        self.assertTrue(frl.shown_during(col, 150, 400))

    def test_a_read_after_now_may_still_show_it(self):  # MF099, MF317, SG04
        col = column(reads=[("self", [(100, 200)])], open_=True, now=250)
        self.assertTrue(frl.shown_during(col, 300, 400))

    def test_but_not_in_a_span_that_ends_by_now(self):  # MF030, MF099, MF191, MF193, MF316, MF317, SG03, SG04
        col = column(reads=[("self", [(300, 400)])], open_=True, now=250)
        self.assertFalse(frl.shown_during(col, 100, 250))
        self.assertTrue(frl.shown_during(col, 100, 251))


class VerdictInAWindow(unittest.TestCase):
    """verdict(during=(from, to)): only the reads a tab may have shown then count; undecided while
    TestGrid may read the column again, if the span reaches past now."""

    def test_no_read_shown_then_but_one_may_come(self):  # MF087
        col = column(reads=[("self", [(100, 200)])], open_=True, now=250)
        self.assertEqual(frl.verdict(col, "job", during=(300, 400)), ("undecided", []))

    def test_a_span_that_ends_by_now_is_decided(self):  # MF186
        col = column(reads=[("self", [(0, 1000)])], open_=True, now=250)
        self.assertEqual(frl.verdict(col, "job", during=(0, 250)), ("not-red", []))
        self.assertEqual(frl.verdict(col, "job", during=(0, 251)), ("undecided", []))

    def test_red_rows_of_every_read_shown_then_when_the_last_is_not(self):  # MF088, SF99
        a, b = column("a"), column("b")
        col = column(reads=[("self", [(300, INF)]), (a, [(100, 200)]), (b, [(150, 260)])])
        self.assertEqual(rows(frl.verdict(col, "job", during=(100, 260))), ("red", ["a", "b"]))
        # (the last read red: its red rows, as TestGrid shows them from then on)
        col = column("c", reads=[("self", [(300, INF)]), (a, [(100, 200)])])
        self.assertEqual(rows(frl.verdict(col, "job", during=(100, 400))), ("red", ["c"]))

    def test_a_column_gcs_column_did_not_read_counts_its_alternatives(self):  # SF95
        col = column()
        col.alternatives = [column("a")]
        self.assertEqual(rows(frl.verdict(col, "job")), ("red", ["a"]))


class ConfigInForceWhenShown(unittest.TestCase):
    """config_verdict and tab_verdict judge each config of a tab by the reads the tab may have
    shown while it was in force: here a red read (of row a) shown from 100 to 200, then a passing
    one from 300 on."""

    def setUp(self):
        self.col = column(reads=[("self", [(300, INF)]), (column("a"), [(100, 200)])])

    def test_each_way_config_verdict_judges_keeps_the_window(self):  # MF315, SG01, SG11, SG14
        later = (300, 400)
        self.assertEqual(frl.config_verdict(self.col, "job", {"rows": None}, T0, later), ("not-red", []))
        self.assertEqual(frl.config_verdict(self.col, "job", {"shows": lambda row: True}, T0, later), ("not-red", []))
        unreliable = {"shows": lambda row: row != "a", "unreliable_until": INF}
        self.assertEqual(frl.config_verdict(self.col, "job", unreliable, T0, later), ("not-red", []))
        # (and the read shown then is red by each)
        self.assertEqual(rows(frl.config_verdict(self.col, "job", {"rows": None}, T0, (100, 200))), ("red", ["a"]))

    def test_filters_that_run_out_of_time_fall_back_to_the_rows_shown_then(self):  # MF315, SG01, SG13
        # (a read red on b shown from 300 to 400, between the red read of a and the last, passing)
        col = column(reads=[("self", [(500, INF)]), (column("b"), [(300, 400)]), (column("a"), [(100, 200)])])

        def slow(row):
            raise tgc.FilterTooSlow("too slow")
        self.assertEqual(rows(frl.config_verdict(col, "job", {"shows": slow}, T0, (300, 400))), ("red", ["b"]))

    def eras(self, first, second):
        return {"eras": [(0, 250, first), (300, 1000, second)], "days": 15}

    def test_the_config_before_hid_the_red_row(self):  # MF315, SG01, SG08
        hides, shows = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^a$")), tgc.TabRule(ttc.entry(""))
        self.assertEqual(frl.tab_verdict(self.col, "job", self.eras(hides, shows), 0), ("not-red", []))
        self.assertEqual(rows(frl.tab_verdict(self.col, "job", self.eras(shows, hides), 0)), ("red", ["a"]))

    def test_a_config_in_force_only_when_no_read_was_shown_does_not_count(self):  # MF030, MF198, MF315, SG01, SG09
        col = column(reads=[("self", [(100, 200)])], now=5000)
        rule = {"eras": [(0, 250, tgc.TabRule(ttc.entry(""))), (300, 1000, frl.UNKNOWN)], "days": 15}
        self.assertEqual(frl.tab_verdict(col, "job", rule, 0), ("not-red", []))
        rule = {"eras": [(0, 250, tgc.TabRule(ttc.entry(""))), (150, 1000, frl.UNKNOWN)], "days": 15}
        self.assertEqual(frl.tab_verdict(col, "job", rule, 0), ("undecided", []))


# ------------------------------------------------------------ Timeline: older columns and twins

M = T0  # (when the build judged, B, was made)
B = snowflake(M)
S = M + 60  # (when it started)
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
REPROCESS_END = 40 * MINUTE  # (the updates that begin less than 20 minutes after a build started end then)


LATER = M + 10 * HOUR  # (a Timeline's now)


class Jobs(frl.Timeline):
    """A Timeline of a job's builds given by their BuildRuns (no GCS): builds() lists those made in
    the window asked for, as GCS would, and keeps the windows asked for."""

    def __init__(self, *runs, now=LATER):
        super().__init__("http://gcs.invalid", "bucket/logs/job", now)
        self.of = {r.build: r for r in runs}
        self.asked = []

    def builds(self, lo, hi):
        self.asked.append((lo, hi))
        return [b for b in sorted(self.of, key=int) if lo <= frl.created(b) <= hi]

    def runs(self, build):
        return self.of[build]

    def held_until(self, build, started, events=(), days=15):
        """The latest the grid may hold a running column older than the build's (see
        Timeline.grid), -INF when it holds none after the build was made."""
        holds, _ = self.grid(build, started, days, events)
        return max((hi for _, hi in holds), default=-INF)

    def vanishes(self, build, started, events=(), days=15):
        """(done, gone) when the build's column vanishes (see HoldModel.vanishing), else None."""
        _, vanish = self.grid(build, started, days, events)
        return None if vanish is None else vanish[:2]


def buildruns(made, started=60, running_until=-INF, reach=None, nostart=False):
    """A build's BuildRuns: made then, started `started` seconds after (None: no usable start;
    nostart: none at all), running until running_until (-INF: no read found it running), read of
    its own accord until reach: by default as a BuildRuns has it, a gap after it was made and
    after it last ran, and 40 minutes after it started (one still running gives it)."""
    start = None if started is None or nostart else made + started
    if reach is None:  # (one still running: read until a gap after the Timeline's now)
        reach = max([made + GAP, *([start + REPROCESS_END] if start else []),
                     (running_until if running_until < INF else LATER) + GAP])
    return types.SimpleNamespace(build=snowflake(made), started=start, nostart=nostart, running_until=running_until,
                                 reach=reach, good_until=INF,
                                 runs_at=lambda t, every=True: t <= running_until)  # (it runs until then)


class HowFarBack(unittest.TestCase):
    """Timeline.window lists the job's builds made from 37 h before the build (a build's
    column may run that long after its ID was made: OLDER) to 6 h after (PENDING); further back
    for a change of the group whose update may read the build again, or start a chain of holds
    that reaches it (its builds of 7 days before the update, or of its days of results for a new
    group, and 6 h), and a gap more for each build without a start; 21 days back at most.
    (Ported from older_end's window, which the hold model replaced.)"""

    def asked(self, *runs, events=(), days=15):
        t = Jobs(*runs)
        t.window(M, events, days * 86400, S, B)
        return [(M - lo, hi - M) for lo, hi in t.asked]

    def test_without_events(self):  # MF150, MF405, MF679, SF02, SF03
        self.assertEqual(self.asked(), [(frl.OLDER, 6 * HOUR)])

    def test_after_a_change(self):
        # MF149, MF150, MF274, MF402-MF405, MF518-MF520, MF612-MF614, MF668, MF671, MF672, MF705, MF706, SF37, SF38, SF62
        changed = [(M - 3 * DAY, M - HOUR, "changed")]  # (its update may begin as the build is made)
        self.assertEqual(self.asked(events=changed, days=5), [(10 * DAY + 6 * HOUR, 6 * HOUR)])
        # (one that ended well before the build was made, with no builds without a start: no chain reaches it)
        self.assertEqual(self.asked(events=[(M - 3 * DAY, M - 2 * DAY, "changed")], days=5),
                         [(frl.OLDER, 6 * HOUR)])

    def test_after_a_new_group(self):
        # MF149, MF150, MF274, MF402-MF405, MF518-MF520, MF612, MF613, MF615, MF668-MF670, MF703, MF704, SF36, SF38, SF62
        new = [(M - 3 * DAY, M - HOUR, "new")]
        self.assertEqual(self.asked(events=new, days=5), [(8 * DAY + 6 * HOUR, 6 * HOUR)])

    def test_at_most_the_days_of_results_and_a_week(self):
        # MF040-MF043, MF061, MF149, MF150, MF274, MF404, MF518-MF520, MF615, MF668-MF670, MF704, SF07-SF09, SF36, SF39
        # (and 6 h: 22 days 6 h on a tab of 15 days; 21 days on a shorter one)
        new = [(M - 10 * DAY, M - HOUR, "new")]
        self.assertEqual(self.asked(events=new), [(22 * DAY + 6 * HOUR, 6 * HOUR)])
        self.assertEqual(self.asked(events=new, days=5), [(15 * DAY + 6 * HOUR, 6 * HOUR)])
        self.assertEqual(self.asked(events=[(M - 30 * DAY, M - HOUR, "new")], days=5), [(21 * DAY, 6 * HOUR)])

    def test_a_gap_more_for_each_build_without_a_start(self):
        # MF150, MF275, MF405, MF522, MF617-MF619, MF679, MF737-MF739, SF02, SF03, SF40, SF41
        runs = [buildruns(M - HOUR, nostart=True), buildruns(M - 2 * HOUR, nostart=True), buildruns(M - 3 * HOUR, started=None)]
        self.assertEqual(self.asked(*runs), [(frl.OLDER, 6 * HOUR), (frl.OLDER + 2 * GAP, 6 * HOUR)])

    def test_still_at_most_that(self):
        # MF040-MF043, MF150, MF275, MF405, MF522, MF617-MF619, MF679, MF737, SF02, SF03, SF07-SF09, SF40, SF42
        runs = [buildruns(M - HOUR - i * MINUTE, nostart=True) for i in range(200)]
        self.assertEqual(self.asked(*runs), [(frl.OLDER, 6 * HOUR), (22 * DAY + 6 * HOUR, 6 * HOUR)])


class OlderEnd(unittest.TestCase):
    """The holds of a build (Timeline.grid): the latest an older column (by start; of one start,
    by a higher ID) may run, and the holds of builds without a start: one is put at the start
    of the highest good column below it at each read of that column after it was made, and
    holds the newer columns of higher IDs until then. (Ported from older_end, which the hold
    model replaced: a hold that ends before the build was made holds nothing.)"""

    def test_the_latest_of_all_that_started_before(self):  # MF680
        runs = [buildruns(M - 10 * HOUR, running_until=M - 5 * HOUR), buildruns(M - 8 * HOUR, running_until=M + 4 * HOUR),
                buildruns(M - 6 * HOUR, running_until=M - 3 * HOUR)]
        self.assertEqual(Jobs(*runs).held_until(B, S), M + 4 * HOUR)

    def test_of_the_same_start_a_higher_id_is_older(self):  # SF43, SF44
        start = M + 2 * HOUR  # (B, and builds made half an hour before and after it, all pending until then)
        higher = buildruns(M + 30 * MINUTE, started=start - M - 30 * MINUTE, running_until=M + 3 * HOUR)
        lower = buildruns(M - 30 * MINUTE, started=start - M + 30 * MINUTE, running_until=M + 7 * HOUR)
        self.assertEqual(Jobs(higher, lower).held_until(B, start), M + 3 * HOUR)

    def test_one_made_after_that_read_does_not(self):  # MF418, SF55
        good = buildruns(M - 5 * HOUR, running_until=M - 4 * HOUR)
        self.assertEqual(Jobs(good, buildruns(M - 30 * MINUTE, nostart=True)).held_until(B, S), -INF)

    def test_a_build_without_a_start_is_held_by_lower_ids(self):  # MF283, MF420, SF56
        good = buildruns(M - 5 * HOUR, running_until=M + 4 * HOUR)
        shorter = buildruns(M - 7 * HOUR, running_until=M + 2 * HOUR)
        higher = buildruns(M + HOUR, running_until=M + 10 * HOUR)
        self.assertEqual(Jobs(good, shorter, higher).held_until(B, None), M + 4 * HOUR)

    def test_its_own_lack_of_a_start_does_not_look_further_back(self):  # MF150, MF405, MF521, MF679
        # (a build made 38 h before B, which ran until 15 h after: outside B's 37 h)
        old = buildruns(M - 38 * HOUR, running_until=M + 15 * HOUR)
        jobs = Jobs(old, buildruns(M, nostart=True))
        self.assertEqual(jobs.held_until(B, None), -INF)
        self.assertEqual([M - lo for lo, _ in jobs.asked][:1], [frl.OLDER])

    def test_holds_chain(self):  # MF529-MF531, MF622, SF02, SF03, SF49, SF51
        # (N1 is put at G1's start while G1 is read; G2 is read again a gap after that; so N2, made
        # after G2's own reads, is held until then: a build made then is held a moment)
        g1, n1, g2 = buildruns(M - 6 * HOUR), buildruns(M - 5.5 * HOUR, nostart=True), buildruns(M - 5 * HOUR)
        n2 = buildruns(M - HOUR, nostart=True)
        b = snowflake(M - 50 * MINUTE)
        self.assertEqual(Jobs(g1, n1, g2, n2).held_until(b, M - 49 * MINUTE), M - 6 * HOUR + 2 * GAP)

    def test_a_column_is_read_again_a_gap_after_an_older_one_ran(self):  # MF529-MF531, MF622, SF02, SF03, SF51
        # (G2, of a lower ID, started after G0: read again a gap after G0 ran; N, between them by ID, is
        # held until then)
        g0 = buildruns(M - HOUR, started=MINUTE, running_until=M + 2 * HOUR)
        g2 = buildruns(M - 3 * HOUR, started=150 * MINUTE)
        self.assertEqual(Jobs(g0, g2, buildruns(M - 2 * HOUR, nostart=True)).held_until(B, S), M + 2 * HOUR + GAP)

    def test_its_own_reads_count_not_its_running(self):  # SF46
        # (G started after it was made and still runs: N, put at G's start, holds the columns newer than
        # that until G's last read so far, not forever)
        g, n = buildruns(M - 3 * HOUR, started=5 * HOUR, running_until=INF), buildruns(M - HOUR, nostart=True)
        self.assertEqual([(lo, hi) for lo, hi, _, _ in frl.HoldModel([g, n], 15 * DAY).entries],
                         [(frl.created(n.build), LATER + GAP)])
        # (B, started before G's start, sorts older than N, and than G: not held)
        self.assertEqual(Jobs(g, n).held_until(B, S), -INF)

    def test_an_update_after_a_change_reads_older_builds_again(self):  # MF685, MF686, SF54
        good = buildruns(M - 5 * HOUR)  # (read until a gap after it was made, 2 h 20 min before M)
        changed = [(M - 2.5 * HOUR, M - 1.5 * HOUR, "changed")]
        b = snowflake(M - 1.5 * HOUR)  # (made after N, while that update may run)
        jobs = Jobs(good, buildruns(M - 2 * HOUR, nostart=True))
        self.assertEqual(jobs.held_until(b, M - 1.5 * HOUR + 60, changed), M - 1.5 * HOUR + SPAN)

    def test_that_started_less_than_7_days_before_it(self):
        # MF149, MF274, MF402-MF404, MF413, MF418, MF518-MF520, MF532, MF534, MF626, MF669-MF672, MF682, MF683, MF685, MF686,
        # MF706, MF740, MF741, SF53-SF55, SF62
        # (G started 5 h after it was made: 7 days after its start is 7 days 5 h after it was made; each
        # build judged is made just after the one without a start)
        good = buildruns(M - 8 * DAY, started=5 * HOUR, reach=M - 7 * DAY)

        def held(n_made, events):
            return Jobs(good, buildruns(n_made, nostart=True)).held_until(snowflake(n_made + 30 * MINUTE),
                                                                        n_made + 31 * MINUTE, events)
        changed = [(M - 8 * DAY + 7 * DAY + 2 * HOUR, M - 21 * HOUR, "changed")]
        self.assertEqual(held(M - 21.5 * HOUR, changed), M - 21 * HOUR + SPAN)
        changed = [(M - 8 * DAY + 7 * DAY + 6 * HOUR, M - 17 * HOUR, "changed")]
        self.assertEqual(held(M - 17.5 * HOUR, changed), -INF)
        new = [(M - 8 * DAY + 7 * DAY + 6 * HOUR, M - 17 * HOUR, "new")]  # (a new group: every build again)
        self.assertEqual(held(M - 17.5 * HOUR, new), M - 17 * HOUR + SPAN)
        changed = [(good.started + 7 * DAY, M - 17 * HOUR, "changed")]  # (begins just 7 days after its start: no)
        self.assertEqual(held(M - 17.5 * HOUR, changed), -INF)

    def test_a_hold_reaches_a_build_made_as_the_read_may_last_happen(self):  # MF536
        good = buildruns(M - 5 * HOUR)
        t = M - 5 * HOUR + GAP
        n = buildruns(t, nostart=True)
        b = str(int(n.build) + 1)  # (made in the same millisecond, after it)
        self.assertEqual(Jobs(good, n).held_until(b, t + 60), t)


class Vanishes(unittest.TestCase):
    """HoldModel.vanishing: with builds of a higher ID that sort older (here, of the same start;
    made at most 6 h after it), the build's column vanishes once one of them is settled in the
    grid (no longer running, read since), unless a column older than it runs after that:
    (done, gone), gone a gap later. (Ported from Timeline.vanishes.)"""

    def twin(self, made, running_until, start=S):
        return buildruns(made, started=start - made, running_until=running_until)

    def test_the_first_settled_twin_decides(self):  # MF160, MF288, MF542, MF543, MF635, SF02, SF03, SF65, SF66, SF68, SF70
        twins = [self.twin(M + 0.25, M + 5 * HOUR), self.twin(M + 0.5, M + 3 * HOUR)]  # (made in the same second)
        self.assertEqual(Jobs(*twins).vanishes(B, S), (M + 3 * HOUR, M + 3 * HOUR + GAP))

    def test_a_twin_of_a_lower_id_does_not(self):  # MF541, SF64
        self.assertIsNone(Jobs(self.twin(M - 0.5, M + 3 * HOUR)).vanishes(B, S))

    def test_one_made_up_to_6_hours_later(self):  # MF288, SF02, SF03, SF63, SF68
        # (B pending 5.5 h, a build made 5 h after it 30 minutes: both start then)
        start = M + 5.5 * HOUR
        twin = self.twin(M + 5 * HOUR, M + 8 * HOUR, start)
        self.assertEqual(Jobs(twin).vanishes(B, start), (M + 8 * HOUR, M + 8 * HOUR + GAP))

    def test_not_while_it_may_still_run(self):  # MF072, MF161, MF162, SF69
        self.assertIsNone(Jobs(self.twin(M + 0.5, INF)).vanishes(B, S))

    def test_one_that_never_ran_is_settled_after_its_first_read(self):
        # (done at its first read, a gap after it was made at the latest: in the grid from then on)
        self.assertEqual(Jobs(self.twin(M + 0.5, -INF)).vanishes(B, S), (M, M + GAP))  # (made: whole seconds)

    def test_not_while_an_older_column_runs_after_it(self):  # MF072, MF161, MF163, SF71
        # (one that started 30 minutes before B runs 5 h longer than the twin: the grid is cut there, and B
        # listed, until a gap after it last ran)
        older = buildruns(M - HOUR, started=HOUR - 30 * MINUTE + 60, running_until=M + 8 * HOUR)
        self.assertEqual(Jobs(self.twin(M + 0.5, M + 3 * HOUR), older).vanishes(B, S), (M + 8 * HOUR, M + 8 * HOUR + GAP))

    def test_an_older_column_that_ran_as_long_as_it(self):  # MF072, MF161, MF163, MF285, MF288, MF634, SF02, SF03, SF68, SF71
        # (one started a second before B ran until the twin did: then nothing older runs)
        older = buildruns(M - HOUR, started=HOUR + 59, running_until=M + 3 * HOUR)
        self.assertEqual(Jobs(self.twin(M + 0.5, M + 3 * HOUR), older).vanishes(B, S), (M + 3 * HOUR, M + 3 * HOUR + GAP))
        older = buildruns(M - HOUR, started=HOUR + 59, running_until=M + 3 * HOUR + 1)  # (a second longer: then)
        self.assertEqual(Jobs(self.twin(M + 0.5, M + 3 * HOUR), older).vanishes(B, S),
                         (M + 3 * HOUR + 1, M + 3 * HOUR + 1 + GAP))

    def test_nor_while_a_build_without_a_start_holds_it(self):  # MF072, MF161, MF163, MF288, SF02, SF03, SF67, SF68
        good = buildruns(M - 5 * HOUR)  # (read until 2 h 20 min before B was made)
        changed = [(M + 2 * HOUR, M + 4 * HOUR, "changed")]
        jobs = Jobs(self.twin(M + 0.5, M + 3 * HOUR), good, buildruns(M - 2 * HOUR, nostart=True))
        # (N, put at G's start by that update, holds the twin until a gap after it: B vanishes then)
        self.assertEqual(jobs.vanishes(B, S, changed), (M + 4 * HOUR + SPAN, M + 4 * HOUR + SPAN + GAP))
        self.assertEqual(jobs.vanishes(B, S), (M + 3 * HOUR, M + 3 * HOUR + GAP))

    def test_a_build_without_a_start_does_not(self):  # MF068
        self.assertIsNone(Jobs(buildruns(M + 0.5, nostart=True, running_until=M + 3 * HOUR)).vanishes(B, None))


class ItsColumnVanishes(k8.Reads):
    """gcs_column with twins (builds of the same start and higher IDs, as a fake Timeline gives
    them): a build that runs at every read from when its highest twin last ran (done) to a gap
    later (gone) may vanish from the grid then; but that twin's column may also never be appended
    (an update appends the columns after its first only until its grace, review W7a-2), so the
    build is read as if it never vanished too."""

    def twins(self, b, *running_until):
        """A Timeline with twins of b, made in the same second, each running until then."""
        made, start = frl.created(b), frl.created(b) + MINUTE
        return Jobs(*[buildruns(made + 0.1 * (i + 1), started=start - made - 0.1 * (i + 1), running_until=t)
                      for i, t in enumerate(running_until)], now=self.now)

    @staticmethod
    def reads(col):
        """(the last time it may be read, the last its tabs may show a read, whether it may be read again)"""
        windows = [w for _, ws in col.reads for w in ws]
        return max(lo for lo, _, _ in windows), max(until for _, until, _ in windows), col.open

    def test_its_reads_stay_as_if_it_never_vanished(self):
        # MF171, MF288, MF434, MF549, MF550, MF554, MF638-MF640, SF02, SF03, SF68, SF80-SF82, SF87-SF89
        b = self.build(30 * HOUR)
        done = frl.created(b) + MINUTE + 5 * HOUR
        jobs = self.twins(b, done)
        self.assertEqual(jobs.vanishes(b, frl.created(b) + MINUTE), (done, done + GAP))
        self.assertEqual(self.reads(self.column(b, timeline=jobs)), self.reads(self.column(b, timeline=self.twins(b))))

    def test_it_is_read_as_long_as_it_runs(self):  # MF177, MF302
        b = self.build(20 * HOUR)  # (running now, 4 h before its deadline)
        start = frl.created(b) + MINUTE
        col = self.column(b, timeline=self.twins(b, start + 5 * HOUR))
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job"), ("undecided", []))

    def test_a_twin_between_that_ran_longer_does_not_keep_it(self):
        # MF160, MF171, MF434, MF542, MF543, MF635, SF65, SF66, SF70, SF80
        # (the update after the highest twin stopped running cuts the grid there: the twin between
        # vanishes with it, though it still ran; read as if it never vanished, B fails at its deadline)
        b = self.build(30 * HOUR)
        start = frl.created(b) + MINUTE
        jobs = self.twins(b, start + 23.5 * HOUR, start + 5 * HOUR)
        self.assertEqual(jobs.vanishes(b, start), (start + 5 * HOUR, start + 5 * HOUR + GAP))
        self.assertEqual(frl.verdict(self.column(b, timeline=jobs), "job")[0], "red")

    def test_not_while_a_build_without_a_start_holds_it(self):  # MF072, MF161, MF163, SF67, SF79
        # (an update after a change of the group reads G, of a lower ID, again 4 h after B started; N,
        # without a start, made between them, holds every newer column until then: B does not vanish
        # when its twin stops running, 3 h after it started, but a gap after N's hold)
        b = self.build(30 * HOUR)
        made = frl.created(b)
        start = made + MINUTE
        jobs = self.twins(b, start + 3 * HOUR)
        for r in (buildruns(made - 5 * HOUR), buildruns(made - 2 * HOUR, nostart=True)):
            jobs.of[r.build] = r
        changed = [(start + 2 * HOUR, start + 4 * HOUR, "changed")]
        self.assertEqual([jobs.vanishes(b, start, events)[1] for events in (changed, ())],
                         [start + 4 * HOUR + SPAN + GAP, start + 3 * HOUR + GAP])

    def test_one_that_stopped_running_as_it_would_be_gone_stays(self):
        # MF080, MF173, MF175, MF288, MF301, MF437, MF438, SF03, SF68, SF94
        # (its finished.json and podinfo.json written just as it would be gone: the read then finds it done)
        gone = MINUTE + 5 * HOUR + GAP
        b = self.build(30 * HOUR, finished=gone, podinfo=gone, junits=[("junit_01.xml", gone + 30 * MINUTE, r9.FAILING)])
        start = frl.created(b) + MINUTE
        col = self.column(b, timeline=self.twins(b, start + 5 * HOUR))
        self.assertEqual(frl.verdict(col, "job")[0], "red")

    def test_one_that_stopped_running_before_it_was_gone_stays(self):  # MF080, MF173, MF175, MF288, MF301, MF437, SF68, SF94
        # (it finished 6 h after it started, between done and gone; a failing junit file written 8 h
        # after: read before its own reads end, after gone)
        b = self.build(30 * HOUR, finished=6 * HOUR + MINUTE, podinfo=6 * HOUR + 2 * MINUTE,
                       junits=[("junit_01.xml", 8 * HOUR, r9.FAILING)])
        start = frl.created(b) + MINUTE
        col = self.column(b, timeline=self.twins(b, start + 5 * HOUR))
        self.assertEqual(frl.verdict(col, "job")[0], "red")


class ReadsOverTime(k8.Reads):
    """gcs_column's reads of a build's column, and when its tabs may have shown each."""

    def test_an_update_reads_what_it_listed_before_it_began(self):  # MF424
        # (a new group's first update, 48 h 5 min after the build started, on a tab of 2 days: it may
        # begin 20 minutes before it reads the build, so the build is not too old for it until 48 h 20 min)
        b = self.short(10 * DAY, junits=[("junit_01.xml", MINUTE + 48 * HOUR + 10 * MINUTE, r9.FAILING)])
        start = frl.created(b) + MINUTE
        new = [(start + 48 * HOUR, start + 48 * HOUR + 5 * MINUTE, "new")]
        self.assertEqual(self.judged(b, days=2, events=new), "red")

    def test_a_new_groups_first_update_reads_every_build(self):  # MF298, MF426
        # (8.5 days after the build: past the 7 days a change reads again, not for a new group)
        b = self.short(10 * DAY, junits=[("junit_01.xml", 8 * DAY, r9.FAILING)])
        self.assertEqual(self.judged(b, events=[(self.at(b, 8.5 * DAY), self.at(b, 8.5 * DAY + HOUR), "new")]), "red")
        self.assertEqual(self.judged(b, events=[(self.at(b, 8.5 * DAY), self.at(b, 8.5 * DAY + HOUR), "changed")]),
                         "not-red")

    def test_a_read_while_an_older_column_runs_is_shown_until_the_next(self):  # MF094, MF315, MF605, SF87, SG01
        # (O, made 2 h before B, runs until its deadline, 22 h after B was made: B is read again every
        # gap until then. B's junit_01 fails t 30 minutes after it was made, junit_02 passes it 10 h
        # after: t is flaky from the next read on, which replaces the red one by 13 h)
        self.build(30 * HOUR)
        b = self.short(28 * HOUR, junits=[("junit_01.xml", 30 * MINUTE, r9.FAILING),
                                          ("junit_02.xml", 10 * HOUR, r9.PASSING)])
        col = self.column(b)
        self.assertEqual(frl.verdict(col, "job", during=(self.at(b, 14 * HOUR), self.at(b, 15 * HOUR))),
                         ("not-red", []))
        self.assertEqual(frl.verdict(col, "job", during=(self.at(b, 9 * HOUR), self.at(b, 10 * HOUR)))[0], "red")

    def test_the_red_rows_of_the_last_read_are_its_later_columns(self):  # SF19
        # (its deadline ten minutes from now: a column TestGrid makes now may be made 20 minutes later,
        # failed; t failed in junit_01, then passed in junit_02: flaky in the last read)
        b = self.build(24 * HOUR - 9 * MINUTE, junits=[("junit_01.xml", HOUR, r9.FAILING),
                                                       ("junit_02.xml", 2 * HOUR, r9.PASSING)])
        self.assertEqual(rows(frl.verdict(self.column(b), "job")), ("red", ["job.Overall"]))

    def test_a_column_made_after_the_deadline_stays_until_read_again(self):  # SF21
        # (never finished; t failed in junit_01, passed in junit_02, written 10 minutes before its
        # deadline: a read in the 20 minutes before may make its column after the deadline, failed,
        # with t failed; that column does not run, so nothing reads it again because of it: a tab
        # that hides Overall may show t red long after, though the column made at the read still ran)
        b = self.build(40 * HOUR, junits=[("junit_01.xml", MINUTE + HOUR, r9.FAILING),
                                          ("junit_02.xml", MINUTE + 23 * HOUR + 50 * MINUTE, r9.PASSING)])
        col = self.column(b)
        later = (self.at(b, MINUTE + 30 * HOUR), self.at(b, MINUTE + 31 * HOUR))
        self.assertEqual(rows(frl.verdict(col, "job", shows=r9.NO_OVERALL, during=later)), ("red", ["t"]))

class WhileItIsNew(k8.Reads):
    """An update that begins less than 20 minutes after a build started reads its column again,
    within its own 20 minutes: TestGrid reads a build until 40 minutes after its start, from when
    it was made. Here the build was pending 3 h, and its finished.json and podinfo.json say no time
    (so they count as there from the first read: its column never runs)."""

    def pending(self, *junits):
        b = self.build(10 * HOUR, started=3 * HOUR, junits=junits)
        made = frl.created(b)
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"timestamp": int(made) + 3 * HOUR + 5 * MINUTE, "passed": True, "result": "SUCCESS"}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, r9.POD_OK))
        return b

    def test_until_40_minutes_after_its_start(self):  # MF049
        b = self.pending(("junit_01.xml", 3 * HOUR + 40 * MINUTE, r9.FAILING))
        self.assertEqual(self.judged(b), "red")

    def test_not_later(self):  # MF048
        b = self.pending(("junit_01.xml", 3 * HOUR + 40 * MINUTE + 30, r9.FAILING))
        self.assertEqual(self.judged(b), "not-red")

    def test_from_when_it_was_made(self):  # SF77
        # (t failed in junit_01, 2 h 50 min after it was made, and passed in junit_02, 5 minutes later)
        b = self.pending(("junit_01.xml", 170 * MINUTE, r9.FAILING), ("junit_02.xml", 175 * MINUTE, r9.PASSING))
        self.assertEqual(self.judged(b), "red")


class BuildRunsReach(k8.Reads):
    """BuildRuns.reach: the last time TestGrid may read a build of its own accord, at the latest
    the end of the updates that begin less than 20 minutes after it started (40 minutes after its
    start); .running_until: the last time a read may find it running."""

    def test_reads_while_it_is_new(self):  # MF048, MF049, MF273, MF401, MF609-MF611, MF665-MF667, SF30, SF31
        # (pending 4 h; its finished.json and podinfo.json written 10 minutes after it was made)
        b = self.build(10 * HOUR, started=4 * HOUR, finished=10 * MINUTE, podinfo=10 * MINUTE)
        got = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b)
        self.assertEqual(got.reach, frl.created(b) + 4 * HOUR + 40 * MINUTE)
        self.assertEqual(got.running_until, frl.created(b) + 10 * MINUTE)

    def test_its_pod_row_runs_until_podinfo_json_is_in(self):  # MF401
        b = self.short(10 * HOUR)  # (finished.json 5 minutes after it was made, podinfo.json 6)
        got = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b)
        self.assertEqual(got.running_until, frl.created(b) + 6 * MINUTE)

    def test_one_that_may_be_running_now_runs_until_inf(self):  # MF597
        # (its deadline ten minutes from now: a column made now runs, one made 20 minutes later fails)
        b = self.build(24 * HOUR - 10 * MINUTE)
        got = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b)
        self.assertEqual(got.running_until, INF)


# ------------------------------------------------------------ TestGrid's config over time

NOW = tch.NOW
E1, E2, E3 = tch.E1, tch.E2, tch.E3


def history(runs, built, **kw):
    h = frl.ConfigHistory()
    h.merge(tch.loaded(runs, built, **kw))
    return h


class WhenAConfigIsTaken(unittest.TestCase):
    """TestGrid takes a run's config at most 26 min 30 s after the run ended (a merge every 15
    minutes, taking up to 10, a read every minute, and retries); an update that reads the group's
    columns again after it changed may begin a gap (3 h) after that, less its 20 minutes."""

    def test_a_config_is_in_force_until_26_and_a_half_minutes_after_the_next_run_ended(self):
        # MF008, MF009, MF054, MF055, MF134, MF135, MF242, MF244, MF245, MF385-MF388, SF05, SF06
        h = history([tch.run(NOW, "a"), tch.run(NOW + 100, "b")], tch.configs(a={"d#t": E1}, b={"d#t": E2}))
        pickup = 26 * MINUTE + 30
        self.assertEqual(h.eras("d#t"), [(-INF, NOW + 5 + pickup, frl.UNKNOWN), (NOW, NOW + 105 + pickup, E1),
                                         (NOW + 100, INF, E2)])

    def test_its_update_may_begin_until_3_hours_6_minutes_30_seconds_after(self):
        # MF008, MF009, MF054, MF055, MF134, MF135, MF242, MF244, MF245, MF385-MF388, SF02, SF03, SF05, SF06
        h = history([tch.run(NOW, "a"), tch.run(NOW + 100, "b")], tch.configs(a={"d#t": E1}, b={"d#t": E3}))
        late = 26 * MINUTE + 30 + 3 * HOUR - 20 * MINUTE
        self.assertEqual(h.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, "changed")])


class RelistAfter(unittest.TestCase):
    """ConfigHistory.relist_after: the time after which testgrid_config.plan lists the upload job's
    runs again, as state.json's "config_relist_after" says (else None) or a run dropped from it
    (malformed) needs, the earlier of the two; cleared once a listing goes back that far."""

    def dropped(self, flag=None):
        """A history read back from state.json with its second run malformed, and that flag."""
        h = history([tch.run(NOW, "a"), tch.run(NOW + 100, "b"), tch.run(NOW + 200, "c")],
                    tch.configs(a={"d#t": E1}, b={"d#t": E2}, c={"d#t": E3}))
        uploads, entries = h.encode()
        return frl.ConfigHistory([uploads[0], dict(uploads[1], commit="x"), uploads[2]], entries, [], flag)

    def test_the_flag_in_state_json(self):  # MF201, MF202, SG17
        self.assertEqual(frl.ConfigHistory(relist_after=NOW + 0.5).relist_after, NOW + 0.5)
        for bad in ("x", True, -1, None):
            self.assertIsNone(frl.ConfigHistory(relist_after=bad).relist_after, bad)

    def test_a_dropped_run_whose_place_is_not_known(self):  # SG18
        h = history([tch.run(NOW, "a")], tch.configs(a={"d#t": E1}))
        uploads, entries = h.encode()
        warnings = []
        self.assertEqual(frl.ConfigHistory(uploads + ["x"], entries, warnings).relist_after, 0)
        self.assertEqual(warnings, ["state.json: dropped malformed runs of TestGrid's config uploads; the runs of the "
                                    "upload job from the first one kept on are listed again"])

    def test_the_earlier_of_the_flag_and_a_dropped_run(self):  # MF202, MF326, MF460, SG16, SG17
        made = tgc.made(tch.run(NOW, "a")["build"])  # (the run before the dropped one)
        self.assertEqual(self.dropped().relist_after, made)
        self.assertEqual(self.dropped(NOW - 100).relist_after, NOW - 100)
        self.assertEqual(self.dropped(NOW + 100).relist_after, made)

    def test_cleared_once_listed_from_it(self):  # MF110, MF111, MF202, MF218, MF219, MF348, MF349, SG17, SG23
        for after, left in ((NOW - 1, None), (NOW, None), (NOW + 1, NOW)):
            h = frl.ConfigHistory(relist_after=NOW)
            h.merge(tch.loaded([tch.run(NOW + 300, "d")], {}, after=after))
            self.assertEqual(h.relist_after, left, after)


class GroupChanges(unittest.TestCase):
    """How runs of the upload job changed their tabs' test groups (testgrid_config.merge's
    "group_changes"): ConfigHistory keeps them for the runs whose config it knows, in state.json
    too, and events() takes them over what the digests of the run before would say."""

    def runs(self):
        return [tch.run(NOW, "a"), tch.run(NOW + 100, "b"), tch.run(NOW + 200, "c")]

    def test_kept_for_a_run_whose_config_is_known(self):  # MF215, MF343, MF344, MF346, MF469, MF470, SG21
        a, b, c = self.runs()
        h = history([a, b, c], tch.configs(a={"d#t": E1}, b={"d#t": E2}),  # (c's config not built)
                    changes={b["build"]: {"job": "changed"}, c["build"]: {"job": "new"}})
        self.assertEqual([r["group_changes"] for r in h.runs], [None, {"job": "changed"}, None])
        h.merge(tch.loaded([b], {}, changes={b["build"]: {"job": "new"}}))  # (worked out again: kept as it was)
        self.assertEqual(h.runs[1]["group_changes"], {"job": "changed"})

    def test_kept_in_state_json(self):  # MC495, MC509, MC568, MF215, MF346, MF352, MF469, MF470, MF472, MF473
        a, b, _ = self.runs()
        h = history([a, b], tch.configs(a={"d#t": E1}, b={"d#t": E2}), changes={b["build"]: {"job": "changed"}})
        uploads, entries = h.encode()
        again = frl.ConfigHistory(json.loads(json.dumps(uploads)), json.loads(json.dumps(entries)))
        self.assertEqual([r["group_changes"] for r in again.runs], [None, {"job": "changed"}])

    def test_events_take_them_first(self):
        # MF008, MF009, MF054, MF055, MF134, MF135, MF215, MF242, MF244, MF245, MF346, MF385-MF388, MF469, MF470, MF480, MF564,
        # SF02, SF03, SF05, SF06, SG26
        a, b, c = self.runs()
        h = history([a, b, c], tch.configs(a={"d#t": E1}, b={"d#t": E1}, c={"d#t": E3}),
                    changes={b["build"]: {"job": "new"}, c["build"]: {"other": "changed"}})
        late = 26 * MINUTE + 30 + GAP - SPAN
        # (b: the same group as a's by its digest, but new to TestGrid; c changed only another group)
        self.assertEqual(h.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, "new")])

    def test_by_the_digest_of_the_tabs_own_group(self):  # MF715, MF730, MF731, SG32
        # (the run before had another group too, of another digest; the tab's group is as it was)
        a, b, _ = self.runs()
        other = dict(group_entry("other"), group_digest="2" * 64)
        h = history([a, b], tch.configs(a={"d#t": group_entry("j"), "d#u": other}, b={"d#t": group_entry("j")}))
        self.assertEqual(h.events("d#t", NOW + 1000), [])

    def test_by_the_digests_of_the_run_before(self):
        # MF008, MF009, MF054, MF055, MF134, MF135, MF242, MF244, MF245, MF385-MF388, MF715, MF717-MF719, MF730-MF732, SF02, SF03,
        # SF05, SF06, SG32
        a, b, _ = self.runs()
        j, k = group_entry("j"), group_entry("k")
        undigested = dict(j, group_digest=None)  # (a config from before the digest)
        late = 26 * MINUTE + 30 + GAP - SPAN
        # (not known until the run's own comparison is made: "unknown", taken as new)
        for before, now, how in ((k, j, "unknown"),  # (the run before had no tab of the group)
                                 (undigested, j, "unknown"), (j, undigested, "unknown"),  # (a digest not known tells nothing)
                                 (j, j, None)):
            h = history([a, b], tch.configs(a={"d#t": before}, b={"d#t": now}))
            self.assertEqual(h.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, how)] if how else [], (before, now))
        # (a tab of the group without it, in the run before, tells nothing either)
        h = history([a, b], tch.configs(a={"d#t": j, "d#u": dict(j, group=None, group_digest=None)}, b={"d#t": j}))
        self.assertEqual(h.events("d#t", NOW + 1000), [])

    def test_a_tab_without_its_group_is_not_read(self):  # MF477
        a, b, _ = self.runs()
        lost = dict(E1, group=None, group_digest=None)  # (its test group is not in the config)
        h = history([a, b], tch.configs(a={"d#t": E1}, b={"d#t": lost}))
        self.assertEqual(h.events("d#t", NOW + 1000), [])


class MergeListing(unittest.TestCase):
    """ConfigHistory.merge: a run the archive saw running and the listing does not have failed only
    if it was made after the time the listing began."""

    def test_a_run_made_as_the_listing_began_is_not_dropped(self):  # MF332, MF462, SG19
        a, b, c = tch.run(NOW, "a"), tch.run(NOW + 100, "b", finished=False), tch.run(NOW + 200, "c")
        h = history([a, b], tch.configs(a={"d#t": E1}, b={"d#t": E2}))
        h.merge(tch.loaded([c], tch.configs(c={"d#t": E3}), after=frl.created(b["build"])))
        self.assertEqual([r["commit"][0] for r in h.runs], ["a", "b", "c"])
        h.merge(tch.loaded([c], {}, after=frl.created(b["build"]) - 1))
        self.assertEqual([r["commit"][0] for r in h.runs], ["a", "c"])


class ConfigRuns(k8.OverTime):
    """OverTime with a history of runs of the upload job given by their times."""

    def history_of(self, *runs):
        """state.json's config history of these runs of the upload job, each (started, finished,
        dashboards or None: not known); returns a config file of the last run, as read now."""
        listed, built = [], {}
        for i, (started, finished, dashboards) in enumerate(runs):
            commit = f"{i + 1:040x}"
            listed.append({"build": str(snowflake(started)), "commit": commit, "started": int(started),
                           "finished": None if finished is None else int(finished)})
            if dashboards is not None:
                built[commit] = ({f"{d}#{t}": e for d, tabs in dashboards.items() for t, e in tabs.items()},
                                 {"test_infra": commit})
        h = frl.ConfigHistory()
        h.merge(tch.loaded(listed, built))
        uploads, entries = h.encode()
        state = self.state()
        state.update(config_uploads=uploads, config_entries=entries)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        last = listed[-1]
        return ttc.write_config(os.path.join(self.dir, "now.json"), [last], {} if runs[-1][2] is None else {
            last["commit"]: {"committed": last["started"] - 60, "image": "img", "dashboards": runs[-1][2]}})

    def warning(self, run_, words):
        return next((w for w in run_["warnings"] if words in w), None)


class RunOverConfigs(ConfigRuns):
    """What a run of the fetch says of TestGrid's config over time, and keeps in state.json."""

    def test_configs_that_could_not_be_built_are_named(self):
        # MF039, MF122, MF365-MF367, MF498, MF499, MF587, MF588, MF659, MF660, MF744, MF745, SG50
        self.no_gap()
        a, failed = "a" * 40, {c * 40: f"why {c}" for c in "bcde"}
        failed["c" * 40] = "x" * 400
        path = ttc.write_config(os.path.join(self.dir, "failed.json"), [
            {"build": str(snowflake(self.now - 30 * DAY)), "commit": a, "started": int(self.now - 30 * DAY) - 5,
             "finished": int(self.now - 30 * DAY)}],
            {a: {"committed": int(self.now - 31 * DAY), "image": "img", "dashboards": self.tab(ttc.entry(""))}}, failed=failed)
        run_ = self.fetched(path)
        rid = {c: ttc.recipe_of(c * 40) for c in "bcd"}
        self.assertIn(f"TestGrid's config of 4 runs of its upload job could not be built now: {rid['b']}: why b; "
                      f"{rid['c']}: {'x' * 300}; {rid['d']}: why d; ...", run_["warnings"])
        del failed["e" * 40]
        three = ttc.write_config(os.path.join(self.dir, "three.json"), [
            {"build": str(snowflake(self.now - 30 * DAY)), "commit": a, "started": int(self.now - 30 * DAY) - 5,
             "finished": int(self.now - 30 * DAY)}],
            {a: {"committed": int(self.now - 31 * DAY), "image": "img", "dashboards": self.tab(ttc.entry(""))}}, failed=failed)
        run_ = self.fetched(three)
        self.assertIn(f"TestGrid's config of 3 runs of its upload job could not be built now: {rid['b']}: why b; "
                      f"{rid['c']}: {'x' * 300}; {rid['d']}: why d", run_["warnings"])
        one = ttc.write_config(os.path.join(self.dir, "one.json"), [
            {"build": str(snowflake(self.now - 30 * DAY)), "commit": a, "started": int(self.now - 30 * DAY) - 5,
             "finished": int(self.now - 30 * DAY)}],
            {a: {"committed": int(self.now - 31 * DAY), "image": "img", "dashboards": self.tab(ttc.entry(""))}},
            failed={"b" * 40: "why b"})
        run_ = self.fetched(one)
        self.assertIn(f"TestGrid's config of 1 runs of its upload job could not be built now: {rid['b']}: why b",
                      run_["warnings"])

    def test_a_config_not_known_three_days_after_its_run_ended_is_serious(self):
        # MF231, MF369-MF372, MF503, MF504, MF589, MF590, SF11
        self.no_gap()
        tab = self.tab(ttc.entry(""))
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, tab),
                                            (self.now - 4 * DAY, self.now - 4 * DAY + 5, None)))
        self.assertIn("is still not known 3 days after the archive listed it", self.warning(run_, "runs of its upload job"))
        self.assertTrue(any("still not known" in w for w in run_["serious_warnings"]))
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, tab),
                                            (self.now - 2 * DAY, self.now - 2 * DAY + 5, None)))
        self.assertIn("is not known yet", self.warning(run_, "runs of its upload job"))
        self.assertFalse(any("still not known" in w for w in run_["serious_warnings"]))

    def test_one_still_running_counts_from_when_it_began(self):  # MF231, MF500, MF502, MF503, MF590
        self.no_gap()
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, self.tab(ttc.entry(""))),
                                            (self.now - 4 * DAY, None, None)))
        self.assertIn("is still not known 3 days after the archive listed it", self.warning(run_, "runs of its upload job"))

    def test_three_days_after_it_ended_not_began(self):  # MF231, MF369-MF372, MF500, MF501, MF504, MF589, SG45
        self.no_gap()
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, self.tab(ttc.entry(""))),
                                            (self.now - 3.5 * DAY, self.now - 2 * DAY, None)))
        self.assertIn("is not known yet", self.warning(run_, "runs of its upload job"))

    def test_after_the_oldest_run_not_known(self):  # MF231, MF503, MF590
        self.no_gap()
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, self.tab(ttc.entry(""))),
                                            (self.now - 4 * DAY, self.now - 4 * DAY + 5, None),
                                            (self.now - DAY, self.now - DAY + 5, None)))
        self.assertIn("is still not known 3 days after the archive listed it", self.warning(run_, "runs of its upload job"))

    def dropped_t(self, ended_ago):
        """A run that dropped tab t (TestGrid still shows it) ended `ended_ago`: the run's report."""
        self.no_gap()
        return self.fetched(self.history_of(
            (self.now - 30 * DAY, self.now - 30 * DAY + 5, {"dash": {"t": ttc.entry(""), "u": ttc.entry("")}}),
            (self.now - ended_ago - 10 * MINUTE, self.now - ended_ago, {"dash": {"u": ttc.entry("")}})))

    def test_a_tab_the_config_dropped_an_hour_ago(self):  # MF362, MF493-MF497, MF584, MF586, SG44, SG54
        # (TestGrid may still show t by the config before: it takes a config up to 26.5 minutes after
        # its run ended, and the fetch allows an hour more)
        run_ = self.dropped_t(70 * MINUTE)
        self.assertIn(", which TestGrid may not have taken yet", self.warning(run_, "has no tab dash#t"))
        self.assertFalse(any("lacks tabs" in w for w in run_["serious_warnings"]))

    def test_two_hours_ago_it_should_have_been_taken(self):  # MF583
        run_ = self.dropped_t(2 * HOUR)
        self.assertIn("lacks tabs TestGrid shows", self.warning(run_, "has no tab dash#t"))

    def test_a_tab_whose_table_cannot_be_read_is_not_said_to_lack_its_config(self):  # MF359, MF487, MF489
        self.no_gap()
        self.srv.summary("dash", {"t": "FAILING", "u": "PASSING"})
        self.srv.table("dash", "u", (500, {}, b""))
        run_ = self.fetched(self.config(""))
        self.assertIsNone(self.warning(run_, "has no tab"))
        self.assertIsNone(self.warning(run_, "was never read"))

    def test_a_config_testgrid_applies_as_the_port_does_warns_of_nothing(self):  # MF232, MF234, MF236
        self.no_gap()
        run_ = self.fetched(self.config(""))
        self.assertEqual([w for w in run_["warnings"] if "dash#t" in w], [])
        run_ = self.fetched(self.config("width=10&foo=1"))
        self.assertEqual([w for w in run_["warnings"] if "dash#t" in w], [
            f"dash#t: TestGrid's config for the tab ({self.about(run_)}) sets base options not known here (foo): "
            "TestGrid's tabulator ignores them, and so does the archive; if its page hid rows by them, a build could "
            "count as red here that it does not show red"])

    def about(self, run_):
        return frl.config_about(run_["testgrid_config"]["source"])

    def test_state_json_keeps_no_empty_history(self):  # MF381, MF383, MF511
        self.no_gap()
        self.fetch()
        self.assertFalse({"config_uploads", "config_entries", "config_relist_after"} & set(self.state()))

    def test_the_relist_flag_is_kept_until_a_listing_goes_back_that_far(self):
        # MF111, MF202, MF219, MF349, MF383, MF384, MF511, SG17, SG38, SG39, SG49
        self.no_gap(config_relist_after=self.now - 2 * DAY)
        self.fetch()
        self.assertEqual(self.state()["config_relist_after"], self.now - 2 * DAY)
        self.fetch_with(self.config(""))  # (listed from 30 days ago: the flag is done)
        self.assertNotIn("config_relist_after", self.state())

    def test_a_watched_build_its_held_tab_shows_is_decided_by_it(self):  # MF126, MF240
        # (its tab shows it, not running, and the port does not follow the tab's config; GCS no longer
        # has its files)
        b = snowflake(self.now - 5 * HOUR)
        self.show([self.visible, b], {"job.Pod": ([1, 1], ["", ""]), "kubetest.Up": ([1, 1], ["", ""])},
                  start=int(self.now - HOUR))
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (500, {}, b""))
        self.no_gap(watch=self.watch(b))
        run_ = self.fetched(self.config("", build_override_configuration_value="x"))  # (an option not followed)
        self.assertEqual(run_["outcomes"], {"not-red": [f"job/{b}"]})
        self.assertEqual(run_["errors"], [])

    def test_a_watched_build_shown_on_a_tab_known_only_now(self):  # MF727
        # (t2, new, shows the build in a column without a start: no rule for it yet; the build passed)
        b = snowflake(self.now - 5 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 5 * HOUR) + 60}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"timestamp": int(self.now - 4 * HOUR), "passed": True, "result": "SUCCESS"}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, r9.POD_OK))
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        self.srv.table("dash", "t2", k8.ttc_table("bucket/logs/job", [b], {"job.Pod": ([1], [""])}, start=0))
        self.no_gap(watch=self.watch(b, tabs=("t", "t2")))
        run_ = self.fetched(self.config(""))
        self.assertEqual((run_["outcomes"], run_["errors"]), ({"not-red": [f"job/{b}"]}, []))

    def test_a_held_build_shown_running_for_sixty_days_says_why_it_was_held(self):  # MF379
        b = snowflake(self.now - 61 * DAY)
        start = int(self.now - 61 * DAY)
        self.srv.gcs_listing("bucket", [])
        self.show([b], {"job.Overall": ([4], [""])}, start=start)  # (TestGrid still shows it running)
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR / 4, "oldest": start}},
                       watch={f"job/{b}": {"rec": self.watched(b), "first_seen": self.now - 61 * DAY + HOUR}})
        run_ = self.fetched(self.config("", build_override_configuration_value="x"))  # (an option not followed)
        self.assertEqual(run_["outcomes"], {"stopped-holding": [f"job/{b}"]})
        self.assertIn(f"job/{b}: cannot be judged from GCS: its test group sets build_override_configuration_value, "
                      "which testgrid_rule.py does not follow; no longer watched 60 days after it was made, though "
                      "TestGrid may have shown it red", run_["serious_warnings"])


class ConfigReadBefore(k8.OverTime):
    """A tab whose config the archive read before (state.json's tab_configs) and whose history it
    does not have: its builds are judged by that config's filters, or by the rows its table shows
    where TestGrid does not apply them (its tabulator fails the tab, or its updater does not update
    the group)."""

    def judged_by(self, entry):
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}},
                       tab_configs={"dash#t": {"config": entry, "read": self.now - HOUR}})
        self.fetch()
        return self.runs()[-1]

    def test_by_its_filters(self):  # MF485
        run_ = self.judged_by(ttc.entry(self.HIDE))
        self.assertEqual((run_["outcomes"], run_["errors"]), ({}, []))

    def test_by_the_rows_shown_where_testgrid_cannot_filter(self):  # MF486, MF570, MF572, SG51
        run_ = self.judged_by(ttc.entry("exclude-filter-by-regex=a**"))
        self.assertEqual((run_["outcomes"], run_["errors"]), ({"backfilled": [f"job/{self.b}"]}, []))

    def test_or_does_not_update_the_group(self):  # MF486, MF570, MF573, SG51
        run_ = self.judged_by(ttc.entry(self.HIDE, use_kubernetes_client=False))
        self.assertEqual((run_["outcomes"], run_["errors"]), ({"backfilled": [f"job/{self.b}"]}, []))


class TabsOverTime(ConfigRuns):
    """Builds on tabs whose config changed while TestGrid may have shown them."""

    def test_a_tab_a_run_may_be_dropping_does_not_hold_a_build(self):  # MF568
        # (the run that drops the tab "gone" still runs: the config before may still be in force; the
        # build, made 5 h ago, still runs: watched, not held)
        b = snowflake(self.now - 5 * HOUR)
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 5 * HOUR + 60)}))
        self.no_gap(watch=self.watch(b, tabs=("gone",)))
        run_ = self.fetched(self.history_of(
            (self.now - 30 * DAY, self.now - 30 * DAY + 5, self.tab(ttc.entry(""), gone=ttc.entry(""))),
            (self.now - 2 * HOUR, None, self.tab(ttc.entry("")))))
        self.assertEqual(run_["outcomes"], {"watching": [f"job/{b}"]})

    def test_a_tab_the_config_lacks_is_judged_by_its_configs_before(self):  # MF581
        # (TestGrid still shows t, which the config of 10 h ago lacks: the build, red under the config
        # before, is red; after that, not known)
        run_ = self.judged((self.now - 30 * DAY, {"dash": {"t": ttc.entry(""), "u": ttc.entry("")}}),
                           (self.now - 10 * HOUR, {"dash": {"u": ttc.entry("")}}))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def judged(self, *configs):
        return self.fetched(self.history(*configs))

    def old_watched(self, days_ago):
        """A watched build made `days_ago` days ago that failed, red only on kubetest.Test (HIDE hides it)."""
        w = snowflake(self.now - days_ago * DAY)
        self.failed_in_gcs(w, days_ago * DAY)
        self.junit(w, tch.KUBETEST_FAILED)
        self.no_gap(watch=self.watch(w))
        return w

    def test_a_lacking_tabs_config_is_not_known_only_after_its_last_entry(self):  # MF361, MF581, MF656, MF658, MF699, SG42
        # (t: in a's config, not in b's, hiding the build's red row in c's, not in d's, the latest,
        # though TestGrid shows it: from d on its config is not known; the build, made between b and c,
        # was shown under b's config (no such tab) and c's (not red), long before d)
        w = self.old_watched(20)
        tabs = lambda t: {"dash": dict({"u": ttc.entry("")}, **({"t": t} if t else {}))}  # noqa: E731
        run_ = self.fetched(self.history_of((self.now - 30 * DAY, self.now - 30 * DAY + 5, tabs(ttc.entry(""))),
                                            (self.now - 25 * DAY, self.now - 25 * DAY + 5, tabs(None)),
                                            (self.now - 18 * DAY, self.now - 18 * DAY + 5, tabs(ttc.entry(self.HIDE))),
                                            (self.now - 2 * DAY, self.now - 2 * DAY + 5, tabs(None))))
        self.assertEqual(run_["outcomes"], {"not-red": [f"job/{w}"]})

    def test_nor_after_a_run_whose_config_is_not_known(self):  # MF655, MF699
        # (t: in a's config, not in b's; c's config not known; not in d's, the latest: from b on, its
        # config is not known; the build, made after b, was shown long before c)
        w = self.old_watched(38)
        tabs = lambda t: {"dash": dict({"u": ttc.entry("")}, **({"t": t} if t else {}))}  # noqa: E731
        run_ = self.fetched(self.history_of((self.now - 45 * DAY, self.now - 45 * DAY + 5, tabs(ttc.entry(""))),
                                            (self.now - 40 * DAY, self.now - 40 * DAY + 5, tabs(None)),
                                            (self.now - 20 * DAY, self.now - 20 * DAY + 5, None),
                                            (self.now - 2 * DAY, self.now - 2 * DAY + 5, tabs(None))))
        self.assertEqual(run_["outcomes"], {"held": [f"job/{w}"]})

    def test_the_readme_names_the_config_as_markdown_text(self):  # SG37
        source = {"test_infra": "a*b|c_d" + "0" * 33}
        self.no_gap(testgrid_config={"read": self.now - HOUR, "source": source})
        self.fetch()
        with open(os.path.join(self.root, self.runs()[-1]["dir"], "README.md")) as f:
            readme = f.read()
        self.assertIn(f"TestGrid's config: {frl.md_cell(frl.config_about(source))}, read by the run of ", readme)


# ------------------------------------------------------------ testgrid_rule: a start before the stop

class StartBeforeTheStop(unittest.TestCase):
    """testgrid_rule._before: time.Unix(sec, 0).Before(T), for any T column() may be given."""

    def test_unbounded_stops(self):  # MR002, MR005-MR008, MR014, SR05
        self.assertTrue(tr._before(T0, INF))
        self.assertFalse(tr._before(T0, -INF))
        self.assertFalse(tr._before(T0, float("nan")))

    def test_a_column_without_a_stop_is_never_too_old(self):  # MR002, MR005, MR007, MR008, MR014
        started = json.dumps({"timestamp": T0}).encode()
        self.assertEqual(tr.column("job", started, None, None, [], -INF, T0 + 600).errors, [])
        self.assertEqual(tr.column("job", started, None, None, [], INF, T0 + 600).errors, ["ancient"])


# ------------------------------------------------------------ oracle/relevant_diff.py: Bazel

WORKSPACE = ('load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains")\n\n'
             'go_register_toolchains(\n    version = "{}",\n)\n')


def repos(*modules):
    """repos.bzl's go_repository rules for (importpath, version or None)."""
    return "def go_repositories():\n" + "".join(
        f'    go_repository(\n        name = "m{i}",\n        importpath = "{path}",\n        sum = "h1:x=",\n'
        + (f'        version = "{version}",\n' if version else "") + "    )\n" for i, (path, version) in enumerate(modules))


class BazelBuild(unittest.TestCase):
    """relevant_diff: the Go and the modules TestGrid's Bazel build uses (WORKSPACE, repos.bzl),
    and its .bazelversion."""

    def setUp(self):
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle"))
        import relevant_diff
        self.rd = relevant_diff

    def tree(self, files):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        for name, text in files.items():
            with open(os.path.join(d, name), "w") as f:
                f.write(text)
        return d

    MODULES = [("k8s.io/api", "v0.27.4"), ("k8s.io/apimachinery", "v0.27.5"), ("cloud.google.com/go/storage", "v1.31.0")]

    def test_what_the_build_uses(self):  # MD003, MD011-MD013, MD015, MD022, MD024, MD025, MD027, MD031, MD032, SD02, SD03, SD08
        root = self.tree({"WORKSPACE": WORKSPACE.format("1.26.5"), "repos.bzl": repos(*self.MODULES)})
        self.assertEqual(self.rd.bazel(root), {"go": "1.26.5", "k8s.io/api": "v0.27.4", "k8s.io/apimachinery": "v0.27.5",
                                               "cloud.google.com/go/storage": "v1.31.0"})

    def test_a_rule_without_a_version_has_none(self):  # MD003, MD012, MD013, MD015, MD027, MD031, MD032, SD01, SD02
        # (a module's rule without a version: not the version of the rule after it)
        root = self.tree({"repos.bzl": repos(("k8s.io/api", None), ("k8s.io/apimachinery", "v0.27.5"))})
        self.assertEqual(self.rd.bazel(root), {"go": None, "k8s.io/api": None, "k8s.io/apimachinery": "v0.27.5",
                                               "cloud.google.com/go/storage": None})

    def test_changes(self):
        # MD003, MD007, MD011-MD013, MD015, MD020, MD022, MD024, MD025, MD027, MD031, MD032, SD02-SD05, SD07, SD08
        old = self.tree({"WORKSPACE": WORKSPACE.format("1.26.5"), "repos.bzl": repos(*self.MODULES)})
        new = self.tree({"WORKSPACE": WORKSPACE.format("1.26.6"), "repos.bzl": repos(("k8s.io/api", "v0.28.0"),
                                                                                    *self.MODULES[1:]),
                         ".bazelversion": ""})
        self.assertEqual(self.rd.changes(old, new), [".bazelversion", "Bazel: go 1.26.5 -> 1.26.6",
                                                     "Bazel: k8s.io/api v0.27.4 -> v0.28.0"])


# ------------------------------------------------------------ cross_check: the rule a run's config gives

class CrossCheckByTheRunsConfig(k8.OverTime):
    """cross_check judges a build TestGrid no longer lists by the tab's config the run recorded
    (without a history of the config in state.json): by the rows the tab's table showed where
    TestGrid cannot apply its filters (its tabulator fails the tab) or does not update the group."""

    def checked_with(self, entry):
        self.earlier_run(self.now - 3 * DAY)  # (the archive began before the build)
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)  # (hidden: not archived)
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        run_ = frl.read_json(run_path)
        for t in run_["tabs"]:
            t["config"] = entry
        frl.write_json(run_path, run_)
        state = self.state()
        for k in ("config_uploads", "config_entries"):
            state.pop(k)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        return self.check()

    def test_where_testgrid_cannot_apply_the_filters(self):  # MX003, MX004, MX006, SX01
        code, out = self.checked_with(ttc.entry("exclude-filter-by-regex=a**"))
        self.assertEqual(code, 1, out)
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_where_testgrid_does_not_update_the_group(self):  # MX003, MX004, MX007, SX01
        code, out = self.checked_with(ttc.entry(self.HIDE, use_kubernetes_client=False))
        self.assertEqual(code, 1, out)
        self.assertIn(f"MISSED red build job/{self.b}", out)


# ------------------------------------------------------------ testgrid_config: the upload job's runs

class IsMoment(unittest.TestCase):
    """testgrid_config.is_moment: a Unix time from 0 (the epoch: "from the first run kept") to 2^40."""

    def test_bounds(self):  # MC267, MC269, MC418, MC517-MC519, MF202, SG17
        for v in (0, 0.5, (1 << 40) - 1, 1_790_000_000.25):
            self.assertTrue(tgc.is_moment(v), v)
        for v in (-1, 1 << 40, True, "1", None):
            self.assertFalse(tgc.is_moment(v), v)
        self.assertEqual(frl.ConfigHistory(relist_after=0).relist_after, 0)


def group_entry(group):
    """A tab's entry of test group `group` (None: a tab whose group is not in the config)."""
    return {"test_group": group, "base_options": "", "group": None, "group_digest": None} if group is None else \
        dict(ttc.entry(""), test_group=group, group=dict(ttc.GROUP, name=group))


class Unexplained(unittest.TestCase):
    """testgrid_config.unexplained: the test groups of a run's tabs no tab of the run before had."""

    def test_groups_new_to_the_tabs(self):  # MC035, MC037, MC148, MC149, MC282, MC283, MC286, MC425, MC426, MC522, SC37, SC38
        tabs = {"d#t": group_entry("a"), "d#u": group_entry("b"), "d#v": dict(group_entry("c"), group=None)}
        self.assertEqual(tgc.unexplained(tabs, {"x#t": group_entry("a")}), {"b"})
        self.assertEqual(tgc.unexplained(tabs, {"x#t": group_entry("a"), "x#u": dict(group_entry("b"), group=None)}),
                         {"b"})  # (a tab before without its group explains nothing)
        self.assertEqual(tgc.unexplained(tabs, {"x#t": group_entry("a"), "x#u": group_entry("b")}), set())
        self.assertIsNone(tgc.unexplained(tabs, None))
        self.assertEqual(tgc.unexplained({"d#v": dict(group_entry("c"), group=None)}, None), set())


def state_of(*runs, flag=None):
    """state.json of a history of runs of the upload job, each (run, its tabs or None, its group
    changes or None), as the fetch writes it."""
    h = frl.ConfigHistory()
    h.merge({"after": 0, "runs": [dict(r, recipe=ttc.recipe_of(r["commit"])) for r, _, _ in runs],
             "configs": {ttc.recipe_of(r["commit"]): (tabs, {}) for r, tabs, _ in runs if tabs is not None},
             "group_changes": {r["build"]: changes for r, _, changes in runs if changes is not None}, "failed": {}})
    uploads, entries = h.encode()
    return {"config_uploads": json.loads(json.dumps(uploads)), "config_entries": json.loads(json.dumps(entries)),
            **({"config_relist_after": flag} if flag is not None else {})}


class DecodeUploads(unittest.TestCase):
    """testgrid_config.decode_uploads: state.json's runs of the upload job, and the time after which
    they are to be listed again when one was dropped (malformed, or listed twice): after the run
    before the first dropped; from the first run kept (0) when where it was is not known."""

    def runs(self):
        return [tch.run(NOW + 100 * i, c) for i, c in enumerate("abcd")]

    def decoded(self, uploads, entries):
        return tgc.decode_uploads(uploads, entries)

    def test_a_run_listed_twice(self):  # MC123, MC252, MC253, MC569, MC596, SC14
        st = state_of(*[(r, {"d#t": E1}, None) for r in self.runs()])
        u = st["config_uploads"]
        runs, relist = self.decoded([u[0], u[1], u[1], u[2], u[3]], st["config_entries"])
        self.assertEqual([r["build"] for r in runs], [r["build"] for r in self.runs()])
        self.assertEqual(relist, tgc.made(u[0]["build"]))

    def test_the_first_dropped_and_the_run_right_before_it(self):  # MC416, MC515, MC598, MC599, SC18
        st = state_of(*[(r, {"d#t": E1}, None) for r in self.runs()])
        u, e = st["config_uploads"], st["config_entries"]
        self.assertEqual(self.decoded([u[0], u[1], dict(u[2], commit="x"), dict(u[3], commit="x")], e)[1],
                         tgc.made(u[1]["build"]))
        self.assertEqual(self.decoded([u[0], dict(u[1], commit="x"), u[2], dict(u[3], commit="x")], e)[1],
                         tgc.made(u[0]["build"]))
        self.assertEqual(self.decoded([dict(u[0], commit="x"), u[1], u[2], u[3]], e)[1], 0)

    def test_malformed_changes_or_digest(self):  # MC396, MC397, SC55
        st = state_of(*[(r, {"d#t": E1}, None) for r in self.runs()[:2]])
        u, e = st["config_uploads"], st["config_entries"]
        for bad in (dict(u[1], group_changes={"job": "gone"}), dict(u[1], group_changes=["job"]),
                    dict(u[1], config_digest=5)):
            runs, relist = self.decoded([u[0], bad], e)
            self.assertEqual(([r["build"] for r in runs], relist), ([u[0]["build"]], tgc.made(u[0]["build"])), bad)

    def test_changes_kept_with_the_config_they_go_with(self):
        # MC495, MC508, MC509, MC568, MF215, MF346, MF352, MF469, MF470, MF472, MF473, SC17
        a, b, c, _ = self.runs()
        st = state_of((a, {"d#t": E1}, None), (b, {"d#t": E2}, {"job": "changed"}), (c, {"d#t": E3}, {"job": "new"}))
        u, e = st["config_uploads"], st["config_entries"]
        runs, _ = self.decoded([u[0], u[1], dict(u[2], config_digest="0" * 16)], e)  # (c's digest is not its config's)
        self.assertEqual([(r["tabs"] is not None, r["group_changes"]) for r in runs],
                         [(True, None), (True, {"job": "changed"}), (False, None)])


class Containers(unittest.TestCase):
    """job_of / checked_container / upload_job: the container a run of the upload job ran, refused
    when testgrid_config.sh could not run it as docker run's image, entrypoint and arguments."""

    def prowjob(self, **container):
        c = {"image": "gcr.io/k8s-staging-test-infra/configurator:v1", "command": ["configurator"],
             "args": ["--oneshot"]}
        c.update(container)
        return {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [{k: v for k, v in c.items() if v != "-"}]}}}

    def test_what_is_refused(self):  # MC176-MC178, MC180, MC185, MC318, MC327, MC537-MC539, MC585, MC586, MC608, SC42
        for bad, why in ((dict(image=5), "no image"), (dict(image=""), "no image"), (dict(command="configurator"), "no image"),
                         (dict(args="--oneshot"), "no image"), (dict(command=["configurator", 5]), "no image"),
                         (dict(args=["--a\n--b"]), "does not run"), (dict(args=["ok", "--a\0"]), "does not run"),
                         (dict(command=["configurator", "x\0"]), "does not run")):
            got = tgc.job_of(self.prowjob(**bad))
            self.assertIn(why, got.get("error", ""), bad)
        self.assertEqual(tgc.job_of(self.prowjob(args="-")),  # (no arguments: none)
                         {"image": "gcr.io/k8s-staging-test-infra/configurator:v1", "command": ["configurator"],
                          "args": []})  # MC318

    def test_a_prowjob_of_another_job_or_shape(self):  # MC330, MC331, MC333, SC40
        good = self.prowjob()
        other = json.loads(json.dumps(good))
        other["spec"]["job"] = "another-job"
        listed = json.loads(json.dumps(good))
        listed["spec"]["pod_spec"]["containers"] = {"0": good["spec"]["pod_spec"]["containers"][0]}
        named = json.loads(json.dumps(good))
        named["spec"]["pod_spec"]["containers"] = ["configurator"]
        for bad in (other, listed, named):
            self.assertIn("with one container", tgc.job_of(bad).get("error", ""), bad)

    def test_the_jobs_definition_with_other_than_one_container(self):  # MC059, MC195-MC197
        two = ttc.UploadJob.JOB.replace("      - image: gcr.io", "      - image: other/x\n      - image: gcr.io")
        bare = ttc.UploadJob.JOB.replace("      - image: gcr.io/k8s-staging-test-infra/configurator:v1\n"
                                         "        command: [configurator]\n", "      - just-a-name\n").replace(
            "        args: [--yaml=config/testgrids, --output=gs://bucket/config, --output, /tmp/x, "
            "--prowjob-url-prefix=https://x/,\n               --update-description, --oneshot]\n", "")
        for job in (two, bare):
            d = tempfile.mkdtemp()
            self.addCleanup(shutil.rmtree, d)
            os.makedirs(os.path.join(d, "config", "jobs"))
            with open(os.path.join(d, "config", "jobs", "j.yaml"), "w") as f:
                f.write(job)
            with self.assertRaisesRegex(ValueError, "containers"):
                tgc.upload_job(d)


class TabRuleOfAnEntry(unittest.TestCase):
    def test_an_entry_without_base_options(self):  # MC361
        rule = tgc.TabRule({"test_group": "job", "group": dict(ttc.GROUP), "group_digest": tgc.group_digest(ttc.GROUP)})
        self.assertEqual((rule.unknown_options, rule.broken, rule.filter_problem, rule.shows("x")), ([], None, None, True))


class MergeChanges(unittest.TestCase):
    """testgrid_config.merge: how a run changed its tabs' test groups."""

    def test_a_tab_whose_group_is_not_in_the_config_changed_nothing(self):  # MC545, SC48
        planned = {"after": 1, "runs": [], "compare": [{"build": "1", "recipe": "r1", "prev": "r0"}]}
        mine = {"recipe": "r1", "dashboards": {"d": {"t": group_entry("a"), "u": group_entry(None)}}, "groups": {}}
        before = {"recipe": "r0", "dashboards": {}, "groups": {"a": group_entry("a")["group_digest"]}}
        self.assertEqual(tgc.merge(planned, [mine, before], {})["group_changes"], {"1": {}})


class Plans(unittest.TestCase):
    """testgrid_config.plan: the runs of the upload job to list, and the configs to build (newest
    first) to know each run's config and how it changed its tabs' test groups (against a GCS of the
    upload job's runs, as round 8's UploadJobRuns serves them)."""

    upload = k8.UploadJobRuns.upload

    def setUp(self):
        k8.UploadJobRuns.setUp(self)
        self.srv.gcs_listing("kubernetes-ci-logs", self.objects)
        self.t0 = int(time.time()) - 2 * DAY

    def job_run(self, i, commit, finished=True):
        at = self.t0 + 600 * i
        return {"build": str(snowflake(at)), "commit": commit * 40, "started": at, "finished": at + 5 if finished else None}

    def recipes(self, out, runs):
        names = {tgc.recipe_id(r["commit"], None): r["commit"][0] for r in runs}
        return [names.get(b["recipe"], b["recipe"]) for b in out["build"]], \
            [(names.get(c["recipe"]), names.get(c["prev"])) for c in out["compare"]]

    def history(self, d_listed=True):
        """a, b (a tab of a new group j2), c (one of j3, its changes known), d (still running), e (one
        of j4): all their configs known; d and e listed now (d running, if d_listed; else it failed)."""
        e_ = group_entry
        runs = a, b, c, d, e = [self.job_run(i, x, x != "d") for i, x in enumerate("abcde")]
        state = state_of((a, {"d#t": e_("j1")}, None), (b, {"d#t": e_("j1"), "d#u": e_("j2")}, None),
                         (c, {"d#t": e_("j1"), "d#v": e_("j3")}, {"j3": "new"}), (d, {"d#t": e_("j1")}, None),
                         (e, {"d#t": e_("j1"), "d#w": e_("j4")}, None))
        self.upload(d["started"], finished=None if d_listed else False, commit=d["commit"])
        self.upload(e["started"], commit=e["commit"])
        return runs, state

    def test_the_configs_to_tell_how_runs_changed_their_groups(self):
        # MC035, MC037, MC148, MC149, MC283, MC291, MC307, MC425, MC447, MC449, MC495, MC509, MC522, MC531, MC533, MC535, MC568,
        # MC578, MF215, MF346, MF352, MF469, MF470, MF472, MF473
        # (b's new group j2: its config and a's are built; c's changes are known; d's group is a's; e's
        # new group j4: d still runs, not known whether d's config or c's came before)
        runs, state = self.history()
        out = tgc.plan(state, time.time())
        self.assertEqual(out["after"], tgc.made(runs[2]["build"]))  # (the last run whose config is known and that ended)
        self.assertEqual(self.recipes(out, runs), (["b", "a"], [("b", "a")]))

    def test_a_run_seen_running_that_failed_is_no_run_before(self):
        # MC148, MC291, MC296, MC307, MC425, MC447, MC495, MC509, MC533, MC535, MC568, MC577, MC578, MF215, MF346, MF352, MF469,
        # MF470, MF472, MF473
        runs, state = self.history(d_listed=False)
        self.assertEqual(self.recipes(tgc.plan(state, time.time()), runs), (["e", "c", "b", "a"], [("e", "c"), ("b", "a")]))

    def test_a_run_made_in_the_millisecond_the_listing_starts_after_is_kept(self):  # MC530, MC576, MC577, SC60
        a = self.job_run(0, "a")
        r = dict(self.job_run(0, "b", finished=False), build=str(int(a["build"]) + 1))
        out = tgc.plan(state_of((a, {"d#t": E1}, None), (r, None, None)), time.time())
        self.assertEqual(self.recipes(out, [a, r]), (["b", "a"], [("b", "a")]))

    def relisted(self, *bad, flag=None):
        runs = [self.job_run(i, x) for i, x in enumerate("abc")]
        state = state_of(*[(r, {"d#t": E1}, None) for r in runs], flag=flag)
        state["config_uploads"][2:2] = bad
        return runs, tgc.plan(state, time.time())["after"]

    def test_listed_again_from_before_a_dropped_run(self):
        # MC148, MC287, MC428, MC435, MC436, MC527, MC599-MC601, MC610, MC611, SC28, SC59
        runs, after = self.relisted("x")  # (where it was is not known: from the first run kept)
        self.assertEqual(after, tgc.made(runs[0]["build"]) - 1)
        between = {"build": str(snowflake(self.t0 + 900)), "commit": "x"}  # (malformed, between b and c)
        runs, after = self.relisted(between)
        self.assertEqual(after, tgc.made(runs[1]["build"]))
        runs, after = self.relisted(between, flag=self.t0 + 1000)  # (the flag after b: b's)
        self.assertEqual(after, tgc.made(runs[1]["build"]))
        runs, after = self.relisted(between, flag=self.t0 + 100)  # (before: the flag's)
        self.assertEqual(after, self.t0 + 100)

    def test_a_config_two_runs_ran_is_built_once(self):  # MC167, MC303
        runs = [self.job_run(0, "a"), self.job_run(1, "a")]
        for r in runs:
            self.upload(r["started"], commit=r["commit"])
        out = tgc.plan({}, time.time())
        self.assertEqual(self.recipes(out, runs), (["a"], [("a", "a"), ("a", None)]))

    def test_a_state_that_is_not_an_object_is_none(self):  # MC039, MC153
        self.upload(self.t0, commit="a" * 40)
        now = time.time()
        self.assertEqual(tgc.plan("x", now), tgc.plan({}, now))

    def test_the_gcs_it_is_given(self):  # MC150, MC151
        self.upload(self.t0, commit="a" * 40)
        with mock.patch.object(tgc, "GCS", "http://127.0.0.1:9"), mock.patch.object(tgc.time, "sleep"):
            out = tgc.plan({}, time.time(), gcs=self.srv.url)
        self.assertEqual([r["commit"] for r in out["runs"]], ["a" * 40])

    def test_a_run_the_archive_knows_is_built_as_it_ran(self):  # MC148, MC307, MC425, MC533, MC535, SC30
        # (b's group j2 is new to its tabs: b's config and a's are built, as their runs ran them (their
        # prowjob.json), though the listing, from c on, does not have them)
        a, b, c = [self.job_run(i, x) for i, x in enumerate("abc")]
        ran = {}
        for r, version in ((a, "v1"), (b, "v2")):
            ran[r["commit"]] = {"image": f"gcr.io/k8s-staging-test-infra/configurator:{version}",
                                "command": ["configurator"], "args": ["--x"]}
            self.upload(r["started"], commit=r["commit"])
            self.srv.route(f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{r['build']}/prowjob.json",
                           (200, {}, {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [ran[r["commit"]]]}}}))
        state = state_of((a, {"d#t": group_entry("j1")}, None), (b, {"d#t": group_entry("j1"), "d#u": group_entry("j2")}, None),
                         (c, {"d#t": group_entry("j1"), "d#u": group_entry("j2")}, {}))
        out = tgc.plan(state, time.time())
        self.assertEqual(out["build"], [{"recipe": tgc.recipe_id(r["commit"], ran[r["commit"]]), "commit": r["commit"],
                                         "job": ran[r["commit"]]} for r in (b, a)])
        self.assertEqual(out["runs"], [])

    def test_a_run_after_one_whose_config_is_not_known(self):  # MC167, MC286, MC303, MC307, MC425, MC533, MC535, SC31
        # (b's groups cannot be told apart from a's, whose config is not known: both are built)
        a, b = self.job_run(0, "a"), self.job_run(1, "b")
        state = state_of((a, None, None), (b, {"d#t": group_entry("j")}, None))
        self.assertEqual(self.recipes(tgc.plan(state, time.time()), [a, b]), (["b", "a"], [("b", "a"), ("a", None)]))


class Commands(unittest.TestCase):
    """testgrid_config.py's commands testgrid_config.sh runs: recipes, merge --failed, extract."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def write(self, name, text):
        path = os.path.join(self.dir, name)
        with open(path, "w") as f:
            f.write(text if isinstance(text, str) else json.dumps(text))
        return path

    def main(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = tgc.main(list(argv))
        return code, out.getvalue()

    def test_recipes(self):  # MC204, MC342
        uploads = self.write("uploads.json", {"build": [{"recipe": "r1", "commit": "a" * 40, "job": None},
                                                         {"recipe": "r2", "commit": "b" * 40, "job": None}]})
        self.assertEqual(self.main("recipes", uploads), (0, f"r1 {'a' * 40}\nr2 {'b' * 40}\n"))

    def test_merge_takes_the_configs_that_could_not_be_built(self):  # MC353, MC556, MC615, MC616, SC51
        rid = ttc.recipe_of("a" * 40)
        uploads = self.write("uploads.json", {"after": 1, "runs": [], "build": [], "compare": []})
        other = ttc.recipe_of("b" * 40)
        failed = self.write("failed.tsv", f"{rid}\t{'x' * 600}\nnot-a-recipe\twhy\n{other}\tshort\n")
        code, out = self.main("merge", "--failed", failed, uploads)
        self.assertEqual((code, json.loads(out)["failed"]), (0, {rid: "x" * 500, other: "short"}))
        code, out = self.main("merge", "--failed", os.path.join(self.dir, "missing.tsv"), uploads)
        self.assertEqual((code, json.loads(out)["failed"]), (0, {}))

    def test_extract_of_a_config_that_is_not_a_mapping(self):  # MC076, MC215
        config = self.write("config.yaml", "[1, 2]\n")
        self.assertEqual(self.main("extract", config, "--recipe", "r", "--commit", "a" * 40, "--committed", "1",
                                   "--image", "img", "--dashboard", "dash")[0], 1)

    def test_extract_digests_the_groups_that_have_a_name(self):  # MC354, MC479, MC480, MC482
        group = dict(ttc.GROUP)
        dashboards = [{"name": "dash", "dashboard_tab": [{"name": "t", "test_group_name": "job"}]}]
        args = ["--recipe", "r", "--commit", "a" * 40, "--committed", "1", "--image", "img", "--dashboard", "dash"]
        config = self.write("config.yaml", {"test_groups": [{"name": 3}, {"gcs_prefix": "x"}, group],
                                            "dashboards": dashboards})
        code, out = self.main("extract", config, *args)
        self.assertEqual((code, json.loads(out)["groups"]), (0, {"job": tgc.group_digest(group)}))
        config = self.write("bare.yaml", {"dashboards": dashboards})  # (no test groups)
        code, out = self.main("extract", config, *args)
        self.assertEqual((code, json.loads(out)["groups"]), (0, {}))


class RecipeIds(unittest.TestCase):
    def test_a_config_is_named_by_its_commit_and_the_container_it_ran(self):  # SC23, SC24
        job = {"image": "gcr.io/x/configurator:v1", "command": ["configurator"], "args": []}
        names = {tgc.recipe_id("a" * 40, None), tgc.recipe_id("a" * 40, job), tgc.recipe_id("a" * 12 + "b" * 28, None)}
        self.assertEqual(len(names), 3)
        self.assertTrue(all(tgc.RECIPE_RE.fullmatch(n) for n in names))

    def test_state_json_checks_a_config_whatever_the_order_of_its_tabs(self):  # SC12
        a, b = tch.run(NOW, "a"), tch.run(NOW + 100, "b")
        h = history([a, b], tch.configs(a={"d#t": E1, "d#u": E2}, b={"d#a": E3, "d#t": E1, "d#u": E2}))
        uploads, entries = h.encode()
        self.assertEqual(frl.ConfigHistory(json.loads(json.dumps(uploads)), json.loads(json.dumps(entries))).runs, h.runs)


FAILING_DOCKER = r10.FAKE_DOCKER.replace(
    'echo "configurator: cannot build this commit" >&2; exit 1',
    'printf "configurator: line one\\tand a tab\\nline two\\n" >&2; exit 1').replace(
    "cat \"$src/config/testgrids/config.yaml\"",
    "[ -e \"$src/config/testgrids/SLOW\" ] && sleep 9\n    cat \"$src/config/testgrids/config.yaml\"")


class ScriptRuns(unittest.TestCase):
    """testgrid_config.sh offline, as round 10's Script runs it (its own tests are not run again)."""

    setUp, git, built, run_script = r10.Script.setUp, r10.Script.git, r10.Script.built, r10.Script.run_script

    def commit(self, base_options, fail=False, slow=False):
        sha = r10.Script.commit(self, base_options, fail)
        if slow:
            open(os.path.join(self.src, "config/testgrids/SLOW"), "w").close()
            self.git("-C", self.src, "add", "-A")
            self.git("-C", self.src, "commit", "-q", "-m", "slow")
            sha = self.git("-C", self.src, "rev-parse", "HEAD")
            os.remove(os.path.join(self.src, "config/testgrids/SLOW"))
        return sha

    def listing(self, commits, jobs=None):
        runs = r10.Script.listing(self, commits)
        if jobs:
            with open(self.env["W4B_UPLOADS"]) as f:
                planned = json.load(f)
            for r, b, job in zip(runs, reversed(planned["build"]), jobs, strict=True):
                b["job"], r["job"] = job, job
                b["recipe"] = r["recipe"] = tgc.recipe_id(r["commit"], job)
            planned["runs"] = runs
            with open(self.env["W4B_UPLOADS"], "w") as f:
                json.dump(planned, f)
        return runs

    def docker(self, script):
        with open(os.path.join(self.tmp, "bin", "docker"), "w") as f:
            f.write(script)

    def test_why_a_config_could_not_be_built_on_one_line(self):  # SC51, SS03, SS10, SS11
        self.docker(FAILING_DOCKER)
        bad = self.commit("width=1", fail=True)
        self.listing([bad])
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        got = tgc.load(self.out)
        self.assertEqual(got["failed"], {tgc.recipe_id(bad, None):
                                         "the configurator failed: configurator: line one and a tab line two"})

    def test_a_commit_it_cannot_fetch(self):  # SS02
        good = self.commit("width=1")
        self.listing([good])
        with open(self.env["W4B_UPLOADS"]) as f:
            planned = json.load(f)
        planned["build"][0]["commit"] = "f" * 40  # (not in test-infra)
        with open(self.env["W4B_UPLOADS"], "w") as f:
            json.dump(planned, f)
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        why = tgc.load(self.out)["failed"][planned["build"][0]["recipe"]]
        self.assertTrue(why.startswith("git fetch: "), why)

    def test_each_config_with_the_container_its_run_ran_and_its_digest(self):  # SS13, SS14
        a, b = self.commit("width=1"), self.commit("width=2")
        jobs = [{"image": f"gcr.io/x/configurator:v{i}", "command": ["configurator"], "args": ["--oneshot"]} for i in (1, 2)]
        self.listing([a, b], jobs)
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        got = tgc.load(self.out)
        self.assertEqual({source["test_infra"]: (source["image"], source["digest"]) for _, source in got["configs"].values()},
                         {a: ("gcr.io/x/configurator:v1", "sha256:" + "0" * 64),
                          b: ("gcr.io/x/configurator:v2", "sha256:" + "0" * 64)})

    def test_no_build_begins_once_the_budget_is_spent(self):  # SS09
        # (bash counts SECONDS in whole seconds of the clock, from where the environment sets it: the
        # script starts just after a second begins, and its recipes step ends just after the third
        # second after that begins, so the check before the first build reads SECONDS 3 more than it
        # started at)
        self.listing([self.commit("width=1")])
        python = os.path.join(self.tmp, "python")
        with open(python, "w") as f:
            f.write(f'#!/usr/bin/env bash\nif [ "$3" = recipes ]; then "{sys.executable}" -c '
                    '"import os, time; time.sleep(max(0, float(os.environ[\'W5C_AT\']) - time.time()))"; fi\n'
                    f'exec "{sys.executable}" "$@"\n')
        os.chmod(python, 0o755)
        for start, built in ((996, 1), (997, 0)):  # (999 left one second; 1000, none)
            while time.time() % 1 > 0.05:
                time.sleep(0.005)
            at = math.floor(time.time()) + 3.05
            done = self.run_script(PYTHON=python, CONFIG_SECONDS="1000", SECONDS=str(start), W5C_AT=str(at))
            self.assertEqual(done.returncode, 0, done.stderr)
            got = tgc.load(self.out)
            self.assertEqual((len(got["runs"]), len(got["configs"])), (1, built), start)

    def test_a_step_may_run_a_minute_past_the_budget(self):  # SS01
        # (8 seconds of the budget left when the build begins; its configurator takes 9)
        self.docker(FAILING_DOCKER)
        slow = self.commit("width=1", slow=True)
        self.listing([slow])
        done = self.run_script(CONFIG_SECONDS="1000", SECONDS="992")
        self.assertEqual(done.returncode, 0, done.stderr)
        got = tgc.load(self.out)
        self.assertEqual((len(got["configs"]), got["failed"]), (1, {}))


# ------------------------------------------------------------ oracle/drift.sh

FAKE_CURL = """#!/usr/bin/env bash
# curl as drift.sh uses it: cluster/prod/COMPONENT.yaml with the image tag $TAG_COMPONENT
url=${*: -1}
component=${url##*/}
component=${component%.yaml}
tag=TAG_$component
echo "        image: gcr.io/k8s-testgrid/$component:${!tag}"
"""


class Drift(unittest.TestCase):
    """oracle/drift.sh: whether TestGrid production's updater and tabulator run the pinned commit
    (their image tags, as cluster/prod's YAML gives them: -g<commit> after a release, or the release
    itself), offline (curl a stand-in)."""

    def drift(self, updater, tabulator, pinned=None):
        """drift.sh's exit code and outputs, production running these image tags."""
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        oracle = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle")
        shutil.copy(os.path.join(oracle, "drift.sh"), d)
        with open(os.path.join(oracle, "pinned.json")) as f:
            pin = json.load(f)
        with open(os.path.join(d, "pinned.json"), "w") as f:
            json.dump(dict(pin, **(pinned or {})), f)
        bindir = os.path.join(d, "bin")
        os.makedirs(bindir)
        with open(os.path.join(bindir, "curl"), "w") as f:
            f.write(FAKE_CURL)
        os.chmod(os.path.join(bindir, "curl"), 0o755)
        out = os.path.join(d, "outputs")
        env = dict(os.environ, PATH=bindir + os.pathsep + os.environ["PATH"], GITHUB_OUTPUT=out,
                   TAG_updater=updater, TAG_tabulator=tabulator)
        done = subprocess.run(["bash", os.path.join(d, "drift.sh")], env=env, capture_output=True, text=True, timeout=60)
        with open(out) as f:
            return done.returncode, f.read().split()

    def test_the_pinned_commit_after_a_release(self):  # ST02, ST03, ST10
        # (a hash longer or shorter than the pinned one, of the same commit)
        self.assertEqual(self.drift("v20260902-v0.0.176-37-gf15bfa4808a4ab", "v20260902-v0.0.176-37-gf15bfa48"),
                         (0, ["updater=f15bfa4808a4ab", "tabulator=f15bfa48", "moved=false"]))

    def test_another_commit_or_a_tag_not_read(self):  # ST01, ST04, ST08, ST10
        self.assertEqual(self.drift("v20260902-v0.0.176-37-g0123456789ab", "v20260902-v0.0.176-37-gf15bfa4808a4-dirty"),
                         (1, ["updater=0123456789ab", "tabulator=", "moved=true"]))

    def test_a_release(self):  # ST04, ST05, ST07, ST08, ST10
        pinned = {"testgrid_module_version": "v0.0.178"}
        self.assertEqual(self.drift("v20261001-v0.0.178", "v20261001-v0.0.178", pinned),
                         (0, ["updater=v0.0.178", "tabulator=v0.0.178", "moved=false"]))
        self.assertEqual(self.drift("v20261001-v0.0.178", "v20261001-v0.0.178-rc1", pinned),
                         (1, ["updater=v0.0.178", "tabulator=", "moved=true"]))


if __name__ == "__main__":
    unittest.main()
