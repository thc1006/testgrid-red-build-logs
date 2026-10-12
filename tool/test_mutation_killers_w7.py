"""Mutation-testing gaps of review W7c, in the code changed since 14ff6b1 (to 1169263): the read
model's hold chains and vanishing (HoldModel: held_by, the pinned spans, a vanished build listed
again; spans_minus), the window a build's job is read in (Timeline.back, .most, .window, .grid),
gcs_column's reads of a vanished build, list_build_ids's endOffset, the tabulator's lag (TABULATE)
in tab_verdict and config_changed, a tab whose group reads another job (Elsewhere), the config
history's recipes and group changes (ConfigHistory, testgrid_config: decode_uploads, upload_runs,
plan), and the decode cache of testgrid_rule.column. Each test is tagged with the W7c mutants (run
on 1169263) it kills that the suite did not: MF, MC and MR ones are mechanical mutants of
fetch_red_logs.py, testgrid_config.py and testgrid_rule.py; SF, SC and SR ones hand-made. The tests
follow the code as it is now."""
import concurrent.futures as cf
import json
import math
import os
import signal
import sys
import tempfile
import time
import unittest
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_fetch_red_logs as tf  # noqa: E402  (module imports: their tests are not collected again here)
import test_mutation_killers_r8 as k8  # noqa: E402
import test_config_history as tch  # noqa: E402
import test_mutation_killers_r9 as k9  # noqa: E402
import test_review_round9 as r9  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_integrity import snowflake  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
M = k9.M  # (when the build judged, B, is made)


def runs(made, started=60, running_until=-INF, reach=None, nostart=False, good_until=INF):
    """A build's BuildRuns (see k9.buildruns), with when a read first finds it errored."""
    r = k9.buildruns(made, started, running_until, reach, nostart)
    r.good_until = good_until
    return r


# ------------------------------------------------------------ list_build_ids: GCS's offsets

def offsets_listing(objects, asked, served):
    """The GCS JSON list API as GCS answers it with a delimiter: the folders of the objects whose
    names are at or after startOffset and before endOffset (compared as strings)."""
    def serve(h):
        q = {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query).items()}
        asked.append(q)
        prefix, lo, hi = q["prefix"], q.get("startOffset", ""), q.get("endOffset")
        names = [o for o in objects if o.startswith(prefix) and o >= lo and (hi is None or o < hi)]
        folders = sorted({prefix + o[len(prefix):].split("/", 1)[0] + "/" for o in names if "/" in o[len(prefix):]})
        served.extend(folders)
        tf.send(h, 200, {}, json.dumps({"prefixes": folders}).encode())
    return serve


def build_at(t_ms, low=0):
    """The build ID Prow makes at Unix time t_ms (milliseconds), with these low 22 bits."""
    return str(((t_ms - frl.PROW_EPOCH_MS) << 22) | low)


class ListingEnds(unittest.TestCase):
    """list_build_ids asks GCS for the folders from the first ID of `lo` to (not including) the
    first ID of the second after `hi`, a string bound only when both have as many digits."""

    def setUp(self):
        self.srv = tf.FakeServer()
        self.addCleanup(self.srv.close)
        self.asked, self.served = [], []

    def listed(self, builds, lo, hi):
        objects = [f"logs/job/{b}/started.json" for b in builds]
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = offsets_listing(objects, self.asked, self.served)
        return frl.list_build_ids(self.srv.url, "bucket/logs/job", lo, hi)

    def test_the_listing_ends_a_second_after_hi(self):  # MF0026, MF0030-MF0042, MF0045-MF0047, SF001, SF003
        hi = M + 5 * HOUR
        first = build_at(int(M * 1000))
        last = build_at(hi * 1000 + 999, (1 << 22) - 1)  # (the last ID made in the second of hi)
        later = build_at((hi + 2) * 1000)  # (two seconds after: GCS need not list it)
        self.assertEqual(self.listed([first, last, later], M, hi), [first, last])
        self.assertIn("endOffset", self.asked[0])
        self.assertNotIn(f"logs/job/{later}/", self.served)

    def test_no_string_bound_across_a_digit(self):
        # MF0026, MF0032, MF0035, MF0036, MF0038, MF0044, MF0046, SF003, SF004
        # (IDs reached 19 digits on 2018-05-25: one made just before has 18, and sorts after every
        # 19-digit name as a string; from the epoch on, the listing starts at "0")
        hi = 1_527_254_000
        before = build_at(1_527_253_500_000)
        after = build_at(1_527_253_900_000)
        self.assertEqual((len(before), len(after)), (18, 19))
        self.assertEqual(sorted(self.listed([before, after], 0, hi)), sorted([before, after]))
        self.assertNotIn("endOffset", self.asked[0])


# ------------------------------------------------------------ spans_minus

class SpansMinus(unittest.TestCase):
    """spans_minus: the parts of closed spans outside open cuts."""

    def test_a_cut_inside_a_span_splits_it(self):  # MF0135, MF0142, MF0144, SF009
        self.assertEqual(frl.spans_minus([(0, 10)], [(3, 5)]), [(0, 3), (5, 10)])
        self.assertEqual(frl.spans_minus([(0, 10)], [(2, 3), (5, 6)]), [(0, 2), (3, 5), (6, 10)])

    def test_a_cut_outside_a_span_leaves_it(self):  # MF0135
        self.assertEqual(frl.spans_minus([(0, 10)], [(20, 30)]), [(0, 10)])
        self.assertEqual(frl.spans_minus([(0, 10)], [(-30, -20)]), [(0, 10)])

    def test_a_cut_over_either_end(self):  # MF0142, MF0144, SF009
        self.assertEqual(frl.spans_minus([(0, 10)], [(-5, 3)]), [(3, 10)])
        self.assertEqual(frl.spans_minus([(0, 10)], [(8, 15)]), [(0, 8)])
        self.assertEqual(frl.spans_minus([(0, 10)], [(-5, 15)]), [])

    def test_a_cut_that_touches_an_end_leaves_the_span(self):
        self.assertEqual(frl.spans_minus([(0, 10)], [(10, 15)]), [(0, 10)])
        self.assertEqual(frl.spans_minus([(0, 10)], [(-5, 0)]), [(0, 10)])

    def test_several_spans_and_cuts_as_one_union(self):  # MF0142, MF0144, SF009, SF011
        self.assertEqual(frl.spans_minus([(0, 4), (6, 10), (3, 5)], [(4.5, 7)]), [(0, 4.5), (7, 10)])


# ------------------------------------------------------------ HoldModel

class ListedAgain(unittest.TestCase):
    """A build that vanished (X: a column of a higher ID, C, that started before it settled while X
    ran) is listed again once no column pins it out (C too old for a tab of one day): read at the
    next update, it holds the newer columns of higher IDs again (B) while a column made then may
    run, until the next update after its last running read; and every read of it again puts a
    build without a start above it (N) at its start, which holds them too."""

    def test_a_vanished_build_listed_again_holds_newer_columns(self):
        # MF0143, MF0144, MF0465, MF0466, MF0468, MF0500, MF0555, MF0565-MF0567, MF0572-MF0574, NMF0026, NMF0031,
        # NMF0041, NMF0042, NMF0045, NMF0046, SF007, SF031, SF032, SF046, SF048, SF066
        # (X, pending 4 h, runs until M+25h10m: a column made at M+25h runs, one made 20 minutes later
        # does not; C, made a second later, starts first)
        until = M + 25 * HOUR + 10 * MINUTE
        x = runs(M, started=4 * HOUR, running_until=until)
        x.runs_at = lambda t, every=True: t <= (until - SPAN if every else until)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        b, b_start = snowflake(M + 6 * HOUR), M + 6 * HOUR + MINUTE
        # (C settles a gap after it stopped running: X vanishes; C is too old at M+25h, when X is listed
        # again; and as if it never vanished, it holds B from when B was made, review W7a-2)
        self.assertEqual(k9.Jobs(x, c).grid(b, b_start, 1)[0], [(M + 6 * HOUR, until)])
        self.assertIn((M + 25 * HOUR, until, x.started, -1), frl.HoldModel([x, c], DAY).entries)
        model = frl.HoldModel([x, c], DAY)
        self.assertEqual(model.gone[x.build][-1], [M + 25 * HOUR])  # (when it may be listed again)

    def test_it_holds_newer_columns_until_it_is_gone(self):
        # MF0465, MF0468, MF0500, MF0501, MF0555, MF0565, MF0567, MF0572, NMF0026, NMF0031, NMF0041, NMF0042, NMF0046,
        # SF031, SF034, SF036, SF045, SF066
        x = runs(M, started=4 * HOUR, running_until=INF)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        b, b_start = snowflake(M + 3 * HOUR), M + 4 * HOUR + 30 * MINUTE  # (made after C was done, started after X)
        # (as if it never vanished, review W7a-2, and once listed again, at M+25h: until the update after
        # its last running read before it is too old)
        self.assertEqual(k9.Jobs(x, c).grid(b, b_start, 1)[0], [(M + 3 * HOUR, x.started + DAY + SPAN - GAP)])

    def test_its_reads_once_listed_again_hold_through_a_build_without_a_start(self):
        # MF0465, MF0468, MF0500, MF0501, MF0555, MF0558, MF0559, NMF0031, SF031, SF036, SF043, SF044, SF052, SF066
        # (X stops running at M+24h, before it is listed again, but is read again a gap after that; N,
        # made between X and C, is put at X's start at each read of X)
        x = runs(M, started=4 * HOUR, running_until=M + 24 * HOUR)
        n = runs(M + 0.5, nostart=True)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        b, b_start = snowflake(M + 3 * HOUR), M + 4 * HOUR + 30 * MINUTE  # (made before X vanished, started after X)
        self.assertEqual(k9.Jobs(x, n, c).grid(b, b_start, 1)[0], [(M + 3 * HOUR, x.started + DAY + SPAN - GAP)])
        # (its reads: a gap after its last running read, and after it is listed again, later)
        self.assertEqual(frl.HoldModel([x, n, c], DAY).reads[x.build][-1][1], M + 25 * HOUR + GAP)

    def test_one_whose_read_at_gone_may_not_run_does_not_vanish(self):  # SF028, SF029
        # (X's deadline at M+4h50m: a column made at M+4h40m runs, one made 20 minutes later does not)
        until = M + 4 * HOUR + 50 * MINUTE
        x = runs(M, started=4 * HOUR, running_until=until)
        x.runs_at = lambda t, every=True: t <= (until - SPAN if every else until)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        self.assertEqual(frl.HoldModel([x, c], DAY).gone, {})

    def test_one_that_stopped_running_before_gone_does_not_vanish(self):  # SF029
        # (X runs when C is done, M+2h, but no longer at the last read before C settles, M+4h40m)
        x = runs(M, started=4 * HOUR, running_until=M + 3 * HOUR)
        c = runs(M + 1, started=HOUR - 1, running_until=M + 2 * HOUR)
        self.assertEqual(frl.HoldModel([x, c], DAY).gone, {})


class NoStartPutAtAGoodStart(unittest.TestCase):
    """A build without a start (N) is put just after the start of the good column read before it
    (G, +0.01 ms): it holds the columns that sort newer than that, not one of G's own start."""

    def test_not_a_column_of_the_same_start(self):  # SF055
        g = runs(M - 3 * HOUR, started=3 * HOUR)  # (pending 3 h: starts at M)
        n = runs(M - 2 * HOUR, nostart=True)
        b = snowflake(M - HOUR)  # (a higher ID than N's, started at M too)
        self.assertEqual(k9.Jobs(g, n).grid(b, M, 15)[0], [])
        self.assertEqual(k9.Jobs(g, n).grid(b, M + 1, 15)[0], [(M - HOUR, g.started + 40 * MINUTE)])


class AChangeReadsAGoodColumnAgainWithinAWeekOfItsStart(unittest.TestCase):
    """The update after a change of the group reads a good column (G) again only if it began less
    than 7 days after G started (to then at the latest): each read of G puts a build without a start
    (N) at G's start, which holds the newer columns of higher IDs (B) until the next update."""

    def test_only_reads_that_begin_within_the_week_hold(self):  # MF0535, MF0541, MF0543, MF0545, MF0552, MF0797, MF0805
        g = runs(M - 8 * DAY, started=5 * HOUR, reach=M - 8 * DAY + GAP)
        recent = g.started + 7 * DAY
        n = runs(recent - 3 * HOUR, nostart=True)
        b, b_start = snowflake(recent - 2 * HOUR), recent - 2 * HOUR + MINUTE
        within = [(recent - HOUR, recent + 3 * HOUR, "changed")]  # (it may begin before the week ends)
        self.assertEqual(k9.Jobs(g, n).grid(b, b_start, 15, within)[0], [(recent - HOUR, recent + SPAN)])
        after = [(recent, recent + HOUR, "changed"), (recent + 10 * MINUTE, recent + HOUR, "changed")]  # (these not)
        self.assertEqual(k9.Jobs(g, n).grid(b, b_start, 15, after)[0], [])


class Pinned(unittest.TestCase):
    """HoldModel.vanishing: X runs on (never finishes); a column of a higher ID that started before
    it (C) pins it out of the listing from when C is settled (a gap after its last running read,
    or once the updates that begin less than 20 minutes after its start are over) while C is good
    and not too old, but while an older column runs or an update after a change reads C again:
    (done, gone, pinned), gone where the first pin begins, done a gap before."""

    def vanishing(self, *others, events=(), x=None):
        """(done, gone, pinned), or None."""
        x = x or runs(M, running_until=INF)
        v = frl.HoldModel([x, *others], 15 * DAY, events).vanishing(x.build, x.started)
        return None if v is None else v[:3]

    def test_a_column_that_sorts_newer_does_not_pin(self):  # MF0692, SF066
        c1 = runs(M + 0.5, started=30, running_until=M + 5 * HOUR)  # (a twin: settles a gap after M+5h)
        c2 = runs(M + HOUR, started=MINUTE, running_until=M + HOUR + 5 * MINUTE)  # (newer: would settle first)
        settled = M + 5 * HOUR + GAP
        self.assertEqual(self.vanishing(c1, c2), (settled - GAP, settled, [(settled, c1.started + 15 * DAY)]))

    def test_one_never_seen_running_settles_after_its_first_updates(self):  # MF0718-MF0723, SF062, SF066
        # (C, pending 3 h, never read running: settled once the updates that begin less than 20 minutes
        # after its start are over, 40 minutes after it, later than a gap after it was made)
        x = runs(M, started=4 * HOUR, running_until=INF)
        c = runs(M + 1, started=3 * HOUR, running_until=-INF)
        settled = c.started + 40 * MINUTE
        self.assertEqual(self.vanishing(c, x=x), (settled - GAP, settled, [(settled, c.started + 15 * DAY)]))

    def test_one_errored_by_the_time_it_settles_does_not_pin(self):  # MF0726, SF065-SF067
        settled = M + 5 * HOUR + GAP
        self.assertIsNone(self.vanishing(runs(M + 0.5, started=30, running_until=M + 5 * HOUR, good_until=settled)))
        # (errored later: not read again while it is settled, so its good column stays in the grid)
        c = runs(M + 0.5, started=30, running_until=M + 5 * HOUR, good_until=settled + 1)
        self.assertEqual(self.vanishing(c), (settled - GAP, settled, [(settled, c.started + 15 * DAY)]))

    def test_a_column_that_vanished_itself_pins_only_where_it_never_did(self):  # MF0701-MF0703, MF0708, SF060, SF066
        # (C, between X and D by ID and by start, vanishes when D settles, as X does: D pins X until D
        # is too old, when X and C are both listed again; but C may never vanish, review W8a, and then
        # pins X until C is too old: X may be listed again at either)
        x = runs(M, running_until=INF)
        c = runs(M + 1, started=49, running_until=M + 5 * HOUR)
        d = runs(M + 2, started=8, running_until=M + HOUR)
        settled = M + HOUR + GAP
        self.assertEqual(frl.HoldModel([x, c, d], 15 * DAY).vanishing(x.build, x.started),
                         (settled - GAP, settled, [(settled, c.started + 15 * DAY)], d.started + 15 * DAY,
                          [d.started + 15 * DAY, c.started + 15 * DAY]))

    def test_a_change_cuts_the_pin_of_a_column_that_started_within_its_week(self):
        # MF0142, MF0144, MF0718-MF0723, MF0735, MF0779, MF0783, NMF0083, SF009, SF062, SF066, SF071
        # (C pending 3 h: an update that begins 7 days and an hour after C was made is still within C's
        # week, and reads it again)
        x = runs(M, started=5 * HOUR, running_until=INF)
        c = runs(M + 1, started=3 * HOUR - 1, running_until=-INF)
        lo = frl.created(c.build) + 7 * DAY + HOUR
        settled = c.started + 40 * MINUTE
        self.assertEqual(self.vanishing(c, x=x, events=[(lo, lo + HOUR, "changed")]),
                         (settled - GAP, settled, [(settled, lo), (lo + HOUR + SPAN, c.started + 15 * DAY)]))

    def test_updates_after_changes_that_read_it_again_cut_the_pin(self):
        # MF0135, MF0142, MF0144, MF0734, MF0735, MF0746-MF0748, MF0760, MF0762, MF0779, MF0783, NMF0083, SF009, SF066,
        # SF071
        c = runs(M + 0.5, started=30, running_until=M + 5 * HOUR)
        cs, settled = c.started, M + 5 * HOUR + GAP
        events = [(cs + 7 * DAY - HOUR, cs + 7 * DAY + 2 * HOUR, "changed"),  # (reads C again: may begin in its week)
                  (cs + 8 * DAY, cs + 8 * DAY + HOUR, "changed"),  # (C is no longer read again by this one)
                  (cs + 9 * DAY, cs + 9 * DAY + HOUR, "new")]  # (a new group's first update reads every build)
        pinned = [(settled, cs + 7 * DAY - HOUR), (cs + 7 * DAY + SPAN, cs + 9 * DAY),
                  (cs + 9 * DAY + HOUR + SPAN, cs + 15 * DAY)]
        self.assertEqual(self.vanishing(c, events=events), (settled - GAP, settled, pinned))

    def test_an_update_that_begins_a_week_after_its_start_does_not_cut(self):
        # MF0734, MF0752, MF0754, MF0756, MF0762, MF0763, SF066
        c = runs(M + 0.5, started=30, running_until=M + 5 * HOUR)
        cs, settled = c.started, M + 5 * HOUR + GAP
        events = [(cs + 7 * DAY, cs + 7 * DAY + HOUR, "changed"), (cs + 7 * DAY + 10 * MINUTE, cs + 7 * DAY + HOUR, "changed")]
        self.assertEqual(self.vanishing(c, events=events), (settled - GAP, settled, [(settled, cs + 15 * DAY)]))

    def test_the_pins_of_several_columns_as_one(self):  # MF0771, SF066, SF072
        # (A, older, made an hour after X, and B, made a second after it but pending 2 h, never read
        # running: B settles first, 2 h 40 min after it was made, A an hour later)
        x = runs(M, started=3 * HOUR, running_until=INF)
        b = runs(M + 1, started=2 * HOUR, running_until=-INF)
        a = runs(M + HOUR, started=MINUTE, running_until=-INF)
        settled = M + 1 + GAP
        self.assertEqual(self.vanishing(a, b, x=x), (settled - GAP, settled, [(settled, b.started + 15 * DAY)]))


class SeededReads(unittest.TestCase):
    """possible_reads: a read in a stretch an update after a change may read the build in (a seed),
    past its own reads, may happen until the end of that stretch."""

    def test_a_seeded_read_may_happen_until_the_seed_ends(self):  # SF017
        col = r9.Reads.col()
        reads, _, _ = frl.possible_reads(lambda t: col, 1000.0, [], [], 1000.0 + 10 * GAP, [(30000.0, 34000.0)])
        self.assertEqual([(t, end) for t, _, _, end in reads],
                         [(1000.0, 1000.0), (1000.0, 1000.0 + GAP), (30000.0, 30000.0), (30000.0, 34000.0),
                          (34000.0, 34000.0)])


class HowFarBack(unittest.TestCase):
    """Timeline.back: an update after a change of the group reads the build again (and so the
    builds of the 7 days before, or of a new group's days of results, and 6 h) if it may begin
    less than 7 days after the build started (6 h after it was made, while its start is not
    known)."""

    def test_a_change_within_the_week_after_its_start(self):  # MF0797, MF0805, MF0806, MF0808
        changed = [(M + 4 * DAY, M + 4 * DAY + HOUR, "changed")]
        self.assertEqual(frl.Timeline.back(M, changed, 15 * DAY, 0, M + MINUTE), 3 * DAY + 6 * HOUR)

    def test_a_change_before_a_build_not_started_yet(self):  # MF0801
        changed = [(M - DAY, M - 2 * HOUR, "changed")]
        self.assertEqual(frl.Timeline.back(M, changed, 15 * DAY, 0, None), 8 * DAY + 6 * HOUR)

    def test_one_that_ended_before_the_build_was_made_reaches_it_through_a_gap(self):  # MF0809-MF0812, MF0816, MF0818
        # (its update may begin by `to`, and read a build without a start, which holds the newer columns
        # until the next update, a gap later at most, and its reads 20 minutes more: a gap more for each
        # build without a start in the window)
        for nostart, ended in ((0, GAP + SPAN), (1, 2 * GAP + SPAN)):
            for before, want in ((ended - 10 * MINUTE, 9 * DAY + 6 * HOUR), (ended + 10 * MINUTE, frl.OLDER + nostart * GAP)):
                changed = [(M - 2 * DAY, M - before, "changed")]
                self.assertEqual(frl.Timeline.back(M, changed, 15 * DAY, nostart, M + MINUTE), want, (nostart, before))


class AChainBegunBeforeTheBuildWasMade(unittest.TestCase):
    """Timeline.grid reads the job's builds back as far as every event that may start a chain of
    holds reaching the build (Timeline.back), not only the events that read it again: an update
    after a change, over before B was made, reads G (made 2 days before) again; N1, without a start,
    put at G's start, holds G2 until a gap after; N2, put at G2's start at those reads, holds B,
    made after both, for its first half hour."""

    def test_a_change_over_before_the_build_was_made_holds_it_through_a_chain(self):  # SF086
        g = runs(M - 2 * DAY, started=MINUTE, reach=M - 2 * DAY + GAP)
        n1 = runs(M - 5 * HOUR, nostart=True)
        g2 = runs(M - 4 * HOUR, started=MINUTE, reach=M - 4 * HOUR + GAP)
        n2 = runs(M - HOUR, nostart=True)
        changed = [(M - 3 * HOUR, M - 2 * HOUR - 50 * MINUTE, "changed")]
        self.assertEqual(k9.Jobs(g, n1, g2, n2).grid(k9.B, k9.S, 15, changed)[0], [(M, M - 150 * MINUTE + GAP)])


class AWindowCutShort(unittest.TestCase):
    """Timeline.grid: a build whose window reaches `most` back (cut short: here the first update of
    a new group, 8 days before, reads every build of its 15 days) gets a HoldModel of its own
    window's builds."""

    def test_holds_by_a_model_of_its_window(self):  # MF0929-MF0933
        g = runs(M - 3 * DAY, started=MINUTE, reach=M - 3 * DAY + GAP)
        n = runs(M - 2 * HOUR, nostart=True)
        jobs = k9.Jobs(g, n)
        holds, _ = jobs.grid(k9.B, k9.S, 15, [(M - 8 * DAY, M + HOUR, "new")])
        self.assertEqual(jobs.asked[0][0], M - frl.Timeline.most(15 * DAY))
        # (N, put at G's start by that update's reads of G, holds B until a gap after)
        self.assertEqual(holds, [(M, M + HOUR + SPAN)])


class HoldEndsBeforeTheColumnIsTooOld(unittest.TestCase):
    """held_by: an older column that runs until its 24-hour deadline holds the newer ones until
    the last update that begins before it is too old (a tab of one day) re-reads them: its last
    running read may then be at most a gap before 24 h 20 min after its start."""

    def test_a_hold_ends_a_gap_before_the_column_is_too_old(self):  # MF0500, MF0501, SF036
        o = runs(M - 3 * HOUR, started=MINUTE, running_until=M - 3 * HOUR + MINUTE + DAY)
        start = M - 3 * HOUR + MINUTE
        self.assertEqual(k9.Jobs(o).held_until(k9.B, k9.S, days=1), start + DAY + SPAN - GAP)


class BuildRunsByTheGroupsOptions(r9.JobWorld):
    """A job's other builds run by its test group's cell options: without the Pod row
    (disable_prowjob_analysis), a finished build waits for no podinfo.json."""

    def test_a_build_without_its_pod_row_stops_running_at_its_finish(self):  # MF0398
        o = self.build(10 * HOUR, finished=5 * MINUTE)  # (no podinfo.json)
        made = frl.created(o)
        default = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(o)
        no_pod = frl.Timeline(self.srv.url, "bucket/logs/job", self.now, tr.Options(pod=False)).runs(o)
        self.assertGreaterEqual(default.running_until, made + 5 * MINUTE + frl.POD_WAIT - SPAN)
        self.assertLess(no_pod.running_until, made + 5 * MINUTE + 1)


class VanishedColumnReads(k8.Reads):
    """gcs_column of a build X that never finishes and vanished from the grid (C, a twin of a higher
    ID that started 30 seconds before it, stops running an hour after X was made and pins it out a
    gap later): X is read as if it never vanished too (C's column may never be appended, review
    W7a-2), and the reads once it is listed again are shown until the next."""

    def pinned_out(self, b, events=(), days=15, c_started=30):
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=c_started - 0.5, running_until=made + HOUR), now=self.now)
        col = self.column(b, days=days, timeline=jobs, events=events)
        _, gone, pinned = jobs.grid(b, made + 60 if c_started < 60 else made + 5 * HOUR, days, events)[1][:3]
        return col, gone, pinned, [(lo, until) for _, windows in col.reads for lo, until, _ in windows]

    def test_read_while_pinned_out_too_and_reads_between_pins_shown(self):
        # MF0142, MF0144, MF0779, MF1250, MF1253, MF1258, MF1261-MF1264, MF1277-MF1279, NMF0133, NMF0135, NMF0138,
        # NMF0140, NMF0143, NSF029, SF106, SF109, SF110, SF112, SF114
        # (an update after a change, 10 h after X was made, reads C again: X is listed again between
        # the pins, read, and shown until the next read)
        b = self.build(40 * HOUR)
        made = frl.created(b)
        col, gone, pinned, reads = self.pinned_out(b, events=[(made + 10 * HOUR, made + 11 * HOUR, "changed")])
        self.assertEqual(len(pinned), 2)
        self.assertNotEqual([lo for lo, _ in reads if any(a < lo < z for a, z in pinned)], [])
        between = [(lo, until) for lo, until in reads if pinned[0][1] <= lo < pinned[1][0]]
        self.assertTrue(between)
        self.assertTrue(all(until > lo for lo, until in between), between)
        self.assertIn(gone, [lo for lo, _ in reads])

    def test_one_listed_again_while_it_runs_is_read_on(self):
        # MF1227, MF1258, MF1261, MF1264, MF1277-MF1279, MF1288, NMF0138, NMF0143, NSF029, SF066, SF101, SF109, SF114,
        # SF115
        # (X pending 5 h, C started 2 minutes after it was made: on a tab of one day C is too old a day
        # after its start, when X, still running, is listed again; read since)
        b = self.build(26 * HOUR, started=5 * HOUR)
        made = frl.created(b)
        col, gone, pinned, reads = self.pinned_out(b, days=1, c_started=2 * MINUTE)
        back = pinned[-1][1]
        self.assertEqual(back, made + 2 * MINUTE + DAY)
        self.assertIn(back, [lo for lo, _ in reads])
        self.assertTrue(all(until > lo for lo, until in reads if lo >= back))
        self.assertTrue(col.open)


class BuildRunsOfFiles(r9.JobWorld):
    """BuildRuns from a build's files: a podinfo.json that is not JSON makes every later read of it
    an errored column (good_until: when it was written; one without a time counts as there from the
    start); runs_at(t): whether every column a read at t may make runs (or, with every=False, any)."""

    def test_an_errored_file_without_a_time_errs_from_the_start(self):  # SF024
        b = self.build(10 * HOUR, finished=5 * MINUTE)
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, b"{not json"))  # (no Last-Modified)
        self.assertEqual(frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b).good_until, -INF)
        b = self.build(10 * HOUR, finished=5 * MINUTE, podinfo=6 * MINUTE, pod=None)
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, r9.written(frl.created(b) + 6 * MINUTE), b"{not json"))
        self.assertEqual(frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b).good_until,
                         frl.created(b) + 6 * MINUTE)

    def test_a_read_just_before_the_deadline_may_make_a_running_or_a_failed_column(self):  # SF025
        b = self.build(30 * HOUR)  # (never finishes: FAIL 24 h after its start, a minute after it was made)
        deadline = frl.created(b) + MINUTE + frl.DEADLINE
        got = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).runs(b)
        self.assertEqual((got.runs_at(deadline - 10 * MINUTE), got.runs_at(deadline - 10 * MINUTE, every=False)),
                         (False, True))
        self.assertEqual((got.runs_at(deadline - 30 * MINUTE), got.runs_at(deadline + MINUTE, every=False)),
                         (True, False))


class RelevantEvents(unittest.TestCase):
    """relevant_events: an update after a change reads a build again if it may begin less than 7 days
    after the build started (6 h after it was made, while its start is not known)."""

    def test_while_its_start_is_not_known(self):  # SF020
        changed = [(M + 7 * DAY + HOUR, M + 7 * DAY + 2 * HOUR, "changed")]
        self.assertEqual(frl.relevant_events(M, None, changed), changed)
        self.assertEqual(frl.relevant_events(M, M + MINUTE, changed), [])


# ------------------------------------------------------------ a tab whose group reads another job

HERE_SHOWS, HERE_HIDES = tgc.TabRule(ttc.entry("")), tgc.TabRule(ttc.entry("exclude-filter-by-regex=^a$"))
AWAY_SHOWS = tgc.TabRule(ttc.entry("", gcs_prefix="bucket/logs/other"))
AWAY_HIDES = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^a$", gcs_prefix="bucket/logs/other"))


def judged(eras, read_at, made=0, since=None):
    """tab_verdict's state for a build of bucket/logs/job made at `made` whose one read, red on row a,
    was made at `read_at` and shown from then on, on a tab with these eras."""
    col = k9.column("a", reads=[("self", [(read_at, INF, read_at)])], now=30 * DAY)
    return frl.tab_verdict(col, "job", {"eras": eras, "days": 15}, made, query="bucket/logs/job", since=since)[0]


class ElsewhereEras(unittest.TestCase):
    """tab_verdict with eras whose test group reads another job (Elsewhere): such an era shows this
    job's columns made before the group left (by its own filters; TestGrid does not read a changed
    group's columns again), as read before it left; once the group reads this job again, the builds
    older than the week before that are not read under the new config; a tab's filters for row a
    decide each case (a: red on row a)."""

    def test_a_config_without_a_source_reads_no_other_job(self):  # MF1363, SF119
        none = tgc.TabRule(ttc.entry("", use_kubernetes_client=False))
        self.assertIsNone(none.prefix)
        (_, _, r), = frl.tab_eras({"eras": [(-INF, INF, none)]}, 0, "bucket/logs/job")
        self.assertIs(r, none)

    def test_only_the_config_right_after_it_comes_back_skips_old_builds(self):  # MF1431, MF1441, SF123
        eras = [(-INF, DAY, HERE_HIDES), (DAY - 30 * MINUTE, 9 * DAY, AWAY_HIDES), (9 * DAY - 30 * MINUTE, 12 * DAY, HERE_HIDES),
                (12 * DAY - 30 * MINUTE, INF, HERE_SHOWS)]
        self.assertEqual(judged(eras, 13 * DAY), "red")  # (a read made under the last config: by its options)

    def test_no_read_under_the_config_it_came_back_with_for_an_old_build(self):
        # MF1424, MF1425, MF1427-MF1429, MF1431, MF1437, MF1445, MF1450, MF1455, SF127
        eras = [(-INF, DAY, HERE_HIDES), (DAY - 30 * MINUTE, 9 * DAY, AWAY_HIDES), (9 * DAY - 30 * MINUTE, 12 * DAY, HERE_HIDES),
                (12 * DAY - 30 * MINUTE, INF, HERE_SHOWS)]
        self.assertEqual(judged(eras, 10 * DAY), "not-red")  # (made 9 days before it came back: never read then)

    def test_one_that_may_have_started_within_the_week_is_read(self):
        # MF1443, MF1446-MF1448, MF1450-MF1454, MF1456, SF125
        # (made 7 days 3 h before the group reads this job again: it may have started within the week)
        eras = [(-INF, 9 * DAY, AWAY_HIDES), (9 * DAY - 30 * MINUTE, INF, HERE_SHOWS)]
        self.assertEqual(judged(eras, 10 * DAY, made=9 * DAY - 30 * MINUTE - 7 * DAY - 3 * HOUR), "red")

    def test_reads_made_after_it_left_are_not_shown_by_its_config(self):  # MF1479, SF130, SF133
        away = 8 * DAY - 30 * MINUTE
        once = [(-INF, 8 * DAY, HERE_HIDES), (away, INF, AWAY_SHOWS)]
        twice = [(-INF, 8 * DAY, HERE_HIDES), (away, 10 * DAY, AWAY_HIDES), (10 * DAY - 30 * MINUTE, INF, AWAY_SHOWS)]
        for eras in (once, twice):
            self.assertEqual(judged(eras, 8 * DAY), "not-red")  # (made after it left: not by its config)
            self.assertEqual(judged(eras, away - MINUTE), "red")

    def test_nor_in_the_hour_after_it_changed_again(self):  # SF133, SF135
        eras = [(-INF, 8 * DAY, HERE_HIDES), (8 * DAY - 30 * MINUTE, 9 * DAY, AWAY_SHOWS),
                (9 * DAY - 30 * MINUTE, INF, HERE_HIDES)]
        self.assertEqual(judged(eras, 8 * DAY, since=9 * DAY), "not-red")
        self.assertEqual(judged(eras, 8 * DAY - 31 * MINUTE, since=9 * DAY), "red")

    def test_the_week_before_it_left_is_kept_too(self):  # MF1485, MF1488-MF1491, MF1493, MF1494, SF131, SF132
        # (no update reads a changed group's columns again: no stored grid holds the Config that
        # reprocessColumn compares, review W7a-3/W7a-4)
        away = 8 * DAY - 30 * MINUTE
        eras = [(-INF, 8 * DAY, HERE_HIDES), (away, INF, AWAY_SHOWS)]
        self.assertEqual(judged(eras, 6 * DAY, made=away - 3 * DAY), "red")
        self.assertEqual(judged(eras, 2 * DAY, made=away - 5 * DAY), "red")  # MF1493
        self.assertEqual(judged(eras, 2 * DAY, made=away - 7 * DAY - 3 * HOUR), "red")
        self.assertEqual(judged(eras, 8 * DAY, made=away), "not-red")  # (made as it left: not in its grid)


class ReadsUnderAConfigNotKnown(unittest.TestCase):
    """tab_verdict: a read made while a config not known yet was in force, shown after it (since),
    is undecided: its options then are not known."""

    def test_undecided_though_not_red_by_the_options_known(self):  # MF1469, SF129
        col = k9.column(reads=[("self", [(500, INF, 500)])], now=30 * DAY)  # (passes)
        rule = {"eras": [(-INF, 1000, frl.UNKNOWN), (900, INF, HERE_SHOWS)], "days": 15}
        # (from after the hour the tabulator may show that config on: only the read made under it)
        self.assertEqual(frl.tab_verdict(col, "job", rule, 0, since=10 * HOUR)[0], "undecided")


PROBLEM = tgc.TabRule(ttc.entry("", build_override_strftime="%Y"))  # (a test group option the port does not follow)


def problem_state(eras, reads, since=None, made=0):
    """tab_verdict's state for a build whose reads (passing columns) are given as
    [(read at, shown until)], the last first (None: a column with no reads kept)."""
    if reads is None:
        col = k9.column(now=30 * DAY)
    else:
        col = k9.column(reads=[("self" if i == 0 else k9.column(), [(a, b, a)]) for i, (a, b) in enumerate(reads)],
                        now=30 * DAY)
    return frl.tab_verdict(col, "job", {"eras": eras, "days": 15}, made, since=since)[0]


class ReadsUnderOptionsNotFollowed(unittest.TestCase):
    """tab_verdict: a read made while a config whose cell options the port does not follow (or one
    not known) was in force (until 20 minutes after it: an update that began by then), shown while a
    config of the tab was, leaves the build undecided; others do not."""

    def test_made_in_the_20_minutes_after(self):  # MF1459, MF1469, MF1470, MF1528, MF1580, SF129
        after, later = 1000 + 10 * MINUTE, 1000 + 30 * MINUTE
        self.assertEqual(problem_state([(-INF, 1000, PROBLEM), (900, INF, HERE_SHOWS)], [(after, INF)]), "undecided")
        self.assertEqual(problem_state([(-INF, 1000, PROBLEM), (900, INF, HERE_SHOWS)], [(later, INF)]), "not-red")
        unknown = [(-INF, 1000, frl.UNKNOWN), (900, INF, HERE_SHOWS)]
        self.assertEqual(problem_state(unknown, [(after, INF)], since=10 * HOUR), "undecided")
        self.assertEqual(problem_state(unknown, [(later, INF)], since=10 * HOUR), "not-red")

    def test_not_one_made_later(self):  # MF1580
        eras = [(-INF, 1000, HERE_SHOWS), (900, 2000, PROBLEM), (1900, INF, HERE_SHOWS)]
        self.assertEqual(problem_state(eras, [(2000 + 30 * MINUTE, INF)]), "not-red")

    def test_not_one_shown_only_before(self):  # MF1579, MF1580
        # (one made under it, shown until 1500; the scan since 1800 sees only the later read)
        eras = [(-INF, 1000, HERE_SHOWS), (900, 2000, PROBLEM), (1900, INF, HERE_SHOWS)]
        self.assertEqual(problem_state(eras, [(2000 + 30 * MINUTE, INF), (950, 1500)], since=1800), "not-red")

    def test_not_one_made_once_the_tab_was_gone(self):  # MF1578, MF1580
        # (made 100 s after its config was replaced by none, shown since: no tab showed it; the read
        # shown while that config was in force was made under the one before)
        eras = [(-INF, 400, HERE_SHOWS), (300, 1000, PROBLEM), (900, INF, None)]
        self.assertEqual(problem_state(eras, [(1100, INF), (200, 1500)]), "not-red")

    def test_a_column_with_no_reads_kept_counts_as_shown_any_time(self):  # MF1594-MF1596
        eras = [(-INF, 500, HERE_SHOWS), (400, 1000, PROBLEM), (900, INF, HERE_SHOWS)]
        self.assertEqual(problem_state(eras, None, since=600), "undecided")


# ------------------------------------------------------------ TestGrid's config over time

LATE = frl.PICKUP + GAP - SPAN  # (after a run ends, the update after its change may begin this long later)


class GroupChangesOfARun(unittest.TestCase):
    """ConfigHistory.events: how a run of the upload job changed the test groups the tab showed."""

    def test_a_changed_group_no_tab_had_before_is_new(self):  # SF151
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        h = k9.history([a, b], tch.configs(a={"d#u": k9.group_entry("other")}, b={"d#t": tch.E1}),
                       changes={b["build"]: {"job": "changed"}})
        self.assertEqual(h.events("d#t", tch.NOW + 1000), [(tch.NOW + 100, tch.NOW + 105 + LATE, "new")])

    def test_a_changed_group_and_one_not_known_are_both_events(self):  # SF153, SF155
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        job, job2 = k9.group_entry("job"), k9.group_entry("job2")
        changed = dict(job, group=dict(job["group"], days_of_results=10), group_digest="1" * 64)
        h = k9.history([a, b], tch.configs(a={"d#t": dict(job2, group_digest=None), "d#u": job},
                                           b={"d#t": changed, "d#u": job2}))
        self.assertEqual(sorted(h.events("d#t", tch.NOW + 1000)),
                         [(tch.NOW + 100, tch.NOW + 105 + LATE, "changed"), (tch.NOW + 100, tch.NOW + 105 + LATE, "unknown")])


class RecipesKept(unittest.TestCase):
    """ConfigHistory.merge keeps each run's recipe, and takes the one listed now for a run it knew."""

    def test_a_run_kept_without_one_takes_it(self):  # MF1638, SF149
        a = tch.run(tch.NOW, "a")
        h = k9.history([a], tch.configs(a={"d#t": tch.E1}))
        uploads, entries = h.encode()
        uploads = [{k: v for k, v in u.items() if k != "recipe"} for u in uploads]  # (kept before recipes were)
        again = frl.ConfigHistory(uploads, entries)
        self.assertIsNone(again.runs[0]["recipe"])
        again.merge(tch.loaded([a], {}))
        self.assertEqual(again.runs[0]["recipe"], ttc.recipe_of(a["commit"]))
        self.assertEqual(again.encode()[0][0]["recipe"], ttc.recipe_of(a["commit"]))


class RecipesInStateJson(unittest.TestCase):
    """testgrid_config.decode_uploads drops a run whose kept recipe is not one recipe_id makes."""

    def test_a_malformed_recipe_drops_the_run(self):  # MC0149, MC0202-MC0204, SC002, SC003
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (b, {"d#t": tch.E1}, {}))
        uploads, entries = state["config_uploads"], state["config_entries"]
        runs, relist = tgc.decode_uploads(uploads, entries, tch.NOW + 1000)
        self.assertEqual(([r["recipe"] for r in runs], relist), ([ttc.recipe_of("a" * 40), ttc.recipe_of("b" * 40)], None))
        for bad in ("x", 5, ttc.recipe_of("b" * 40) + "0"):
            runs, relist = tgc.decode_uploads([uploads[0], dict(uploads[1], recipe=bad)], entries, tch.NOW + 1000)
            self.assertEqual(([r["build"] for r in runs], relist), ([a["build"]], tgc.made(a["build"])), bad)


class KnownRunsNotReadAgain(unittest.TestCase):
    """testgrid_config.plan: the upload job's runs listed again that the archive knows (ended, with
    their recipes) are taken from state.json, not read from GCS again, nor is the container they ran
    (needed only to build a config)."""

    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def test_known_runs_listed_again(self):  # MC0389, MC0392, MC0474, MC0490, MC0505, SC008, SC011, SC015
        t = int(time.time()) - DAY
        a, b = self.upload(t, commit="a" * 40), self.upload(t + 60, commit="b" * 40)
        state = k9.state_of((tch.run(t, "a"), {"d#t": tch.E1}, {}), (tch.run(t + 60, "b"), {"d#t": tch.E1}, {}), flag=0)
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        out = tgc.plan(state, time.time())
        self.assertEqual({r["build"]: r["recipe"] for r in out["runs"]},
                         {a: ttc.recipe_of("a" * 40), b: ttc.recipe_of("b" * 40)})
        self.assertEqual((out["build"], out["compare"]), ([], []))
        for x in (a, b):
            for name in ("started.json", "finished.json", "prowjob.json"):
                self.assertEqual(self.srv.hits(f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{x}/{name}"), 0, (x, name))


class ExtractMappingsOnly(unittest.TestCase):
    """testgrid_config.extract takes only mappings as test groups, dashboards and tabs."""

    def test_anything_else_is_skipped(self):  # MC0296, MC0306, MC0314, MC0316, SC005
        config = {"test_groups": ["x", None, ttc.GROUP],
                  "dashboards": ["d", {"name": "d", "dashboard_tab": ["t", {"name": "t", "test_group_name": "job"}]}]}
        out = tgc.extract(config, ["d"])
        self.assertEqual(list(out["d"]), ["t"])
        self.assertEqual(out["d"]["t"]["group"], {k: ttc.GROUP[k] for k in tgc.GROUP_FIELDS if k in ttc.GROUP})


class UploadJobStopped(k9.ConfigRuns):
    """run() warns when the upload job lists no run after the last whose config the archive knows,
    and the latest run it knows began more than 3 days ago (not by when the archive listed it), or
    it knows none; main() hands SIGINT and the thread's context back when it returns."""

    def without_new_runs(self, *started):
        """A run of the fetch with the upload job's runs that began then known, and none listed now."""
        self.no_gap()
        self.history_of(*[(t, t + 60, self.tab(ttc.entry(""))) for t in started])
        path = os.path.join(self.root, "state.json")
        state = frl.read_json(path)
        if state.get("config_uploads"):
            state["config_uploads"][-1]["listed"] = self.now - HOUR  # (listed again an hour ago)
        frl.write_json(path, state)
        after = frl.created(state["config_uploads"][-1]["build"]) if state.get("config_uploads") else self.now - DAY
        return self.fetched(ttc.write_config(os.path.join(self.dir, "none.json"), [], {}, after=after))

    def test_by_when_its_last_run_began(self):  # MF2163, MF2166, SF166
        def mine(signum, frame):
            pass
        self.addCleanup(signal.signal, signal.SIGINT, signal.signal(signal.SIGINT, mine))
        caller = frl.context()
        self.assertIsNotNone(self.warning(self.without_new_runs(self.now - 4 * DAY), "has not run since"))
        self.assertIs(signal.getsignal(signal.SIGINT), mine)  # (main() hands SIGINT back)
        self.assertIs(frl.context(), caller)  # (and the context it was called with)

    def test_by_the_latest_run(self):  # MF2173
        self.assertIsNone(self.warning(self.without_new_runs(self.now - 10 * DAY, self.now - DAY), "has not run since"))

    def test_with_none_known(self):  # MF2174
        self.no_gap()
        run_ = self.fetched(ttc.write_config(os.path.join(self.dir, "none.json"), [], {}, after=self.now - DAY))
        self.assertIn(f"{tgc.HISTORY_DAYS} days ago or more", self.warning(run_, "has not run since"))


class StateJsonRuns(unittest.TestCase):
    """ConfigHistory reads state.json's runs as of `now` (time.time() by default): a run made more
    than a day after it is dropped (and listed again); why a config could not be built is kept, to
    300 characters, for a run whose config is not known only."""

    def test_a_run_from_the_future_is_dropped(self):  # MF1612-MF1615
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 3 * DAY, "b")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (b, {"d#t": tch.E1}, {}))
        uploads, entries = state["config_uploads"], state["config_entries"]
        h = frl.ConfigHistory(uploads, entries, [], None, tch.NOW + DAY)
        self.assertEqual(([r["build"] for r in h.runs], h.relist_after), ([a["build"]], tgc.made(a["build"])))
        self.assertEqual(len(frl.ConfigHistory(uploads, entries, [], None, tch.NOW + 3 * DAY).runs), 2)
        later = int(time.time()) + 3 * DAY  # (and by default, as of now)
        c = tch.run(later, "c")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (c, {"d#t": tch.E1}, {}))
        h = frl.ConfigHistory(state["config_uploads"], state["config_entries"])
        self.assertEqual([r["build"] for r in h.runs], [a["build"]])

    def test_why_a_config_was_not_built(self):  # MC0258, MC0259, MC0261
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (b, None, None))
        uploads = [dict(state["config_uploads"][0], failed="old"), dict(state["config_uploads"][1], failed="x" * 400)]
        runs, _ = tgc.decode_uploads(uploads, state["config_entries"], tch.NOW + 1000)
        self.assertEqual([r["failed"] for r in runs], [None, "x" * 300])

    def test_a_run_with_a_malformed_listing_time_or_failure_is_dropped(self):  # MC0147, MC0148
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (b, None, None))
        uploads, entries = state["config_uploads"], state["config_entries"]
        for bad in ({"listed": "x"}, {"listed": tch.NOW + 3 * DAY}, {"failed": 5}):
            runs, relist = tgc.decode_uploads([uploads[0], dict(uploads[1], **bad)], entries, tch.NOW + 1000)
            self.assertEqual(([r["build"] for r in runs], relist), ([a["build"]], tgc.made(a["build"])), bad)


class StateJsonTabs(unittest.TestCase):
    """load_state keeps a tab's "unsearched" (the older part of a history gap not searched yet) only
    as [from, to], two plausible times, from before to; and its "changed_pending" only as a list of
    {"since", "lo", "hi"} of plausible times, lo before hi."""

    def tab(self, **fields):
        with tempfile.TemporaryDirectory() as root:
            now = time.time()
            frl.write_json(os.path.join(root, "state.json"), {"tabs": {"d#t": {
                "scan": now - HOUR, "oldest": now - 2 * HOUR, **fields}}})
            return frl.load_state(root, [])["tabs"]["d#t"]

    def unsearched(self, value):
        return self.tab(unsearched=value).get("unsearched")

    def test_changed_pending_only_as_scans_of_ranges(self):  # MF2076, MF2083-MF2087, MF2089, MF2094, MF2097, MF2098
        t = time.time() - 3 * DAY
        good = {"since": t, "lo": t - DAY, "hi": t - HOUR}
        self.assertEqual(self.tab(changed_pending=[good])["changed_pending"], [good])
        for bad in (5, ["since", "lo", "hi"], [["since", "lo", "hi"]], [dict(good, since="x")], [dict(good, lo=t)],
                    [dict(good, x=1)]):
            self.assertNotIn("changed_pending", self.tab(changed_pending=bad), bad)
        many = [dict(good, since=t + i) for i in range(61)]  # (the last 60 at most)
        self.assertEqual(self.tab(changed_pending=many)["changed_pending"], many[1:])

    def test_only_a_span_of_two_times(self):  # MF2056, MF2060, MF2062, MF2066
        t = time.time() - 3 * DAY
        self.assertEqual(self.unsearched([t, t + HOUR]), [t, t + HOUR])
        for bad in ([t, t + HOUR, t + 2 * HOUR], [t + HOUR, t], [t, "x"], [t], "x", None):
            self.assertIsNone(self.unsearched(bad), bad)


# ------------------------------------------------------------ backfill_gaps: a watched build's `since`

class WatchedSinceOfATab(unittest.TestCase):
    """backfill_gaps: a watched build listed by a range of a scan after its tab's config changed is
    judged on that tab from the earliest such scan (its tab's `since`); one judged on it from any
    time keeps that."""

    def gap(self, b, since):
        srv = tf.FakeServer()
        self.addCleanup(srv.close)
        srv.gcs_listing("bucket", [f"logs/job/{b}/started.json"])
        made = frl.created(b)
        report = {"query": "bucket/logs/job", "dashboard": "dash", "tab": "t", "oldest": made + DAY}
        return srv, ("dash#t", report, None, None, [(made - HOUR, made + HOUR, since)])

    def watched(self, b, tab):
        return {"job": "job", "build": b, "query": "bucket/logs/job", "started": None, "created": frl.created(b),
                "watch": True, "tabs": [dict({"dashboard": "dash", "tab": "t", "red_cells": []}, **tab)]}

    def test_a_since_that_is_no_time_is_replaced(self):  # MF2149, SF161
        b = snowflake(M)
        srv, gap = self.gap(b, M + DAY)
        rec = self.watched(b, {"since": "x"})
        with cf.ThreadPoolExecutor(2) as pool:
            frl.backfill_gaps([gap], srv.url, pool, time.monotonic() + 60, {}, set(), watched={f"job/{b}": rec})
        self.assertEqual(rec["tabs"][0]["since"], M + DAY)

    def test_the_earliest_scan_that_saw_it(self):  # MF2150
        b = snowflake(M)
        srv, gap = self.gap(b, M + DAY)
        rec = self.watched(b, {"since": M + HOUR})
        with cf.ThreadPoolExecutor(2) as pool:
            frl.backfill_gaps([gap], srv.url, pool, time.monotonic() + 60, {}, set(), watched={f"job/{b}": rec})
        self.assertEqual(rec["tabs"][0]["since"], M + HOUR)

    def test_one_judged_from_any_time_stays_so(self):  # MF2144, SF162
        b = snowflake(M)
        srv, gap = self.gap(b, M + DAY)
        rec = self.watched(b, {})
        with cf.ThreadPoolExecutor(2) as pool:
            frl.backfill_gaps([gap], srv.url, pool, time.monotonic() + 60, {}, set(), watched={f"job/{b}": rec})
        self.assertNotIn("since", rec["tabs"][0])


# ------------------------------------------------------------ the tabulator's lag

class TabulatorHour(unittest.TestCase):
    """TestGrid's tabulator may show a tab's old filters on the columns read before for an hour
    after TestGrid took the new config (TABULATE): a scan 45 minutes after that may still have
    seen the old filters, one an hour and a minute after cannot."""

    HIDES = tgc.TabRule(ttc.entry("exclude-filter-by-regex=^a$"))
    SHOWS = tgc.TabRule(ttc.entry(""))

    def col(self):
        # (a read red on row a, shown from 100 until long after the config changed at 1000)
        return k9.column("a", reads=[("self", [(100, 10 * DAY)])], now=20 * DAY)

    def test_a_scan_45_minutes_after_the_change_may_have_seen_the_old_filters(self):
        # MF0076, MF0077, MF1509, MF1513-MF1517, MF1528
        rule = {"eras": [(0, 1000, self.SHOWS), (900, INF, self.HIDES)], "days": 15}
        self.assertEqual(k9.rows(frl.tab_verdict(self.col(), "job", rule, 0, since=1000 + 45 * MINUTE)), ("red", ["a"]))
        self.assertEqual(frl.tab_verdict(self.col(), "job", rule, 0, since=1000 + 61 * MINUTE), ("not-red", []))

    def test_config_changed_counts_the_hour(self):  # MF0076, MF0077
        rule = {"eras": [(0, 1000, self.HIDES), (900, INF, self.SHOWS)], "events": []}
        self.assertTrue(frl.config_changed(rule, 1000 + 59 * MINUTE))
        self.assertTrue(frl.config_changed(rule, 1000 + 45 * MINUTE))
        self.assertFalse(frl.config_changed(rule, 1000 + HOUR))
        self.assertFalse(frl.config_changed(rule, 1000 + 61 * MINUTE))


if __name__ == "__main__":
    unittest.main()
