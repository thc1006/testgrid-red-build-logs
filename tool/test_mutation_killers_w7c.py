"""Mutation-testing gaps of review W7c on snap-7caed27: the read model's vanishing after the fix
of its circular `back` (HoldModel.vanishing: the model without the build, how `back` is formed),
gcs_column's reads of a vanished build, the cut-short windows of Timeline.grid, a build without a
start put at a good start (HoldModel._pass), list_build_ids, a kept margin of a tab, and the code
7caed27 adds (testgrid_config.valid_built and plan's steps, ConfigHistory's "built",
judged_since and the late rule). Each test is tagged with the mutants it kills that the suite
did not (VN, LB, PL, VB, DB, MB, EN, JS, LR, CS, TC, GW: W7c's on 7caed27; SF, MF, NSF, NMF: the
earlier W7c passes, located again on 7caed27)."""
import math
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected again here)
import test_fetch_red_logs as tf  # noqa: E402
import test_mutation_killers_r8 as k8  # noqa: E402
import test_mutation_killers_r9 as k9  # noqa: E402
import test_mutation_killers_w7 as w7  # noqa: E402
import test_review_round4 as r4  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_integrity import snowflake  # noqa: E402

MINUTE, HOUR, DAY = 60, 3600, 86400
INF = math.inf
GAP, SPAN = frl.READ_GAP, frl.UPDATE_SPAN
M = k9.M
runs = w7.runs
FAILING = b'<testsuite><testcase name="t"><failure/></testcase></testsuite>'


# ------------------------------------------------------------ HoldModel.vanishing after the fix

class NewerColumnsPinItOut(unittest.TestCase):
    """X (never finishes) vanishes once C1, a column of a higher ID that started before it, is
    settled; C2, of a higher ID too but started after X, is done early and pins X out from the
    update after X's last running read in the grid (a gap after `gone`), when it no longer holds
    C2: X is listed again (`back`) when the later of their pins ends. On a tab of one day."""

    def setUp(self):
        self.x = runs(M, started=60, running_until=INF)
        self.c2 = runs(M + 2, started=118, running_until=M + HOUR)  # (started at M+2m)

    def c1(self, settled_before_too_old):
        # (started at M+30s: settled a gap after its last running read, so long before it is too old)
        return runs(M + 1, started=29, running_until=M + 30 + DAY - settled_before_too_old - GAP)

    def back(self, *others):
        return frl.HoldModel([self.x, *others], DAY).vanishing(self.x.build, self.x.started)[3]

    def test_VN13_VN16_the_model_without_it_holds_nothing_of_it_after_gone(self):
        # (C1 settles a gap and a half hour before it is too old: X's hold on C2 ends at gone, C2 pins X
        # from a gap later, while C1 still does; N, without a start, is put at X's start at X's reads, so
        # holds C2 too, until gone: X is not read once out of the grid, nor runs a moment longer there)
        n = runs(M + 0.5, nostart=True)
        c1 = self.c1(GAP + 30 * MINUTE)
        self.assertEqual(self.back(c1, self.c2, n), self.c2.started + DAY)
        self.assertEqual(self.back(c1, self.c2), self.c2.started + DAY)

    def test_VN14_it_holds_the_newer_columns_until_gone(self):
        # (C1 settles half an hour before it is too old: C2's pin, a gap after gone, begins after C1's ends)
        c1 = self.c1(30 * MINUTE)
        self.assertEqual(self.back(c1, self.c2), c1.started + DAY)

    def test_VN18_other_vanished_builds_stay_gone_in_the_model_without_it(self):
        # (Y, of the lowest ID, started between X and C2 and never finishes, vanishes too: C1 and C0 (an ID
        # between Y's and X's, started between C1 and X, done early) pin it out from gone until C0 is too
        # old; so it no longer holds C2 after gone, and C2 pins X out until Y is listed again. Were Y still
        # in the grid, it would hold C2 until a gap before Y is too old, after C1 is: X back at C1's end)
        y = runs(M - 10, started=110, running_until=INF)
        c0 = runs(M - 5, started=50, running_until=M + HOUR)
        c1 = self.c1(GAP + 30 * MINUTE)
        # (but Y may never vanish either, review W7a-2: as it would, it holds C2 until it is too old,
        # and X comes back once C1 is)
        self.assertEqual(self.back(c1, self.c2, y, c0), c1.started + DAY)

    def test_VN06_a_newer_column_of_a_lower_id_does_not_pin_it_out(self):
        # (L, made before X and pending 2 h, sorts newer; the listing after C2's hint has X, of a higher ID)
        lower = runs(M - 1, started=2 * HOUR + 1, running_until=M + HOUR)
        c1 = self.c1(GAP + 30 * MINUTE)
        self.assertEqual(self.back(c1, self.c2, lower), self.c2.started + DAY)

    def test_VN11_it_leaves_the_model_it_is_asked_of_as_it_was(self):
        # (one HoldModel serves every build of the job judged with the same days and events)
        model = frl.HoldModel([self.x, self.c1(GAP + 30 * MINUTE), self.c2], DAY)
        gone = dict(model.gone)
        model.vanishing(self.x.build, self.x.started)
        self.assertEqual(model.gone, gone)


class NoStartAtTheNextGoodStart(unittest.TestCase):
    """A build without a start (N) is put at the start of the highest good column below it (G)
    while G is good and not too old: only from then on at the next one (G2: a lower ID, started
    later, read all along), whose reads then hold the newer columns of higher IDs (C)."""

    def test_SF053_not_while_the_first_is_not_too_old_yet(self):
        g2 = runs(M - 31 * HOUR, started=2 * HOUR, reach=INF)
        g = runs(M - 30 * HOUR, started=MINUTE)
        n = runs(M - 29.5 * HOUR, nostart=True)
        c = runs(M - 20 * HOUR, started=MINUTE)
        too_old = g.started + DAY + SPAN
        holds = frl.HoldModel([g2, g, n, c], DAY).holds(c.build, c.started)
        self.assertEqual([(lo, hi) for lo, hi in holds if lo < too_old], [])


# ------------------------------------------------------------ Timeline.grid: a window cut short

class ACutShortWindowIsItsOwn(unittest.TestCase):
    """B's window reaches Timeline.most back (the first update of a new group, 8 days before it,
    reads every build of its 15 days): it gets a HoldModel of its own builds, so it is judged the
    same whatever was judged before. A, made 2 days earlier, has a window that is not cut short and
    reaches further back, to G: made 23 days before B, but pending 20 days (a stand-in for the long
    chain of holds, through some 190 builds without a start, that a real G would need), so not too
    old at B; N, without a start, is put at G's start at that update's reads of G, and holds B.
    So does the window of B2, made a day before B, cut short too."""

    def test_SF088_SF089_MF0926_MF0927_MF0928_b_is_judged_alike_after_a(self):
        new = [(M - 8 * DAY, M + HOUR, "new")]

        def jobs():
            return k9.Jobs(runs(M - 23 * DAY, started=20 * DAY), runs(M - 2 * HOUR, nostart=True))
        alone = jobs().grid(k9.B, k9.S, 15, new)
        self.assertEqual(alone, ([], None))
        for days_before in (2, 1):  # (A; and B2, whose window is cut short too, and has G)
            after = jobs()
            after.grid(snowflake(M - days_before * DAY), M - days_before * DAY + 60, 15, new)
            self.assertLess(after.asked[0][0], M - 23 * DAY)
            self.assertEqual(after.grid(k9.B, k9.S, 15, new), alone, days_before)


# ------------------------------------------------------------ gcs_column of a vanished build

class OutOfTheGridForGood(k8.Reads):
    """X never finishes; C1 (a higher ID, started first) settles an hour after X was made and pins it
    out a gap later; C2 (a higher ID, started 3 h after X was made, never seen running) pins it out
    until C2 is too old, after X is (a tab of one day): X is never listed again; but C1's column may
    never be appended (review W7a-2), so X is read on as if it never vanished, FAIL at its deadline."""

    def test_NSF030_its_reads_go_on_as_if_it_never_vanished(self):
        b = self.build(DAY + 11 * MINUTE)  # (now: 10 minutes before X is too old)
        made = frl.created(b)
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=29.5, running_until=made + HOUR),
                       k9.buildruns(made + 1, started=3 * HOUR - 1), now=self.now)
        col = self.column(b, days=1, timeline=jobs)
        self.assertEqual(frl.verdict(col, "job")[0], "red")


class ListedAgainWhileItMayRun(k8.Reads):
    """X vanishes (C1, a higher ID that started first, settles); an update after a change of the
    group reads C1 again 10 h after X was made, which cuts C1's pin: X is listed again then
    (`back`), while its Pod row waits for podinfo.json (it finished passing 9 h 10 min after it
    was made, and none came): a column made then may run, so X is listed again once C1's second
    pin ends too, 15 days on, and read: a failing junit file written two days after X was made is
    read then."""

    def test_NMF0118_NSF024_it_is_read_again_if_one_column_made_then_runs(self):
        junits = [("junit_01.xml", 2 * DAY, FAILING)]
        b = self.build(15 * DAY + 4 * HOUR, finished=9 * HOUR + 10 * MINUTE, junits=junits)
        made = frl.created(b)
        events = [(made + 10 * HOUR, made + 11 * HOUR, "changed")]
        jobs = k9.Jobs(k9.buildruns(made + 0.5, started=29.5, running_until=made + HOUR), now=self.now)
        _, gone, pinned, back, ends = jobs.grid(b, made + 60, 15, events)[1]
        self.assertEqual((back, len(pinned), ends[-1]), (made + 10 * HOUR, 2, made + 30 + 15 * DAY))
        self.assertEqual(self.judged(b, timeline=jobs, events=events), "red")


class JunitFileNames(unittest.TestCase):
    """gcs_column's columns name a junit file by the object TestGrid opens (artifacts/./junit_2.xml,
    as listed, opens artifacts/junit_2.xml)."""

    def test_MF1147_the_object_opened_names_one_that_is_not_there(self):
        srv = tf.FakeServer()
        self.addCleanup(srv.close)
        frl.BACKOFF = 0
        build = "2109000000000000000"
        base = f"/bucket/logs/job/{build}"
        srv.route(base + "/started.json", (200, {}, {"timestamp": frl.created(build)}))
        srv.route(base + "/finished.json", (200, {}, {"timestamp": frl.created(build) + 600, "passed": True}))
        srv.gcs_listing("bucket", [f"logs/job/{build}/artifacts/./junit_2.xml"])
        col = frl.gcs_column(srv.url, "bucket/logs/job", build)
        self.assertEqual(col.malformed, ["artifacts/junit_2.xml: open: storage: object doesn't exist"])


# ------------------------------------------------------------ list_build_ids

class ListingStart(unittest.TestCase):
    """list_build_ids asks GCS for the folders from the first ID of `lo` on (startOffset), when
    that ID has as many digits as the last: not for the job's older builds."""

    def test_LB4_the_listing_starts_at_the_first_id_of_lo(self):
        srv = tf.FakeServer()
        self.addCleanup(srv.close)
        asked, served = [], []
        earlier, first = w7.build_at(int((M - HOUR) * 1000)), w7.build_at(int(M * 1000))
        srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = w7.offsets_listing(
            [f"logs/job/{b}/started.json" for b in (earlier, first)], asked, served)
        self.assertEqual(frl.list_build_ids(srv.url, "bucket/logs/job", M, M + HOUR), [first])
        self.assertEqual(served, [f"logs/job/{first}/"])


# ------------------------------------------------------------ run(): a tab's kept margin

class KeptMargin(r4.World):
    """A tab's margin kept in state.json (its job's longest run at its last scan) widens the part
    of its history the last scan saw only if it is one (more than 0, less than 7 days)."""

    def test_MF2233_a_kept_margin_of_a_week_is_no_margin(self):
        visible, seen = snowflake(self.now - HOUR), snowflake(self.now - 58 * HOUR)  # (the last scan saw it)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        self.failed_in_gcs(seen, 58 * HOUR)
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (seen, visible)])
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR,
                                        "margin": 7 * 24}})
        self.fetch()
        run = self.runs()[-1]
        self.assertNotIn("backfilled", run["outcomes"])
        gap = next(w for w in run["warnings"] if "needed them from" in w)
        self.assertNotIn(f"needed them from {frl.iso(self.now - 60 * HOUR)}", gap)


# ------------------------------------------------------------ 7caed27: what a run's config was built from

class ValidBuilt(unittest.TestCase):
    """testgrid_config.valid_built: {"committed": test-infra's commit time, "image": the
    configurator's, and its "digest" if known}, nothing else."""

    def test_VB1_VB2_VB3_VB4_VB5_what_a_config_was_built_from(self):
        good = {"committed": tch.NOW - DAY, "image": "gcr.io/k8s-staging-test-infra/configurator:v1"}
        self.assertTrue(tgc.valid_built(good))
        self.assertTrue(tgc.valid_built(dict(good, digest="sha256:" + "0" * 64)))
        for bad in (dict(good, test_infra="a" * 40), dict(good, committed="yesterday"), dict(good, image=""),
                    dict(good, digest=""), dict(good, digest=5), {"image": "x"}):
            with self.subTest(bad):
                self.assertFalse(tgc.valid_built(bad))

    def test_DB1_a_run_whose_built_is_not_one_is_dropped_from_state_json(self):
        a, b = tch.run(tch.NOW, "a"), tch.run(tch.NOW + 100, "b")
        state = k9.state_of((a, {"d#t": tch.E1}, {}), (b, {"d#t": tch.E1}, {}))
        uploads, entries = state["config_uploads"], state["config_entries"]
        for bad in ({"image": ""}, {"committed": tch.NOW, "image": "x", "test_infra": "b" * 40}, "x"):
            runs, relist = tgc.decode_uploads([uploads[0], dict(uploads[1], built=bad)], entries, tch.NOW + 1000)
            self.assertEqual(([r["build"] for r in runs], relist), ([a["build"]], tgc.made(a["build"])), bad)
        good = {"committed": tch.NOW, "image": "x"}
        runs, _ = tgc.decode_uploads([uploads[0], dict(uploads[1], built=good)], entries, tch.NOW + 1000)
        self.assertEqual(runs[1]["built"], good)

    def test_MB2_a_run_keeps_only_a_valid_one(self):
        a = tch.run(tch.NOW, "a")
        for source, want in (({"test_infra": "a" * 40, "image": "img"}, None),  # (no commit time)
                             ({"test_infra": "a" * 40, "committed": tch.NOW - 100, "image": "img"},
                              {"committed": tch.NOW - 100, "image": "img"})):
            h = frl.ConfigHistory()
            h.merge(tch.loaded([a], {"a" * 40: ({"d#t": tch.E1}, source)}))
            self.assertEqual(h.runs[0]["built"], want, source)
            runs, relist = tgc.decode_uploads(*h.encode(), tch.NOW + 1000)  # (and state.json keeps the run)
            self.assertEqual(([r["build"] for r in runs], relist), ([a["build"]], None))


# ------------------------------------------------------------ 7caed27: plan()'s steps

class PlanSteps(unittest.TestCase):
    """testgrid_config.plan: each run whose config to build, compared with the run before when its
    changes are to be worked out now; the containers they ran, of runs listed before (the archive
    kept only their recipes), read at once, in a pool; a run listed now is not read again."""

    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload
    CONTAINER = {"image": "gcr.io/k8s-staging-test-infra/configurator:v1", "command": ["configurator"],
                 "args": ["--yaml=config/testgrids", "--output=gs://x/y"]}

    def uploads(self, t, letters):
        prowjob = {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [self.CONTAINER]}}}
        return [self.upload(t + 60 * i, commit=c * 40, prowjob=prowjob) for i, c in enumerate(letters)]

    def prowjob(self, b):
        return f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{b}/prowjob.json"

    def test_PL1_a_run_whose_config_could_not_be_built_is_built_but_not_compared(self):
        t = int(time.time()) - DAY
        _, b = self.uploads(t, "ab")
        state = k9.state_of((tch.run(t, "a"), {"d#t": tch.E1}, {}), (tch.run(t + 60, "b"), None, None))
        state["config_uploads"][1]["failed"] = "the configurator failed"
        out = tgc.plan(state, time.time(), self.srv.url)
        self.assertEqual(([x["commit"] for x in out["build"]], out["compare"]), (["b" * 40], []))

    def main_thread_reads(self):
        reads, orig = [], tgc.gcs_get

        def gcs_get(url):
            if threading.current_thread() is threading.main_thread() and "/storage/v1/" not in url:
                reads.append(url)
            return orig(url)
        tgc.gcs_get = gcs_get
        self.addCleanup(setattr, tgc, "gcs_get", orig)
        return reads

    def test_PL5_the_container_of_the_run_before_is_read_in_the_pool_too(self):
        t = int(time.time()) - DAY
        a, b = self.uploads(t, "ab")
        state = k9.state_of((tch.run(t, "a"), {"d#t": tch.E1}, {}), (tch.run(t + 60, "b"), None, None))
        reads = self.main_thread_reads()
        out = tgc.plan(state, time.time(), self.srv.url)
        self.assertEqual([x["build"] for x in out["compare"]], [b])
        self.assertEqual(reads, [])
        self.assertEqual(self.srv.hits(self.prowjob(a)), 1)

    def test_PL6_a_run_listed_now_is_not_read_again(self):
        t = int(time.time()) - DAY
        _, b = self.uploads(t, "ab")
        state = k9.state_of((tch.run(t, "a"), {"d#t": tch.E1}, {}))  # (b: listed for the first time now)
        out = tgc.plan(state, time.time(), self.srv.url)
        self.assertEqual([x["build"] for x in out["compare"]], [b])
        self.assertEqual(self.srv.hits(self.prowjob(b)), 1)

    def test_PL7_the_containers_are_read_at_once(self):
        t = int(time.time()) - DAY
        self.uploads(t, "ab")
        state = k9.state_of((tch.run(t, "a"), None, None), (tch.run(t + 60, "b"), None, None))
        lock, both, flying, peak = threading.Lock(), threading.Event(), [0], [0]
        orig = tgc.gcs_get

        def gcs_get(url):  # (a container read waits, 10 s at most, until another is under way)
            if not url.endswith("/prowjob.json"):
                return orig(url)
            with lock:
                flying[0] += 1
                peak[0] = max(peak[0], flying[0])
                if flying[0] == 2:
                    both.set()
            both.wait(10)
            try:
                return orig(url)
            finally:
                with lock:
                    flying[0] -= 1
        tgc.gcs_get = gcs_get
        self.addCleanup(setattr, tgc, "gcs_get", orig)
        out = tgc.plan(state, time.time(), self.srv.url)
        self.assertEqual(len(out["build"]), 2)
        self.assertEqual(peak[0], 2)


# ------------------------------------------------------------ 7caed27: judged_since and the late rule

class JudgedSince(unittest.TestCase):
    """judged_since: the earliest a build the archive may still judge from GCS could have been
    shown: one in a gap a run finds (searched GIVE_UP_DAYS back), the older part of a gap not
    searched yet, one a scan saw before its tab's config changed, a watched build; not the
    builds a tab showed at its last scan (judged by what it showed)."""

    NOW = 1_800_000_000

    def since(self, tabs=None, watch=None):
        return frl.judged_since({"tabs": tabs or {}, "watch": watch or {}}, self.NOW)

    def test_JS1_JS2_a_gap_a_run_finds_is_searched_give_up_days_back(self):
        self.assertEqual(self.since(), self.NOW - frl.GIVE_UP_DAYS * DAY)
        self.assertEqual(self.since({"d#t": {"scan": self.NOW, "oldest": self.NOW - 60 * DAY}}),
                         self.NOW - frl.GIVE_UP_DAYS * DAY)

    def test_JS3_JS4_JS5_JS6_JS7_what_a_tab_or_a_watched_build_keeps(self):
        t = self.NOW - 30 * DAY
        for tabs, watch in (({"d#t": {"scan": self.NOW, "oldest": None, "unsearched": [t, self.NOW - 25 * DAY]}}, {}),
                            ({"d#t": {"scan": self.NOW, "oldest": None,
                                      "changed_pending": [{"since": t, "lo": t - DAY, "hi": t + DAY}]}}, {}),
                            ({}, {"job/1": {"rec": {"created": t}, "first_seen": self.NOW}})):
            with self.subTest(tabs=tabs, watch=watch):
                self.assertEqual(self.since(tabs, watch), t)


class LateRule(ttc.ConfigWorld):
    """A run of the upload job whose config is still not known 3 days after the archive listed it
    fails the run if its config was in force after the earliest a build the archive may still
    judge from GCS could have been shown (judged_since: here the older part of a gap not searched
    yet): until the upload of a run that began after it (and PICKUP)."""

    def late(self, configs, unsearched_from):
        """The serious warnings of a run whose config step failed, with the upload job's runs
        (when each ended, whether its config is known), listed 4 days ago."""
        runs, built = [], {}
        for i, (at, known) in enumerate(configs):
            commit = f"{i + 1:040x}"
            runs.append({"build": str(snowflake(at)), "commit": commit, "started": int(at) - 5, "finished": int(at)})
            if known:
                built[commit] = ({"dash#t": ttc.entry("")}, {"test_infra": commit})
        h = frl.ConfigHistory()
        h.merge(tch.loaded(runs, built), self.now - 4 * DAY)
        uploads, entries = h.encode()
        state = self.state()
        state.update(config_uploads=uploads, config_entries=entries)
        state["tabs"]["dash#t"]["unsearched"] = [unsearched_from, self.now - 15 * DAY]
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.fetch_with(os.path.join(self.dir, "missing.json"))
        return [w for w in self.runs()[-1]["serious_warnings"] if "still not known" in w]

    def test_LR3_a_runs_own_upload_does_not_end_its_config(self):
        # (the run not known ended 20.5 days ago, the next 19 days ago: its config was in force until
        # then, after the gap still to search begins, 20 days ago)
        configs = [(self.now - 30 * DAY, True), (self.now - 20.5 * DAY, False), (self.now - 19 * DAY, True),
                   (self.now - 2 * HOUR, True)]
        self.assertNotEqual(self.late(configs, int(self.now) - 20 * DAY), [])

    def test_LR2_a_config_replaced_as_the_earliest_build_could_be_shown_does_not_matter(self):
        # (the gap still to search begins at `since`; the next run's upload replaced the config of the
        # one not known just then)
        since = int(self.now) - 17 * DAY
        ended = since - frl.PICKUP
        configs = [(self.now - 30 * DAY, True), (ended - DAY, False), (ended, True), (self.now - 2 * HOUR, True)]
        self.assertEqual(self.late(configs, since), [])


if __name__ == "__main__":
    unittest.main()
