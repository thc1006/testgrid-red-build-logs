"""TestGrid's config over time: the runs of test-infra's upload job (testgrid_config uploads,
plan, load), the history the fetch keeps of them (ConfigHistory), and builds judged by every
config their tab had while TestGrid may have shown them."""
import json
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

KUBETEST_FAILED = (b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                   b'<testcase name="Up"/></testsuite>')
NOW = 1_790_000_000


def run(at, commit, finished=True):
    """A run of the upload job that began at `at` (its build ID made then)."""
    return {"build": str(snowflake(at)), "commit": commit * 40 if len(commit) == 1 else commit, "started": at,
            "finished": at + 5 if finished else None}


def configs(**tabs_of):
    """{commit: (tabs, source)}: commit letter -> tabs (see loaded)."""
    return {c * 40: (tabs, {"test_infra": c * 40}) for c, tabs in tabs_of.items()}


def loaded(runs, built, after=None, changes=None, failed=None):
    """What testgrid_config.load gives for the upload job's `runs`, listed after `after` (by
    default, just before the first), and the configs `built` ({commit: (tabs, source)})."""
    if after is None:
        after = min((frl.created(r["build"]) for r in runs), default=2) - 1
    return {"after": after, "runs": [dict(r, recipe=ttc.recipe_of(r["commit"])) for r in runs],
            "configs": {ttc.recipe_of(c): v for c, v in built.items()}, "group_changes": changes or {},
            "failed": failed or {}}


E1, E2, E3 = ttc.entry(""), ttc.entry(ttc.ConfigWorld.HIDE), ttc.entry("", days_of_results=1)


class History(unittest.TestCase):
    def history(self, runs, built, changes=None):
        h = frl.ConfigHistory()
        h.merge(loaded(runs, built, changes=changes))
        return h

    def test_kept_as_changes_from_a_base(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c")],
                         configs(a={"d#t": E1, "d#u": E2}, b={"d#t": E1, "d#u": E3}, c={"d#t": E1}))
        uploads, entries = h.encode()
        self.assertTrue(uploads[0]["base"])
        self.assertEqual(uploads[0]["tabs"], {"d#t": frl.entry_id(E1), "d#u": frl.entry_id(E2)})
        self.assertEqual((uploads[1]["tabs"], uploads[2]["tabs"]), ({"d#u": frl.entry_id(E3)}, {"d#u": None}))
        self.assertNotIn("base", uploads[1])
        self.assertEqual(sorted(entries), sorted({frl.entry_id(e) for e in (E1, E2, E3)}))
        again = frl.ConfigHistory(json.loads(json.dumps(uploads)), json.loads(json.dumps(entries)))
        self.assertEqual(again.runs, h.runs)

    def test_a_run_not_known_is_kept_and_the_next_one_is_a_base(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c")],
                         configs(a={"d#t": E1}, c={"d#t": E1}))
        uploads, entries = h.encode()
        self.assertIsNone(uploads[1]["tabs"])
        self.assertTrue(uploads[2]["base"])
        self.assertEqual([r["tabs"] for r in frl.ConfigHistory(uploads, entries).runs], [{"d#t": E1}, None, {"d#t": E1}])
        self.assertEqual([r["build"] for r in h.unknown()], [str(snowflake(NOW + 100))])

    def test_a_malformed_run_is_read_again(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c"), run(NOW + 300, "d")],
                         configs(a={"d#t": E1, "d#u": E1}, b={"d#t": E1, "d#u": E2}, c={"d#t": E3, "d#u": E2},
                                 d={"d#t": E2, "d#u": E2}))
        uploads, entries = h.encode()
        for bad in ({**uploads[1], "commit": "x"}, {**uploads[1], "started": "x"}, {**uploads[1], "tabs": {"d#t": "nope"}},
                    {**uploads[1], "finished": uploads[1]["started"] - 1}, "x"):
            warnings = []
            got = frl.ConfigHistory([uploads[0], bad, uploads[2], uploads[3]], entries, warnings)
            # (c only changed d#t: with b's change to d#u lost, its config is not known, and it is
            # read again too; d's changes give the config its digest says: known)
            self.assertEqual([r["tabs"] for r in got.runs], [h.runs[0]["tabs"], None, None], bad)
            self.assertIn("dropped malformed runs of TestGrid's config uploads", " ".join(warnings))
            # (listed again from the run before it; from the first kept when where it was is not known)
            self.assertEqual(got.relist_after, 0 if bad == "x" else frl.created(uploads[0]["build"]))

    def test_merge(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b", finished=False), run(NOW + 200, "c", finished=False)],
                         configs(a={"d#t": E1}))
        # the run that still ran has finished; the other is gone from the listing: it failed
        h.merge(loaded([run(NOW + 100, "b"), run(NOW + 300, "d")], configs(b={"d#t": E2}, a={"d#t": E3})))
        self.assertEqual([(r["commit"][0], r["finished"] is not None, r["tabs"]) for r in h.runs],
                         [("a", True, {"d#t": E1}), ("b", True, {"d#t": E2}), ("d", True, None)])  # (a's kept)

    def test_pruned_back_to_the_last_run_before_what_it_keeps(self):
        keep = frl.CONFIG_KEEP_DAYS * DAY
        h = self.history([run(NOW - keep - 2 * DAY, "a"), run(NOW - keep - DAY, "b"), run(NOW - DAY, "c")],
                         configs(a={"d#t": E1}, b={"d#t": E2}, c={"d#t": E3}))
        h.prune(NOW)
        self.assertEqual([r["commit"][0] for r in h.runs], ["b", "c"])

    def test_the_config_now_is_the_latest_known_that_ended(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c", finished=False)],
                         configs(a={"d#t": E1}, c={"d#t": E3}))
        tabs, latest = h.current()
        self.assertEqual((tabs, latest["commit"][0]), ({"d#t": E1}, "a"))

    def test_eras_overlap_where_a_run_may_have_switched_it(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c")],
                         configs(a={"d#t": E1}, b={"d#t": E1}, c={"d#u": E1}))
        late = frl.PICKUP
        self.assertEqual(h.eras("d#t"), [(-float("inf"), NOW + 5 + late, frl.UNKNOWN),
                                         (NOW, NOW + 205 + late, E1),  # (b changed nothing)
                                         (NOW + 200, float("inf"), None)])  # (no such tab)

    def test_a_run_still_running_may_be_in_force_with_the_one_before(self):
        h = self.history([run(NOW, "a"), run(NOW + 100, "b", finished=False)], configs(a={"d#t": E1}, b={"d#t": E2}))
        self.assertEqual(h.eras("d#t")[1:], [(NOW, float("inf"), E1), (NOW + 100, float("inf"), E2)])

    def test_events_where_the_group_may_have_changed(self):
        changed = dict(E1, group_digest="1" * 64)
        h = self.history([run(NOW, "a"), run(NOW + 100, "b"), run(NOW + 200, "c"), run(NOW + 300, "d"),
                          run(NOW + 400, "e", finished=False)],
                         configs(a={"d#t": E1}, b={"d#t": dict(E1, base_options="width=10")}, c={"d#t": changed},
                                 e={"d#t": changed}))
        late = frl.PICKUP + frl.READ_GAP - frl.UPDATE_SPAN
        self.assertEqual(h.events("d#t", NOW + 1000),
                         [(NOW + 200, NOW + 205 + late, "changed"),  # (its group changed; b's base options do not count)
                          (NOW + 300, NOW + 305 + late, "unknown"),  # (d not known: taken as new)
                          (NOW + 400, NOW + 1000 + late, "unknown")])  # (e still runs; the config before not known)


class Uploads(unittest.TestCase):
    """The upload job's runs in GCS, as testgrid_config lists them."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        self.objects = []

    def upload(self, at, started=True, finished=True, commit="a" * 40, prowjob=None):
        b = str(snowflake(at))
        base = f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{b}"
        if started is not None:
            body = started if not isinstance(started, bool) else {"timestamp": at, "repo-commit": commit}
            self.srv.route(f"{base}/started.json", (200, {}, body))
        if finished is not None:
            body = finished if not isinstance(finished, bool) else {"timestamp": at + 5, "passed": finished}
            self.srv.route(f"{base}/finished.json", (200, {}, body))
        if prowjob is not None:
            self.srv.route(f"{base}/prowjob.json", (200, {}, prowjob))
        self.objects.append(f"logs/{tgc.UPLOAD_JOB}/{b}/prowjob.json")
        self.srv.gcs_listing("kubernetes-ci-logs", self.objects)
        return b

    def use_gcs(self):
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)

    def test_the_runs_that_uploaded_a_config_or_still_run(self):
        t = int(time.time()) - DAY
        ok = self.upload(t)
        self.upload(t + 60, finished=False)  # failed: it uploaded nothing
        running = self.upload(t + 120, finished=None)
        self.upload(t + 180, started=None)  # never started
        self.upload(t + 240, commit="x")
        self.upload(t + 300, started=b"{not json")
        self.upload(t + 360, finished={"timestamp": "x", "passed": True})
        late = self.upload(t + 420, finished={"timestamp": t + 400, "passed": True})  # (its finish before its start)
        runs = tgc.upload_runs(t - 1, gcs=self.srv.url)
        self.assertEqual([(r["build"], r["finished"]) for r in runs], [(ok, t + 5), (running, None), (late, t + 420)])
        self.assertEqual(tgc.upload_runs(t + 1, gcs=self.srv.url)[0]["build"], running)  # (made after it)

    def test_the_plan_lists_what_the_archive_does_not_have(self):
        t = int(time.time()) - DAY
        known, pending, new = self.upload(t, commit="a" * 40), self.upload(t + 60, finished=None, commit="b" * 40), \
            self.upload(t + 120, commit="c" * 40)
        old = str(snowflake(t - 600))
        state = {"config_uploads": [
            {"build": old, "commit": "d" * 40, "started": t - 600, "finished": t - 595, "tabs": None},
            {"build": known, "commit": "a" * 40, "started": t, "finished": t + 5, "tabs": {}, "base": True},
            {"build": pending, "commit": "b" * 40, "started": t + 60, "finished": None, "tabs": None}]}
        self.use_gcs()
        out = tgc.plan(state, time.time())
        # (listed from the first run whose config is not known: d, gone from GCS, is still known)
        self.assertEqual(out["after"], frl.created(old) - 1)
        self.assertEqual([r["build"] for r in out["runs"]], [known, pending, new])
        # newest first; a: known, built to tell how b changed the test groups
        self.assertEqual([b["commit"][0] for b in out["build"]], ["c", "b", "a", "d"])
        self.assertEqual([(c["build"], c["prev"] is None) for c in out["compare"]],
                         [(pending, False), (old, True)])  # (c: the run before still runs)

    def test_a_run_is_built_as_it_ran(self):
        t = int(time.time()) - DAY
        container = {"image": "gcr.io/k8s-staging-test-infra/configurator:v1", "command": ["configurator"],
                     "args": ["--yaml=config/testgrids", "--output=gs://x/y"]}
        prowjob = {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [container]}}}
        ran = self.upload(t, commit="a" * 40, prowjob=prowjob)
        plain = self.upload(t + 60, commit="b" * 40)  # (no prowjob.json: the job as defined at its commit)
        other = self.upload(t + 120, commit="c" * 40, prowjob={"spec": {"job": "another"}})
        self.use_gcs()
        out = tgc.plan({}, time.time())
        jobs = {b["commit"][0]: b["job"] for b in out["build"]}
        self.assertEqual(jobs["a"], container)
        self.assertIsNone(jobs["b"])
        self.assertIn("not one of", jobs["c"]["error"])
        self.assertEqual({r["build"]: r["recipe"] for r in out["runs"]},
                         {ran: tgc.recipe_id("a" * 40, container), plain: tgc.recipe_id("b" * 40, None),
                          other: tgc.recipe_id("c" * 40, jobs["c"])})
        for bad in ({"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [container, container]}}},
                    {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [dict(container, image="-x")]}}},
                    {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [dict(container, args=[1])]}}},
                    {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [dict(container, command=[])]}}},
                    {"spec": []}, [], "x", None):
            self.assertIn("error", tgc.job_of(bad), bad)

    def test_with_no_state_the_last_days_are_listed(self):
        t = int(time.time())
        old, recent = self.upload(t - (tgc.HISTORY_DAYS + 1) * DAY), self.upload(t - DAY)
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        self.assertEqual([r["build"] for r in tgc.plan({}, t)["runs"]], [recent])
        self.assertTrue(old)


class ConfigOverTime(ttc.ConfigWorld):
    """Builds TestGrid may have shown under several configs of their tab: self.b, made 30 hours
    ago, is red only on kubetest.Test (HIDE hides it)."""

    def history(self, *configs):
        """TestGrid's config over time in the archive's state: each (when, dashboards) a run of
        the upload job then (its config not known when dashboards is None). Returns a config
        file the fetch reads now: the last run again."""
        runs, built = [], {}
        for i, (at, dashboards) in enumerate(configs):
            commit = f"{i + 1:040x}"
            runs.append({"build": str(snowflake(at)), "commit": commit, "started": int(at) - 5, "finished": int(at)})
            if dashboards is not None:
                built[commit] = ({f"{d}#{t}": e for d, tabs in dashboards.items() for t, e in tabs.items()},
                                 {"test_infra": commit})
        h = frl.ConfigHistory()
        h.merge(loaded(runs, built))
        uploads, entries = h.encode()
        state = self.state()
        state.update(config_uploads=uploads, config_entries=entries)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        last = runs[-1]
        return ttc.write_config(os.path.join(self.dir, "now.json"), [last], {} if configs[-1][1] is None else {
            last["commit"]: {"committed": last["started"] - 60, "image": "img", "dashboards": configs[-1][1]}})

    def judged(self, *configs):
        self.fetch_with(self.history(*configs))
        return self.runs()[-1]

    def tab(self, entry):
        return {"dash": {"t": entry}}

    def test_a_build_shown_before_its_tab_hid_its_red_row_is_red(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(ttc.entry(self.HIDE))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_one_made_after_its_tab_hid_it_is_not(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 40 * HOUR, self.tab(ttc.entry(self.HIDE))))
        self.assertEqual((run_["outcomes"], run_["serious_warnings"]), ({}, []))
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)

    def test_a_build_shown_while_the_config_is_not_known_waits_for_it(self):
        configs = [(self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))), (self.now - 10 * HOUR, None)]
        run_ = self.judged(*configs)
        self.assertEqual(run_["outcomes"], {"held": [f"job/{self.b}"]})  # (for as long as a held build)
        self.assertIn("runs of its upload job", " ".join(run_["warnings"]))
        # read now: it shows every row, so the build is red
        unknown = next(u for u in self.state()["config_uploads"] if u["tabs"] is None)
        path = ttc.write_config(os.path.join(self.dir, "built.json"),
                                [{k: unknown[k] for k in ("build", "commit", "started", "finished")}],
                                {unknown["commit"]: {"committed": unknown["started"] - 60, "image": "img",
                                                     "dashboards": self.tab(ttc.entry(""))}})
        self.fetch_with(path)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_tab_its_config_did_not_have_showed_nothing(self):
        rule = {"eras": [(-float("inf"), self.now - 40 * HOUR, tgc.TabRule(ttc.entry(""))),
                         (self.now - 40 * HOUR, float("inf"), None)]}  # (t gone before the build was made)
        col = frl.gcs_column(self.srv.url, "bucket/logs/job", self.b, now=self.now)
        self.assertEqual(frl.tab_verdict(col, "job", rule, frl.created(self.b)), ("not-red", []))
        self.assertEqual(frl.tab_verdict(col, "job", dict(rule, eras=rule["eras"][:1] + [
            (self.now - 40 * HOUR, float("inf"), tgc.TabRule(ttc.entry("")))]), frl.created(self.b))[0], "red")

    def test_a_tab_testgrid_shows_that_the_config_lacks_is_not_known_there(self):
        run_ = self.judged((self.now - 30 * DAY, {"dash": {"t": ttc.entry(""), "u": ttc.entry("")}}),
                           (self.now - 40 * HOUR, {"dash": {"u": ttc.entry("")}}))  # (TestGrid still shows t)
        self.assertEqual(run_["outcomes"], {"held": [f"job/{self.b}"]})
        self.assertIn("lacks tabs TestGrid shows", " ".join(run_["serious_warnings"]))

    BROKEN = ttc.entry("exclude-filter-by-regex=a**")  # (Go rejects it: TestGrid's tabulator fails the tab)

    def test_a_tab_testgrid_could_not_filter_showed_no_new_build(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 40 * HOUR, self.tab(self.BROKEN)))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)
        self.assertIn("TestGrid cannot apply its filters either", " ".join(run_["warnings"]))

    NOT_UPDATED = ttc.entry("", use_kubernetes_client=False)  # (TestGrid's updater skips the group)

    def test_a_tab_whose_group_testgrid_did_not_update_showed_no_new_build(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 40 * HOUR, self.tab(self.NOT_UPDATED)))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)
        self.assertEqual(run_["serious_warnings"], [])
        self.assertIn("never updates the group: the tab shows no new column", " ".join(run_["warnings"]))

    def test_a_build_shown_before_testgrid_stopped_updating_its_group_is_judged_by_the_config_then(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(self.NOT_UPDATED)))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_build_shown_before_its_tabs_filters_broke_is_judged_by_the_config_then(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(self.BROKEN)))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def late_junit(self, ago):
        """self.b (finished an hour after it started, its podinfo in) with a junit file written `ago`."""
        self.srv.route(f"/bucket/logs/job/{self.b}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        self.srv.route(f"/bucket/logs/job/{self.b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 29 * HOUR)}))
        name = f"logs/job/{self.b}/artifacts/junit_01.xml"
        self.srv.route(f"/bucket/{name}", (200, {}, KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b)],
                             written={name: self.now - ago})

    def test_a_change_of_its_test_groups_config_has_its_recent_columns_read_again(self):
        self.late_junit(20 * HOUR)  # (written long after the build's own reads ended)
        changed = dict(ttc.entry(""), group_digest="1" * 64)  # (a field of the test group the entry does not keep)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(changed)))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_without_a_change_its_reads_ended_with_its_own(self):
        self.late_junit(20 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                           (self.now - 10 * HOUR, self.tab(ttc.entry("", days_of_results=15))))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)

    def test_a_change_before_the_junit_was_written_reads_nothing_new(self):
        self.late_junit(5 * HOUR)
        changed = dict(ttc.entry(""), group_digest="1" * 64)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(changed)))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)


if __name__ == "__main__":
    unittest.main()
