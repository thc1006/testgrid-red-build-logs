"""Review wave W7b, on 1169263 (the first deploy of the config history on the real archive, a
rollback to the deployed code, the backlog of configs to build): each test fails on cdc7bf1 for
the reason its docstring gives.

Run: cd tool && python -m unittest test_review_round19 -v
"""
import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected here)
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

# (the fields of a tab's entry in tab_configs that the code deployed before the config history takes)
DEPLOYED_ENTRY = {"test_group", "base_options", "group", "duplicate"}


class StateTheDeployedCodeReads(ttc.ConfigWorld):
    """The code deployed before the config history (a006907) reads state.json after the new code
    wrote it, should it be rolled back: it drops, with a warning that fails the run, a tab's
    config with a field it does not know (the new code added each group's digest, which only the
    history uses), and a config record without the commit time of the config's test-infra (the
    new code kept it only in a run that built the config in force; the history now keeps it)."""

    def test_a_tabs_config_has_only_the_fields_the_deployed_code_takes(self):
        self.fetch_with(self.config(""))
        config = self.state()["tab_configs"]["dash#t"]["config"]
        self.assertLessEqual(set(config), DEPLOYED_ENTRY)
        self.assertTrue(tgc.valid_entry(config))

    def test_a_run_that_did_not_build_the_config_in_force_says_where_it_came_from(self):
        path = self.config("")
        self.fetch_with(path)
        first = self.state()["testgrid_config"]["source"]
        cfg = frl.read_json(path)
        again = ttc.write_config(os.path.join(self.dir, "again.json"),
                                 [{k: r[k] for k in ("build", "commit", "started", "finished")} for r in cfg["runs"]],
                                 {}, after=cfg["after"])  # (listed again, nothing built)
        self.fetch_with(again)
        self.assertEqual(self.state()["testgrid_config"]["source"], first)
        self.assertEqual(set(first), {"test_infra", "committed", "image", "upload", "uploaded"})
        self.assertEqual(self.runs()[-1]["testgrid_config"]["source"], first)


class LateOnlyIfABuildMayDependOnIt(ttc.ConfigWorld):
    """A run of the upload job whose config is still not known three days after the archive
    listed it fails the run only if its config was in force while a build the archive may still
    judge could have been shown: the first listing's backlog (18 days, built newest first) holds
    old runs that matter to no build, and failed every run whose config step failed or built
    nothing (review W7b). (The second test holds on cdc7bf1 too: there every such run failed.)"""

    def state_with(self, configs, listed, unsearched=None):
        runs, built = [], {}
        for i, (at, dashboards) in enumerate(configs):
            commit = f"{i + 1:040x}"
            runs.append({"build": str(snowflake(at)), "commit": commit, "started": int(at) - 5, "finished": int(at)})
            if dashboards is not None:
                built[commit] = ({f"{d}#{t}": e for d, tabs in dashboards.items() for t, e in tabs.items()},
                                 {"test_infra": commit})
        h = frl.ConfigHistory()
        h.merge(tch.loaded(runs, built), listed)
        uploads, entries = h.encode()
        state = self.state()
        state.update(config_uploads=uploads, config_entries=entries)
        if unsearched:
            state["tabs"]["dash#t"]["unsearched"] = unsearched
        frl.write_json(os.path.join(self.root, "state.json"), state)

    def serious_after(self, unsearched=None):
        tab = {"dash": {"t": ttc.entry("", days_of_results=60)}}  # (as the real archive's longest)
        # (listed 4 days ago: 18 days of runs, the newest built, two of 17.5 days ago not yet)
        self.state_with([(self.now - 30 * DAY, tab), (self.now - 17.5 * DAY, None), (self.now - 17.4 * DAY, None),
                         (self.now - 17 * DAY, tab), (self.now - 2 * HOUR, tab)], self.now - 4 * DAY, unsearched)
        self.fetch_with(os.path.join(self.dir, "missing.json"))  # (the config step failed)
        return [w for w in self.runs()[-1]["serious_warnings"] if "still not known" in w]

    def test_old_runs_no_build_depends_on_do_not_fail_the_run(self):
        # (the builds the tab showed then were seen on it: what it showed decides them)
        self.assertEqual(self.serious_after(), [])
        self.assertEqual(self.state()["watch"], {})

    def test_they_do_while_a_gap_of_that_time_is_still_to_search(self):
        self.assertNotEqual(self.serious_after(unsearched=[self.now - 18 * DAY, self.now - 16 * DAY]), [])


class PlanReadsTheBacklogAtOnce(unittest.TestCase):
    """testgrid_config.plan read the container of every run whose config it is to build (all of
    a backlog: 87 runs after three runs on the real archive) one request at a time in the main
    thread, three requests a run over a new connection each: 169 of the 480 seconds the config
    step had (review W7b). They are read at once now."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        self.objects = []
        self.container = {"image": "gcr.io/k8s-staging-test-infra/configurator:v1", "command": ["configurator"],
                          "args": ["--yaml=config/testgrids", "--output=gs://x/y"]}
        self.prowjob = {"spec": {"job": tgc.UPLOAD_JOB, "pod_spec": {"containers": [self.container]}}}

    def upload(self, at, commit):
        b = str(snowflake(at))
        base = f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{b}"
        self.srv.route(f"{base}/started.json", (200, {}, {"timestamp": at, "repo-commit": commit}))
        self.srv.route(f"{base}/finished.json", (200, {}, {"timestamp": at + 5, "passed": True}))
        self.srv.route(f"{base}/prowjob.json", (200, {}, self.prowjob))
        self.objects.append(f"logs/{tgc.UPLOAD_JOB}/{b}/prowjob.json")
        self.srv.gcs_listing("kubernetes-ci-logs", self.objects)
        return b

    def test_the_runs_to_build_are_not_read_one_by_one(self):
        t, n = int(time.time()) - 10 * DAY, 40
        state = {"config_uploads": []}
        for i in range(n):  # (as the first run left them: listed, their recipe kept, their config not known yet)
            commit = f"{i:040x}"
            b = self.upload(t + i * 3600, commit)
            state["config_uploads"].append({"build": b, "commit": commit, "started": t + i * 3600,
                                            "finished": t + i * 3600 + 5, "listed": t + n * 3600,
                                            "recipe": tgc.recipe_id(commit, self.container), "tabs": None})
        main_reads = []
        orig = tgc.gcs_get

        def gcs_get(url):
            if threading.current_thread() is threading.main_thread():
                main_reads.append(url)
            return orig(url)
        tgc.gcs_get = gcs_get
        self.addCleanup(setattr, tgc, "gcs_get", orig)
        out = tgc.plan(state, time.time())
        self.assertEqual(len(out["build"]), n)
        self.assertEqual({b["job"]["image"] for b in out["build"]}, {self.container["image"]})
        # (the listing's pages are read in the main thread; the runs' own files are what this counts)
        self.assertEqual([u for u in main_reads if "/storage/v1/" not in u], [])


class HeldIsNotStillRunning(ttc.ConfigWorld):
    """Builds held because the config their tab had then is not known yet were reported as "not
    decided yet: still running, or unreadable", and INDEX.md counted them as "Builds watched ...
    (still running)": 37 builds that had finished on the real archive (review W7b)."""

    history, judged, tab = tch.ConfigOverTime.history, tch.ConfigOverTime.judged, tch.ConfigOverTime.tab

    def held(self):
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(self.HIDE))), (self.now - 10 * HOUR, None))
        self.assertEqual(run_["outcomes"], {"held": [f"job/{self.b}"]})  # (finished 30 hours ago)
        return run_

    def test_the_gap_warning_says_why_a_finished_build_is_not_decided(self):
        gap = next(w for w in self.held()["warnings"] if "GCS was searched instead" in w)
        self.assertNotIn("still running", gap)
        self.assertIn("1 held until the config TestGrid had then is known and followed here", gap)

    def test_the_index_does_not_call_a_held_build_still_running(self):
        self.held()
        with open(os.path.join(self.root, "INDEX.md"), encoding="utf-8") as f:
            line = next(x for x in f.read().splitlines() if x.startswith("| Builds watched"))
        self.assertNotIn("(still running)", line)


if __name__ == "__main__":
    unittest.main()
