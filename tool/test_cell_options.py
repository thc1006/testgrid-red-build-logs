"""A test group's cell options (testgrid_rule.Options): builds TestGrid no longer shows,
judged from GCS by the options of each config their tab had (rows merged or split, the Pod
row or not, a custom evaluator status, test names from test_name_config)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected again here)
import test_review_round9 as r9  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

PASSED = (b'<testsuite name="kubetest"><testcase name="Test"/><testcase name="Up"/></testsuite>')
POD_FAILED = {"pod": {"status": {"phase": "Failed", "containerStatuses": [
    {"name": "sidecar", "state": {"terminated": {"exitCode": 1}}}]}}}


class Options(ttc.ConfigWorld):
    """self.b (made 30 hours ago) failed only on kubetest.Test (ConfigWorld)."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged

    def tab(self, entry):
        return {"dash": {"t": entry}}

    def red_pod_only(self):
        """self.b passed, and every test, but its pod failed: red only on job.Pod."""
        self.junit(self.b, PASSED)
        self.srv.route(f"/bucket/logs/job/{self.b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 29 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{self.b}/podinfo.json", (200, {}, POD_FAILED))

    def test_a_build_red_only_on_its_pod_row_is_not_red_where_the_group_skips_that_row(self):
        self.red_pod_only()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual([c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]], ["job.Pod"])

    def test_without_the_pod_row(self):
        self.red_pod_only()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry("", disable_prowjob_analysis=True))))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertEqual(run_["serious_warnings"], [])
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)

    def test_each_config_by_its_own_options(self):
        # (the tab dropped the Pod row 10 hours ago: the build, shown before, was red with it)
        self.red_pod_only()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                           (self.now - 10 * HOUR, self.tab(ttc.entry("", disable_prowjob_analysis=True))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        # (and the other way round: dropped since before the build was made, then shown again 10 hours ago,
        # after its reads: shown red from then on)
        self.setUp()
        self.red_pod_only()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry("", disable_prowjob_analysis=True))),
                           (self.now - 10 * HOUR, self.tab(ttc.entry(""))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_read_shows_the_cells_of_the_options_it_was_made_with(self):
        # (the group added the Pod row a day ago: an update read again then only builds that started in the
        # 7 days before; one older keeps its column of before, without the Pod row, on the tab: not red)
        for made_ago, want in ((10 * DAY, {}), (5 * DAY, {"backfilled": True})):
            self.setUp()
            b = snowflake(self.now - made_ago)
            self.failed_in_gcs(b, made_ago)
            self.srv.route(f"/bucket/logs/job/{b}/finished.json", (200, {}, {
                "result": "SUCCESS", "passed": True, "timestamp": int(self.now - made_ago + HOUR)}))
            self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, POD_FAILED))
            self.junit(b, PASSED)
            self.put_state(tabs={"dash#t": {"scan": self.now - made_ago - DAY, "oldest": self.now - made_ago - 2 * DAY}})
            run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry("", disable_prowjob_analysis=True))),
                               (self.now - DAY, self.tab(ttc.entry(""))))
            self.assertEqual(f"job/{b}" in run_["outcomes"].get("backfilled", []), bool(want), made_ago / DAY)
            if not want:
                self.assertNotIn(f"job/{b}", self.state()["watch"])  # (judged: not red)

    def test_a_custom_status_for_every_test(self):
        # (a rule without comparisons gives every test result its status; job.Overall hidden, the test rows
        # decide: kubetest.Test failed, but the rule makes it PASS; and a passed one TIMED_OUT, which is red)
        hide = "exclude-filter-by-regex=" + "%5Ejob%5C.Overall%24"
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(hide))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.setUp()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(
            hide, custom_evaluator_rule_set={"rules": [{"computed_status": 1}]}))))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)
        self.setUp()
        self.junit(self.b, PASSED)
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(
            hide, custom_evaluator_rule_set={"rules": [{"computed_status": 9}]}))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual({c["status"] for c in self.meta(self.b)["tabs"][0]["red_cells"]}, {"TIMED_OUT"})

    def test_rows_named_by_the_test_name_config_are_filtered_by_those_names(self):
        # (the tab hides "kubetest.Test [01]": the name its test name config gives the failing row, with the
        # junit file's Context; job.Overall hidden too)
        names = {"name_format": "%s [%s]", "name_elements": [{"target_config": "Tests name"},
                                                             {"target_config": "Context"}]}
        hide = "exclude-filter-by-regex=" + "%5E%28kubetest%5C.Test%20%5C%5B01%5C%5D%7Cjob%5C.Overall%29%24"
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(hide, test_name_config=names))))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.assertEqual(self.state()["watch"], {})  # (judged: not red)
        self.setUp()
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(hide))))  # (named as it is: shown)
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_split_rows_of_one_name(self):
        # (two results of kubetest.Test, one failed: merged, FLAKY, which is not red; split, the failed one
        # is a row of its own)
        self.junit(self.b, b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                           b'<testcase name="Test"/></testsuite>')
        hide = "exclude-filter-by-regex=" + "%5Ejob%5C.Overall%24"
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(hide))))
        self.assertNotIn("backfilled", run_["outcomes"])
        self.setUp()
        self.junit(self.b, b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                           b'<testcase name="Test"/></testsuite>')
        run_ = self.judged((self.now - 30 * DAY, self.tab(ttc.entry(hide, disable_merged_status=True))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual([c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]], ["kubetest.Test"])


class OneReadOfTheBuild(r9.JobWorld):
    def test_columns_of_several_options_read_gcs_once(self):
        b = self.build(30 * HOUR, finished=HOUR, podinfo=HOUR + 60, junits=[("junit_01.xml", HOUR, r9.FAILING)])
        older = self.build(31 * HOUR, finished=HOUR, podinfo=HOUR + 60)
        timelines = frl.Timelines(self.now)
        cols = frl.Columns(self.srv.url, "bucket/logs/job", b, 15, self.now, timelines)
        default, no_pod = cols(), cols(tr.Options(pod=False))
        self.assertIs(cols(None), default)
        self.assertIn("job.Pod", default.cells)
        self.assertNotIn("job.Pod", no_pod.cells)
        for build in (b, older):  # (the build's files and its job's other builds': each read once)
            for name in ("started.json", "finished.json", "podinfo.json"):
                self.assertEqual(self.srv.hits(f"/bucket/logs/job/{build}/{name}"), 1, (build, name))
        self.assertEqual(self.srv.hits(f"/bucket/logs/job/{b}/artifacts/junit_01.xml"), 1)


class Unfollowed(unittest.TestCase):
    def test_options_still_held(self):
        for group in ({"build_override_strftime": "%Y"}, {"gcs_prefix": "a/b,c/d"},
                      {"custom_evaluator_rule_set": {"rules": [{"computed_status": 99}]}}):
            self.assertIsNotNone(tgc.TabRule(ttc.entry("", **group)).group_problem, group)
        self.assertEqual(tgc.TabRule(ttc.entry("")).options, tr.DEFAULT_OPTIONS)


if __name__ == "__main__":
    unittest.main()
