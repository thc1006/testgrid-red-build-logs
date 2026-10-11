"""TestGrid's config for the tabs (testgrid_config.py), and how the fetch and cross_check use it."""
import contextlib
import gzip
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_review_round4 as r4  # noqa: E402  (module import: its tests are not collected again here)
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

# test-infra's base options for gce-master-scale-correctness (sig-scalability-release-blocking-jobs.yaml)
CORRECTNESS = "exclude-filter-by-regex=%5E(kubetest%5C.Test%7Cci-kubernetes-e2e-gce-scale-correctness%5C.Overall)%24"
GROUP = {"name": "job", "gcs_prefix": "bucket/logs/job", "days_of_results": 15, "use_kubernetes_client": True}


def entry(base_options="", **group):
    return {"test_group": "job", "base_options": base_options, "group": dict(GROUP, **group)}


class QueryParsing(unittest.TestCase):
    """url.ParseQuery, as TestGrid's tabulator parses a tab's base options."""

    def test_the_real_base_options(self):
        values, err = tgc.go_parse_query(CORRECTNESS + "&width=10")
        self.assertIsNone(err)
        self.assertEqual(values, {b"exclude-filter-by-regex":
                                  [rb"^(kubetest\.Test|ci-kubernetes-e2e-gce-scale-correctness\.Overall)$"],
                                  b"width": [b"10"]})

    def test_plus_is_a_space_and_percent_escapes_are_bytes(self):
        self.assertEqual(tgc.go_parse_query("k=a+b%2B%e6%97%a5")[0], {b"k": ["a b+日".encode()]})

    def test_what_go_rejects(self):
        for q in ("a=1;b=2", "a=%zz", "a=%4", "%=1", "a=%"):
            self.assertIsNotNone(tgc.go_parse_query(q)[1], q)
        values, err = tgc.go_parse_query("a=1;b=2&c=3")  # Go keeps going after a bad pair
        self.assertEqual((values, err), ({b"c": [b"3"]}, "invalid semicolon separator in query"))
        self.assertIsNotNone(tgc.go_parse_query("&" * tgc.MAX_QUERY_PARAMS)[1])
        self.assertIsNone(tgc.go_parse_query("&" * (tgc.MAX_QUERY_PARAMS - 1))[1])

    def test_empty_pairs_and_repeated_keys(self):
        self.assertEqual(tgc.go_parse_query("&&a&a=&b=1&a=2"), ({b"a": [b"", b"", b"2"], b"b": [b"1"]}, None))


class GoRegexpSemantics(unittest.TestCase):
    """Matches as Go's regexp.MatchString does (tool/oracle/fuzz/filterdiff.py checks this
    against TestGrid's own code)."""

    def matches(self, pattern, text):
        return tgc.go_regexp(pattern).search(text)

    def test_go_semantics(self):
        cases = [  # (pattern, text, Go's answer)
            (r"^(kubetest\.Test|x\.Overall)$", "kubetest.Test", True),
            (r"^(kubetest\.Test|x\.Overall)$", "kubetest.Tests", False),
            ("Overall$", "Overall\n", False),  # $ is the end of the text only (Python's $ is not)
            ("Overall$", "job.Overall", True),  # anywhere, like MatchString
            (r"Monitor\scluster", "Monitor\tcluster", True),
            (r"Monitor\scluster", "Monitor\vcluster", False),  # Go's \s has no \v
            (r"Monitor\scluster", "Monitor" + chr(0xA0) + "cluster", False),  # ASCII only: no no-break space
            (r"\d", chr(0x663), False), (r"\w", "é", False), (r"\bfoo\b", "a foo.", True), (r"\bfoo\b", "éfoo", True),
            (r"\bé", "aé", True), (r"\bé", " é", False),  # (é is not a word character to Go)
            (".", "\n", False), ("[^a]", "\n", True), (r"\x{1F600}", "\U0001F600", True),
            ("a{01}", "a{01}", True), ("a{01}", "a", False), ("a{,3}", "a{,3}", True), ("a{2}", "aa", True),
            ("a{2}", "a", False), ("", "", True), ("a|", "zzz", True), ("(?:a|b)c", "bc", True),
            ("[]a]", "]", True), ("[a-]", "-", True), (r"[\d-z]", "-", True), ("K", chr(0x212A), False),  # (no case folding)
        ]
        for pattern, text, want in cases:
            self.assertEqual(self.matches(pattern, text), want, (pattern, text))

    def test_what_is_not_followed_or_go_rejects(self):
        for pattern in ("(?i)a", "(?s).", "(?P<n>a)", "(?<n>a)", r"\pL", r"\p{Greek}", r"\Q.\E", r"\C", "[[:alpha:]]",
                        "a**", "a*+", "a{2}{3}", "*a", "(*)", "a|*", "^*", r"\b+", "a{1001}", "a{3,2}",
                        "(?:a{20}){60}", "(a", "a)", "[a", "a\\", r"\e", r"\1", r"[\b]", "[z-a]", r"[\D]", r"\x{D800}"):
            with self.assertRaises(tgc.Unsupported, msg=pattern):
                tgc.go_regexp(pattern)

    def test_linear_time_where_backtracking_is_exponential(self):
        start = time.monotonic()
        for pattern in (".*.*.*x", "(?:a*)*b", "(?:a|a)*b", "a*a*a*a*a*b"):
            self.assertFalse(self.matches(pattern, "a" * 5000), pattern)
        self.assertLess(time.monotonic() - start, 5)  # Python's re takes minutes to centuries on these


class Rules(unittest.TestCase):
    def test_the_correctness_tab(self):
        rule = tgc.TabRule(entry(CORRECTNESS, days_of_results=30))
        self.assertIsNone(rule.problem)
        self.assertEqual(rule.days, 30)
        self.assertFalse(rule.shows("kubetest.Test"))
        self.assertFalse(rule.shows("ci-kubernetes-e2e-gce-scale-correctness.Overall"))
        self.assertTrue(rule.shows("kubetest.Up"))
        self.assertTrue(rule.shows("ci-kubernetes-e2e-gce-scale-correctness.Pod"))
        self.assertTrue(rule.shows("Overall"))  # (the row of a column TestGrid could not read)

    def test_include_filters_all_apply_then_excludes(self):
        rule = tgc.TabRule(entry("include-filter-by-regex=sig-&include-filter-by-regex=storage&exclude-filter-by-regex=Slow"))
        self.assertEqual([rule.shows(n) for n in ("sig-storage x", "sig-node", "storage", "sig-storage Slow")],
                         [True, False, False, False])

    def test_days_of_results_zero_is_seven(self):
        self.assertEqual(tgc.TabRule(entry(days_of_results=0)).days, 7)
        self.assertEqual(tgc.TabRule({"test_group": "job", "base_options": "", "group": {"name": "job", "gcs_prefix": "b/j",
                                                                                        "use_kubernetes_client": True}}).days, 7)

    def test_drawing_options_are_fine_and_unknown_ones_are_not(self):
        self.assertIsNone(tgc.TabRule(entry("width=10&sort-by-name=&group-by-directory=&expand-groups=")).problem)
        self.assertIn("options not known here: hide-rows", tgc.TabRule(entry("hide-rows=x")).problem)

    def test_base_options_testgrid_cannot_parse(self):
        self.assertIn("TestGrid cannot parse them", tgc.TabRule(entry("exclude-filter-by-regex=a;b")).problem)
        self.assertIn("invalid UTF-8", tgc.TabRule(entry("exclude-filter-by-regex=%ff")).problem)
        self.assertIn("nested repetition", tgc.TabRule(entry("exclude-filter-by-regex=a**")).problem)

    def test_group_options_the_port_does_not_follow(self):
        for option, value in (("disable_prowjob_analysis", True), ("disable_merged_status", True), ("ignore_skip", True),
                              ("ignore_old_results", True), ("test_name_config", {"name_format": "%s"}),
                              ("build_override_strftime", "%Y"), ("build_override_configuration_value", "x"),
                              ("custom_evaluator_rule_set", {"rules": [{}]}), ("result_source", {"gcs_config": {}}),
                              ("gcs_prefix", "a/b,c/d"), ("use_kubernetes_client", False)):
            self.assertIsNotNone(tgc.TabRule(entry(**{option: value})).problem, option)
        self.assertIsNone(tgc.TabRule(entry(custom_evaluator_rule_set={"rules": []})).problem)

    def test_a_missing_group_or_a_duplicate_tab(self):
        self.assertIn("not in the config", tgc.TabRule({"test_group": "x", "base_options": "", "group": None}).problem)
        self.assertIn("more than one tab", tgc.TabRule(dict(entry(), duplicate=True)).problem)


class Extract(unittest.TestCase):
    CONFIG = {"test_groups": [dict(GROUP, num_columns_recent=3, alert_stale_results_hours=24),
                              {"name": "other", "gcs_prefix": "b/o", "use_kubernetes_client": True}],
              "dashboards": [{"name": "dash", "dashboard_tab": [
                  {"name": "t", "test_group_name": "job", "base_options": CORRECTNESS, "description": "x"},
                  {"name": "t", "test_group_name": "other"},
                  {"name": "gone", "test_group_name": "missing"}]},
                  {"name": "elsewhere", "dashboard_tab": [{"name": "x", "test_group_name": "job"}]}]}

    def test_the_dashboards_tabs_with_the_fields_that_matter(self):
        out = tgc.extract(self.CONFIG, ["dash"])
        self.assertEqual(sorted(out), ["dash"])
        self.assertEqual(out["dash"]["t"], {"test_group": "other", "base_options": "",
                                            "group": {"name": "other", "gcs_prefix": "b/o", "use_kubernetes_client": True},
                                            "duplicate": True})
        self.assertEqual(out["dash"]["gone"], {"test_group": "missing", "base_options": "", "group": None})

    def write(self, data):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_load_checks_what_it_reads(self):
        good = {"format": 1, "source": {"test_infra": "abc", "committed": 1_790_000_000, "image": "img"},
                "dashboards": {"dash": {"t": entry(CORRECTNESS)}}}
        tabs, source = tgc.load(self.write(good))
        self.assertEqual((sorted(tabs), source["test_infra"]), (["dash#t"], "abc"))
        for bad in ({}, dict(good, format=2), dict(good, source={"test_infra": "abc", "image": "i", "committed": True}),
                    dict(good, source={"test_infra": "", "image": "i", "committed": 1}),
                    dict(good, source={"test_infra": "a", "image": "i", "committed": 1 << 41}),
                    dict(good, dashboards={"dash": {"t": {"test_group": 1}}}),
                    dict(good, dashboards={"dash": {"t": dict(entry(), extra=1)}}),
                    dict(good, dashboards={"dash": {"t": entry(days_of_results=True)}}),
                    dict(good, dashboards={"dash": {"t": dict(entry(), duplicate=False)}}),
                    dict(good, dashboards={"dash": {"t": dict(entry(), group=dict(GROUP, bogus=1))}})):
            with self.assertRaises(ValueError, msg=bad):
                tgc.load(self.write(bad))

    def test_the_command_line(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        import yaml
        with open(os.path.join(d, "config.yaml"), "w") as f:
            yaml.safe_dump(self.CONFIG, f)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = tgc.main(["extract", os.path.join(d, "config.yaml"), "--commit", "abc", "--committed", "1790000000",
                             "--image", "img", "--dashboard", "dash"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue())["dashboards"], tgc.extract(self.CONFIG, ["dash"]))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tgc.main(["extract", os.path.join(d, "config.yaml"), "--commit", "abc", "--committed",
                                       "1790000000", "--image", "img", "--dashboard", "nope"]), 1)


class UploadJob(unittest.TestCase):
    JOB = """postsubmits:
  kubernetes/test-infra:
  - name: other
    spec: {containers: [{image: x}]}
  - name: post-test-infra-upload-testgrid-config
    spec:
      containers:
      - image: gcr.io/k8s-staging-test-infra/configurator:v1
        command: [configurator]
        args: [--yaml=config/testgrids, --output=gs://bucket/config, --output, /tmp/x, --prowjob-url-prefix=https://x/,
               --update-description, --oneshot]
"""

    def tree(self, *docs):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        os.makedirs(os.path.join(d, "config", "jobs", "a"))
        for i, doc in enumerate(docs):
            with open(os.path.join(d, "config", "jobs", "a", f"{i}.yaml"), "w") as f:
                f.write(doc)
        return d

    def test_the_job_and_its_arguments_made_to_print(self):
        image, command, args = tgc.upload_job(self.tree("periodics: []\n", self.JOB))
        self.assertEqual((image, command), ("gcr.io/k8s-staging-test-infra/configurator:v1", ["configurator"]))
        self.assertEqual(tgc.printing_args(args), ["--yaml=config/testgrids", "--prowjob-url-prefix=https://x/",
                                                   "--update-description", "--oneshot", "--print-text", "--output-yaml"])
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(tgc.main(["job", self.tree(self.JOB)]), 0)
        self.assertEqual(out.getvalue().split("\0")[:3],
                         ["gcr.io/k8s-staging-test-infra/configurator:v1", "configurator", "--yaml=config/testgrids"])

    def test_none_or_two_is_an_error(self):
        with self.assertRaises(ValueError):
            tgc.upload_job(self.tree("periodics: []\n"))
        with self.assertRaises(ValueError):
            tgc.upload_job(self.tree(self.JOB, self.JOB))


    def test_odd_images_and_commands_are_refused(self):
        # (they go to docker run: one that starts with "-" would be docker's own option)
        for image, command in (("--privileged", "configurator"), ("-v/:/host", "configurator"),
                               ("gcr.io/x/configurator:v1", "--privileged"), ("gcr.io/x/c:v1 --rm", "configurator"),
                               ("GCR.io/x/c:v1", "configurator"), ("configurator", "configurator"),
                               ("gcr.io/x/c:v1", "configurator\n--privileged")):
            job = self.JOB.replace("gcr.io/k8s-staging-test-infra/configurator:v1", json.dumps(image)) \
                .replace("command: [configurator]", f"command: [{json.dumps(command)}]")
            with self.assertRaises(ValueError, msg=(image, command)):
                tgc.upload_job(self.tree(job))
        for image in ("gcr.io/k8s-staging-test-infra/configurator:v20260729-f66136c16c",
                      "us-docker.pkg.dev/k8s-infra-prow/images/configurator:v1",
                      "registry.k8s.io/x/y@sha256:" + "a" * 64, "localhost:5000/x/y:t"):
            self.assertEqual(tgc.upload_job(self.tree(self.JOB.replace("gcr.io/k8s-staging-test-infra/configurator:v1",
                                                                         image)))[0], image)

class Verdicts(unittest.TestCase):
    """verdict() on the rows a tab's config shows."""

    def column(self, junit=None, finished=b'{"timestamp": 2000, "passed": false, "result": "FAILURE"}'):
        pod = b'{"pod": {"status": {"phase": "Succeeded"}}}'
        junits = [("logs/job/1/artifacts/junit_01.xml", len(junit), junit)] if junit else []
        return tr.column("job", b'{"timestamp": 1000}', finished, pod, junits, 0, 10_000, folder="logs/job/1/")

    def test_red_only_on_rows_the_tab_hides(self):
        junit = b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase></testsuite>'
        col = self.column(junit)
        col.has_finished, col.partial = True, None
        rule = tgc.TabRule(entry("exclude-filter-by-regex=" + urllib.parse.quote(r"^(kubetest\.Test|job\.Overall)$")))
        self.assertEqual(frl.verdict(col, "job", shows=rule.shows), ("not-red", []))
        self.assertEqual(frl.verdict(col, "job")[0], "red")  # (without the config every test row counts as shown)
        both = b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>' \
               b'<testcase name="Up"><failure>y</failure></testcase></testsuite>'
        col = self.column(both)
        col.has_finished, col.partial = True, None
        self.assertEqual([c["test"] for c in frl.verdict(col, "job", shows=rule.shows)[1]], ["kubetest.Up"])

    def test_a_tool_fail_column_on_a_tab_that_hides_overall(self):
        col = tr.Column(["started"], [], {"TOOL_FAIL"}, None)
        col.has_finished = False
        hides = tgc.TabRule(entry("exclude-filter-by-regex=Overall%24")).shows
        self.assertEqual(frl.verdict(col, "job", shows=hides), ("not-red", []))  # dropped as an empty column
        self.assertEqual(frl.verdict(col, "job")[0], "red")
        col = tr.Column(["started", "nostart"], [], {"TOOL_FAIL", "RUNNING"}, None)
        col.has_finished = False
        self.assertEqual(frl.verdict(col, "job", shows=hides)[0], "undecided")  # read again while RUNNING


class ConfigWorld(r4.World):
    """A tab whose gap holds a build that failed with kubetest.Test as its only failing test."""

    HIDE = "exclude-filter-by-regex=" + urllib.parse.quote(r"^(kubetest\.Test|job\.Overall)$", safe="")

    def setUp(self):
        super().setUp()
        self.visible = snowflake(self.now - HOUR)
        # (the tab as HIDE shows it: without the rows its base options hide)
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Up": ([1], [""])}, start=int(self.now - HOUR))
        self.b = snowflake(self.now - 30 * HOUR)
        self.failed_in_gcs(self.b, 30 * HOUR)
        self.junit(self.b, b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                           b'<testcase name="Up"/></testsuite>')
        self.put_state(tabs={"dash#t": {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}})
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def junit(self, b, xml):
        self.srv.route(f"/bucket/logs/job/{b}/artifacts/junit_01.xml", (200, {}, xml))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, b)])

    def config(self, base_options="", **group):
        path = os.path.join(self.dir, "testgrid-config.json")
        frl.write_json(path, {"format": 1, "source": {"test_infra": "c0cc6281b102" + "0" * 28,
                                                      "committed": int(self.now - DAY), "image": "img:v1"},
                              "dashboards": {"dash": {"t": entry(base_options, **group)}}})
        return path

    def fetch_with(self, path, *extra):
        return self.fetch("--testgrid-config", path, *extra)


class FetchWithConfig(ConfigWorld):
    def test_a_build_red_only_on_rows_the_tab_hides_is_not_taken(self):
        self.earlier_run(self.now - 3 * DAY)  # (the archive began before the build: cross_check judges it)
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        run = self.runs()[-1]
        self.assertNotIn("backfilled", run["outcomes"])
        self.assertIsNone(self.bdir(self.b))
        self.assertEqual(self.state()["watch"], {})
        self.assertEqual(run["tabs"][0]["config"]["base_options"], self.HIDE)  # (for cross_check)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_without_the_filter_it_is_taken(self):
        self.assertEqual(self.fetch_with(self.config("")), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual(sorted(c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]),
                         ["job.Overall", "kubetest.Test"])

    def test_a_shown_failing_test_makes_it_red_without_the_hidden_rows(self):
        self.junit(self.b, b'<testsuite name="kubetest"><testcase name="Test"><failure>x</failure></testcase>'
                           b'<testcase name="Up"><failure>y</failure></testcase></testsuite>')
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual([c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]], ["kubetest.Up"])

    def test_the_days_of_results_come_from_the_config(self):
        # started.json says the build started two days before it was made: older than one day
        # of results, so TestGrid calls it too old (UNKNOWN), never red
        self.srv.route(f"/bucket/logs/job/{self.b}/started.json", (200, {}, {"timestamp": int(self.now - 30 * HOUR - 2 * DAY)}))
        self.assertEqual(self.fetch_with(self.config("", days_of_results=1)), 0)
        self.assertNotIn("backfilled", self.runs()[-1]["outcomes"])
        self.assertIsNone(self.bdir(self.b))

    def test_a_table_showing_a_row_the_filter_hides_makes_the_tables_rows_count_too(self):
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 1)
        run = self.runs()[-1]
        self.assertIn("the tab shows 1 rows its base options hide, such as 'kubetest.Test'",
                      " ".join(run["serious_warnings"]))
        # TestGrid shows kubetest.Test there: the build is red on it
        self.assertEqual(run["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual([c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]], ["kubetest.Test"])
        self.assertIsNone(self.state()["tab_configs"]["dash#t"]["mismatch"]["until"])

    def test_another_gcs_prefix_makes_the_tables_rows_count_too(self):
        self.assertEqual(self.fetch_with(self.config(self.HIDE, gcs_prefix="bucket/logs/other")), 1)
        run = self.runs()[-1]
        self.assertIn("its test group reads gs://bucket/logs/other, the tab shows gs://bucket/logs/job",
                      " ".join(run["serious_warnings"]))
        self.assertEqual(run["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_an_option_the_port_does_not_follow_stops_judging_the_tab(self):
        self.assertEqual(self.fetch_with(self.config("", disable_prowjob_analysis=True)), 1)
        warning = " ".join(self.runs()[-1]["serious_warnings"])
        self.assertIn("disable_prowjob_analysis, which testgrid_rule.py does not follow", warning)
        self.assertIn("cannot be judged from GCS", warning)
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{self.b}"]})
        self.assertIn(f"job/{self.b}", self.state()["watch"])
        self.assertIsNone(self.bdir(self.b))
        # once the config is one the port follows, the watched build is judged
        self.assertEqual(self.fetch_with(self.config("")), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_the_config_read_last_is_kept_and_used(self):
        self.assertEqual(self.fetch_with(self.config(self.HIDE), "--list-only"), 0)  # (lists, stores nothing)
        path = self.config(self.HIDE)
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}})
        self.assertEqual(self.fetch_with(path), 0)
        state = self.state()
        self.assertEqual(state["tab_configs"]["dash#t"]["config"]["base_options"], self.HIDE)
        self.assertEqual(state["testgrid_config"]["source"]["test_infra"][:12], "c0cc6281b102")
        read = state["testgrid_config"]["read"]
        # the next run cannot read it: the one kept is used, with a warning, and a gap is judged by it
        self.assertNotIn("config", state["tabs"]["dash#t"])  # (kept apart from the tab's scans)
        state["tabs"]["dash#t"].update(scan=self.now - 40 * HOUR, oldest=self.now - 60 * HOUR)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.assertEqual(self.fetch_with(os.path.join(self.dir, "missing.json")), 0)
        run = self.runs()[-1]
        self.assertIn("TestGrid's config could not be read", " ".join(run["warnings"]))
        self.assertNotIn("backfilled", run["outcomes"])  # hidden by the kept filter
        self.assertEqual(self.state()["testgrid_config"]["read"], read)
        self.assertEqual(run["testgrid_config"]["read"], read)
        self.assertIn("TestGrid's config: built from test-infra c0cc6281b102", self.readme())

    def readme(self):
        with open(os.path.join(self.root, self.runs()[-1]["dir"], "README.md")) as f:
            return f.read()

    def test_a_config_not_read_for_days_is_serious(self):
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        state = self.state()
        state["tab_configs"]["dash#t"]["read"] = self.now - 4 * DAY
        frl.write_json(os.path.join(self.root, "state.json"), state)
        self.assertEqual(self.fetch(), 1)
        self.assertIn("TestGrid's config was last read", " ".join(self.runs()[-1]["serious_warnings"]))

    def test_a_malformed_config_in_state_is_dropped(self):
        self.put_state(tabs={"dash#t": {"scan": self.now - 2 * HOUR, "oldest": self.now - 3 * HOUR}},
                       tab_configs={"dash#t": {"config": {"test_group": 1}}, "dash#u": "x"},
                       testgrid_config={"read": "yesterday"})
        self.fetch()
        warnings = " ".join(self.runs()[-1]["warnings"])
        self.assertIn("dropped the malformed TestGrid config of 'dash#t'", warnings)
        self.assertIn("dropped a malformed tab_configs entry 'dash#u'", warnings)
        self.assertIn("dropped a malformed testgrid_config record", warnings)
        self.assertEqual(self.state()["tab_configs"], {})

    def test_without_any_config_the_judging_says_so(self):
        self.assertEqual(self.fetch(), 0)
        self.assertIn("no TestGrid config known for 1 tabs (dash#t)", " ".join(self.runs()[-1]["warnings"]))
        # learned from the table: the tab hides job.Overall; kubetest.Test counts as shown
        self.assertEqual(self.runs()[-1]["outcomes"], {"backfilled": [f"job/{self.b}"]})
        self.assertEqual([c["test"] for c in self.meta(self.b)["tabs"][0]["red_cells"]], ["kubetest.Test"])

    def test_cross_check_judges_by_the_runs_config(self):
        self.earlier_run(self.now - 3 * DAY)  # (the archive began before the build: cross_check judges it)
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        code, out = self.check()
        self.assertEqual(code, 0, out)
        # the same run judged without its config: the build is red on kubetest.Test
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        run = frl.read_json(run_path)
        for t in run["tabs"]:
            t.pop("config", None)
        frl.write_json(run_path, run)
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"MISSED red build job/{self.b}", out)


class ConfigScript(unittest.TestCase):
    """testgrid_config.sh: shellcheck-clean (actionlint checks the workflows' scripts)."""

    def test_shellcheck(self):
        here = os.path.dirname(os.path.abspath(__file__))
        shellcheck = shutil.which("shellcheck", path=os.path.dirname(sys.executable)) or shutil.which("shellcheck")
        if not shellcheck:
            self.skipTest("shellcheck is not installed (the dev dependencies have shellcheck-py)")
        out = subprocess.run([shellcheck, os.path.join(here, "testgrid_config.sh")], capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)


if __name__ == "__main__":
    unittest.main()


class RelevantDiff(unittest.TestCase):
    """oracle/relevant_diff.py: what changed between two TestGrid versions in the code the port follows."""

    def tree(self, files):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        for name, text in files.items():
            os.makedirs(os.path.dirname(os.path.join(d, name)), exist_ok=True)
            with open(os.path.join(d, name), "w") as f:
                f.write(text)
        return d

    def test_what_counts(self):
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle"))
        import relevant_diff
        base = {"go.mod": "module m\n\ngo 1.26.5\n\nrequire (\n\tk8s.io/api v0.27.4\n\tother.example/x v1\n)\n",
                "pkg/updater/read.go": "a", "pkg/updater/read_test.go": "t", "pkg/updater/resultstore/x.go": "r",
                "pkg/tabulator/filter.go": "f", "pkg/summarizer/summary.go": "s", "internal/result/result.go": "o",
                "pb/config/config.proto": "c", "cluster/prod/updater.yaml": "image: x"}
        old = self.tree(base)
        self.assertEqual(relevant_diff.changes(old, self.tree(base)), [])
        unrelated = dict(base, **{"pkg/updater/read_test.go": "t2", "pkg/updater/resultstore/x.go": "r2",
                                  "pkg/summarizer/summary.go": "s2", "cluster/prod/updater.yaml": "image: y",
                                  "go.mod": base["go.mod"].replace("other.example/x v1", "other.example/x v2")})
        self.assertEqual(relevant_diff.changes(old, self.tree(unrelated)), [])
        related = dict(base, **{"pkg/updater/read.go": "a2", "pkg/tabulator/new.go": "n", "internal/result/result.go": "o2",
                                "pb/config/config.proto": "c2",
                                "go.mod": base["go.mod"].replace("go 1.26.5", "go 1.27.0").replace("v0.27.4", "v0.28.0")})
        self.assertEqual(relevant_diff.changes(old, self.tree(related)),
                         ["pkg/updater/read.go", "pkg/tabulator/new.go", "internal/result/result.go",
                          "pb/config/config.proto", "go.mod: go 1.26.5 -> 1.27.0", "go.mod: k8s.io/api v0.27.4 -> v0.28.0"])

    def test_every_part_the_port_follows(self):
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle"))
        import relevant_diff
        base = {"go.mod": "module m\n\ngo 1.26.5\n\ntoolchain go1.26.5\n\nrequire k8s.io/api v0.27.4\n\nrequire (\n"
                          "\tk8s.io/apimachinery v0.27.4 // indirect\n\tcloud.google.com/go/storage v1.31.0\n)\n",
                "util/gcs/read.go": "g", "metadata/job.go": "m", "metadata/junit/junit.go": "j",
                "pb/test_status/test_status.proto": "s", "pkg/updater/gone.go": "x"}
        old = self.tree(base)
        changed = dict(base, **{"util/gcs/read.go": "g2", "metadata/job.go": "m2", "metadata/junit/junit.go": "j2",
                                "pb/test_status/test_status.proto": "s2",
                                "go.mod": base["go.mod"].replace("toolchain go1.26.5", "toolchain go1.26.6")
                                .replace("apimachinery v0.27.4", "apimachinery v0.28.0").replace("v1.31.0", "v1.32.0")
                                .replace("require k8s.io/api v0.27.4", "require k8s.io/api v0.27.5")})
        del changed["pkg/updater/gone.go"]
        self.assertEqual(relevant_diff.changes(old, self.tree(changed)),
                         ["pkg/updater/gone.go", "util/gcs/read.go", "metadata/job.go", "metadata/junit/junit.go",
                          "pb/test_status/test_status.proto", "go.mod: cloud.google.com/go/storage v1.31.0 -> v1.32.0",
                          "go.mod: k8s.io/api v0.27.4 -> v0.27.5", "go.mod: k8s.io/apimachinery v0.27.4 -> v0.28.0",
                          "go.mod: toolchain go1.26.5 -> go1.26.6"])


class FilterGolden(unittest.TestCase):
    """What TestGrid's own filterGrid kept, recorded by tool/oracle/fuzz/filtergolden.py (with
    Go), replayed without it: where TestGrid rejects base options the port must not apply
    them; where it keeps rows the port keeps the same ones, or, for syntax it does not
    follow, does not apply them."""

    def test_the_port_keeps_what_testgrid_keeps(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "testgrid_filter_golden.jsonl.gz")
        with gzip.open(path, "rt", encoding="utf-8") as f:
            cases = [json.loads(line) for line in f]
        self.assertGreater(len(cases), 1500)
        applied = 0
        for c in cases:
            rule = tgc.TabRule(entry(c["options"]))
            if rule.filter_problem:
                self.assertFalse(c["ok"] and c["plain"] and "options not known here" not in rule.filter_problem, c)
                continue
            self.assertTrue(c["ok"], c)
            self.assertEqual([n for n in c["names"] if rule.shows(n)], c["kept"], c["options"])
            applied += 1
        self.assertGreater(applied, 1000)

