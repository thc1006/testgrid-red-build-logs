"""TestGrid's config for the tabs (testgrid_config.py), and how the fetch and cross_check use it."""
import contextlib
import gzip
import hashlib
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
    return {"test_group": "job", "base_options": base_options, "group": dict(GROUP, **group),
            "group_digest": tgc.group_digest(dict(GROUP, **group))}


def recipe_of(commit):
    """The recipe the fixtures give a commit's config (testgrid_config.recipe_id's form)."""
    return commit[:12] + "-" + hashlib.sha1(commit.encode()).hexdigest()[:12]


def write_config(path, runs, configs, after=None, changes=None, failed=None):
    """A config file as testgrid_config.sh writes it: the upload job's `runs` ({"build",
    "commit", "started", "finished"}) listed after `after` (by default, just before the
    first), the configs built ({commit: {"committed", "image", "dashboards"}}), how runs
    changed their tabs' test groups ({build: changes}) and the configs that could not be
    built ({commit: why})."""
    if after is None:
        after = min((frl.created(r["build"]) for r in runs), default=2) - 1
    frl.write_json(path, {"format": tgc.FORMAT, "after": after,
                          "runs": [dict(r, recipe=recipe_of(r["commit"])) for r in runs],
                          "configs": {recipe_of(c): dict(v, commit=c) for c, v in configs.items()},
                          "group_changes": changes or {},
                          "failed": {recipe_of(c): why for c, why in (failed or {}).items()}})
    return path


def config_file(path, dashboards, uploaded, commit=None):
    """A config as testgrid_config.sh writes it: one run of the upload job, which uploaded
    `dashboards` ({dashboard: {tab: entry}}) at `uploaded`, from test-infra `commit` (one
    made from that time when not given)."""
    commit = commit or "c0cc6281b102" + hashlib.sha1(str(uploaded).encode()).hexdigest()[:28]
    return write_config(path, [{"build": str(snowflake(uploaded)), "commit": commit, "started": int(uploaded) - 5,
                                "finished": int(uploaded)}],
                        {commit: {"committed": int(uploaded) - 60, "image": "img:v1", "dashboards": dashboards}})


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

    def test_flags_unicode_classes_and_the_rest_of_gos_syntax(self):
        cases = [  # (pattern, text, Go's answer: regexp.MatchString in Go 1.26.5)
            ('(?i)kubetest', 'KUBETEST.Test', True),
            ('(?i)kubetest', 'kubetest', True),
            ('(?i)k', '\u212a', True),
            ('(?i)k', 'K', True),
            ('(?i)\u017f', 'S', True),
            ('(?i)\u017f', 's', True),
            ('(?i)[k-l]', '\u212a', True),
            ('(?i)[k-l]', 'L', True),
            ('(?i:a)b', 'Ab', True),
            ('(?i:a)b', 'AB', False),
            ('a(?i)b', 'aB', True),
            ('a(?i)b', 'AB', False),
            ('(?i)a(?-i)b', 'Ab', True),
            ('(?i)a(?-i)b', 'AB', False),
            ('(?m)^b$', 'a\nb\nc', True),
            ('(?m)^b$', 'ab', False),
            ('^b$', 'a\nb\nc', False),
            ('(?s)a.b', 'a\nb', True),
            ('a.b', 'a\nb', False),
            ('(?U)a+?', 'a', True),
            ('(?P<x>a)b', 'ab', True),
            ('(?<x>a)b', 'ab', True),
            ('\\Q.*\\E', '.*', True),
            ('\\Q.*\\E', 'ab', False),
            ('\\Q.*', '.*x', True),
            ('\\pL', '\xe9', True),
            ('\\pL', '1', False),
            ('\\p{Greek}+', '\u03b1\u03b2\u03b3', True),
            ('\\p{Greek}+', 'abc', False),
            ('\\P{Greek}', '\u03b1', False),
            ('\\P{Greek}', 'a', True),
            ('\\p{^Greek}', '\u03b1', False),
            ('\\p{Letter}', 'x', True),
            ('\\p{lu}', 'A', True),
            ('\\p{lu}', 'a', False),
            ('(?i)\\p{Lu}', 'a', True),
            ('\\p{Any}', '\n', True),
            ('\\p{ASCII}', '\xe9', False),
            ('\\p{ASCII}', 'e', True),
            ('\\p{Assigned}', 'a', True),
            ('\\p{Assigned}', '\U000e0080', False),
            ('[[:alpha:]]+', 'abc', True),
            ('[[:alpha:]]+', '123', False),
            ('[[:^digit:]]', '1', False),
            ('[[:^digit:]]', 'a', True),
            ('[\\d\\pL]', '\xe9', True),
            ('[\\d\\pL]', '-', False),
            ('\\101', 'A', True),
            ('\\0', '\x00', True),
            ('\\012', '\n', True),
            ('[\\x41-\\x43]', 'B', True),
            ('[\\x41-\\x43]', 'D', False),
            ('^*a', 'a', True),
            ('\\b+a', 'a', True),
            ('[\\D]', '1', False),
            ('[\\D]', 'a', True),
            ('(?i)\u01c5', '\u01c4', True),
            ('(?i)\u01c5', '\u01c6', True),
            ('(?i)\u03c3', '\u03a3', True),
            ('(?i)\u03c3', '\u03c2', True),
            ('x{1000}', "x" * 1000, True),
            ('x{1000}', "x" * 999, False),
            ('(?:ab){2,}', 'abab', True),
            ('(?:ab){2,}', 'ab', False),
            ('a|b|c', 'c', True),
            ('abc|abd|aef', 'aef', True),
            ('abc|abd|aef', 'ab', False),
        ]
        for pattern, text, want in cases:
            self.assertEqual(self.matches(pattern, text), want, (pattern, text))

    def test_what_go_rejects_is_broken(self):
        # (TestGrid's tabulator fails a tab with such a filter: tab_filters raises Broken)
        for pattern in (r"\C", "a**", "a*+", "a{2}{3}", "*a", "(*)", "a|*", "a{1001}", "a{3,2}", "(?:a{20}){60}", "(a",
                        "a)", "[a", "a\\", r"\e", r"\1", r"[\b]", "[z-a]", "(?P<>a)", "(?P<a-b>a)", "(?x)a", "(?i-)a",
                        r"\p{Bogus}", "[[:bogus:]]", r"\x{110000}", "(" * 1000 + "a" + ")" * 1000):
            with self.assertRaises(tgc.Broken, msg=pattern[:40]):
                tgc.go_regexp(pattern)

    def test_what_go_accepts_is_applied(self):
        for pattern in ("(?i)a", "(?s).", "(?P<n>a)", "(?<n>a)", r"\pL", r"\p{Greek}", r"\Q.\E", "[[:alpha:]]", "^*",
                        r"\b+", r"[\D]", r"\x{D800}", "a" * 5000, "(" * 999 + "a" + ")" * 999, "(?:" * 1000 + "a" + ")" * 1000):
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

    def test_options_other_than_the_filters_are_ignored_as_testgrid_does(self):
        rule = tgc.TabRule(entry("width=10&sort-by-name=&group-by-directory=&expand-groups="))
        self.assertEqual((rule.problem, rule.unknown_options), (None, []))
        # (one not known here is named, for a warning; the filters still apply, as in TestGrid's tabulator)
        rule = tgc.TabRule(entry("hide-rows=x&exclude-filter-by-regex=a"))
        self.assertEqual((rule.problem, rule.unknown_options), (None, ["hide-rows"]))
        self.assertEqual([rule.shows("a"), rule.shows("b")], [False, True])

    def test_base_options_testgrid_cannot_parse(self):
        # (its tabulator fails the tab: .broken, not a problem of the port's)
        for options, why in (("exclude-filter-by-regex=a;b", "TestGrid cannot parse them"),
                             ("exclude-filter-by-regex=%ff", "invalid UTF-8"),
                             ("exclude-filter-by-regex=a**", "invalid nested repetition operator")):
            rule = tgc.TabRule(entry(options))
            self.assertIn(why, rule.broken, options)
            self.assertIsNone(rule.problem, options)
            with self.assertRaises(tgc.Broken):
                rule.shows("x")

    def test_group_options_the_port_does_not_follow(self):
        # (one column holds builds of several jobs, or several builds; or a custom evaluator rule that
        # compares a test result, which test-infra's configurator cannot write)
        compared = {"rules": [{"computed_status": 12, "test_result_comparisons": [
            {"TestResultInfo": {"PropertyKey": "k"}, "comparison": {"op": 1}}]}]}
        for option, value in (("build_override_strftime", "%Y"), ("build_override_configuration_value", "x"),
                              ("gcs_prefix", "a/b,c/d"), ("result_source", {"gcs_config": {"gcs_prefix": "a/b,c/d"}}),
                              ("custom_evaluator_rule_set", compared),
                              ("custom_evaluator_rule_set", {"rules": [{"computed_status": 16}]}),
                              ("custom_evaluator_rule_set", {"rules": ["x"]}),
                              ("test_name_config", {"name_format": 5}),
                              ("test_name_config", {"name_elements": [{"target_config": 5}]})):
            self.assertIsNotNone(tgc.TabRule(entry(**{option: value})).problem, (option, value))
        self.assertIsNone(tgc.TabRule(entry(custom_evaluator_rule_set={"rules": []})).problem)

    def test_the_cell_options_the_port_follows(self):
        never = {"computed_status": 12, "test_result_comparisons": [{"TestResultInfo": None, "comparison": {
            "ComparisonValue": None, "op": 2}}]}
        for group, options in (
                ({}, tr.Options()),
                ({"disable_prowjob_analysis": True}, tr.Options(pod=False)),
                ({"disable_merged_status": True}, tr.Options(merge=False)),
                ({"test_name_config": {"name_format": "%s [%s]", "name_elements": [
                    {"target_config": "Tests name"}, {"target_config": "Context"}, {"test_property": "k"}, {}]}},
                 tr.Options(names=("%s [%s]", ("Tests name", "Context", "k", "")))),
                ({"test_name_config": {}}, tr.Options(names=("", ()))),
                ({"custom_evaluator_rule_set": {"rules": [never, {"computed_status": 9}, {"computed_status": 12}]}},
                 tr.Options(all_tests="TIMED_OUT")),  # (the first rule without comparisons)
                ({"custom_evaluator_rule_set": {"rules": [never, {"test_result_comparisons": [{"TestResultInfo": None}]}]}},
                 tr.Options()),  # (no rule matches: a comparison without its Comparison never does)
                ({"custom_evaluator_rule_set": {"rules": [{}]}}, tr.Options(all_tests="NO_RESULT"))):
            rule = tgc.TabRule(entry(**group))
            self.assertEqual((rule.problem, rule.options), (None, options), group)
    def test_a_table_is_compared_with_its_config_only_where_testgrid_updates_its_group(self):
        table = {"tests": [{"name": "kubetest.Test"}]}
        self.assertEqual(frl.config_mismatch(tgc.TabRule(entry()), table, "bucket/logs/job"), "")
        self.assertIn("its test group reads gs://bucket/logs/job, the tab shows gs://other/job",
                      frl.config_mismatch(tgc.TabRule(entry()), table, "other/job"))
        self.assertIsNone(frl.config_mismatch(tgc.TabRule(entry(use_kubernetes_client=False)), table, "other/job"))

    def test_group_options_testgrid_production_does_not_apply(self):
        # (its updater runs without --enable-ignore-skip; ignore_old_results only picks among the
        # columns a build override merges)
        for option in ("ignore_skip", "ignore_old_results"):
            rule = tgc.TabRule(entry(**{option: True}))
            self.assertEqual((rule.problem, rule.not_updated, rule.prefix), (None, None, GROUP["gcs_prefix"]), option)

    def test_where_testgrid_reads_a_groups_builds_from(self):
        # (cmd/updater's source() and gcsPrefix(), as TestGrid production runs them)
        for group, source in (
                ({"use_kubernetes_client": True, "gcs_prefix": "a/b"}, ("a/b", None)),
                ({"use_kubernetes_client": True, "gcs_prefix": "a/b",
                  "result_source": {"gcs_config": {"gcs_prefix": "c/d"}}}, ("c/d", None)),
                ({"result_source": {"gcs_config": {"gcs_prefix": "c/d", "pubsub_project": "p"}}}, ("c/d", None)),
                ({"result_source": {"gcs_config": {}}}, ("", None)),
                ({"use_kubernetes_client": True, "gcs_prefix": "a/b",
                  "result_source": {"resultstore_config": {"project": "p"}}}, ("a/b", None)),
                ({"use_kubernetes_client": True}, ("", None))):
            self.assertEqual(tgc.group_source(group), source, group)
        for group, why in (({"gcs_prefix": "a/b"}, "sets no source of results"),
                           ({"gcs_prefix": "a/b", "use_kubernetes_client": False}, "sets no source of results"),
                           ({"gcs_prefix": "a/b", "result_source": {}}, "sets no source of results"),
                           ({"result_source": {"resultstore_config": {"project": "p"}}}, "ResultStore")):
            prefix, not_updated = tgc.group_source(group)
            self.assertIsNone(prefix, group)
            self.assertIn(why, not_updated, group)
            rule = tgc.TabRule({"test_group": "job", "base_options": "", "group": dict(group, name="job")})
            self.assertEqual((rule.not_updated, rule.problem), (not_updated, None), group)
        # (a tab whose test group the config lacks: not known)
        rule = tgc.TabRule({"test_group": "gone", "base_options": "", "group": None})
        self.assertEqual((rule.prefix, rule.not_updated), (None, None))
        self.assertIn("not in the config", rule.problem)

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
        other = {"name": "other", "gcs_prefix": "b/o", "use_kubernetes_client": True}
        self.assertEqual(out["dash"]["t"], {"test_group": "other", "base_options": "", "group": other,
                                            "group_digest": tgc.group_digest(other), "duplicate": True})
        self.assertEqual(out["dash"]["gone"], {"test_group": "missing", "base_options": "", "group": None,
                                               "group_digest": None})

    def test_the_digest_tells_any_change_of_the_group_apart(self):
        # (TestGrid's updater reads a group's columns again after any field of it changed: one the
        # entry does not keep too)
        job = tgc.extract(self.CONFIG, ["dash"])["dash"]["gone"]
        changed = dict(self.CONFIG, test_groups=[dict(GROUP, num_columns_recent=4, alert_stale_results_hours=24),
                                                 *self.CONFIG["test_groups"][1:]])
        before = tgc.extract(dict(self.CONFIG, dashboards=[{"name": "d", "dashboard_tab": [
            {"name": "t", "test_group_name": "job"}]}]), ["d"])["d"]["t"]
        after = tgc.extract(dict(changed, dashboards=[{"name": "d", "dashboard_tab": [
            {"name": "t", "test_group_name": "job"}]}]), ["d"])["d"]["t"]
        self.assertEqual(before["group"], after["group"])  # (num_columns_recent is not kept)
        self.assertNotEqual(before["group_digest"], after["group_digest"])
        self.assertIsNone(job["group_digest"])

    def write(self, data):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_load_checks_what_it_reads(self):
        commit, recipe = "c" * 40, recipe_of("c" * 40)
        run = {"build": "2109000000000000000", "commit": commit, "started": 1_790_000_000, "finished": 1_790_000_005,
               "recipe": recipe}
        built = {"commit": commit, "committed": 1_789_990_000, "image": "img",
                 "dashboards": {"dash": {"t": entry(CORRECTNESS)}}}
        good = {"format": 3, "after": 1_789_999_000.5, "runs": [run, dict(run, build="2109000000000000001", finished=None)],
                "configs": {recipe: built}, "group_changes": {"2109000000000000000": {"job": "new"}},
                "failed": {"a" * 12 + "-" + "b" * 12: "why"}}
        loaded = tgc.load(self.write(good))
        self.assertEqual([r["finished"] for r in loaded["runs"]], [1_790_000_005, None])
        tabs, source = loaded["configs"][recipe]
        self.assertEqual((sorted(tabs), source), (["dash#t"], {"test_infra": commit, "committed": 1_789_990_000,
                                                               "image": "img"}))
        self.assertEqual((loaded["after"], loaded["group_changes"], loaded["failed"]),
                         (1_789_999_000.5, good["group_changes"], good["failed"]))
        for bad in ({}, dict(good, format=2), dict(good, runs={}), dict(good, configs=[]), dict(good, after="x"),
                    dict(good, after=-1), dict(good, after=True), dict(good, group_changes=[]), dict(good, failed=[]),
                    dict(good, group_changes={"x": {}}), dict(good, group_changes={"1": {"job": "gone"}}),
                    dict(good, group_changes={"1": []}), dict(good, failed={"x": "why"}),
                    dict(good, failed={recipe: 1}),
                    dict(good, runs=[dict(run, build="x")]), dict(good, runs=[dict(run, commit="c" * 39)]),
                    dict(good, runs=[dict(run, started=True)]), dict(good, runs=[dict(run, finished=1)]),
                    dict(good, runs=[dict(run, extra=1)]), dict(good, runs=[dict(run, recipe="x")]),
                    dict(good, runs=[{k: v for k, v in run.items() if k != "recipe"}]),
                    dict(good, configs={"x": built}), dict(good, configs={recipe: dict(built, commit="x")}),
                    dict(good, configs={recipe: {k: v for k, v in built.items() if k != "commit"}}),
                    dict(good, configs={recipe: dict(built, committed=True)}),
                    dict(good, configs={recipe: dict(built, committed=1 << 41)}),
                    dict(good, configs={recipe: dict(built, image="")})):
            with self.assertRaises(ValueError, msg=bad):
                tgc.load(self.write(bad))
        # (a config with an entry extract does not write is not taken, as one that could not be built)
        for entries in ({"t": {"test_group": 1}}, {"t": dict(entry(), extra=1)}, {"t": entry(days_of_results=True)},
                        {"t": dict(entry(), duplicate=False)}, {"t": dict(entry(), group_digest="x")},
                        {"t": dict(entry(), group=dict(GROUP, bogus=1))}, []):
            loaded = tgc.load(self.write(dict(good, configs={recipe: dict(built, dashboards={"dash": entries})})))
            self.assertEqual(loaded["configs"], {}, entries)
            self.assertIn("its config as written", loaded["failed"][recipe], entries)
            self.assertEqual(loaded["failed"]["a" * 12 + "-" + "b" * 12], "why")

    def test_the_command_line(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        import yaml
        with open(os.path.join(d, "config.yaml"), "w") as f:
            yaml.safe_dump(self.CONFIG, f)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = tgc.main(["extract", os.path.join(d, "config.yaml"), "--recipe", "r", "--commit", "abc",
                             "--committed", "1790000000", "--image", "img", "--dashboard", "dash"])
        self.assertEqual(code, 0)
        got = json.loads(out.getvalue())
        self.assertEqual(got["dashboards"], tgc.extract(self.CONFIG, ["dash"]))
        self.assertEqual(got["groups"], {g["name"]: tgc.group_digest(g) for g in self.CONFIG["test_groups"]})
        self.assertEqual((got["recipe"], got["commit"], got["committed"], got["image"]), ("r", "abc", 1790000000, "img"))
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(tgc.main(["extract", os.path.join(d, "config.yaml"), "--recipe", "r", "--commit", "abc",
                                       "--committed", "1790000000", "--image", "img", "--dashboard", "nope"]), 1)

    def test_extract_takes_no_dashboard_name_holding_a_hash(self):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        import yaml
        # (the config holds an "a#b" dashboard, so that only the name check can refuse it)
        config = dict(self.CONFIG, dashboards=[*self.CONFIG["dashboards"],
                                               {"name": "a#b", "dashboard_tab": [{"name": "c", "test_group_name": "job"}]}])
        with open(os.path.join(d, "config.yaml"), "w") as f:
            yaml.safe_dump(config, f)
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            with self.assertRaises(SystemExit) as raised:
                tgc.main(["extract", os.path.join(d, "config.yaml"), "--recipe", "r", "--commit", "abc", "--committed",
                          "1790000000", "--image", "img", "--dashboard", "dash", "--dashboard", "a#b"])  # fails: returned 0
        self.assertEqual(raised.exception.code, 2)
        self.assertIn("must hold no '#', got 'a#b'", err.getvalue())

    def test_a_config_whose_dashboard_name_holds_a_hash_is_not_taken(self):
        # (its tabs key as another dashboard's: "a#b" with tab "c", and "a" with tab "b#c", are both "a#b#c"; "a#b" is
        # listed first, so it is refused before the two collide)
        commit, recipe = "c" * 40, recipe_of("c" * 40)
        built = {"commit": commit, "committed": 1_789_990_000, "image": "img",
                 "dashboards": {"a#b": {"c": entry()}, "a": {"b#c": entry(CORRECTNESS)}}}
        loaded = tgc.load(self.write({"format": 3, "after": 1_789_999_000.5, "runs": [], "configs": {recipe: built},
                                      "group_changes": {}, "failed": {}}))
        self.assertEqual(loaded["configs"], {})  # fails: taken, with one tab for the two
        self.assertEqual(loaded["failed"], {recipe: "its config as written: a#b: a dashboard name with '#'"})  # fails: {}

    def test_control_a_config_whose_tab_name_holds_a_hash_is_taken(self):
        # (its key, "a#b#c", splits at its first "#" back to dashboard "a")
        commit, recipe = "c" * 40, recipe_of("c" * 40)
        built = {"commit": commit, "committed": 1_789_990_000, "image": "img", "dashboards": {"a": {"b#c": entry()}}}
        loaded = tgc.load(self.write({"format": 3, "after": 1_789_999_000.5, "runs": [], "configs": {recipe: built},
                                      "group_changes": {}, "failed": {}}))
        self.assertEqual((loaded["configs"][recipe][0], loaded["failed"]), ({"a#b#c": entry()}, {}))

    def test_merge_works_out_how_runs_changed_their_test_groups(self):
        a, b, c = ({"build": str(1000 + i), "commit": x * 40, "started": 1_790_000_000 + i, "finished": 1_790_000_005 + i,
                    "recipe": tgc.recipe_id(x * 40, None)} for i, x in enumerate("abc"))
        job, other = dict(GROUP), {"name": "other", "gcs_prefix": "b/o", "use_kubernetes_client": True}

        def built(r, *groups, tabs=("job",)):
            config = {"test_groups": list(groups), "dashboards": [{"name": "dash", "dashboard_tab": [
                {"name": f"t{i}", "test_group_name": g} for i, g in enumerate(tabs)]}]}
            return {"recipe": r["recipe"], "commit": r["commit"], "committed": 1, "image": "i",
                    "dashboards": tgc.extract(config, ["dash"]), "groups": {g["name"]: tgc.group_digest(g) for g in groups}}
        planned = {"after": 999, "runs": [a, b, c], "build": [], "compare": [
            {"build": a["build"], "recipe": a["recipe"], "prev": None},
            {"build": b["build"], "recipe": b["recipe"], "prev": a["recipe"]},
            {"build": c["build"], "recipe": c["recipe"], "prev": b["recipe"]}]}
        # a: job alone; b: job changed, other added (for another dashboard); c: a tab of other, which b had
        extracts = [built(a, job), built(b, dict(job, num_columns_recent=4), other),
                    built(c, dict(job, num_columns_recent=4), other, tabs=("job", "other"))]
        out = tgc.merge(planned, extracts, {"x" * 12 + "-" + "y" * 12: "why", a["recipe"]: "built after all"})
        self.assertEqual(out["group_changes"], {a["build"]: {}, b["build"]: {"job": "changed"}, c["build"]: {}})
        self.assertEqual(out["failed"], {"x" * 12 + "-" + "y" * 12: "why"})
        self.assertNotIn("groups", out["configs"][a["recipe"]])
        # (a run whose run before was not built: not worked out)
        out = tgc.merge(planned, extracts[1:], {})
        self.assertNotIn(b["build"], out["group_changes"])
        self.assertEqual(out["group_changes"][c["build"]], {})
        # a group none of the config before had at all: new
        extracts[1] = built(b, dict(job, num_columns_recent=4))
        self.assertEqual(tgc.merge(planned, extracts, {})["group_changes"][c["build"]], {"other": "new"})


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
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        uploads = os.path.join(d, "uploads.json")
        ran = {"image": "gcr.io/k8s-staging-test-infra/configurator:v0", "command": ["configurator"],
               "args": ["--yaml=config/testgrids", "--output=gs://x"]}
        with open(uploads, "w") as f:
            json.dump({"build": [{"recipe": "r1", "commit": "a" * 40, "job": None},
                                 {"recipe": "r2", "commit": "b" * 40, "job": ran},
                                 {"recipe": "r3", "commit": "c" * 40, "job": {"error": "no good"}}]}, f)
        tree = self.tree(self.JOB)
        for recipe, want in (("r1", ["gcr.io/k8s-staging-test-infra/configurator:v1", "configurator",
                                     "--yaml=config/testgrids"]),  # (no prowjob.json: as defined at the commit)
                             ("r2", ["gcr.io/k8s-staging-test-infra/configurator:v0", "configurator",
                                     "--yaml=config/testgrids", "--oneshot"])):  # (as the run ran it)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(tgc.main(["job", uploads, recipe, tree]), 0)
            self.assertEqual(out.getvalue().split("\0")[:len(want)], want)
        err = io.StringIO()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            self.assertEqual(tgc.main(["job", uploads, "r3", tree]), 1)
        self.assertIn("no good", err.getvalue())

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
        return self.config_of({"dash": {"t": entry(base_options, **group)}})

    def config_of(self, dashboards, uploaded=None):
        """A config file for a new run of the upload job, at `uploaded` (else a minute after the
        last, from 30 days ago: the builds of these tests were all made under the config it
        uploaded)."""
        self.uploads = getattr(self, "uploads", 0) + 1
        return config_file(os.path.join(self.dir, f"testgrid-config-{self.uploads}.json"), dashboards,
                           uploaded or self.now - 30 * DAY + 60 * self.uploads)

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
        self.assertEqual(self.fetch_with(self.config("", build_override_configuration_value="x")), 1)
        warning = " ".join(self.runs()[-1]["serious_warnings"])
        self.assertIn("build_override_configuration_value, which testgrid_rule.py does not follow", warning)
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
        self.assertIn("TestGrid's config: test-infra c0cc6281b102 of ", self.readme())
        self.assertIn(", uploaded ", self.readme())

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
        # the same run without its config: judged by the config over time the archive keeps
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        run = frl.read_json(run_path)
        for t in run["tabs"]:
            t.pop("config", None)
        frl.write_json(run_path, run)
        code, out = self.check()
        self.assertEqual(code, 0, out)
        # ... and without that too: the build is red on kubetest.Test
        state = self.state()
        for k in ("config_uploads", "config_entries"):
            state.pop(k)
        frl.write_json(os.path.join(self.root, "state.json"), state)
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

    def test_cmd_updater_counts(self):
        # (testgrid_config.group_source follows its source(): where the updater reads a group's builds from)
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle"))
        import relevant_diff
        old = self.tree({"cmd/updater/main.go": "s", "cmd/updater/main_test.go": "t"})
        new = self.tree({"cmd/updater/main.go": "s2", "cmd/updater/main_test.go": "t2"})
        self.assertEqual(relevant_diff.changes(old, new), ["cmd/updater/main.go"])  # fails: []

    def test_the_code_the_fetchs_timing_follows_counts(self):
        # (fetch_red_logs's READ_GAP, PICKUP and TABULATE: when a group is handed out again, when a new config is
        # taken, when a tab is written again)
        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "oracle"))
        import relevant_diff
        files = {"util/queue/queue.go": "q", "config/queue.go": "c", "config/config.go": "x",
                 "config/snapshot/config_snapshot.go": "s", "cmd/config_merger/main.go": "m", "pkg/merger/merger.go": "g",
                 "cmd/tabulator/main.go": "t", "go.mod": "module m\n\nrequire github.com/sethvargo/go-retry v0.2.4\n"}
        old = self.tree(files)
        new = self.tree({k: v.replace("v0.2.4", "v0.2.5") if k == "go.mod" else v + "2" for k, v in files.items()})
        self.assertEqual(relevant_diff.changes(old, new),
                         ["util/queue/queue.go", "config/snapshot/config_snapshot.go", "cmd/config_merger/main.go",
                          "pkg/merger/merger.go", "cmd/tabulator/main.go", "config/queue.go",
                          "go.mod: github.com/sethvargo/go-retry v0.2.4 -> v0.2.5"])  # fails: []

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
    Go), replayed without it: where TestGrid rejects base options the port says so (Broken);
    where it keeps rows the port keeps the same ones."""

    def test_the_port_keeps_what_testgrid_keeps(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "testgrid_filter_golden.jsonl.gz")
        with gzip.open(path, "rt", encoding="utf-8") as f:
            cases = [json.loads(line) for line in f]
        self.assertGreater(len(cases), 1500)
        applied = rejected = 0
        for c in cases:
            rule = tgc.TabRule(entry(c["options"]))
            if not c["ok"]:  # (TestGrid's tabulator fails the tab)
                self.assertTrue(rule.broken, c)
                rejected += 1
                continue
            self.assertIsNone(rule.broken, c)
            self.assertIsNone(rule.filter_problem, c)
            self.assertEqual([n for n in c["names"] if rule.shows(n)], c["kept"], c["options"])
            applied += 1
        self.assertGreater(applied, 1000)
        self.assertGreater(rejected, 100)

