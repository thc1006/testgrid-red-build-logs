#!/usr/bin/env python3
# Ported in part from TestGrid (2bbb98db9f1a; Copyright The TestGrid Authors and The Kubernetes Authors),
# Apache License 2.0 (LICENSE), and Go's net/url (1.26.9; Copyright The Go Authors), BSD 3-Clause license in
# third_party/go/LICENSE. THIRD_PARTY_NOTICES.md lists what was ported and what changed.
"""What TestGrid's config says about the archive's tabs, built as testgrid.k8s.io's is.

testgrid.k8s.io's config is built by test-infra's configurator from the TestGrid
YAML in kubernetes/test-infra's config/testgrids and the testgrid-* annotations of
the Prow jobs in config/jobs: the postsubmit job post-test-infra-upload-testgrid-config
runs it on every change to those, and uploads the config to TestGrid. testgrid_config.sh
runs the image that job ran, with the job's own arguments, on the commit of each of its
runs, and `extract` keeps each tab of the archive's dashboards with its base options
and its test group.

TabRule applies a tab's entry to builds the fetch judges from GCS (testgrid_rule.py):
- the rows the tab shows: those its base options' include-filter-by-regex and
  exclude-filter-by-regex keep, as TestGrid's tabulator filters them (Go's
  url.ParseQuery, then regexp.MatchString on each row's name);
- the days of results its test group reads;
- .problem: why the port cannot judge the tab's builds as TestGrid does (a test
  group option it does not follow, or base options it cannot apply), else None.

TestGrid production takes the config each run of the upload job uploads: its
config merger copies it into the config TestGrid reads every 15 minutes (a merge
takes up to 10), and TestGrid's components read that again within a minute; so the
config TestGrid used at a time is the one of the job's last run before (or the run
before it, around the switch). `uploads` lists those runs, and testgrid_config.sh
builds the config of each run the archive does not know yet, newest first, at the
run's commit with the image and arguments the run itself ran (its prowjob.json);
`merge` writes them, with the runs and how each run changed the test groups of the
archive's tabs, for the fetch, which keeps each tab's config over time (see
fetch_red_logs: ConfigHistory).

Usage:
  testgrid_config.py uploads [--state STATE.json]
      what testgrid_config.sh is to do (see plan): the upload job's runs since the
      last one up to which the archive's state knows every run's config (else of the
      last HISTORY_DAYS days), and the configs to build, newest first
  testgrid_config.py recipes UPLOADS.json
      the configs to build, one "RECIPE COMMIT" per line
  testgrid_config.py job UPLOADS.json RECIPE TEST_INFRA
      the image the run ran, its command, then its arguments made to print the
      config as YAML instead of uploading it, NUL-separated (for a run without a
      prowjob.json: the job's definition in the checkout of its commit, TEST_INFRA)
  testgrid_config.py extract CONFIG.yaml --recipe R --commit SHA --committed UNIX --image IMAGE [--dashboard D ...]
      the archive's tabs from the configurator's YAML, with every test group's
      digest, as JSON
  testgrid_config.py merge [--failed FAILED.tsv] UPLOADS.json [EXTRACT.json ...]
      the runs, the configs extracted, how each run changed its tabs' test groups,
      and the configs that could not be built (RECIPE, a tab, why), as JSON for
      fetch_red_logs.py --testgrid-config
"""
import argparse
import concurrent.futures as cf
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# (its neighbours: testgrid_config.sh runs it with python -I, which leaves the script's folder off sys.path)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import goregexp  # noqa: E402
import testgrid_rule as tr  # noqa: E402

DASHBOARDS = ["sig-release-master-blocking", "sig-release-master-informing"]  # (the archive's: fetch_red_logs reads them)
UPLOAD_JOB = "post-test-infra-upload-testgrid-config"
# What the upload job's container may name (testgrid_config.sh passes them to docker run: an
# image or command that starts with "-" would be taken as docker's own options)
IMAGE_RE = re.compile(r"[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)+(?::[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?"
                      r"(?:@sha256:[0-9a-f]{64})?")
COMMAND_RE = re.compile(r"[A-Za-z0-9_./][A-Za-z0-9_./-]*")
FORMAT = 3  # of the JSON merge writes
GCS = "https://storage.googleapis.com"
UPLOAD_RUNS = "kubernetes-ci-logs/logs/" + UPLOAD_JOB  # where Prow keeps the upload job's runs
PROW_EPOCH_MS = 1288834974657  # build IDs count milliseconds from it (fetch_red_logs.PROW_EPOCH_MS)
HISTORY_DAYS = 18  # how far back the archive needs TestGrid's config (fetch_red_logs.GIVE_UP_DAYS, and the run
# in force then: in 90 days to 2026-10-12 the upload job's runs were at most 2.5 days apart)
BUILD_RE = re.compile(r"[0-9]{1,20}")
SHA_RE = re.compile(r"[0-9a-f]{40}")
RECIPE_RE = re.compile(r"[0-9a-f]{12}-[0-9a-f]{12}")  # (see recipe_id)
CHANGES = ("new", "changed")  # how a run changed a test group (see plan)
DIGEST_RE = re.compile(r"[0-9a-f]{64}")
MAX_JSON = 1 << 20  # a started.json, finished.json or listing page larger than this is refused
FUTURE_SLACK = 86400  # a run in state.json made or timed more than this after now is refused (fetch_red_logs's)
RETRIES = 4

# The TestGroup fields TestGrid's updater (pkg/updater at the commit testgrid_rule.py
# ports) reads that decide whether it reads the group's builds, from where, a column's
# rows or their results, as extract keeps them, with their types (see group_source and
# group_problems).
FIELD_TYPES = {"name": str, "gcs_prefix": str, "days_of_results": int, "use_kubernetes_client": bool,
               "result_source": dict, "disable_merged_status": bool, "disable_prowjob_analysis": bool,
               "ignore_skip": bool, "custom_evaluator_rule_set": dict, "test_name_config": dict,
               "build_override_strftime": str, "build_override_configuration_value": str, "ignore_old_results": bool}
GROUP_FIELDS = tuple(FIELD_TYPES)
DEFAULT_DAYS = 7  # the updater's days of results when the group's is 0

# Base options that change how a tab is drawn, never whether a column shows a red cell:
# sorting, width, graphs, grouping rows (a group shows the worst of its rows), and hiding
# rows with no failure (a red cell is one).
DRAWING_OPTIONS = frozenset({"width", "sort-by-name", "sort-by-failures", "sort-by-flakiness", "graph-metrics",
                             "group-by-directory", "group-by-target", "group-by-hierarchy-pattern",
                             "expand-groups", "exclude-non-failed-tests"})
INCLUDE, EXCLUDE = "include-filter-by-regex", "exclude-filter-by-regex"


class Unsupported(ValueError):
    """What TestGrid would do with this is not known here (or TestGrid rejects it)."""


class FilterTooSlow(Unsupported):
    """A tab's filters took longer to apply than their Budget allows."""


FILTER_SECONDS = 30.0  # the CPU time one tab's filters may spend matching in a run (real ones take milliseconds)


class Budget:
    """The CPU time a tab's filters may still spend matching (each thread's own, so
    other work does not count), shared by its regexes and threads."""

    def __init__(self, seconds=None):
        self.left, self.lock = FILTER_SECONDS if seconds is None else seconds, threading.Lock()

    def check(self, spent):
        if spent > self.left:
            raise FilterTooSlow(f"its filters took more than {FILTER_SECONDS:.0f} s to apply")

    def spend(self, spent):
        with self.lock:
            self.left -= spent
            self.check(0)


# ------------------------------------------------------------ Go's url.ParseQuery

MAX_QUERY_PARAMS = 10000  # net/url's limit (urlmaxqueryparams)
_HEX = frozenset(b"0123456789abcdefABCDEF")


def go_query_unescape(s):
    """url.QueryUnescape: bytes, "+" a space; a malformed %-escape raises ValueError."""
    b = s.encode("utf-8", "surrogateescape")
    i = 0
    while i < len(b):
        if b[i] == 0x25 and (i + 2 >= len(b) or b[i + 1] not in _HEX or b[i + 2] not in _HEX):
            raise ValueError(f"invalid URL escape {b[i:i + 3].decode('latin-1')!r}")
        i += 3 if b[i] == 0x25 else 1
    out, i = bytearray(), 0
    while i < len(b):
        if b[i] == 0x25:
            out.append(int(b[i + 1:i + 3], 16))
            i += 3
            continue
        out.append(0x20 if b[i] == 0x2B else b[i])
        i += 1
    return bytes(out)


def go_parse_query(query):
    """url.ParseQuery: ({key: [values]} as bytes, in order, and the error it returns
    or None). Like Go it keeps going after a bad pair; TestGrid's tabulator gives up
    on a tab whose base options return any error."""
    if query.count("&") + 1 > MAX_QUERY_PARAMS:
        return {}, "number of URL query parameters exceeded limit"
    values, err = {}, None
    while query != "":
        key, _, query = query.partition("&")
        if ";" in key:
            err = "invalid semicolon separator in query"
            continue
        if key == "":
            continue
        key, _, value = key.partition("=")
        try:
            k, v = go_query_unescape(key), go_query_unescape(value)
        except ValueError as e:
            err = err or str(e)
            continue
        values.setdefault(k, []).append(v)
    return values, err


# ------------------------------------------------------------ Go's regexp

class Broken(ValueError):
    """TestGrid itself cannot apply the tab's base options (url.ParseQuery or regexp.Compile
    returns an error): its tabulator fails the tab, which keeps the state it had."""


def go_regexp(pattern):
    """regexp.Compile(pattern) for MatchString (goregexp: Go's parser, matched without
    backtracking): Broken when Go rejects it, FilterTooSlow when its automaton is too
    large to run here."""
    try:
        return _Filter(goregexp.compile(pattern))
    except goregexp.Error as e:
        raise Broken(str(e)) from None
    except goregexp.TooLarge as e:
        raise FilterTooSlow(f"too large to apply here ({e})") from None


class _Filter:
    """A compiled filter: search(text, budget) spends the time it takes from the Budget."""

    def __init__(self, matcher):
        self.matcher = matcher

    def search(self, text, budget=None):
        return self.matcher.search(text, budget)


# ------------------------------------------------------------ the tab's rule

def group_days(group):
    """The days of results TestGrid's updater reads the group's builds for."""
    days = group.get("days_of_results") or 0
    return days if days > 0 else DEFAULT_DAYS


def group_source(group):
    """Where TestGrid production's updater reads the group's builds from (cmd/updater's
    source() and the GCS updater: a group with use_kubernetes_client or a GCS result
    source, from gcsPrefix(); its ResultStore updater has no client in production, which
    does not set --enable-resultstore, and any other group fails): (the GCS prefix, None),
    or (None, why it never updates the group, whose tabs then show no new column)."""
    source = group.get("result_source")
    gcs_config = source.get("gcs_config") if isinstance(source, dict) else None
    if group.get("use_kubernetes_client") or gcs_config is not None:
        prefix = gcs_config.get("gcs_prefix") if isinstance(gcs_config, dict) else group.get("gcs_prefix")
        return (prefix if isinstance(prefix, str) else ""), None
    if isinstance(source, dict) and source.get("resultstore_config") is not None:
        return None, ("its results are in ResultStore, which TestGrid production does not read "
                      "(--enable-resultstore is off), so it never updates the group")
    return None, ("it sets no source of results (use_kubernetes_client or a GCS config), so TestGrid's "
                  "updater never updates the group")


def cell_options(group):
    """(the cell options TestGrid's updater paints the group's columns with, as
    testgrid_rule.Options; why the port cannot follow them, else None). The config is
    as test-infra's configurator prints it: enums as numbers, and a oneof as its Go
    field ("TestResultInfo", "ComparisonValue"), which it cannot set from YAML (null):
    a custom evaluator rule's first comparison then makes it never match (evalProperties
    returns at a comparison without its Comparison, or without what to compare), and a
    rule without comparisons gives every test result its status."""
    all_tests = None
    rules = (group.get("custom_evaluator_rule_set") or {}).get("rules") or []
    for rule in rules if isinstance(rules, list) else [None]:
        if not isinstance(rule, dict):
            return None, "custom_evaluator_rule_set: a rule that is not one"
        comparisons = rule.get("test_result_comparisons") or []
        if not isinstance(comparisons, list) or not all(isinstance(c, dict) for c in comparisons):
            return None, "custom_evaluator_rule_set: comparisons that are not ones"
        if comparisons:
            first = comparisons[0]
            if first.get("comparison") is not None and first.get("TestResultInfo") is not None:
                return None, "custom_evaluator_rule_set: a comparison of a test result (not known here)"
            continue  # (never matches)
        status = rule.get("computed_status", 0)
        if not (isinstance(status, int) and not isinstance(status, bool) and 0 <= status < len(tr.STATUS_OF)):
            return None, f"custom_evaluator_rule_set: a computed status not known here ({status!r})"
        all_tests = tr.STATUS_OF[status]
        break
    names = None
    config = group.get("test_name_config")
    if config is not None:
        fmt, elements = config.get("name_format", ""), config.get("name_elements") or []
        if not isinstance(fmt, str) or not isinstance(elements, list) or not all(
                isinstance(e, dict) and all(isinstance(e.get(k, ""), str) for k in ("target_config", "test_property"))
                for e in elements):
            return None, "test_name_config: not one TestGrid reads"
        names = (fmt, tuple(e.get("target_config") or e.get("test_property") or "" for e in elements))
    return tr.Options(merge=not group.get("disable_merged_status"), pod=not group.get("disable_prowjob_analysis"),
                      all_tests=all_tests, names=names), None


def group_problems(group):
    """The test group options set otherwise than testgrid_rule.py follows (a group as
    valid_entry accepts it): several GCS prefixes (one column holds builds of several
    jobs), build overrides (one column holds several builds), and cell options the
    port cannot follow (see cell_options). Not among them: where its builds are read
    from (see group_source); ignore_skip, which TestGrid production does not apply (its
    updater's --enable-ignore-skip is off); ignore_old_results, which only decides which
    of the columns a build override merges keeps a row's result."""
    out = []
    if "," in (group_source(group)[0] or ""):
        out.append("several GCS prefixes")
    out += [name for name in ("build_override_strftime", "build_override_configuration_value") if group.get(name)]
    _, problem = cell_options(group)
    if problem:
        out.append(problem)
    return out


def tab_filters(base_options):
    """The include and exclude regexes TestGrid's tabulator filters a tab's rows by
    (filterGrid). Broken when TestGrid cannot apply them (it fails the tab), Unsupported
    when the port cannot."""
    values, err = go_parse_query(base_options)
    if err:
        raise Broken(f"TestGrid cannot parse them: {err}")
    keys = {k.decode("utf-8", "replace"): v for k, v in values.items()}
    compiled = {}
    for name in (INCLUDE, EXCLUDE):
        compiled[name] = []
        for raw in keys.get(name, []):
            try:
                pattern = raw.decode("utf-8")
            except UnicodeDecodeError:
                raise Broken(f"{name}: invalid UTF-8, which Go's regexp rejects") from None
            try:
                compiled[name].append(go_regexp(pattern))
            except (Broken, Unsupported) as e:
                raise type(e)(f"{name}={pattern!r}: {e}") from None
    return compiled[INCLUDE], compiled[EXCLUDE]


def filter_key(base_options):
    """What decides which rows a tab shows (the filters TestGrid's tabulator applies, as
    it parses them, or why it cannot): configs with equal keys show the same rows (its
    other base options only draw the tab)."""
    values, err = go_parse_query(base_options)
    if err:
        return ("unparsed", str(err))
    return tuple(tuple(values.get(name.encode(), ())) for name in (INCLUDE, EXCLUDE))


def unknown_options(base_options):
    """The keys of a tab's base options other than its filters and the drawing options
    known here: TestGrid's tabulator ignores them (filterGrid), so the port applies the
    filters without them; if its page hid rows by one, a build could count as red here
    that it does not show red."""
    values, _ = go_parse_query(base_options)
    return sorted({k.decode("utf-8", "replace") for k in values} - {INCLUDE, EXCLUDE} - DRAWING_OPTIONS)


KNOWN_ROWS = 100_000  # rows a TabRule remembers whether it shows


class TabRule:
    """How a tab shows a build's column, from its entry in the config (one valid_entry
    accepts): .shows(row), .days (of results its test group reads), .prefix (the GCS
    prefix it reads builds from), or .not_updated: why TestGrid never updates its test
    group (see group_source), else None; .options: the cell options its columns are
    painted with (see cell_options); .group_problem: why the port cannot paint its
    columns as TestGrid does (a test group option it does not follow), and
    .filter_problem: why it cannot apply the tab's filters (shows() then raises
    Unsupported), else None; .broken: why TestGrid itself cannot (its tabulator then
    fails the tab: shows() raises Broken), else None; .unknown_options: other keys
    of its base options (see unknown_options)."""

    def __init__(self, entry):
        self.entry, self.budget, self.known = entry, Budget(), {}
        group = entry.get("group")
        problems = []
        if entry.get("duplicate"):
            problems.append("the dashboard has more than one tab of this name")
        self.prefix, self.not_updated = None, None
        if group is None:
            problems.append(f"its test group {entry.get('test_group')!r} is not in the config")
            group = {}
        else:
            self.prefix, self.not_updated = group_source(group)
        problems += [f"its test group sets {p}, which testgrid_rule.py does not follow" for p in group_problems(group)]
        self.group_problem = "; ".join(problems) or None
        self.options = cell_options(group)[0] or tr.DEFAULT_OPTIONS
        self.days = group_days(group)
        self.includes, self.excludes, self.filter_problem, self.broken = [], [], None, None
        self.unknown_options = unknown_options(entry.get("base_options") or "")
        self.filters = filter_key(entry.get("base_options") or "")
        try:
            self.includes, self.excludes = tab_filters(entry.get("base_options") or "")
        except Broken as e:
            self.broken = f"its base options {entry.get('base_options')!r}: {e}"
        except Unsupported as e:
            self.filter_problem = f"its base options {entry.get('base_options')!r}: {e}"

    @property
    def problem(self):
        return "; ".join(p for p in (self.group_problem, self.filter_problem) if p) or None

    def shows(self, row):
        """Whether the tab shows the row; Unsupported (FilterTooSlow when its filters
        run out of time, which then stays the filter_problem) if that is not known,
        Broken when TestGrid shows no row (see .broken)."""
        if self.broken:
            raise Broken(self.broken)
        if self.filter_problem:
            raise Unsupported(self.filter_problem)
        shown = self.known.get(row)  # (a row is asked about once per build that has it)
        if shown is None:
            try:
                shown = all(r.search(row, self.budget) for r in self.includes) and \
                    not any(r.search(row, self.budget) for r in self.excludes)
            except FilterTooSlow as e:
                self.filter_problem = f"its base options {self.entry.get('base_options')!r}: {e}"
                raise
            if len(self.known) >= KNOWN_ROWS:
                self.known.clear()
            self.known[row] = shown
        return shown


def valid_entry(entry):
    """Whether `entry` is a tab's entry as extract writes it, every field of the type
    the config has (TabRule takes nothing else)."""
    if not isinstance(entry, dict) or not set(entry) <= {"test_group", "base_options", "group", "group_digest",
                                                         "duplicate"}:
        return False
    if not isinstance(entry.get("test_group"), str) or not isinstance(entry.get("base_options", ""), str):
        return False
    digest = entry.get("group_digest")
    if digest is not None and not (isinstance(digest, str) and DIGEST_RE.fullmatch(digest)):
        return False
    if "duplicate" in entry and entry["duplicate"] is not True:
        return False
    group = entry.get("group")
    if group is None:
        return True
    if not isinstance(group, dict) or not set(group) <= set(GROUP_FIELDS):
        return False
    for name, value in group.items():
        kind = FIELD_TYPES[name]
        if not isinstance(value, kind) or (kind is int and isinstance(value, bool)):
            return False
    rules = (group.get("custom_evaluator_rule_set") or {}).get("rules", [])
    return isinstance(rules, list)


def load(path):
    """What merge wrote: {"after": the runs of the upload job made after this Unix time
    are all listed, "runs": those runs, as plan() lists them ({"build", "commit",
    "started", "finished", "recipe"}), "configs": {recipe: ({"dashboard#tab": entry}, its
    source)} for the configs built, "group_changes": {build: {test group: "new" or
    "changed"}}: how each run changed its tabs' test groups, where that was worked out
    (see plan), "failed": {recipe: why} for the configs that could not be built}.
    Raises ValueError if it is not that."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or data.get("format") != FORMAT or not isinstance(data.get("runs"), list) \
            or not isinstance(data.get("configs"), dict) or not isinstance(data.get("group_changes"), dict) \
            or not isinstance(data.get("failed"), dict) or not is_moment(data.get("after")):
        raise ValueError("not a config written by testgrid_config.py merge")
    runs = []
    for r in data["runs"]:
        if not (isinstance(r, dict) and set(r) == {"build", "commit", "started", "finished", "recipe"}
                and valid_run(r) and isinstance(r["recipe"], str) and RECIPE_RE.fullmatch(r["recipe"])):
            raise ValueError(f"a malformed run of the upload job: {str(r)[:200]}")
        runs.append(r)
    for recipe, why in data["failed"].items():
        if not (RECIPE_RE.fullmatch(recipe) and isinstance(why, str)):
            raise ValueError(f"{recipe[:40]}: malformed failure")
    configs, failed = {}, dict(data["failed"])
    for recipe, built in data["configs"].items():
        if not (RECIPE_RE.fullmatch(recipe) and isinstance(built, dict) and isinstance(built.get("dashboards"), dict)
                and isinstance(built.get("commit"), str) and SHA_RE.fullmatch(built["commit"])
                and is_time(built.get("committed")) and isinstance(built.get("image"), str) and built["image"]):
            raise ValueError(f"{recipe[:40]}: not a config extract wrote")
        tabs, bad = {}, None
        for dashboard, entries in built["dashboards"].items():
            if "#" in dashboard:  # (see dashboard_name)
                bad = f"{dashboard}: a dashboard name with '#'"
                break
            if not isinstance(entries, dict):
                bad = f"{dashboard}: not an object"
                break
            for tab, entry in entries.items():
                if not valid_entry(entry):
                    bad = f"{dashboard}#{tab}: an entry not as extract writes it"
                    break
                tabs[f"{dashboard}#{tab}"] = entry
            if bad:
                break
        if bad:  # (that config is not taken: as one that could not be built, the others are)
            failed[recipe] = f"its config as written: {bad}"[:500]
            continue
        source = {"test_infra": built["commit"], "committed": built["committed"], "image": built["image"]}
        if isinstance(built.get("digest"), str) and built["digest"]:
            source["digest"] = built["digest"]
        configs[recipe] = (tabs, source)
    for build, changes in data["group_changes"].items():
        if not (BUILD_RE.fullmatch(build) and valid_changes(changes)):
            raise ValueError(f"{build[:40]}: malformed changes of its test groups")
    return {"after": data["after"], "runs": runs, "configs": configs, "group_changes": data["group_changes"],
            "failed": failed}


def valid_run(r, now=None):
    """Whether r holds a run of the upload job as listed: {"build", "commit", "started", "finished"};
    with `now`, one made and timed at most FUTURE_SLACK after it."""
    if not (isinstance(r.get("build"), str) and BUILD_RE.fullmatch(r["build"]) is not None
            and isinstance(r.get("commit"), str) and SHA_RE.fullmatch(r["commit"]) is not None
            and is_time(r.get("started"))
            and (r.get("finished") is None or is_time(r["finished"]) and r["finished"] >= r["started"])):
        return False
    return now is None or max(made(r["build"]), r["started"], r["finished"] or 0) <= now + FUTURE_SLACK


def valid_changes(changes):
    """Whether `changes` is how a run changed test groups: {name: "new" or "changed"}."""
    return isinstance(changes, dict) and all(isinstance(k, str) and v in CHANGES for k, v in changes.items())


def valid_built(built):
    """Whether `built` says what a run's config was built from, as load takes it: {"committed":
    test-infra's commit time, "image": the configurator's, and its "digest" if known}."""
    return (isinstance(built, dict) and set(built) <= {"committed", "image", "digest"} and is_time(built.get("committed"))
            and isinstance(built.get("image"), str) and built["image"] != ""
            and (built.get("digest") is None or isinstance(built["digest"], str) and built["digest"] != ""))


def tabs_digest(ids):
    """What a run's whole config of the tabs ({"dashboard#tab": entry id}) is checked by in
    state.json ("config_digest"), as the runs keep only what changed."""
    return hashlib.sha256(json.dumps(ids, sort_keys=True).encode()).hexdigest()[:16]


def decode_uploads(uploads, entries, now=None):
    """The upload job's runs as state.json keeps them ("config_uploads", each with the tabs
    whose entry it changed, every tab's for a "base" one, and the digest of all of them;
    "config_entries", {id: entry}; see fetch_red_logs.ConfigHistory.encode), oldest first,
    as {"build", "commit", "started", "finished" (None while it ran), "tabs":
    {"dashboard#tab": entry} (None: not known), "group_changes": (see load; None: not
    known), "listed": when the archive first listed it (None: not kept),
    "failed": why its config could not be built (None), "recipe" (None), "built": what its
    config was built from (see valid_built; None)}; and the Unix time after which the
    job's runs are to be listed again (else None): a malformed run, one listed twice, or
    (with `now`) one from the future, is dropped. A run whose changes do not give the config
    its digest says (the list was edited, or merged with another) is not known, and neither
    are the runs after it until a base."""
    entries = entries if isinstance(entries, dict) else {}
    good, dropped = [], []
    for u in uploads if isinstance(uploads, list) else ():
        tabs = u.get("tabs") if isinstance(u, dict) else None
        if not (isinstance(u, dict) and valid_run(u, now)
                and (tabs is None or isinstance(tabs, dict) and all(
                    isinstance(k, str) and (i is None or isinstance(i, str) and valid_entry(entries.get(i)))
                    for k, i in tabs.items()))
                and (u.get("group_changes") is None or valid_changes(u["group_changes"]))
                and (u.get("config_digest") is None or isinstance(u["config_digest"], str))
                and (u.get("listed") is None or is_moment(u["listed"]) and (now is None or u["listed"] <= now + FUTURE_SLACK))
                and (u.get("failed") is None or isinstance(u["failed"], str))
                and (u.get("recipe") is None or isinstance(u["recipe"], str) and RECIPE_RE.fullmatch(u["recipe"]))
                and (u.get("built") is None or valid_built(u["built"]))):
            dropped.append(u)
            continue
        good.append(u)
    good.sort(key=lambda u: int(u["build"]))  # (as encode wrote them)
    runs, ids, relist = [], None, None
    for u in good:
        if runs and runs[-1]["build"] == u["build"]:
            dropped.append(u)
            continue
        tabs = u.get("tabs")
        if tabs is None or not u.get("base") and ids is None:
            ids = None
        else:
            ids = {} if u.get("base") else dict(ids)
            for k, i in tabs.items():
                if i is None:
                    ids.pop(k, None)
                else:
                    ids[k] = i
            if u.get("config_digest") is not None and u["config_digest"] != tabs_digest(ids):
                ids = None  # (not the config it had)
        runs.append({"build": u["build"], "commit": u["commit"], "started": u["started"], "finished": u.get("finished"),
                     "tabs": None if ids is None else {k: entries[i] for k, i in ids.items()},
                     "group_changes": u.get("group_changes") if ids is not None else None,
                     "listed": u.get("listed"), "failed": u.get("failed")[:300] if ids is None and u.get("failed") else None,
                     "recipe": u.get("recipe"), "built": u.get("built")})
    if dropped:
        builds = [int(u["build"]) for u in dropped if isinstance(u, dict) and isinstance(u.get("build"), str)
                  and BUILD_RE.fullmatch(u["build"])]
        known = [int(r["build"]) for r in runs]
        if len(builds) < len(dropped) or not known:
            relist = 0  # (from the first run kept: where it was is not known)
        else:
            before = [b for b in known if b < min(builds)]
            relist = made(str(max(before))) if before else 0
    return runs, relist


# ------------------------------------------------------------ building it

def extract(config, dashboards):
    """{dashboard: {tab: entry}} from the configurator's config (as parsed YAML)."""
    groups = {}
    for g in config.get("test_groups") or []:
        if isinstance(g, dict):  # (the configurator prints mappings: anything else is no group)
            groups.setdefault(g.get("name"), g)  # (the configurator rejects duplicate names)
    out = {}
    for d in config.get("dashboards") or []:
        if not isinstance(d, dict) or d.get("name") not in dashboards:
            continue
        tabs = out.setdefault(d["name"], {})
        for t in d.get("dashboard_tab") or []:
            if not isinstance(t, dict):
                continue
            name = t.get("name")
            g = groups.get(t.get("test_group_name"))
            entry = {"test_group": t.get("test_group_name") or "", "base_options": t.get("base_options") or "",
                     "group": None if g is None else {k: g[k] for k in GROUP_FIELDS if k in g},
                     "group_digest": None if g is None else group_digest(g)}
            if name in tabs:
                entry["duplicate"] = True
            tabs[name] = entry
    return out


def group_digest(group):
    """What tells two configs of a test group apart as TestGrid's updater compares them
    (reprocessColumn: TestGroup.String(), every field): the SHA-256 of the whole group as
    the configurator printed it."""
    text = json.dumps(group, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode()).hexdigest()


def made(build):
    """When Prow made a build of this ID (seconds)."""
    return ((int(build) >> 22) + PROW_EPOCH_MS) / 1000


def gcs_get(url):
    """An object's bytes from GCS (None when there is none), retried as a server asks."""
    for attempt in range(RETRIES):
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                data = r.read(MAX_JSON + 1)
            if len(data) > MAX_JSON:
                raise ValueError(f"{url}: more than {MAX_JSON} bytes")
            return data
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code not in (408, 429, 500, 502, 503, 504) or attempt == RETRIES - 1:
                raise
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            if attempt == RETRIES - 1:
                raise
        time.sleep(2 ** attempt)


def json_of(data):
    """A JSON object's value (None for no object, or one that is not JSON)."""
    try:
        return json.loads(data) if data is not None else None
    except (ValueError, UnicodeDecodeError):
        return None


def is_moment(v):
    """Whether v is a Unix time (an int or a float) as plan() and decode_uploads give them."""
    return isinstance(v, (int, float)) and not isinstance(v, bool) and 0 <= v < 1 << 40


def is_time(v):
    return isinstance(v, int) and not isinstance(v, bool) and 0 < v < 1 << 40


def read_run(build, gcs=None):
    """A run of the upload job, as GCS holds it: {"build", "commit", "started", "finished"
    (None while it runs), "job": the container it ran (see job_of), None when it has no
    prowjob.json}; None for a run that uploaded nothing (it failed, or never started on a
    test-infra commit)."""
    base = f"{gcs or GCS}/{UPLOAD_RUNS}/{build}"
    started = json_of(gcs_get(f"{base}/started.json"))
    commit = started.get("repo-commit") if isinstance(started, dict) else None
    if not (isinstance(commit, str) and SHA_RE.fullmatch(commit) and is_time(started.get("timestamp"))):
        return None  # (it never started on a test-infra commit: it uploaded nothing)
    data = gcs_get(f"{base}/finished.json")
    finished = json_of(data)
    if data is None:
        end = None
    elif isinstance(finished, dict) and finished.get("passed") is True and is_time(finished.get("timestamp")):
        end = max(finished["timestamp"], started["timestamp"])
    else:
        return None  # (it failed: it uploaded nothing, or nothing TestGrid takes)
    data = gcs_get(f"{base}/prowjob.json")
    return {"build": build, "commit": commit, "started": started["timestamp"], "finished": end,
            "job": None if data is None else job_of(json_of(data))}


def upload_runs(after, gcs=None, known=None):
    """The upload job's runs made after `after` (a Unix time), oldest first, as read_run
    gives them: the ones that uploaded TestGrid's config (passed), and the ones still
    running (they may upload it). known: {build: run} the archive knows already: one that
    ended (it passed, then) and whose recipe it keeps is not read again (it has no "job":
    plan() reads that when it builds the run's config)."""
    gcs = gcs or GCS
    bucket, _, prefix = UPLOAD_RUNS.partition("/")
    prefix += "/"
    first = max(0, int(after * 1000) - PROW_EPOCH_MS) << 22
    builds, token = [], None
    for _ in range(1000):
        params = {"prefix": prefix, "delimiter": "/", "startOffset": f"{prefix}{first}",
                  "fields": "prefixes,nextPageToken"}
        if token:
            params["pageToken"] = token
        page = json_of(gcs_get(f"{gcs}/storage/v1/b/{bucket}/o?{urllib.parse.urlencode(params)}"))
        prefixes = page.get("prefixes", []) if isinstance(page, dict) else None
        if not isinstance(prefixes, list):
            raise ValueError(f"unexpected GCS listing of gs://{UPLOAD_RUNS}/")
        for p in prefixes:
            b = p[len(prefix):].rstrip("/") if isinstance(p, str) and p.startswith(prefix) else ""
            if BUILD_RE.fullmatch(b) and made(b) > after:
                builds.append(b)
        token = page.get("nextPageToken")
        if not token:
            break
    else:
        raise ValueError(f"the GCS listing of gs://{UPLOAD_RUNS}/ did not end")
    known = known or {}

    def run_of(b):
        k = known.get(b)
        if k is not None and k.get("finished") is not None and k.get("recipe"):
            return {x: k[x] for x in ("build", "commit", "started", "finished", "recipe")}
        return read_run(b, gcs)
    with cf.ThreadPoolExecutor(8) as pool:
        return [r for r in pool.map(run_of, sorted(set(builds), key=int)) if r]


def entry_id(entry):
    """What names a tab's entry in state.json's "config_entries" (fetch_red_logs.ConfigHistory)."""
    return hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest()[:16]


def recipe_id(commit, job):
    """What names the config a run built: its commit and the container it ran."""
    return commit[:12] + "-" + hashlib.sha256(json.dumps([commit, job], sort_keys=True).encode()).hexdigest()[:12]


def unexplained(tabs, before):
    """The test groups of a run's tabs that no tab of the run before had (None: that run's
    tabs are not known): whether such a group is new to TestGrid, or one it already read
    for another dashboard, only the run before's whole config tells."""
    mine = {e["test_group"] for e in tabs.values() if e.get("group") is not None}
    if not mine or before is None:
        return None if mine else set()
    return mine - {e["test_group"] for e in before.values() if e.get("group") is not None}


NOT_READ = object()  # (a run's container plan() has not read)


def plan(state, now, gcs=None):
    """What testgrid_config.sh is to do for the archive whose state.json holds `state`:
    {"after": the upload job's runs made after this Unix time are listed (after the last
    one up to which the archive knows every run's config, or the first malformed one it
    dropped; else HISTORY_DAYS back), "runs": those runs (read_run's, with their
    "recipe"), "build": the configs to build, newest first, as {"recipe", "commit", "job"},
    "compare": [{"build", "recipe", "prev"}]: the runs whose changes to their tabs' test
    groups to work out (see merge), with the recipe of the run before ("prev"; None for
    the first run of the history)}. Built are the configs of the runs whose config the
    archive does not know, and of those whose changes to their tabs' test groups it cannot
    tell otherwise (a tab of a test group none of the run before's tabs had), each with
    the run before (its whole config tells whether a group is new)."""
    gcs = gcs or GCS
    state = state if isinstance(state, dict) else {}
    known, relist = decode_uploads(state.get("config_uploads"), state.get("config_entries"), now)
    flagged = state.get("config_relist_after")  # (a run the fetch dropped from state.json)
    if is_moment(flagged):
        relist = flagged if relist is None else min(relist, flagged)
    prefix = None
    for r in known:
        if r["tabs"] is None or r["finished"] is None:
            break
        prefix = r
    if prefix is not None:
        after = made(prefix["build"])
    else:
        after = made(known[0]["build"]) - 1 if known else now - HISTORY_DAYS * 86400
    if relist is not None:
        after = min(after, relist if relist else min([made(r["build"]) - 1 for r in known], default=after))
    listed = upload_runs(after, gcs, {r["build"]: r for r in known})
    here = {r["build"] for r in listed}
    # the runs whose config was in force at some time: the archive's (but one that still ran
    # and is not listed now: it failed), and the ones listed now
    timeline = {r["build"]: dict(r) for r in known
                if not (r["finished"] is None and made(r["build"]) > after and r["build"] not in here)}
    for r in listed:
        had = timeline.get(r["build"]) or {"tabs": None, "group_changes": None}
        timeline[r["build"]] = dict(r, tabs=had["tabs"], group_changes=had["group_changes"], failed=had.get("failed"))
    order = sorted(timeline.values(), key=lambda r: int(r["build"]))
    # (newest first: each run whose config to build, and the run before when its changes are worked out
    # from that one's config: None when they are not, now; False for the first run of the history)
    steps = []
    for i in reversed(range(len(order))):
        r, before = order[i], order[i - 1] if i else None
        if r["tabs"] is not None and (r["group_changes"] is not None or before is None or before["finished"] is None
                                      or unexplained(r["tabs"], before["tabs"]) == set()):
            continue
        if r["tabs"] is not None and before["tabs"] is None and before.get("failed"):
            continue  # (the run before's config could not be built last time: compared once it is)
        if r["tabs"] is None and r.get("failed"):
            steps.append((r, None))  # (its own could not be built last time: compared once it is, not before)
        elif before is None:
            steps.append((r, False))
        else:  # (one still running may not upload: not known which came before)
            steps.append((r, before if before["finished"] is not None else None))
    # (the containers they ran, read at once: a run listed before has only its recipe kept)
    unread = sorted({x["build"] for r, before in steps for x in (r, before) if x and "job" not in x})
    with cf.ThreadPoolExecutor(8) as pool:
        files = dict(zip(unread, pool.map(lambda b: read_run(b, gcs), unread), strict=True))
    recipes = {}  # build -> (recipe, the container it ran; NOT_READ while only its kept recipe is)

    def recipe(r, job=False):
        """The run's recipe; with job, the container it ran too (to build its config)."""
        b = r["build"]
        if b not in recipes or job and recipes[b][1] is NOT_READ:
            if not job and r.get("recipe"):
                recipes[b] = (r["recipe"], NOT_READ)
            else:
                got = r if "job" in r else files[b] if b in files else read_run(b, gcs)  # (one listed before: its files)
                container = got["job"] if got else None
                recipes[b] = (recipe_id(r["commit"], container), container)
        return recipes[b][0]
    build, compare, seen = [], [], set()

    def add(r):
        rid = recipe(r, job=True)
        if rid not in seen:
            seen.add(rid)
            build.append({"recipe": rid, "commit": r["commit"], "job": recipes[r["build"]][1]})
    for r, before in steps:
        add(r)
        if before is False:
            compare.append({"build": r["build"], "recipe": recipe(r), "prev": None})
        elif before is not None:
            add(before)
            compare.append({"build": r["build"], "recipe": recipe(r), "prev": recipe(before)})
    return {"after": after, "runs": [dict(r, recipe=recipe(r)) for r in listed], "build": build, "compare": compare}


def checked_container(c, what):
    """(image, command, args) of a job's container, as testgrid_config.sh may run it."""
    image, command, args = c.get("image"), c.get("command") or [], c.get("args") or []
    if not isinstance(image, str) or not image or not isinstance(command, list) or not command \
            or not isinstance(args, list) or not all(isinstance(a, str) for a in command + args):
        raise ValueError(f"{what}: no image, command or arguments")
    if not IMAGE_RE.fullmatch(image) or not COMMAND_RE.fullmatch(command[0]) \
            or any("\0" in a or "\n" in a for a in command + args):
        raise ValueError(f"{what}: an image or command this does not run: {image!r} {command[0]!r}")
    return image, command, args


def job_of(prowjob):
    """The container a run of the upload job ran, from its prowjob.json (Prow's record of
    what it ran, as Prow knew the job then: a commit that changes the job runs it the
    old way): {"image", "command", "args"}, or {"error": why} when that cannot be told."""
    spec = prowjob.get("spec") if isinstance(prowjob, dict) else None
    pod = spec.get("pod_spec") if isinstance(spec, dict) else None
    containers = pod.get("containers") if isinstance(pod, dict) else None
    if not (isinstance(spec, dict) and spec.get("job") == UPLOAD_JOB and isinstance(containers, list)
            and len(containers) == 1 and isinstance(containers[0], dict)):
        return {"error": f"its prowjob.json is not one of {UPLOAD_JOB} with one container"}
    try:
        image, command, args = checked_container(containers[0], "its prowjob.json")
    except ValueError as e:
        return {"error": str(e)}
    return {"image": image, "command": command, "args": args}


def upload_job(test_infra):
    """The container of test-infra's job that uploads TestGrid's config, as defined in a
    checkout of test-infra: (image, command, args)."""
    import yaml

    jobs = os.path.join(test_infra, "config", "jobs")
    found = []
    for root, _, files in os.walk(jobs):
        for name in sorted(files):
            if not name.endswith((".yaml", ".yml")):
                continue
            path = os.path.join(root, name)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            if UPLOAD_JOB not in text:
                continue
            for doc in yaml.safe_load_all(text):
                for kind in ("periodics", "postsubmits", "presubmits"):
                    listed = doc.get(kind) if isinstance(doc, dict) else None
                    lists = listed.values() if isinstance(listed, dict) else [listed]
                    found += [job for jobs_ in lists if isinstance(jobs_, list) for job in jobs_
                              if isinstance(job, dict) and job.get("name") == UPLOAD_JOB]
    if len(found) != 1:
        raise ValueError(f"{len(found)} jobs named {UPLOAD_JOB} under {jobs}")
    containers = ((found[0].get("spec") or {}).get("containers")) or []
    if len(containers) != 1 or not isinstance(containers[0], dict):
        raise ValueError(f"{UPLOAD_JOB} has {len(containers)} containers")
    return checked_container(containers[0], UPLOAD_JOB)


def printing_args(args):
    """The upload job's arguments, made to print the config as YAML instead of writing it."""
    out, i = [], 0
    while i < len(args):
        name, eq, _ = args[i].lstrip("-").partition("=")
        if args[i].startswith("-") and name in ("output", "print-text", "output-yaml", "oneshot"):
            i += 2 if name == "output" and not eq else 1  # (--output X: the path is the next argument)
            continue
        out.append(args[i])
        i += 1
    return out + ["--oneshot", "--print-text", "--output-yaml"]


def merge(planned, extracts, failed):
    """What the fetch reads (see load) from plan()'s output, the configs extract wrote, and
    the configs that could not be built ({recipe: why}). A run's changes to its tabs' test
    groups are worked out where both its config and the one of the run before were built:
    a test group of its tabs is "new" when the config before had no test group of that
    name (TestGrid then reads all its builds), "changed" when it had one that differs in
    any field (TestGrid then reads its builds of the last 7 days again)."""
    built = {e["recipe"]: e for e in extracts}
    changes = {}
    for c in planned["compare"]:
        mine, before = built.get(c["recipe"]), built.get(c["prev"]) if c["prev"] else None
        if mine is None or (c["prev"] and before is None):
            continue
        out = {}
        for tabs in mine["dashboards"].values():
            for e in tabs.values():
                g = e["test_group"]
                if e.get("group") is None or before is None:
                    continue
                if g not in before["groups"]:
                    out[g] = "new"
                elif before["groups"][g] != e["group_digest"]:
                    out[g] = "changed"
        changes[c["build"]] = out
    runs = [{k: r[k] for k in ("build", "commit", "started", "finished", "recipe")} for r in planned["runs"]]
    configs = {rid: {k: v for k, v in e.items() if k not in ("recipe", "groups")} for rid, e in built.items()}
    return {"format": FORMAT, "after": planned["after"], "runs": runs, "configs": configs, "group_changes": changes,
            "failed": {rid: why for rid, why in failed.items() if rid not in built}}


def dashboard_name(text):
    """A dashboard a command line names. The tools key a tab as "dashboard#tab" and split that key
    at its first "#", so a dashboard name holding one could stand for a tab of another; TestGrid's
    names hold none (nameRegex in its config/config.go)."""
    if "#" in text:
        raise argparse.ArgumentTypeError(f"must hold no '#', got {text!r}")
    return text


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("uploads")
    u.add_argument("--state", help="the archive's state.json (what it knows already)")
    r = sub.add_parser("recipes")
    r.add_argument("uploads")
    j = sub.add_parser("job")
    j.add_argument("uploads")
    j.add_argument("recipe")
    j.add_argument("test_infra")
    e = sub.add_parser("extract")
    e.add_argument("config")
    e.add_argument("--recipe", required=True)
    e.add_argument("--commit", required=True)
    e.add_argument("--committed", type=int, required=True)
    e.add_argument("--image", required=True)
    e.add_argument("--digest", default="")
    e.add_argument("--dashboard", action="append", dest="dashboards", type=dashboard_name)
    m = sub.add_parser("merge")
    m.add_argument("uploads")
    m.add_argument("--failed", help="RECIPE<tab>WHY per line: the configs that could not be built")
    m.add_argument("extracts", nargs="*")
    args = p.parse_args(argv)
    if args.cmd == "uploads":
        state = {}
        # (a state.json that is a link is not read, as the fetch does not read it)
        if args.state and os.path.exists(args.state) and not os.path.islink(args.state):
            with open(args.state, encoding="utf-8") as f:
                state = json.load(f)
        json.dump(plan(state, time.time()), sys.stdout, indent=1)
        sys.stdout.write("\n")
        return 0
    if args.cmd in ("recipes", "job", "merge"):
        with open(args.uploads, encoding="utf-8") as f:
            planned = json.load(f)
    if args.cmd == "recipes":
        for b in planned["build"]:
            print(b["recipe"], b["commit"])
        return 0
    if args.cmd == "job":
        job = next(b["job"] for b in planned["build"] if b["recipe"] == args.recipe)
        if job is not None and "error" in job:
            print(job["error"], file=sys.stderr)
            return 1
        image, command, job_args = (job["image"], job["command"], job["args"]) if job else upload_job(args.test_infra)
        sys.stdout.write("\0".join([image, *command, *printing_args(job_args)]) + "\0")
        return 0
    if args.cmd == "merge":
        extracts = []
        for path in args.extracts:
            with open(path, encoding="utf-8") as f:
                extracts.append(json.load(f))
        failed = {}
        if args.failed and os.path.exists(args.failed):
            with open(args.failed, encoding="utf-8") as f:
                for line in f:
                    rid, _, why = line.rstrip("\n").partition("\t")
                    if RECIPE_RE.fullmatch(rid):
                        failed[rid] = why[:500]
        json.dump(merge(planned, extracts, failed), sys.stdout, indent=1, sort_keys=True)
        sys.stdout.write("\n")
        return 0
    import yaml

    with open(args.config, encoding="utf-8") as f:
        config = yaml.load(f, Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader))
    config = config if isinstance(config, dict) else {}
    dashboards = args.dashboards or DASHBOARDS
    tabs = extract(config, dashboards)
    missing = [d for d in dashboards if d not in tabs]
    if missing:
        print(f"ERROR the config has no dashboard {', '.join(missing)}", file=sys.stderr)
        return 1
    bad = [f"{d}#{t}" for d, entries in tabs.items() for t, e in entries.items() if not valid_entry(e)]
    if bad:  # (a field of a type the configurator does not print: load() would not take it)
        print(f"ERROR the config's tabs {', '.join(map(str, bad[:3]))} are not as TestGrid's config defines them",
              file=sys.stderr)
        return 1
    groups = {g["name"]: group_digest(g) for g in config.get("test_groups") or []
              if isinstance(g, dict) and isinstance(g.get("name"), str)}
    built = {"recipe": args.recipe, "commit": args.commit, "committed": args.committed, "image": args.image,
             "dashboards": tabs, "groups": groups}
    if args.digest:
        built["digest"] = args.digest
    json.dump(built, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
