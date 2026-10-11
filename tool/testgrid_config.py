#!/usr/bin/env python3
"""What TestGrid's config says about the archive's tabs, built as testgrid.k8s.io's is.

testgrid.k8s.io's config is built by test-infra's configurator from the TestGrid
YAML in kubernetes/test-infra's config/testgrids and the testgrid-* annotations of
the Prow jobs in config/jobs: the postsubmit job post-test-infra-upload-testgrid-config
runs it on every change to those. testgrid_config.sh runs the image that job runs,
with the job's own arguments, on the tip of test-infra, and `extract` keeps each tab
of the archive's dashboards with its base options and its test group.

TabRule applies a tab's entry to builds the fetch judges from GCS (testgrid_rule.py):
- the rows the tab shows: those its base options' include-filter-by-regex and
  exclude-filter-by-regex keep, as TestGrid's tabulator filters them (Go's
  url.ParseQuery, then regexp.MatchString on each row's name);
- the days of results its test group reads;
- .problem: why the port cannot judge the tab's builds as TestGrid does (a test
  group option it does not follow, or base options it cannot apply), else None.

Usage:
  testgrid_config.py job TEST_INFRA
      the image the upload job runs, its command, then its arguments made to print
      the config as YAML instead of uploading it, NUL-separated
  testgrid_config.py extract CONFIG.yaml --commit SHA --committed UNIX --image IMAGE [--dashboard D ...]
      the archive's tabs from the configurator's YAML, as JSON for fetch_red_logs.py --testgrid-config
"""
import argparse
import json
import os
import re
import sys
import threading
import time

DASHBOARDS = ["sig-release-master-blocking", "sig-release-master-informing"]  # (fetch_red_logs.DASHBOARDS)
UPLOAD_JOB = "post-test-infra-upload-testgrid-config"
# What the upload job's container may name (testgrid_config.sh passes them to docker run: an
# image or command that starts with "-" would be taken as docker's own options)
IMAGE_RE = re.compile(r"[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)+(?::[A-Za-z0-9_][A-Za-z0-9._-]{0,127})?"
                      r"(?:@sha256:[0-9a-f]{64})?")
COMMAND_RE = re.compile(r"[A-Za-z0-9_./][A-Za-z0-9_./-]*")
FORMAT = 1  # of the JSON extract writes

# The TestGroup fields TestGrid's updater (pkg/updater at the commit testgrid_rule.py
# ports) reads that decide a column's rows or their results, as extract keeps them,
# with their types. Any of them set otherwise than the port assumes is a TabRule.group_problem.
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

# Go's regexp (RE2) never backtracks: matching takes time linear in the text, whatever
# the pattern. Python's re backtracks (".*.*.*x" takes 48 s on a 1000-character name),
# so filters are matched here as Go matches them: the pattern parsed by Go's rules,
# compiled to a Thompson NFA, and run on the name in one pass.

_PERL = {"d": ((0x30, 0x39),), "s": ((0x09, 0x0A), (0x0C, 0x0D), (0x20, 0x20)),
         "w": ((0x30, 0x39), (0x41, 0x5A), (0x5F, 0x5F), (0x61, 0x7A))}  # ASCII only, as in Go
_C_ESCAPES = {"a": "\a", "f": "\f", "t": "\t", "n": "\n", "r": "\r", "v": "\v"}
_REPEAT = re.compile(r"\{(0|[1-9][0-9]*)(?:(,)(0|[1-9][0-9]*)?)?\}")  # parseRepeat (no leading zeros)
_HEX_ESCAPE = re.compile(r"x(?:([0-9A-Fa-f]{2})|\{([0-9A-Fa-f]{1,8})\})")
# Go rejects a pattern whose parse tree is over 1000 high or whose program is too
# large; these limits stay far inside those (real filters are under 100 characters).
MAX_PATTERN = 400
MAX_NESTING = 30
MAX_REPEAT = 100  # (Go's own size estimate grows with nested counted repetitions: at 1000, 166 characters
                  # can be too large for it; at 100 nothing of 400 is)
MAX_STATES = 5000
CACHE_ELEMENTS = 2_000_000  # states held in a regex's cached steps ...
CACHE_ENTRIES = 100_000  # ... and steps, before the cache is emptied
BEGIN, END, WORD_BOUNDARY, NOT_WORD_BOUNDARY = "begin", "end", "word", "not-word"


def _is_word(c):
    return c is not None and c < "\x80" and (c.isalnum() or c == "_")


def _negate(ranges):
    out, lo = [], 0
    for a, b in sorted(ranges):
        if a > lo:
            out.append((lo, a - 1))
        lo = max(lo, b + 1)
    if lo <= 0x10FFFF:
        out.append((lo, 0x10FFFF))
    return tuple(out)


def _escape(p, i):
    """parseEscape at p[i] == "\\": (the character it stands for, the index after it)."""
    if i + 1 >= len(p):
        raise Unsupported("trailing backslash")
    c = p[i + 1]
    if c.isascii() and not c.isalnum():
        return c, i + 2  # an escaped punctuation character (or space, or control) is itself
    if c in _C_ESCAPES:
        return _C_ESCAPES[c], i + 2
    m = _HEX_ESCAPE.match(p, i + 1) if c == "x" else None
    if m:
        code = int(m.group(1) or m.group(2), 16)
        if code <= 0x10FFFF and not 0xD800 <= code <= 0xDFFF:
            return chr(code), m.end()
    raise Unsupported(f"escape {p[i:i + 2]!r}")


class _Parser:
    r"""Go's regexp/syntax parser (Perl flags), for the syntax tab filters use: literals,
    escapes, ., ^, $, |, groups, (?:...), bracketed classes, repetitions, and Perl's
    \d \s \w \b \A \z. Anything else, and what Go rejects, raises Unsupported. Nodes:
    ("set", ranges), ("cat", [nodes]), ("alt", [nodes]), ("rep", node, min, max or
    None), ("assert", kind)."""

    def __init__(self, pattern):
        self.p, self.i, self.depth = pattern, 0, 0

    def parse(self):
        if len(self.p) > MAX_PATTERN:
            raise Unsupported(f"longer than {MAX_PATTERN} characters")
        node, _ = self.alternation()
        if self.i < len(self.p):
            raise Unsupported("unexpected )")
        return node

    def alternation(self):
        """(node, whether it holds a counted repetition)."""
        branches, counted = [], False
        while True:
            node, c = self.concatenation()
            branches.append(node)
            counted |= c
            if self.p.startswith("|", self.i):
                self.i += 1
                continue
            return (branches[0] if len(branches) == 1 else ("alt", branches)), counted

    def concatenation(self):
        items, counted, last = [], False, None  # last: "atom", "group", "repeat", "anchor", or None
        counted_in_group = False
        while self.i < len(self.p) and self.p[self.i] not in "|)":
            c = self.p[self.i]
            m = _REPEAT.match(self.p, self.i) if c == "{" else None
            if c in "*+?" or m:
                if last == "repeat":
                    raise Unsupported("nested repetition operator")
                if last not in ("atom", "group"):
                    raise Unsupported("repetition of an anchor" if last == "anchor"
                                      else "missing argument to repetition operator")
                if m:
                    lo, hi = m.group(1), m.group(3) if m.group(2) else m.group(1)
                    if len(lo) > 4 or int(lo) > MAX_REPEAT or (hi is not None and (len(hi) > 4 or int(hi) > MAX_REPEAT)) \
                            or (hi is not None and int(lo) > int(hi)):
                        raise Unsupported("invalid repeat count")
                    lo, hi = int(lo), None if hi is None else int(hi)
                    if (lo >= 2 or (hi or 0) >= 2) and last == "group" and counted_in_group:
                        raise Unsupported("counted repetition of a counted repetition")  # (Go's repeatIsValid)
                    counted = True
                    self.i = m.end()
                else:
                    lo, hi = {"*": (0, None), "+": (1, None), "?": (0, 1)}[c]
                    self.i += 1
                if self.p.startswith("?", self.i):  # non-greedy: matches the same strings
                    self.i += 1
                items[-1] = ("rep", items[-1], lo, hi)
                last = "repeat"
                continue
            if c == "(":
                if self.p.startswith("(?", self.i) and not self.p.startswith("(?:", self.i):
                    raise Unsupported("a group with flags or a name")
                self.depth += 1
                if self.depth > MAX_NESTING:
                    raise Unsupported("groups nested too deeply")
                self.i += 3 if self.p.startswith("(?:", self.i) else 1
                node, counted_in_group = self.alternation()
                if not self.p.startswith(")", self.i):
                    raise Unsupported("missing closing )")
                self.i += 1
                self.depth -= 1
                counted |= counted_in_group
                items.append(node)
                last = "group"
                continue
            last = "atom"
            if c in "^$":
                items.append(("assert", BEGIN if c == "^" else END))  # one line: the text's start and end only
                self.i += 1
                last = "anchor"
            elif c == ".":
                items.append(("set", _negate(((0x0A, 0x0A),))))  # any character but a newline
                self.i += 1
            elif c == "[":
                items.append(self.bracket())
            elif c == "\\" and self.i + 1 < len(self.p) and self.p[self.i + 1] in "AzbB":
                items.append(("assert", {"A": BEGIN, "z": END, "b": WORD_BOUNDARY, "B": NOT_WORD_BOUNDARY}[
                    self.p[self.i + 1]]))
                self.i += 2
                last = "anchor"
            elif c == "\\" and self.i + 1 < len(self.p) and self.p[self.i + 1].lower() in _PERL:
                e = self.p[self.i + 1]
                ranges = _PERL[e.lower()]
                items.append(("set", _negate(ranges) if e.isupper() else ranges))
                self.i += 2
            elif c == "\\" and self.i + 1 < len(self.p) and self.p[self.i + 1] in "pPQC":
                raise Unsupported(f"escape {self.p[self.i:self.i + 2]!r}")
            else:
                if c == "\\":
                    ch, self.i = _escape(self.p, self.i)
                else:
                    ch, self.i = c, self.i + 1  # (a "{" that starts no repetition is itself, as in Go)
                items.append(("set", ((ord(ch), ord(ch)),)))
        if self.i < len(self.p) and self.p[self.i] == ")" and self.depth == 0:
            raise Unsupported("unexpected )")
        return (items[0] if len(items) == 1 else ("cat", items)), counted

    def bracket(self):
        p, j = self.p, self.i + 1
        negate = p.startswith("^", j)
        j += negate
        ranges, first = [], True
        while True:
            if j >= len(p):
                raise Unsupported("missing closing ]")
            if p[j] == "]" and not first:
                break
            first = False
            if p.startswith("[:", j):
                raise Unsupported("a POSIX class [:name:]")
            if p[j] == "\\" and j + 1 < len(p):
                c = p[j + 1]
                if c in _PERL:
                    ranges += _PERL[c]
                    j += 2
                    continue
                if c in "DSWpP":
                    raise Unsupported(f"{p[j:j + 2]!r} in a class")
            lo, j = _escape(p, j) if p[j] == "\\" else (p[j], j + 1)
            hi = lo
            if j + 1 < len(p) and p[j] == "-" and p[j + 1] != "]":  # [a-] is a or -
                hi, j = _escape(p, j + 1) if p[j + 1] == "\\" else (p[j + 1], j + 2)
                if hi < lo:
                    raise Unsupported("invalid character class range")
            ranges.append((ord(lo), ord(hi)))
        self.i = j + 1
        return ("set", _negate(ranges) if negate else tuple(sorted(ranges)))


class GoRegexp:
    """A Go regexp as a Thompson NFA, matched like Go's MatchString: anywhere in the
    text, in one pass over it (a state set per position; the steps are cached, so a
    pattern costs a dictionary lookup per character once warm)."""

    def __init__(self, pattern):
        self.pattern = pattern
        self.states = []  # ("set", ranges, next) | ("split", a, b) | ("assert", kind, next) | ("match",)
        match = self._add(("match",))
        self.start = self._compile(_Parser(pattern).parse(), match)
        self.cache, self.cached = {}, 0

    def _add(self, state):
        if len(self.states) >= MAX_STATES:
            raise Unsupported("too large")
        self.states.append(state)
        return len(self.states) - 1

    def _compile(self, node, nxt):
        kind = node[0]
        if kind == "set":
            return self._add(("set", node[1], nxt))
        if kind == "assert":
            return self._add(("assert", node[1], nxt))
        if kind == "cat":
            for child in reversed(node[1]):
                nxt = self._compile(child, nxt)
            return nxt
        if kind == "alt":
            starts = [self._compile(child, nxt) for child in node[1]]
            out = starts[-1]
            for s in reversed(starts[:-1]):
                out = self._add(("split", s, out))
            return out
        _, sub, lo, hi = node
        if hi is None:  # x* and x+ (and x{n,}): one copy of x in a loop, entered before x or before its split
            loop = self._add(("split", None, nxt))
            body = self._compile(sub, loop)
            self.states[loop] = ("split", body, nxt)
            tail = loop
            if lo:
                tail, lo = body, lo - 1
        else:
            tail = nxt
            for _ in range(hi - lo):
                tail = self._add(("split", self._compile(sub, tail), nxt))
        for _ in range(lo):
            tail = self._compile(sub, tail)
        return tail

    def _step(self, current, at_start, prev_word, c):
        """From the states reached before position i (and the start: a match may begin
        anywhere): (whether one matches at i, the states reached after c)."""
        next_word = _is_word(c)
        seen, stack, reached = set(), [self.start, *current], []
        while stack:
            s = stack.pop()
            if s in seen:
                continue
            seen.add(s)
            st = self.states[s]
            op = st[0]
            if op == "match":
                return True, frozenset()
            if op == "split":
                stack += (st[1], st[2])
            elif op == "assert":
                kind = st[1]
                if (kind == BEGIN and at_start) or (kind == END and c is None) or \
                        (kind == WORD_BOUNDARY and prev_word != next_word) or \
                        (kind == NOT_WORD_BOUNDARY and prev_word == next_word):
                    stack.append(st[2])
            else:
                reached.append(st)
        if c is None:
            return False, frozenset()
        code = ord(c)
        return False, frozenset(st[2] for st in reached if any(lo <= code <= hi for lo, hi in st[1]))

    def search(self, text, budget=None):
        """Whether the pattern matches anywhere in text (Go's MatchString). With a
        Budget, the time it takes is spent from it (FilterTooSlow when it runs out)."""
        start = time.thread_time()
        current, prev_word = frozenset(), False
        n = len(text)
        found = False
        for i in range(n + 1):
            if budget is not None and i & 63 == 63:
                budget.check(time.thread_time() - start)
            c = text[i] if i < n else None
            key = (current, i == 0, prev_word, c)
            hit = self.cache.get(key)
            if hit is None:
                hit = self._step(current, i == 0, prev_word, c)
                if self.cached > CACHE_ELEMENTS or len(self.cache) >= CACHE_ENTRIES:
                    self.cache, self.cached = {}, 0
                self.cache[key] = hit
                self.cached += len(current) + len(hit[1]) + 1
            if hit[0]:
                found = True
                break
            current, prev_word = hit[1], _is_word(c)
        if budget is not None:
            budget.spend(time.thread_time() - start)
        return found


def go_regexp(pattern):
    """regexp.MustCompile(pattern) for MatchString, as GoRegexp; Unsupported for syntax
    not followed here, and for what Go rejects."""
    return GoRegexp(pattern)


# ------------------------------------------------------------ the tab's rule

def group_problems(group):
    """The test group options set otherwise than testgrid_rule.py assumes (a group as
    valid_entry accepts it)."""
    out = []
    if "," in (group.get("gcs_prefix") or ""):
        out.append("several GCS prefixes")
    if group.get("result_source"):
        out.append("result_source")
    if not group.get("use_kubernetes_client"):
        out.append("use_kubernetes_client unset (TestGrid's updater skips the group)")
    out += [name for name in ("disable_merged_status", "disable_prowjob_analysis", "ignore_skip", "ignore_old_results",
                              "test_name_config", "build_override_strftime", "build_override_configuration_value")
            if group.get(name)]
    if (group.get("custom_evaluator_rule_set") or {}).get("rules"):
        out.append("custom_evaluator_rule_set")
    return out


def tab_filters(base_options):
    """The include and exclude regexes TestGrid's tabulator filters a tab's rows by."""
    values, err = go_parse_query(base_options)
    if err:
        raise Unsupported(f"TestGrid cannot parse them: {err}")
    keys = {k.decode("utf-8", "replace"): v for k, v in values.items()}
    unknown = sorted(set(keys) - {INCLUDE, EXCLUDE} - DRAWING_OPTIONS)
    if unknown:
        raise Unsupported(f"options not known here: {', '.join(unknown)}")
    compiled = {}
    for name in (INCLUDE, EXCLUDE):
        compiled[name] = []
        for raw in keys.get(name, []):
            try:
                pattern = raw.decode("utf-8")
            except UnicodeDecodeError:
                raise Unsupported(f"{name}: invalid UTF-8") from None
            try:
                compiled[name].append(go_regexp(pattern))
            except Unsupported as e:
                raise Unsupported(f"{name}={pattern!r}: {e}") from None
    return compiled[INCLUDE], compiled[EXCLUDE]


class TabRule:
    """How a tab shows a build's column, from its entry in the config (one valid_entry
    accepts): .shows(row), .days (of results its test group reads), .prefix (the GCS
    prefix it reads builds from); .group_problem: why the port cannot paint its
    columns as TestGrid does (a test group option it does not follow), and
    .filter_problem: why it cannot apply the tab's filters (shows() then raises
    Unsupported), else None."""

    def __init__(self, entry):
        self.entry, self.budget, self.known = entry, Budget(), {}
        group = entry.get("group")
        problems = []
        if entry.get("duplicate"):
            problems.append("the dashboard has more than one tab of this name")
        if group is None:
            problems.append(f"its test group {entry.get('test_group')!r} is not in the config")
            group = {}
        problems += [f"its test group sets {p}, which testgrid_rule.py does not follow" for p in group_problems(group)]
        self.group_problem = "; ".join(problems) or None
        days = group.get("days_of_results") or 0
        self.days = days if days > 0 else DEFAULT_DAYS
        self.prefix = group.get("gcs_prefix")
        self.includes, self.excludes, self.filter_problem = [], [], None
        try:
            self.includes, self.excludes = tab_filters(entry.get("base_options") or "")
        except Unsupported as e:
            self.filter_problem = f"its base options {entry.get('base_options')!r}: {e}"

    @property
    def problem(self):
        return "; ".join(p for p in (self.group_problem, self.filter_problem) if p) or None

    def shows(self, row):
        """Whether the tab shows the row; Unsupported (FilterTooSlow when its filters
        run out of time, which then stays the filter_problem) if that is not known."""
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
            if len(self.known) >= CACHE_ENTRIES:
                self.known.clear()
            self.known[row] = shown
        return shown


def valid_entry(entry):
    """Whether `entry` is a tab's entry as extract writes it, every field of the type
    the config has (TabRule takes nothing else)."""
    if not isinstance(entry, dict) or not set(entry) <= {"test_group", "base_options", "group", "duplicate"}:
        return False
    if not isinstance(entry.get("test_group"), str) or not isinstance(entry.get("base_options", ""), str):
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
    """The config extract wrote: ({"dashboard#tab": entry}, its source). Raises ValueError
    if it is not one."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or data.get("format") != FORMAT or not isinstance(data.get("dashboards"), dict):
        raise ValueError("not a config written by testgrid_config.py extract")
    source = data.get("source")
    if not isinstance(source, dict) or not all(isinstance(source.get(k), str) and source[k]
                                               for k in ("test_infra", "image")) \
            or not isinstance(source.get("committed"), int) or isinstance(source.get("committed"), bool) \
            or not 0 < source["committed"] < 1 << 40:
        raise ValueError("its source (test-infra commit, configurator image) is missing")
    tabs = {}
    for dashboard, entries in data["dashboards"].items():
        if not isinstance(entries, dict):
            raise ValueError(f"{dashboard}: not an object")
        for tab, entry in entries.items():
            if not valid_entry(entry):
                raise ValueError(f"{dashboard}#{tab}: malformed entry")
            tabs[f"{dashboard}#{tab}"] = entry
    return tabs, {k: source[k] for k in ("test_infra", "committed", "image", "digest") if k in source}


# ------------------------------------------------------------ building it

def extract(config, dashboards):
    """{dashboard: {tab: entry}} from the configurator's config (as parsed YAML)."""
    groups = {}
    for g in config.get("test_groups") or []:
        groups.setdefault(g.get("name"), g)  # (the configurator rejects duplicate names)
    out = {}
    for d in config.get("dashboards") or []:
        if d.get("name") not in dashboards:
            continue
        tabs = out.setdefault(d["name"], {})
        for t in d.get("dashboard_tab") or []:
            name = t.get("name")
            g = groups.get(t.get("test_group_name"))
            entry = {"test_group": t.get("test_group_name") or "", "base_options": t.get("base_options") or "",
                     "group": None if g is None else {k: g[k] for k in GROUP_FIELDS if k in g}}
            if name in tabs:
                entry["duplicate"] = True
            tabs[name] = entry
    return out


def upload_job(test_infra):
    """The container of test-infra's job that uploads TestGrid's config: (image, command, args)."""
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
    if len(containers) != 1:
        raise ValueError(f"{UPLOAD_JOB} has {len(containers)} containers")
    c = containers[0]
    image, command, args = c.get("image"), c.get("command") or [], c.get("args") or []
    if not isinstance(image, str) or not image or not isinstance(command, list) or not command \
            or not isinstance(args, list) or not all(isinstance(a, str) for a in command + args):
        raise ValueError(f"{UPLOAD_JOB}: no image, command or arguments")
    if not IMAGE_RE.fullmatch(image) or not COMMAND_RE.fullmatch(command[0]) \
            or any("\0" in a or "\n" in a for a in command + args):
        raise ValueError(f"{UPLOAD_JOB}: an image or command this does not run: {image!r} {command[0]!r}")
    return image, command, args


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


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("job")
    j.add_argument("test_infra")
    e = sub.add_parser("extract")
    e.add_argument("config")
    e.add_argument("--commit", required=True)
    e.add_argument("--committed", type=int, required=True)
    e.add_argument("--image", required=True)
    e.add_argument("--digest", default="")
    e.add_argument("--dashboard", action="append", dest="dashboards")
    args = p.parse_args(argv)
    if args.cmd == "job":
        image, command, job_args = upload_job(args.test_infra)
        sys.stdout.write("\0".join([image, *command, *printing_args(job_args)]) + "\0")
        return 0
    import yaml

    with open(args.config, encoding="utf-8") as f:
        config = yaml.load(f, Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader))
    dashboards = args.dashboards or DASHBOARDS
    tabs = extract(config if isinstance(config, dict) else {}, dashboards)
    missing = [d for d in dashboards if d not in tabs]
    if missing:
        print(f"ERROR the config has no dashboard {', '.join(missing)}", file=sys.stderr)
        return 1
    source = {"test_infra": args.commit, "committed": args.committed, "image": args.image}
    if args.digest:
        source["digest"] = args.digest
    json.dump({"format": FORMAT, "source": source, "dashboards": tabs}, sys.stdout, indent=1, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
