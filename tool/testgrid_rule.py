# Ported in part from TestGrid (f15bfa4808a4; Copyright The TestGrid Authors and The Kubernetes Authors)
# and k8s.io/apimachinery (v0.27.4; Copyright The Kubernetes Authors), both Apache License 2.0 (LICENSE),
# and from Go (1.26.5; Copyright The Go Authors), BSD 3-Clause license in third_party/go/LICENSE. It uses
# the GCS client's error text for a missing object (cloud.google.com/go/storage v1.31.0; Copyright Google LLC),
# Apache License 2.0 (LICENSE), and Unicode's White_Space characters (15.0.0; Copyright Unicode, Inc.), Unicode
# License in third_party/unicode/LICENSE. THIRD_PARTY_NOTICES.md lists what was ported and what changed.
"""TestGrid's own rule for the cells of one build's column, ported from its Go source.

The fetch takes red cells straight from TestGrid's tables. A build that scrolled
off a tab before the archive saw it (a gap between runs) can only be judged
from GCS, and is judged here by the rule TestGrid's updater applied when it
read the build: readResult and convertResult in pkg/updater at the commit
TestGrid's production manifests deploy (tool/oracle/pinned.json names it, and the Go it is
built with), with the Go behaviour that rule rests on: encoding/json (the
Decoder TestGrid reads started.json, finished.json and podinfo.json with),
encoding/xml (junit), time.Parse and strconv, and the k8s.io/api and
k8s.io/apimachinery types in podinfo.json (testgrid_types.json). tool/oracle
builds TestGrid's own code at that commit to check this port against;
test_testgrid_rule.py replays what it answered.

The column is the one TestGrid paints with its test group's cell options
(Options: rows of the same name merged or split, the Pod row shown or not, the
custom evaluator rules, the test name config; testgrid_config.py reads the
config TestGrid builds from test-infra); by default the defaults, which every
tab of the archive's dashboards has. Which of the column's rows a tab shows is
the caller's (its base options, see testgrid_config.TabRule).
What GCS cannot tell: when TestGrid read the build (fetch_red_logs.gcs_column
works out when it may have), and errors it hit reading GCS itself.
When readResult hits more than one error, whichever its goroutines report first
decides the column; every outcome possible is kept, and a build counts as red
when any of them is.
"""
import json
import math
import os
import re

import gofmt

# TestGrid's TestStatus names, and their severity (internal/result)
SEVERITY = {"NO_RESULT": 0, "BUILD_PASSED": 1, "PASS": 2, "PASS_WITH_SKIPS": 3, "PASS_WITH_ERRORS": 4,
            "RUNNING": 5, "CATEGORIZED_ABORT": 6, "UNKNOWN": 7, "CANCEL": 8, "BLOCKED": 9, "FLAKY": 10,
            "TOOL_FAIL": 11, "TIMED_OUT": 12, "CATEGORIZED_FAIL": 13, "BUILD_FAIL": 14, "FAIL": 15}
RED = {"FAIL", "TIMED_OUT", "CATEGORIZED_FAIL", "TOOL_FAIL"}  # painted #a00 by TestGrid's UI
DAYS_OF_RESULTS = 15  # test-infra config/testgrids/default.yaml
JUNIT_MAX_SIZE = 100_000_000  # util/gcs maxSize: a larger junit file is malformed
MISSING_POD_INFO = "podinfo.json not found in job artifacts (has not uploaded, or Prow's GCS reporter is not enabled)."
NO_POD_UTILS = "Decoration is not enabled; set `decorate: true` on Prowjob."


# ------------------------------------------------------------ Go strings and numbers

REPLACEMENT = chr(0xFFFD)  # utf8.RuneError

# One rune as utf8.DecodeRune reads it (anything else is RuneError for one byte).
_UTF8_RUNE = re.compile(
    rb"[\x00-\x7f]|[\xc2-\xdf][\x80-\xbf]|\xe0[\xa0-\xbf][\x80-\xbf]|[\xe1-\xec\xee\xef][\x80-\xbf]{2}"
    rb"|\xed[\x80-\x9f][\x80-\xbf]|\xf0[\x90-\xbf][\x80-\xbf]{2}|[\xf1-\xf3][\x80-\xbf]{3}"
    rb"|\xf4[\x80-\x8f][\x80-\xbf]{2}")


def go_utf8(b):
    """Bytes read as Go reads a string rune by rune: each byte of a bad sequence is U+FFFD."""
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        pass
    out, i, n = [], 0, len(b)
    while i < n:
        m = _UTF8_RUNE.match(b, i)
        if m:
            out.append(m.group().decode("utf-8"))
            i = m.end()
        else:
            out.append(REPLACEMENT)
            i += 1
    return "".join(out)


# unicode.IsSpace, as strings.TrimSpace uses it
GO_SPACES = "".join(map(chr, (0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x20, 0x85, 0xa0, 0x1680, *range(0x2000, 0x200b),
                                0x2028, 0x2029, 0x202f, 0x205f, 0x3000)))
_SPACE_SEQS = sorted((c.encode() for c in GO_SPACES), key=len, reverse=True)


def go_trim_space(b):
    """strings.TrimSpace on bytes (a bad UTF-8 sequence is not space)."""
    start, end = 0, len(b)
    while start < end:
        for sp in _SPACE_SEQS:
            if b.startswith(sp, start, end):
                start += len(sp)
                break
        else:
            break
    while end > start:
        for sp in _SPACE_SEQS:
            if b.endswith(sp, start, end):
                end -= len(sp)
                break
        else:
            break
    return b[start:end]


def _is_digit(b, i):
    return i < len(b) and 0x30 <= b[i] <= 0x39


def go_parse_int(s, bits=64):
    """strconv.ParseInt(s, 10, bits) on bytes: the value, or None for an error."""
    if not s:
        return None
    neg = s[:1] == b"-"
    if s[:1] in (b"+", b"-"):
        s = s[1:]
    if not s or not all(0x30 <= c <= 0x39 for c in s):
        return None  # ParseUint with base 10: digits only, no sign, no underscores
    n = int(s)
    cutoff = 1 << (bits - 1)
    if n > (1 << 64) - 1 or (not neg and n >= cutoff) or (neg and n > cutoff):
        return None
    return -n if neg else n


def _underscore_ok(s):
    """strconv's underscoreOK: underscores only between digits (or after a base prefix)."""
    saw, i = "^", 0
    if s[:1] in (b"+", b"-"):
        s = s[1:]
    hexa = False
    if len(s) >= 2 and s[0] == 0x30 and (s[1] | 0x20) in (ord("b"), ord("o"), ord("x")):
        i, saw, hexa = 2, "0", (s[1] | 0x20) == ord("x")
    while i < len(s):
        c = s[i]
        if 0x30 <= c <= 0x39 or hexa and ord("a") <= (c | 0x20) <= ord("f"):
            saw = "0"
        elif c == 0x5f:
            if saw != "0":
                return False
            saw = "_"
        else:
            if saw == "_":
                return False
            saw = "!"
        i += 1
    return saw != "_"


def go_parse_float_ok(s):
    """Whether strconv.ParseFloat(s, 64) accepts bytes s: its syntax, and no overflow to +-Inf."""
    if not s:
        return False
    # special(): [+-]inf[inity] and nan (only whole, as ParseFloat needs all of s)
    lower = s.lower()
    if s[0] in b"+-iI":
        t = lower[1:] if s[0] in b"+-" else lower
        k = 0
        while k < min(len(t), 8) and t[k] == b"infinity"[k]:
            k += 1
        if 3 < k < 8:
            k = 3
        if k in (3, 8):
            return (len(s) - len(t)) + k == len(s)
    elif s[0] in b"nN" and lower[:3] == b"nan":
        return len(s) == 3
    # readFloat()
    i, n = 0, len(s)
    if i < n and s[i] in b"+-":
        i += 1
    base, exp_char = 10, ord("e")
    if i + 2 < n and s[i] == 0x30 and (s[i + 1] | 0x20) == ord("x"):
        base, exp_char = 16, ord("p")
        i += 2
    sawdot = sawdigits = underscores = False
    while i < n:
        c = s[i]
        if c == 0x5f:
            underscores = True
        elif c == 0x2e:
            if sawdot:
                break
            sawdot = True
        elif 0x30 <= c <= 0x39 or base == 16 and ord("a") <= (c | 0x20) <= ord("f"):
            sawdigits = True
        else:
            break
        i += 1
    if not sawdigits:
        return False
    if i < n and (s[i] | 0x20) == exp_char:
        i += 1
        if i >= n:
            return False
        if s[i] in b"+-":
            i += 1
        if i >= n or not 0x30 <= s[i] <= 0x39:
            return False
        while i < n and (0x30 <= s[i] <= 0x39 or s[i] == 0x5f):
            underscores |= s[i] == 0x5f
            i += 1
    elif base == 16:
        return False  # a hex float must have an exponent
    if underscores and not _underscore_ok(s[:i]):
        return False
    if i != n:
        return False  # ParseFloat takes all of s
    text = s[:i].replace(b"_", b"").decode("ascii")
    try:
        value = float.fromhex(text) if base == 16 else float(text)
    except OverflowError:
        return False
    return value not in (float("inf"), float("-inf"))


def _wrap(n, bits):
    return (n + (1 << (bits - 1))) % (1 << bits) - (1 << (bits - 1))


# ------------------------------------------------------------ time.Parse(time.RFC3339, s)

def _days_in(month, year):
    if month == 2:
        leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
        return 29 if leap else 28
    return 30 + ((month + (month >> 3)) & 1)


def _rfc3339_fast(s):
    """time's parseRFC3339 (the strict fast path Parse tries first)."""
    if len(s) < 19:
        return False
    ok = True

    def num(part, lo, hi):
        nonlocal ok
        x = 0
        for c in part:
            if not 0x30 <= c <= 0x39:
                ok = False
                return lo
            x = x * 10 + c - 0x30
        if x < lo or x > hi:
            ok = False
            return lo
        return x
    year = num(s[0:4], 0, 9999)
    month = num(s[5:7], 1, 12)
    num(s[8:10], 1, _days_in(month, year))
    num(s[11:13], 0, 23)
    num(s[14:16], 0, 59)
    num(s[17:19], 0, 59)
    if not ok or s[4] != 0x2d or s[7] != 0x2d or s[10] != 0x54 or s[13] != 0x3a or s[16] != 0x3a:
        return False
    s = s[19:]
    if len(s) >= 2 and s[0] == 0x2e and _is_digit(s, 1):
        k = 2
        while _is_digit(s, k):
            k += 1
        s = s[k:]
    if not (len(s) == 1 and s[0] == 0x5a):
        if len(s) != 6:
            return False
        num(s[1:3], 0, 23)
        num(s[4:6], 0, 59)
        if not ok or s[0] not in b"+-" or s[3] != 0x3a:
            return False
    return True


def _getnum(v, fixed):
    if not _is_digit(v, 0):
        return None, v
    if not _is_digit(v, 1):
        return (None, v) if fixed else (v[0] - 0x30, v[1:])
    return (v[0] - 0x30) * 10 + v[1] - 0x30, v[2:]


def _rfc3339_layout(v):
    """time's general parse() with the layout "2006-01-02T15:04:05Z07:00"."""
    if len(v) < 4 or not _is_digit(v, 0) or not all(0x30 <= c <= 0x39 for c in v[:4]):
        return False
    year, v = int(v[:4]), v[4:]
    fields = {}
    for prefix, key, fixed, top in ((b"-", "month", True, 12), (b"-", "day", True, None), (b"T", "hour", False, 23),
                                    (b":", "min", True, 59), (b":", "sec", True, 59)):
        if not v.startswith(prefix):
            return False
        x, v = _getnum(v[len(prefix):], fixed)
        if x is None:
            return False
        if key == "month" and not 1 <= x <= 12 or top is not None and key != "month" and x > top:
            return False
        fields[key] = x
    if len(v) >= 2 and v[0] in b".," and _is_digit(v, 1):  # a fraction the layout does not have
        k = 2
        while _is_digit(v, k):
            k += 1
        v = v[k:]
    if v[:1] == b"Z":
        v = v[1:]
    else:
        if len(v) < 6 or v[3] != 0x3a:
            return False
        sign, hh, mm, v = v[0:1], v[1:3], v[4:6], v[6:]
        hr, _ = _getnum(hh, True)
        mi, _ = _getnum(mm, True)
        if hr is None or mi is None or hr > 24 or mi > 60 or sign not in (b"+", b"-"):
            return False
    if v:
        return False  # extra text
    return 1 <= fields["day"] <= _days_in(fields["month"], year)


def go_rfc3339_ok(text):
    """Whether time.Parse(time.RFC3339, text) succeeds (metav1.Time's UnmarshalJSON)."""
    b = text.encode("utf-8")
    return _rfc3339_fast(b) or _rfc3339_layout(b)


# ------------------------------------------------------------ resource.ParseQuantity

_QUANTITY_SUFFIX_CHARS = frozenset(b"eEinumkKMGTP")
_DEC_SUFFIXES = {b"": 0, b"n": -9, b"u": -6, b"m": -3, b"k": 3, b"M": 6, b"G": 9, b"T": 12, b"P": 15, b"E": 18}
_BIN_SUFFIXES = {b"Ki": 10, b"Mi": 20, b"Gi": 30, b"Ti": 40, b"Pi": 50, b"Ei": 60}


def _quantity_parts(s):
    """parseQuantityString: (value, num, denom, suffix), or None for ErrFormatWrong."""
    pos, end = 0, len(s)
    if s[0] in b"+-":
        pos += 1
    i = pos
    while True:  # leading zeros
        if i >= end:
            return b"0", b"0", b"", b""
        if s[i] != 0x30:
            break
        pos += 1
        i += 1
    i = pos
    while True:  # numerator
        if i >= end:
            return s, s[pos:], b"", b""
        if not 0x30 <= s[i] <= 0x39:
            num, pos = s[pos:i], i
            break
        i += 1
    num = num or b"0"
    denom = b""
    if pos < end and s[pos] == 0x2e:
        pos += 1
        i = pos
        while True:
            if i >= end:
                return s, num, s[pos:], b""
            if not 0x30 <= s[i] <= 0x39:
                denom, pos = s[pos:i], i
                break
            i += 1
    value, start = s[:pos], pos
    i = pos
    while True:
        if i >= end:
            return value, num, denom, s[start:]
        if s[i] not in _QUANTITY_SUFFIX_CHARS:
            pos = i
            break
        i += 1
    if pos < end and s[pos] in b"+-":
        pos += 1
    i = pos
    while True:
        if i >= end:
            return value, num, denom, s[start:]
        if not 0x30 <= s[i] <= 0x39:
            return None
        i += 1


def quantity_ok(s):
    """Whether resource.ParseQuantity accepts bytes s (already TrimSpace'd)."""
    if not s:
        return False
    if s == b"0":
        return True
    parts = _quantity_parts(s)
    if parts is None:
        return False
    value, num, denom, suffix = parts
    if suffix in _DEC_SUFFIXES:
        binary, exponent = False, _DEC_SUFFIXES[suffix]
    elif suffix in _BIN_SUFFIXES:
        binary, exponent = True, _BIN_SUFFIXES[suffix]
    elif len(suffix) > 1 and suffix[0] in b"eE":
        parsed = go_parse_int(suffix[1:], 64)
        if parsed is None:
            return False
        binary, exponent = False, _wrap(parsed, 32)
    else:
        return False
    mantissa = 1
    if binary:
        scale = 0
        if exponent >= 0 and not denom:
            mantissa = 1 << exponent
            precision = 15 - len(num) - exponent * 3 // 10 - 1  # exact for the binary exponents 10..60
        else:
            precision = -1
    else:
        scale, precision = exponent, 18 - (len(num) + len(denom))
    if precision >= 0:
        scale = _wrap(scale - len(denom), 32)
        if scale >= -9:
            v = int(num + denom)
            if v in (0, 1) or mantissa == 1:
                return True  # int64Multiply's trivial cases
            if _wrap(v * mantissa, 64) // mantissa == v and v * mantissa < 1 << 63:
                return True
    # the inf.Dec path: SetString fails only without a digit
    return any(0x30 <= c <= 0x39 for c in value)


# ------------------------------------------------------------ encoding/json: scanning

class JSONSyntaxError(Exception):
    pass


EOF = object()  # Decoder.Decode's io.EOF: nothing but whitespace
_WS = re.compile(rb"[ \t\r\n]*")
_STRING = re.compile(rb'"[^"\\\x00-\x1f]*(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})[^"\\\x00-\x1f]*)*"')
_NUMBER = re.compile(rb"-?(?:0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?")
MAX_JSON_DEPTH = 10000


def _json_literal(data, i):
    c = data[i]
    if c == 0x22:
        m = _STRING.match(data, i)
        if not m:
            raise JSONSyntaxError("bad string")
        return ("s", m.group()), m.end()
    if c == 0x2d or 0x30 <= c <= 0x39:
        m = _NUMBER.match(data, i)
        if not m:
            raise JSONSyntaxError("bad number")
        nxt = data[m.end():m.end() + 1]
        # the scanner commits to a fraction at "." and to an exponent at e/E
        if m.group(2) is None and (nxt in (b"e", b"E") or m.group(1) is None and nxt == b"."):
            raise JSONSyntaxError("bad number")
        return ("n", m.group()), m.end()
    for word, tag in ((b"true", "t"), (b"false", "f"), (b"null", "z")):
        if data.startswith(word, i):
            return (tag,), i + len(word)
    raise JSONSyntaxError("not a value")


def _json_key(data, i, n):
    i = _WS.match(data, i).end()
    if i >= n or data[i] != 0x22:
        raise JSONSyntaxError("want a key")
    m = _STRING.match(data, i)
    if not m:
        raise JSONSyntaxError("bad key")
    i = _WS.match(data, m.end()).end()
    if i >= n or data[i] != 0x3a:
        raise JSONSyntaxError("want :")
    return m.group(), _WS.match(data, i + 1).end()


def json_value(data):
    """The first JSON value in data as Go's json.Decoder reads it: EOF when data is
    only whitespace, JSONSyntaxError for anything its scanner rejects (or ends
    early), else a tree of ("o", [(raw key, node)]), ("a", [nodes]), ("s", raw),
    ("n", raw), ("t",), ("f",) and ("z",). What follows the value is never read."""
    n = len(data)
    i = _WS.match(data).end()
    if i == n:
        return EOF
    stack = []  # [kind, items, key] per open container
    while True:
        if i >= n:
            raise JSONSyntaxError("unexpected end")
        c = data[i]
        if c in (0x7b, 0x5b):
            if len(stack) >= MAX_JSON_DEPTH:
                raise JSONSyntaxError("exceeded max depth")
            j = _WS.match(data, i + 1).end()
            if j < n and data[j] == (0x7d if c == 0x7b else 0x5d):
                node, i = (("o", []) if c == 0x7b else ("a", [])), j + 1
            elif c == 0x7b:
                key, i = _json_key(data, j, n)
                stack.append(["o", [], key])
                continue
            else:
                stack.append(["a", [], None])
                i = j
                continue
        else:
            node, i = _json_literal(data, i)
        while True:  # close what this value completes
            if not stack:
                return node
            top = stack[-1]
            top[1].append((top[2], node) if top[0] == "o" else node)
            i = _WS.match(data, i).end()
            if i >= n:
                raise JSONSyntaxError("unexpected end")
            c = data[i]
            if c == 0x2c:
                if top[0] == "o":
                    top[2], i = _json_key(data, i + 1, n)
                else:
                    i = _WS.match(data, i + 1).end()
                break
            if c == (0x7d if top[0] == "o" else 0x5d):
                stack.pop()
                node, i = (top[0], top[1]), i + 1
                continue
            raise JSONSyntaxError("want , or a closing bracket")


_ESCAPES = {ord('"'): '"', ord("\\"): "\\", ord("/"): "/", ord("b"): "\b", ord("f"): "\f", ord("n"): "\n",
            ord("r"): "\r", ord("t"): "\t"}


def json_unquote(raw):
    """A scanned JSON string literal as Go's unquote gives it (bad UTF-8 and lone surrogates become U+FFFD)."""
    s = raw[1:-1]
    if b"\\" not in s:
        return go_utf8(s)
    out, i, n = [], 0, len(s)
    while i < n:
        j = s.find(b"\\", i)
        if j < 0:
            j = n
        if j > i:
            out.append(go_utf8(s[i:j]))
        if j >= n:
            break
        e = s[j + 1]
        if e != ord("u"):
            out.append(_ESCAPES[e])
            i = j + 2
            continue
        r, i = int(s[j + 2:j + 6], 16), j + 6
        if 0xD800 <= r < 0xE000:
            r2 = int(s[i + 2:i + 6], 16) if s[i:i + 2] == b"\\u" and re.fullmatch(rb"[0-9a-fA-F]{4}", s[i + 2:i + 6]) \
                else -1
            if r < 0xDC00 and 0xDC00 <= r2 < 0xE000:
                r, i = 0x10000 + ((r - 0xD800) << 10) + (r2 - 0xDC00), i + 6
            else:
                r = 0xFFFD
        out.append(chr(r))
    return "".join(out)


# ------------------------------------------------------------ encoding/json: decoding into TestGrid's types

class Ptr:
    """A non-nil Go pointer."""
    __slots__ = ("v",)

    def __init__(self, v=None):
        self.v = v


class Slice:
    """A non-nil Go slice: its backing array (len == cap) and length; elements past
    the length stay in the array, as Go keeps them for a later decode to reuse."""
    __slots__ = ("arr", "n")

    def __init__(self):
        self.arr, self.n = [], 0

    def items(self):
        return self.arr[:self.n]


class Types:
    """The Go types (from testgrid_types.json), and encoding/json's view of their fields."""

    def __init__(self, doc):
        self.t = doc["types"]
        self.roots = doc["roots"]
        self.folds = doc["ascii_folds"]
        self._fields = {}
        self._next_cap = {}
        for t in self.t.values():
            for f in t.get("fields", ()):
                if "string" in f[2].split(","):
                    raise ValueError("a ,string field: not ported")
            if t["kind"] not in ("struct", "ptr", "slice", "map", "string", "bool", "int32", "int64", "interface"):
                raise ValueError(f"kind {t['kind']} not ported")
            if t["kind"] == "map" and self.t[t["key"]]["kind"] != "string":
                raise ValueError("a map key that is not a string: not ported")

    def fold(self, name):
        """encoding/json's foldName, as far as it can reach an ASCII field name."""
        return "".join(self.folds.get(c, c.upper() if c.isascii() else c) for c in name)

    def fields(self, struct):
        """typeFields: (by exact JSON name, by folded name), each {name: (Go names path, type)}."""
        got = self._fields.get(struct)
        if got is not None:
            return got
        found, nxt, visited, nxt_count = [], [(struct, [], [])], set(), {}
        while nxt:
            current, nxt = nxt, []
            count, nxt_count = nxt_count, {}
            for ftyp, index, path in current:
                if ftyp in visited:
                    continue
                visited.add(ftyp)
                for i, (go, jname, _opts, typ, anon, exported) in enumerate(self.t[ftyp]["fields"]):
                    ft = typ
                    if anon:
                        if self.t[ft]["kind"] == "ptr":
                            raise ValueError("an embedded pointer: not ported")
                        if not exported and self.t[ft]["kind"] != "struct":
                            continue
                    elif not exported:
                        continue
                    name = jname if _valid_tag(jname) else ""
                    if not name and anon and self.t[ft]["kind"] == "struct":
                        nxt_count[ft] = nxt_count.get(ft, 0) + 1
                        if nxt_count[ft] == 1:
                            nxt.append((ft, index + [i], path + [go]))
                        continue
                    found.append((name or go, len(index) + 1, not name, index + [i], path + [go], typ))
                    if count.get(ftyp, 0) > 1:
                        found.append(found[-1])
        found.sort(key=lambda f: (f[0], f[1], f[2], f[3]))
        kept = []
        for name in dict.fromkeys(f[0] for f in found):
            group = [f for f in found if f[0] == name]
            if len(group) > 1 and group[0][1] == group[1][1] and group[0][2] == group[1][2]:
                continue  # dominantField: ambiguous at the top level, so dropped
            kept.append(group[0])
        kept.sort(key=lambda f: f[3])
        exact, folded = {}, {}
        for name, _, _, _, path, typ in kept:
            exact[name] = (path, typ)
            folded.setdefault(self.fold(name), (path, typ))
        self._fields[struct] = (exact, folded)
        return exact, folded

    def next_cap(self, slice_type, cap):
        """The capacity reflect.Value.Grow(1) gives a full slice of capacity cap."""
        table = self._next_cap.get(slice_type)
        if table is None:
            caps = [0] + self.t[slice_type]["caps"]
            table = self._next_cap[slice_type] = dict(zip(caps, caps[1:], strict=False))
        if cap in table:
            return table[cap]
        raise ValueError(f"a slice of {slice_type} longer than testgrid_types.json covers")


def _valid_tag(s):
    return bool(s) and all(c in "!#$%&()*+-./:;<=>?@[]^_{|}~ " or c.isalpha() or c.isdigit() for c in s)


_CUSTOM = {"k8s.io/apimachinery/pkg/apis/meta/v1.Time": "time", "*v1.Time": "time",
           "k8s.io/apimachinery/pkg/api/resource.Quantity": "quantity", "*resource.Quantity": "quantity",
           "k8s.io/apimachinery/pkg/util/intstr.IntOrString": "intorstring", "*v1.FieldsV1": "fieldsv1",
           "k8s.io/apimachinery/pkg/apis/meta/v1.FieldsV1": "fieldsv1"}
_INT_BITS = {"int32": 32, "int64": 64}


class _Decoder:
    """Decodes a scanned value into a Go type the way encoding/json's decodeState
    does, into what the value already holds; .err is set by any error (type
    mismatch, a number out of range, a custom UnmarshalJSON failing)."""

    def __init__(self, types):
        self.types = types
        self.err = False

    def value(self, node, t, cur):
        T = self.types.t[t]
        kind, tag = T["kind"], node[0]
        if tag == "z":  # null: nil for pointers, maps, slices and interfaces; ignored otherwise
            return None if kind in ("ptr", "map", "slice", "interface") else cur
        if t in _CUSTOM and T.get("custom"):
            self.custom(node, _CUSTOM[t])
            return cur if cur is not None else Ptr() if kind == "ptr" else True
        if T.get("custom"):
            raise ValueError(f"custom unmarshaler of {t}: not ported")
        if kind == "ptr":
            p = cur if cur is not None else Ptr()
            p.v = self.value(node, T["elem"], p.v)
            return p
        if kind == "interface":  # (a string is kept: Metadata.Strings() reads them)
            self.any(node)
            return json_unquote(node[1]) if tag == "s" else True
        if tag == "o":
            if kind == "struct":
                return self.struct(node, t, cur)
            if kind == "map":
                m = cur if cur is not None else {}
                for key, sub in node[1]:
                    m[json_unquote(key)] = self.value(sub, T["elem"], None)
                return m
        elif tag == "a":
            if kind == "slice":
                return self.slice(node, t, cur)
        elif tag in ("t", "f"):
            if kind == "bool":
                return tag == "t"
        elif tag == "s":
            if kind == "string":
                return json_unquote(node[1])
        elif kind in _INT_BITS:  # a number
            n = go_parse_int(node[1], 64)
            if n is not None and _wrap(n, _INT_BITS[kind]) == n:
                return n
        self.err = True  # UnmarshalTypeError (or a number out of range)
        return cur

    def struct(self, node, t, cur):
        d = cur if cur is not None else {}
        exact, folded = self.types.fields(t)
        for key, sub in node[1]:
            name = json_unquote(key)
            f = exact.get(name) or folded.get(self.types.fold(name))
            if f is None:
                continue  # an unknown field's value is skipped unchecked
            path, typ = f
            target = d
            for go in path[:-1]:  # embedded structs
                target = target.setdefault(go, {})
            target[path[-1]] = self.value(sub, typ, target.get(path[-1]))
        return d

    def slice(self, node, t, cur):
        s = cur if cur is not None else Slice()
        elem = self.types.t[t]["elem"]
        i = 0
        for sub in node[1]:
            if i >= len(s.arr):  # grown as reflect does: the old array is copied, the new part zero
                s.arr = s.arr + [None] * (self.types.next_cap(t, len(s.arr)) - len(s.arr))
            if i >= s.n:
                s.n = i + 1
            s.arr[i] = self.value(sub, elem, s.arr[i])
            i += 1
        if i < s.n:
            s.n = i
        return s if i else Slice()  # [] makes a new empty slice

    def any(self, node):
        """Into interface{}: only a number too large for float64 fails."""
        stack = [node]
        while stack:
            tag, *rest = stack.pop()
            if tag == "o":
                stack.extend(sub for _, sub in rest[0])
            elif tag == "a":
                stack.extend(rest[0])
            elif tag == "n":
                v = float(rest[0])
                if v in (float("inf"), float("-inf")):
                    self.err = True

    def custom(self, node, which):
        tag = node[0]
        if which == "fieldsv1":
            return  # keeps any value
        if which == "time":  # json.Unmarshal into a string, then time.Parse(time.RFC3339, ...)
            if tag != "s" or not go_rfc3339_ok(json_unquote(node[1])):
                self.err = True
        elif which == "intorstring":  # a string, or json.Unmarshal into an int32
            if tag == "n":
                n = go_parse_int(node[1], 64)
                if n is None or _wrap(n, 32) != n:
                    self.err = True
            elif tag != "s":
                self.err = True
        elif which == "quantity":  # ParseQuantity of the raw bytes, quotes stripped
            raw = {"s": lambda: node[1][1:-1], "n": lambda: node[1], "t": lambda: b"true",
                   "f": lambda: b"false"}.get(tag)
            if raw is None or not quantity_ok(go_trim_space(raw())):
                self.err = True


_TYPES = None


def types():
    global _TYPES
    if _TYPES is None:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "testgrid_types.json")) as f:
            _TYPES = Types(json.load(f))
    return _TYPES


def read_json(data, root):
    """What util/gcs makes of started.json, finished.json or podinfo.json (root:
    "started", "finished" or "podinfo"): ("missing", None), ("eof", None) for an
    empty one (malformed to TestGrid), ("error", None) when it cannot be decoded,
    or ("ok", the decoded struct as {Go field: value})."""
    if data is None:
        return "missing", None
    try:
        node = json_value(data)
    except JSONSyntaxError:
        return "error", None
    if node is EOF:
        return "eof", None
    dec = _Decoder(types())
    # Decode(&v): a literal (null included) at the top lands on the struct itself
    value = dec.value(node, types().roots[root], None) if node[0] != "z" else {}
    if dec.err:
        return "error", None
    return "ok", value


def _get(v, *path, default=None):
    for name in path:
        if isinstance(v, Ptr):
            v = v.v
        if not isinstance(v, dict):
            return default
        v = v.get(name)
    if isinstance(v, Ptr):
        v = v.v
    return default if v is None else v


def _items(s):
    return s.items() if isinstance(s, Slice) else []


# ------------------------------------------------------------ util/gcs: PodInfo.Summarize

def _truncate(s, n):
    return s if len(s) < n else s[:n // 2] + "..." + s[len(s) - n // 2:]


def _container_problem(status):
    """checkContainerStatus: what keeps a container from counting as passed, or ""."""
    name = _get(status, "Name", default="")
    state = _get(status, "State", default={})
    if state.get("Waiting") is not None:
        return f"{name} still waiting: {_get(state, 'Waiting', 'Message', default='')}"
    if state.get("Running") is not None:
        return f"{name} still running"
    terminated = state.get("Terminated")
    if terminated is not None and _get(terminated, "ExitCode", default=0) != 0:
        return (f"{name} exited {_get(terminated, 'ExitCode', default=0)}: "
                f"{_truncate(_get(terminated, 'Message', default=''), 140)}")
    return ""


def summarize(pod):
    """PodInfo.Summarize: (pass, message) for a decoded Pod, None for none."""
    if pod is None:
        return False, MISSING_POD_INFO
    status = _get(pod, "Status", default={})
    if _get(status, "Phase", default="") == "Succeeded":
        return True, ""
    conditions = {}
    for c in _items(status.get("Conditions")):
        conditions[_get(c, "Type", default="")] = c
    for kind, what in (("PodScheduled", "pod did not schedule"), ("Initialized", "pod could not initialize")):
        if kind in conditions and _get(conditions[kind], "Status", default="") != "True":
            return False, f"{what}: {_get(conditions[kind], 'Message', default='')}"
    for st in _items(status.get("InitContainerStatuses")):
        problem = _container_problem(st)
        if problem:
            return False, f"init container {problem}"
    sidecar = False
    for st in _items(status.get("ContainerStatuses")):
        name = _get(st, "Name", default="")
        sidecar = sidecar or name == "sidecar"
        problem = _container_problem(st)
        if not problem:
            continue
        if name == "sidecar" or _get(st, "State", default={}).get("Terminated") is None:
            return False, problem
    return (True, NO_POD_UTILS) if not sidecar else (True, "")


# ------------------------------------------------------------ encoding/xml (Strict) and TestGrid's junit

class XMLError(Exception):
    pass


class _XMLEOF(Exception):
    pass


MAX_XML_DEPTH = 10000  # encoding/xml maxUnmarshalDepth
# XML 1.0 Appendix B as encoding/xml's first and second tables have it: code point
# ranges a name starts with, and the ones (besides those) that may follow.
_XML_FIRST = ((0x003a, 0x003a), (0x0041, 0x005a), (0x005f, 0x005f), (0x0061, 0x007a), (0x00c0, 0x00d6),
              (0x00d8, 0x00f6), (0x00f8, 0x0131), (0x0134, 0x013e), (0x0141, 0x0148), (0x014a, 0x017e),
              (0x0180, 0x01c3), (0x01cd, 0x01f0), (0x01f4, 0x01f5), (0x01fa, 0x0217), (0x0250, 0x02a8),
              (0x02bb, 0x02c1), (0x0386, 0x0386), (0x0388, 0x038a), (0x038c, 0x038c), (0x038e, 0x03a1),
              (0x03a3, 0x03ce), (0x03d0, 0x03d6), (0x03da, 0x03da), (0x03dc, 0x03dc), (0x03de, 0x03de),
              (0x03e0, 0x03e0), (0x03e2, 0x03f3), (0x0401, 0x040c), (0x040e, 0x044f), (0x0451, 0x045c),
              (0x045e, 0x0481), (0x0490, 0x04c4), (0x04c7, 0x04c8), (0x04cb, 0x04cc), (0x04d0, 0x04eb),
              (0x04ee, 0x04f5), (0x04f8, 0x04f9), (0x0531, 0x0556), (0x0559, 0x0559), (0x0561, 0x0586),
              (0x05d0, 0x05ea), (0x05f0, 0x05f2), (0x0621, 0x063a), (0x0641, 0x064a), (0x0671, 0x06b7),
              (0x06ba, 0x06be), (0x06c0, 0x06ce), (0x06d0, 0x06d3), (0x06d5, 0x06d5), (0x06e5, 0x06e6),
              (0x0905, 0x0939), (0x093d, 0x093d), (0x0958, 0x0961), (0x0985, 0x098c), (0x098f, 0x0990),
              (0x0993, 0x09a8), (0x09aa, 0x09b0), (0x09b2, 0x09b2), (0x09b6, 0x09b9), (0x09dc, 0x09dd),
              (0x09df, 0x09e1), (0x09f0, 0x09f1), (0x0a05, 0x0a0a), (0x0a0f, 0x0a10), (0x0a13, 0x0a28),
              (0x0a2a, 0x0a30), (0x0a32, 0x0a33), (0x0a35, 0x0a36), (0x0a38, 0x0a39), (0x0a59, 0x0a5c),
              (0x0a5e, 0x0a5e), (0x0a72, 0x0a74), (0x0a85, 0x0a8b), (0x0a8d, 0x0a8d), (0x0a8f, 0x0a91),
              (0x0a93, 0x0aa8), (0x0aaa, 0x0ab0), (0x0ab2, 0x0ab3), (0x0ab5, 0x0ab9), (0x0abd, 0x0abd),
              (0x0ae0, 0x0ae0), (0x0b05, 0x0b0c), (0x0b0f, 0x0b10), (0x0b13, 0x0b28), (0x0b2a, 0x0b30),
              (0x0b32, 0x0b33), (0x0b36, 0x0b39), (0x0b3d, 0x0b3d), (0x0b5c, 0x0b5d), (0x0b5f, 0x0b61),
              (0x0b85, 0x0b8a), (0x0b8e, 0x0b90), (0x0b92, 0x0b95), (0x0b99, 0x0b9a), (0x0b9c, 0x0b9c),
              (0x0b9e, 0x0b9f), (0x0ba3, 0x0ba4), (0x0ba8, 0x0baa), (0x0bae, 0x0bb5), (0x0bb7, 0x0bb9),
              (0x0c05, 0x0c0c), (0x0c0e, 0x0c10), (0x0c12, 0x0c28), (0x0c2a, 0x0c33), (0x0c35, 0x0c39),
              (0x0c60, 0x0c61), (0x0c85, 0x0c8c), (0x0c8e, 0x0c90), (0x0c92, 0x0ca8), (0x0caa, 0x0cb3),
              (0x0cb5, 0x0cb9), (0x0cde, 0x0cde), (0x0ce0, 0x0ce1), (0x0d05, 0x0d0c), (0x0d0e, 0x0d10),
              (0x0d12, 0x0d28), (0x0d2a, 0x0d39), (0x0d60, 0x0d61), (0x0e01, 0x0e2e), (0x0e30, 0x0e30),
              (0x0e32, 0x0e33), (0x0e40, 0x0e45), (0x0e81, 0x0e82), (0x0e84, 0x0e84), (0x0e87, 0x0e88),
              (0x0e8a, 0x0e8a), (0x0e8d, 0x0e8d), (0x0e94, 0x0e97), (0x0e99, 0x0e9f), (0x0ea1, 0x0ea3),
              (0x0ea5, 0x0ea5), (0x0ea7, 0x0ea7), (0x0eaa, 0x0eab), (0x0ead, 0x0eae), (0x0eb0, 0x0eb0),
              (0x0eb2, 0x0eb3), (0x0ebd, 0x0ebd), (0x0ec0, 0x0ec4), (0x0f40, 0x0f47), (0x0f49, 0x0f69),
              (0x10a0, 0x10c5), (0x10d0, 0x10f6), (0x1100, 0x1100), (0x1102, 0x1103), (0x1105, 0x1107),
              (0x1109, 0x1109), (0x110b, 0x110c), (0x110e, 0x1112), (0x113c, 0x113c), (0x113e, 0x113e),
              (0x1140, 0x1140), (0x114c, 0x114c), (0x114e, 0x114e), (0x1150, 0x1150), (0x1154, 0x1155),
              (0x1159, 0x1159), (0x115f, 0x1161), (0x1163, 0x1163), (0x1165, 0x1165), (0x1167, 0x1167),
              (0x1169, 0x1169), (0x116d, 0x116e), (0x1172, 0x1173), (0x1175, 0x1175), (0x119e, 0x119e),
              (0x11a8, 0x11a8), (0x11ab, 0x11ab), (0x11ae, 0x11af), (0x11b7, 0x11b8), (0x11ba, 0x11ba),
              (0x11bc, 0x11c2), (0x11eb, 0x11eb), (0x11f0, 0x11f0), (0x11f9, 0x11f9), (0x1e00, 0x1e9b),
              (0x1ea0, 0x1ef9), (0x1f00, 0x1f15), (0x1f18, 0x1f1d), (0x1f20, 0x1f45), (0x1f48, 0x1f4d),
              (0x1f50, 0x1f57), (0x1f59, 0x1f59), (0x1f5b, 0x1f5b), (0x1f5d, 0x1f5d), (0x1f5f, 0x1f7d),
              (0x1f80, 0x1fb4), (0x1fb6, 0x1fbc), (0x1fbe, 0x1fbe), (0x1fc2, 0x1fc4), (0x1fc6, 0x1fcc),
              (0x1fd0, 0x1fd3), (0x1fd6, 0x1fdb), (0x1fe0, 0x1fec), (0x1ff2, 0x1ff4), (0x1ff6, 0x1ffc),
              (0x2126, 0x2126), (0x212a, 0x212b), (0x212e, 0x212e), (0x2180, 0x2182), (0x3007, 0x3007),
              (0x3021, 0x3029), (0x3041, 0x3094), (0x30a1, 0x30fa), (0x3105, 0x312c), (0x4e00, 0x9fa5),
              (0xac00, 0xd7a3),)
_XML_SECOND = ((0x002d, 0x002e), (0x0030, 0x0039), (0x00b7, 0x00b7), (0x02d0, 0x02d1), (0x0300, 0x0345),
               (0x0360, 0x0361), (0x0387, 0x0387), (0x0483, 0x0486), (0x0591, 0x05a1), (0x05a3, 0x05b9),
               (0x05bb, 0x05bd), (0x05bf, 0x05bf), (0x05c1, 0x05c2), (0x05c4, 0x05c4), (0x0640, 0x0640),
               (0x064b, 0x0652), (0x0660, 0x0669), (0x0670, 0x0670), (0x06d6, 0x06e4), (0x06e7, 0x06e8),
               (0x06ea, 0x06ed), (0x06f0, 0x06f9), (0x0901, 0x0903), (0x093c, 0x093c), (0x093e, 0x094d),
               (0x0951, 0x0954), (0x0962, 0x0963), (0x0966, 0x096f), (0x0981, 0x0983), (0x09bc, 0x09bc),
               (0x09be, 0x09c4), (0x09c7, 0x09c8), (0x09cb, 0x09cd), (0x09d7, 0x09d7), (0x09e2, 0x09e3),
               (0x09e6, 0x09ef), (0x0a02, 0x0a02), (0x0a3c, 0x0a3c), (0x0a3e, 0x0a42), (0x0a47, 0x0a48),
               (0x0a4b, 0x0a4d), (0x0a66, 0x0a71), (0x0a81, 0x0a83), (0x0abc, 0x0abc), (0x0abe, 0x0ac5),
               (0x0ac7, 0x0ac9), (0x0acb, 0x0acd), (0x0ae6, 0x0aef), (0x0b01, 0x0b03), (0x0b3c, 0x0b3c),
               (0x0b3e, 0x0b43), (0x0b47, 0x0b48), (0x0b4b, 0x0b4d), (0x0b56, 0x0b57), (0x0b66, 0x0b6f),
               (0x0b82, 0x0b83), (0x0bbe, 0x0bc2), (0x0bc6, 0x0bc8), (0x0bca, 0x0bcd), (0x0bd7, 0x0bd7),
               (0x0be7, 0x0bef), (0x0c01, 0x0c03), (0x0c3e, 0x0c44), (0x0c46, 0x0c48), (0x0c4a, 0x0c4d),
               (0x0c55, 0x0c56), (0x0c66, 0x0c6f), (0x0c82, 0x0c83), (0x0cbe, 0x0cc4), (0x0cc6, 0x0cc8),
               (0x0cca, 0x0ccd), (0x0cd5, 0x0cd6), (0x0ce6, 0x0cef), (0x0d02, 0x0d03), (0x0d3e, 0x0d43),
               (0x0d46, 0x0d48), (0x0d4a, 0x0d4d), (0x0d57, 0x0d57), (0x0d66, 0x0d6f), (0x0e31, 0x0e31),
               (0x0e34, 0x0e3a), (0x0e46, 0x0e4e), (0x0e50, 0x0e59), (0x0eb1, 0x0eb1), (0x0eb4, 0x0eb9),
               (0x0ebb, 0x0ebc), (0x0ec6, 0x0ec6), (0x0ec8, 0x0ecd), (0x0ed0, 0x0ed9), (0x0f18, 0x0f19),
               (0x0f20, 0x0f29), (0x0f35, 0x0f35), (0x0f37, 0x0f37), (0x0f39, 0x0f39), (0x0f3e, 0x0f3f),
               (0x0f71, 0x0f84), (0x0f86, 0x0f8b), (0x0f90, 0x0f95), (0x0f97, 0x0f97), (0x0f99, 0x0fad),
               (0x0fb1, 0x0fb7), (0x0fb9, 0x0fb9), (0x20d0, 0x20dc), (0x20e1, 0x20e1), (0x3005, 0x3005),
               (0x302a, 0x302f), (0x3031, 0x3035), (0x3099, 0x309a), (0x309d, 0x309e), (0x30fc, 0x30fe),)


def _char_class(*tables):
    return "".join(f"{re.escape(chr(a))}-{re.escape(chr(b))}" for t in tables for a, b in t)


_XML_NAME = re.compile(f"[{_char_class(_XML_FIRST)}][{_char_class(_XML_FIRST, _XML_SECOND)}]*")
_NAME_BYTES = re.compile(rb"[A-Za-z0-9_:.\-\x80-\xff]+")
_XML_SPACE = re.compile(rb"[ \r\n\t]*")
_BAD_XML_CHAR = re.compile("[^" + _char_class(((0x09, 0x0a), (0x0d, 0x0d), (0x20, 0xd7ff), (0xe000, 0xfffd),
                                                 (0x10000, 0x10ffff))) + "]")  # the Char production
_ENTITY = re.compile(rb"&(?:#([0-9]+)|#x([0-9a-fA-F]+)|([A-Za-z0-9_:.\-\x80-\xff]*));")
_ENTITIES = {b"lt": "<", b"gt": ">", b"amp": "&", b"apos": "'", b"quot": '"'}
_PLAIN = {-1: re.compile(rb"[^&<\]\r]+"), "cdata": re.compile(rb"[^\]\r]+"), 0x22: re.compile(rb'[^&<"\r]+'),
          0x27: re.compile(rb"[^&<'\r]+")}


def _xml_is_name(b):
    try:
        s = b.decode("utf-8")  # a bad sequence is not a name
    except UnicodeDecodeError:
        return False
    return _XML_NAME.fullmatch(s) is not None


def _proc_inst(param, s):
    """encoding/xml's procInst: the value of param="..." in a <?xml?> declaration."""
    param += b"="
    lenp, i, sep = len(param), 0, None
    while i < len(s):
        sub = s[i:]
        k = sub.find(param)
        if k < 0 or lenp + k >= len(sub):
            return b""
        i += lenp + k + 1
        if sub[lenp + k] in b"'\"":
            sep = sub[lenp + k]
            break
    if sep is None:
        return b""
    j = s.find(bytes([sep]), i)
    return b"" if j < 0 else s[i:j]


class _XMLDecoder:
    """encoding/xml's Decoder with Strict set, as TestGrid's junit parser uses it
    (its CharsetReader accepts "utf8" besides UTF-8). Tokens: ("start", (space,
    local), [((space, local), value)]), ("end", (space, local)), ("text", str),
    ("other",). Every error is raised as XMLError."""

    def __init__(self, data):
        self.d, self.i, self.n = data, 0, len(data)
        self.need_close, self.to_close = False, None
        # raw names of the open elements, below each an "NS" per namespace it declared, and "EOF" markers
        self.stack = []

    def _byte(self):  # mustgetc
        if self.i >= self.n:
            raise XMLError("unexpected EOF")
        self.i += 1
        return self.d[self.i - 1]

    def _space(self):
        self.i = _XML_SPACE.match(self.d, self.i).end()

    def _name(self):
        """name(): None when no name starts here; an XMLError for a bad one."""
        if self.i >= self.n:
            raise XMLError("unexpected EOF")
        m = _NAME_BYTES.match(self.d, self.i)
        if not m:
            return None
        if m.end() >= self.n:
            raise XMLError("unexpected EOF")  # readName needs the byte after the name
        self.i = m.end()
        if not _xml_is_name(m.group()):
            raise XMLError("invalid XML name")
        return m.group()

    def _nsname(self, what):
        s = self._name()
        if s is None or s.count(b":") > 1:
            raise XMLError(f"expected {what}")
        space, sep, local = s.partition(b":")
        return (space, local) if sep and space and local else (b"", s)

    def _text(self, quote, cdata=False):
        """text(): character data up to quote (an attribute), "]]>" (CDATA) or "<"."""
        d, n, out = self.d, self.n, []
        plain = _PLAIN["cdata" if cdata else quote]
        while True:
            m = plain.match(d, self.i)
            if m:
                out.append(m.group())
                self.i = m.end()
            if self.i >= n:
                if cdata:
                    raise XMLError("unexpected EOF in CDATA section")
                break  # (at EOF: a following read fails)
            c = d[self.i]
            if c == 0x3c:  # "<"
                if quote >= 0:
                    raise XMLError("unescaped < inside quoted string")
                break
            if quote >= 0 and c == quote:
                self.i += 1
                break
            if c == 0x26:  # "&"
                m = _ENTITY.match(d, self.i)
                if not m:
                    raise XMLError("invalid character entity")
                if m.group(3) is not None:
                    text = _ENTITIES.get(m.group(3))
                    if text is None:
                        raise XMLError("invalid character entity")
                else:
                    num = int(m.group(1) or m.group(2), 10 if m.group(1) is not None else 16)
                    if num > 0x10FFFF:
                        raise XMLError("invalid character entity")
                    text = REPLACEMENT if 0xD800 <= num < 0xE000 else chr(num)
                out.append(text.encode("utf-8", "surrogatepass"))
                self.i = m.end()
                continue
            if c == 0x5d:  # "]"
                if quote < 0 and d.startswith(b"]]>", self.i):
                    if cdata:
                        self.i += 3
                        break
                    raise XMLError("unescaped ]]> not in CDATA section")
                out.append(b"]")
                self.i += 1
                continue
            # "\r", and "\r\n", become "\n"
            out.append(b"\n")
            self.i += 2 if d.startswith(b"\r\n", self.i) else 1
        data = b"".join(out)
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            raise XMLError("invalid UTF-8") from None
        if _BAD_XML_CHAR.search(text):
            raise XMLError("illegal character code")
        return text

    def raw_token(self):
        if self.need_close:
            self.need_close = False
            return ("end", self.to_close)
        if self.i >= self.n:
            return None  # io.EOF
        d = self.d
        if d[self.i] != 0x3c:
            return ("text", self._text(-1))
        self.i += 1
        b = self._byte()
        if b == 0x2f:  # </name>
            name = self._nsname("element name after </")
            self._space()
            if self._byte() != 0x3e:
                raise XMLError("invalid characters between </name and >")
            return ("end", name)
        if b == 0x3f:  # <?target ...?>
            target = self._name()
            if target is None:
                raise XMLError("expected target name after <?")
            self._space()
            end = d.find(b"?>", self.i)
            if end < 0:
                raise XMLError("unexpected EOF")
            content, self.i = d[self.i:end], end + 2
            if target == b"xml":
                ver = _proc_inst(b"version", content)
                if ver not in (b"", b"1.0"):
                    raise XMLError("unsupported version")
                enc = _proc_inst(b"encoding", content)
                if enc and enc.lower() != b"utf-8" and enc != b"utf8":
                    raise XMLError("unknown charset")  # TestGrid's CharsetReader
            return ("other",)
        if b == 0x21:  # <!
            b = self._byte()
            if b == 0x2d:  # <!-- comment -->
                if self._byte() != 0x2d:
                    raise XMLError("invalid sequence <!- not part of <!--")
                k = d.find(b"--", self.i)
                if k < 0 or k + 2 >= self.n:
                    raise XMLError("unexpected EOF")
                if d[k + 2] != 0x3e:
                    raise XMLError('invalid sequence "--" not allowed in comments')
                self.i = k + 3
                return ("other",)
            if b == 0x5b:  # <![CDATA[
                for want in b"CDATA[":
                    if self._byte() != want:
                        raise XMLError("invalid <![ sequence")
                return ("text", self._text(-1, cdata=True))
            self._directive()
            return ("other",)
        self.i -= 1  # <name attr="value" ...> or <name ... />
        name = self._nsname("element name after <")
        attrs, empty = [], False
        while True:
            self._space()
            b = self._byte()
            if b == 0x2f:
                if self._byte() != 0x3e:
                    raise XMLError("expected /> in element")
                empty = True
                break
            if b == 0x3e:
                break
            self.i -= 1
            aname = self._nsname("attribute name in element")
            self._space()
            if self._byte() != 0x3d:
                raise XMLError("attribute name without = in element")
            self._space()
            q = self._byte()
            if q not in (0x22, 0x27):
                raise XMLError("unquoted or missing attribute value in element")
            attrs.append((aname, self._text(q)))
        if empty:
            self.need_close, self.to_close = True, name
        return ("start", name, attrs)

    def _directive(self):
        """<!DOCTYPE ...> and the like: quoted angle brackets do not nest, comments are skipped."""
        inquote, depth = 0, 0
        while True:
            b = self._byte()
            if inquote == 0 and b == 0x3e and depth == 0:
                return
            while True:  # HandleB
                if b == inquote:
                    inquote = 0
                elif inquote != 0:
                    pass
                elif b in (0x27, 0x22):
                    inquote = b
                elif b == 0x3e:
                    depth -= 1
                elif b == 0x3c:
                    for want in b"!--":
                        b = self._byte()
                        if b != want:
                            depth += 1
                            break
                    else:
                        b0 = b1 = 0
                        while True:
                            b = self._byte()
                            if b0 == 0x2d and b1 == 0x2d and b == 0x3e:
                                break
                            b0, b1 = b1, b
                        break
                    continue  # handle the byte that broke "<!--"
                break

    def token(self):
        """Token(): raw tokens with start and end elements matched (by prefix and local name).
        A start element's namespace declarations (xmlns, xmlns:p) go on the stack first
        (pushNs), and go with it when its end tag matches. One that does not match
        has already popped the element, but not those."""
        if self.stack and self.stack[-1] == "EOF":
            raise _XMLEOF
        t = self.raw_token()
        if t is None:
            if self.stack:
                raise XMLError("unexpected EOF")
            raise _XMLEOF
        if t[0] == "start":
            self.stack += ["NS" for (space, local), _ in t[2] if space == b"xmlns" or (not space and local == b"xmlns")]
            self.stack.append(t[1])
        elif t[0] == "end":
            top = self.stack.pop() if self.stack else None
            if not isinstance(top, tuple) or top[1] != t[1][1] or top[0] != t[1][0]:
                raise XMLError("unexpected end element")
            while self.stack and self.stack[-1] == "NS":
                self.stack.pop()
        return t

    def skip(self):
        depth = 0
        while True:
            t = self.token()
            if t[0] == "start":
                depth += 1
            elif t[0] == "end":
                if depth == 0:
                    return
                depth -= 1


def _check_depth(depth):
    if depth >= MAX_XML_DEPTH:
        raise XMLError("exceeded max depth")


def _attr_number(value, parse):
    """copyValue into an int or float64 attribute field."""
    if not value:
        return
    text = value.strip(GO_SPACES).encode("utf-8")
    if not parse(text):
        raise XMLError("bad number")


_INT_ATTR = lambda b: go_parse_int(b, 64) is not None  # noqa: E731
_SUITE_NUMBERS = {b"time": go_parse_float_ok, b"failures": _INT_ATTR, b"tests": _INT_ATTR, b"disabled": _INT_ATTR,
                  b"skipped": _INT_ATTR, b"errors": _INT_ATTR}
_RESULT_NUMBERS = {b"time": go_parse_float_ok}


def _junit_element(kind, start, depth):
    """A frame for one element encoding/xml's unmarshal decodes into a junit type:
    [kind, value, depth]. Each unmarshal call checks its depth, as Go counts it:
    a slice field is one call and its new element the next, a pointer field one."""
    _check_depth(depth)
    if kind == "suite":
        value = {"name": "", "suites": [], "results": []}
        numbers = _SUITE_NUMBERS
    elif kind == "result":
        value = {"name": "", "errored": False, "failure": False, "skipped": None, "properties": []}
        numbers = _RESULT_NUMBERS
    elif kind == "property":  # (Property: its name and value attributes, the last of each)
        value = {"name": "", "value": ""}
        for (_, local), attr in start[2]:
            if local in (b"name", b"value"):
                value[local.decode()] = attr
        return [kind, value, depth]
    else:
        value, numbers = [], {}
    for (_, local), attr in start[2] if kind in ("suite", "result") else ():
        if local == b"name":
            value["name"] = attr
        elif local in numbers:
            _attr_number(attr, numbers[local])
    return [kind, value, depth]


# Per kind, the child elements it decodes: (field kind, extra unmarshal calls first)
_JUNIT_CHILDREN = {
    "suites": {b"testsuite": ("suite", 1)},
    "suite": {b"testsuite": ("suite", 1), b"properties": ("properties", 0), b"testcase": ("result", 1)},
    "properties": {b"property": ("property", 1)},
    "result": {b"system-out": ("text", 0), b"system-err": ("text", 0), b"error": ("errored", 0),
               b"failure": ("failure", 0), b"skipped": ("skipped", 0), b"properties": ("properties", 0)},
}


def _decode_junit_root(x, top):
    """DecodeElement of the root into Suites or a Suite (top: its frame), without
    recursion (Go allows thousands of nested testsuites). What it decodes stays
    in top even when it fails part way, as Go's value does."""
    frames = [top]
    while frames:
        t = x.token()
        frame = frames[-1]
        kind, value, depth = frame
        if t[0] == "text":
            if kind in ("text", "errored", "failure", "skipped"):
                value.append(t[1])
            continue
        if t[0] == "end":
            frames.pop()
            if frames:
                parent = frames[-1][1]
                if kind == "suite":
                    (parent if frames[-1][0] == "suites" else parent["suites"]).append(value)
                elif kind == "result":
                    parent["results"].append(value)
                elif kind in ("errored", "failure"):
                    parent[kind] = True
                elif kind == "skipped":
                    parent["skipped"]["value"] = "".join(value)
                elif kind == "property":
                    parent.append((value["name"], value["value"]))
                elif kind == "properties" and frames[-1][0] == "result":  # (each into the one Properties)
                    parent["properties"].extend(value)
            continue
        if t[0] != "start":
            continue
        child = _JUNIT_CHILDREN.get(kind, {}).get(t[1][1])
        if child is None:
            x.skip()  # an element no field takes (its text is not data either)
            continue
        ckind, extra = child
        for k in range(extra):
            _check_depth(depth + 1 + k)
        if ckind == "skipped":  # a second <skipped> decodes into the same Skipped
            skipped = value["skipped"] = value["skipped"] or {"message": ""}
            for (_, local), attr in t[2]:
                if local == b"message":
                    skipped["message"] = attr
        frames.append(_junit_element(ckind, t, depth + 1 + extra))


class Malformed(Exception):
    pass


def parse_junit(data):
    """junit.ParseStream: the top-level suites of a junit file ([] for one without
    a root element); Malformed for anything TestGrid cannot read."""
    x = _XMLDecoder(data)
    try:
        while True:  # unmarshal(start == nil): skip to the first element
            t = x.token()
            if t[0] == "start":
                break
    except _XMLEOF:
        return []
    except XMLError as e:
        raise Malformed(str(e)) from None
    # unmarshalInterface: pushEOF (below the root and its namespace entries),
    # UnmarshalXML (which ignores DecodeElement's error), then the root must have
    # been read to its end: the stack is back at the EOF marker. That also holds
    # when the root's own end tag does not match it, as Token pops the element
    # before it checks the name, unless the root declared a namespace: those
    # entries are still above the marker.
    at = len(x.stack) - 1
    while at and x.stack[at - 1] == "NS":
        at -= 1
    x.stack.insert(at, "EOF")
    if t[1][1] not in (b"testsuites", b"testsuite"):
        raise Malformed("bad element name")
    try:
        top = _junit_element("suites" if t[1][1] == b"testsuites" else "suite", t, 0)
    except XMLError as e:
        raise Malformed(f"did not consume the entire root element ({e})") from None
    try:
        _decode_junit_root(x, top)
    except (XMLError, _XMLEOF):
        pass
    if not x.stack or x.stack[-1] != "EOF":
        raise Malformed("did not consume the entire root element")
    kind, value, _ = top
    return value if kind == "suites" else [value]


# util/gcs: the objects TestGrid reads as junit
JUNIT_NAME = re.compile(r".+/(?:junit((_[^_]+)?(_[0-9]+-[0-9]+)?(_[0-9]+)?|.+)?\.xml|test.xml)\Z")


def resolve(name):
    """The object TestGrid opens for a listed one: url.ResolveReference drops "."
    and ".." path segments."""
    out = []
    elems = name.split("/")
    for elem in elems:
        if elem == "..":
            if out:
                out.pop()
        elif elem != ".":
            out.append(elem)
    if elems[-1] in (".", ".."):
        out.append("")  # resolvePath ends such a path with "/"
    return "/".join(out)


def junit_reads(names):
    """For the object names under a build (as listed, relative to the bucket),
    the ones TestGrid parses as junit, as (listed name, object it opens)."""
    return [(n, resolve(n)) for n in names if JUNIT_NAME.search(n)]


# ------------------------------------------------------------ pkg/updater: readResult and convertResult

def _flatten(suites):
    """flattenResults: every test case under the suites, named suite.inner.case, each
    inner suite's before its parent's own (iterative: suites nest thousands deep)."""
    out = []
    for top in suites:
        stack = [(top, top["name"], iter(top["suites"]))]
        while stack:
            suite, name, inner = stack[-1]
            nxt = next(inner, None)
            if nxt is not None:
                stack.append((nxt, _dot(name, nxt["name"]), iter(nxt["suites"])))
                continue
            stack.pop()
            out += [dict(r, name=_dot(name, r["name"])) for r in suite["results"]]
    return out


def _dot(left, right):
    if left and right:
        return left + "." + right
    return left if not right else right


def merge(statuses):
    """MergeCells(flaky=true): one cell for rows of the same name."""
    if len(statuses) == 1:
        return statuses[0]
    current, passing, failing = statuses[0], 0, 0
    for s in statuses:
        if SEVERITY[s] >= SEVERITY[current]:
            current = s
        if 1 <= SEVERITY[s] <= 4:
            passing += 1
        elif 11 <= SEVERITY[s] <= 15:
            failing += 1
    return "FLAKY" if passing and failing else current


MAX_DUPLICATES = 20  # SplitCells: rows of one name kept apart, then one "[overflow]" row


def split(name, statuses):
    """SplitCells: rows of the same name apart, as [(row, status)]: "name", "name [1]", ...,
    then "name [overflow]", FAIL ("Too many duplicately named rows")."""
    out = []
    for i, status in enumerate(statuses):
        if i == MAX_DUPLICATES:
            out.append((name + " [overflow]", "FAIL"))
            break
        out.append((name if i == 0 else f"{name} [{i}]", status))
    return out


# the junit files TestGrid reads (util/gcs: re), as parseSuitesMeta takes their metadata
_SUITES_META = re.compile(r".+/(?:junit((_[^_]+)?(_[0-9]+-[0-9]+)?(_[0-9]+)?|.+)?\.xml|test.xml)\Z")


def suites_meta(name):
    """parseSuitesMeta: a junit file's Context, Timestamp and Thread, from its name as listed."""
    m = _SUITES_META.match(name)
    if m is None:
        return {}
    c, ti, th = ((g or "")[1:] for g in m.group(2, 3, 4))
    if not (c or ti or th):
        c = m.group(1) or ""
    return {"Context": c, "Timestamp": ti, "Thread": th}


STATUS_OF = ["NO_RESULT", "PASS", "PASS_WITH_ERRORS", "PASS_WITH_SKIPS", "RUNNING", "CATEGORIZED_ABORT", "UNKNOWN",
             "CANCEL", "BLOCKED", "TIMED_OUT", "CATEGORIZED_FAIL", "BUILD_FAIL", "FAIL", "FLAKY", "TOOL_FAIL",
             "BUILD_PASSED"]  # (TestStatus by number, pb/test_status)


class Options:
    """A test group's cell options, as TestGrid's updater takes them (makeOptions,
    makeNameConfig; production does not enable ignore_skip): .merge (rows of one name
    merged, else split), .pod (the Pod row: prowjob analysis), .all_tests (the status a
    custom evaluator rule without comparisons gives every test result, else None: a rule
    with comparisons the configurator can write never matches, see from_group) and the
    test name config (.names: (format, parts) or None)."""

    def __init__(self, merge=True, pod=True, all_tests=None, names=None):
        self.merge, self.pod, self.all_tests, self.names = merge, pod, all_tests, names
        self.key = (merge, pod, all_tests, names)

    def __eq__(self, other):
        return isinstance(other, Options) and self.key == other.key

    def __hash__(self):
        return hash(self.key)

    def render(self, job, test, *metadatas):
        """nameConfig.render: fmt.Sprintf(format, the parts), each part the job's name, the
        test's, or the first of the metadatas (the result's first properties, the file's,
        finished.json's) that has it."""
        fmt, parts = self.names
        args = []
        for p in parts:
            if p == "Job name":
                args.append(job)
            elif p == "Tests name":
                args.append(test)
            else:
                args.append(next((m[p] for m in metadatas if p in m), ""))
        return gofmt.sprintf(fmt, args)


DEFAULT_OPTIONS = Options()


class Column:
    """One build's column: .errors (read errors, by part: "podinfo", "started",
    "nostart", "ancient", "finished"), .malformed, .results (the Overall results
    possible: with read errors whichever error comes first decides) and .cells
    ({row: status}, None with read errors), and .notes: why the job's Overall and
    Pod cells are what they are. .started and .finished: the timestamps TestGrid
    took from started.json and finished.json (None when it read none)."""

    def __init__(self, errors, malformed, results, cells, notes=None, started=None, finished=None):
        self.errors, self.malformed, self.results, self.cells = errors, malformed, results, cells
        self.notes = notes or {}  # why the job's Overall and Pod cells are what they are
        self.started, self.finished = started, finished
        self.either = {}  # (a row several split rows of one name fill: {row: their statuses})

    def red_rows(self):
        return sorted(row for row, status in (self.cells or {}).items() if status in RED)


_UNIX_TO_INTERNAL = 62135596800  # time's seconds from year 1 to 1970


def _unix(sec):
    """time.Unix(sec, 0) as seconds since year 1: the sum wraps, as Go's does."""
    return _wrap(sec + _UNIX_TO_INTERNAL, 64)


def _before(sec, t):
    """time.Unix(sec, 0).Before(T) for the real time T = t (Unix seconds, a float or
    an int): compared in whole seconds and then in the fraction t has, as Go compares
    times (integers only: a float the size of Go's internal seconds would round to
    2048-second steps)."""
    if t != t or t in (math.inf, -math.inf):
        return t == math.inf
    whole = math.floor(t)
    a, b = _unix(sec), _unix(whole)
    return a < b or (a == b and t > whole)


def _add(t, d):
    """Time.Add of d seconds: addSec saturates instead of wrapping."""
    total = _wrap(t + d, 64)
    if (total > t) == (d > 0):
        return total
    return (1 << 63) - 1 if d > 0 else -((1 << 63) - 1)


def _deref(p, zero):
    return zero if p is None else zero if p.v is None else p.v


def column(job, started, finished, podinfo, junits, stop, now, folder="", parsed=None, options=None):
    """TestGrid's column for one build. started, finished, podinfo: the objects'
    bytes (None: no such object). junits: what TestGrid read for each junit_reads()
    entry, in listing order, as (object it opens, size, data[, name as listed]):
    size is GCS's for a plainly stored object and -1 for one stored gzipped
    (TestGrid reads that one decompressed, of unknown length), data None when there
    is no such object (the listed name, the object's by default, gives the file's
    Context, Timestamp and Thread). stop: the start time before which TestGrid
    calls a build too old (its days_of_results before it read the build); now:
    when it read the build; folder: the build's folder (bucket-relative, with "/"),
    for the messages. parsed: a dict that keeps each file's decode and each junit
    file's parse for later calls given the same data (the columns of one build read
    at several times).
    options: the test group's cell options (Options; the defaults when None)."""
    errors, malformed = [], []

    def decoded(data, root):  # (each file decoded once for the calls given the same `parsed`)
        key = ("json", root, id(data))  # (the caller keeps data alive while it keeps `parsed`)
        got = parsed.get(key) if parsed is not None else None
        if got is None:
            got = read_json(data, root)
            if parsed is not None:
                parsed[key] = got
        return got
    kind, pi = decoded(podinfo, "podinfo")
    if kind == "eof":
        malformed.append("podinfo.json")
    elif kind == "error":
        errors.append("podinfo")
    pod = _get(pi, "Pod") if kind == "ok" else None

    started_ts, started_seen = 0, None
    kind, st = decoded(started, "started")
    if kind == "eof":
        malformed.append("started.json")
    elif kind == "error":
        errors.append("started")
    else:
        ts = started_seen = _get(st, "Started", "Timestamp", default=0) if kind == "ok" else 0  # missing: pending
        if _before(ts, stop):
            errors.append("nostart" if ts == 0 else "ancient")
        else:
            started_ts = ts

    fin_ts, passed, result = None, None, ""
    kind, fin = decoded(finished, "finished")
    if kind == "eof":
        malformed.append("finished.json")
    elif kind == "error":
        errors.append("finished")
    elif kind == "ok":
        f = fin.get("Finished") or {}
        fin_ts = None if f.get("Timestamp") is None else _deref(f["Timestamp"], 0)
        passed = None if f.get("Passed") is None else _deref(f["Passed"], False)
        result = f.get("Result") or ""

    files = []  # (the suites of each junit file read, the metadata of its name)
    for junit in junits:
        opened, size, data = junit[:3]
        rel = opened[len(folder):] if folder and opened.startswith(folder) else opened
        if data is None:
            malformed.append(f"{rel}: open: storage: object doesn't exist")
        elif size is not None and size > JUNIT_MAX_SIZE:
            malformed.append(f"{rel}: too large")
        else:
            key = (opened, id(data))  # (the caller keeps data alive while it keeps `parsed`)
            got = parsed.get(key) if parsed is not None else None
            if got is None:
                try:
                    got = (True, parse_junit(data))
                except Malformed as e:
                    got = (False, str(e))
                if parsed is not None:
                    parsed[key] = got
            if got[0]:
                files.append((got[1], suites_meta(junit[3] if len(junit) > 3 else opened)))
            else:
                malformed.append(f"{rel}: parse: {got[1]}")

    if errors:
        results = {"RUNNING" if e == "nostart" else "UNKNOWN" if e == "ancient" else "TOOL_FAIL" for e in errors}
        return Column(errors, sorted(malformed), results, None, started=started_seen, finished=fin_ts)

    # gcsResult.deadline(), compared with time.Now() as Go compares times: in whole
    # seconds plus the fraction now has (integers only: a float the size of Go's
    # internal seconds would round to 2048-second steps)
    deadline = _add(_unix(started_ts), 24 * 3600) if fin_ts is None else _add(_unix(fin_ts), 3600)
    whole = math.floor(now)
    now, past = _unix(whole), now > whole
    after, before = now > deadline or (now == deadline and past), now < deadline
    if malformed:
        overall, why = "FAIL", "malformed artifacts: " + ", ".join(sorted(malformed))
    elif (fin_ts or 0) > 0:
        ok = passed if passed is not None else (result == "SUCCESS" if result else False)
        overall, why = ("PASS", "") if ok else ("FAIL", f"it did not pass; Prow result {result or 'none'}")
    elif after:
        overall, why = "FAIL", "it did not finish within 24 hours"
    else:
        overall, why = "RUNNING", "still running"

    opts = options or DEFAULT_OPTIONS
    meta = {k: v for k, v in _get(fin, "Finished", "Metadata", default={}).items()
            if isinstance(v, str)} if opts.names else {}  # (Metadata.Strings())
    cells = {}
    for file_suites, file_meta in files:
        for r in _flatten(file_suites):
            skipped = r["skipped"]
            if skipped is not None and skipped.get("value", "") == "" and skipped["message"] in ("skipped", ""):
                continue
            status = "FAIL" if r["errored"] or r["failure"] else "PASS_WITH_SKIPS" if skipped is not None else "PASS"
            if opts.all_tests is not None:  # (CustomStatus)
                status = opts.all_tests
            if status == "NO_RESULT":  # (ignoreStatus)
                continue
            name = r["name"]
            if opts.names:
                props = {}
                for k, v in r["properties"]:
                    props.setdefault(k, v)  # (first)
                name = opts.render(job, name, props, file_meta, meta)
            cells.setdefault(name, []).append(status)
    passed_pod, message = summarize(pod)
    if message == MISSING_POD_INFO:
        pod_status = "RUNNING" if before else "PASS_WITH_SKIPS"
    else:
        pod_status = "PASS" if passed_pod else "FAIL"
    injected = {job + ".Overall": overall}
    if opts.pod and (message != MISSING_POD_INFO or overall != "RUNNING"):  # (analyzeProwJob)
        injected[job + ".Pod"] = pod_status
    for name, status in injected.items():
        cells[name] = [status] + cells.get(name, [])
    either = {}  # (rows two splits give one name: Go's map order picks which is shown)
    if opts.merge:
        merged = {name: merge(statuses) for name, statuses in cells.items()}
    else:
        merged = {}
        for name, statuses in cells.items():
            for row, status in split(name, statuses):
                if row in merged:
                    either.setdefault(row, {merged[row]}).add(status)
                if row not in merged or status in RED:  # (either may be shown: a red one counts)
                    merged[row] = status
    # (the messages of the rows TestGrid adds; a junit row of the same name is merged into them)
    notes = {name: why if name == job + ".Overall" else message for name in injected}
    col = Column([], sorted(malformed), {merged[job + ".Overall"]}, merged, notes, started=started_ts, finished=fin_ts)
    col.either = either
    return col
