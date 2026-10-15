# Ported in part from Go's regexp/syntax and unicode (Go 1.26.5). Copyright The Go Authors; BSD 3-Clause
# license in third_party/go/LICENSE. THIRD_PARTY_NOTICES.md lists what was ported and what changed.
"""Go's regexp, as TestGrid's tabulator compiles a tab's filters (regexp.Compile:
regexp/syntax.Parse with its Perl flags) and matches them (MatchString).

The parser is Go's regexp/syntax/parse.go (Go 1.26.5, tool/oracle/pinned.json), ported
function by function: the same parse tree, built the same way (literals merged as it goes,
alternations factored, nodes reused from a free list), so a pattern is accepted or
rejected exactly as Go does, its size and height limits included (they count nodes
and memoize sizes by node, stale entries and all). Unicode classes (\\p{...}) and case
folding use Go's own unicode tables, dumped from that Go (go_unicode.json.gz, by
oracle/go/cmd/unidump).

The parse tree is matched as Go matches it (RE2: no backtracking, time linear in the
text): compiled to a Thompson NFA and run over the text in one pass, a set of states
per position, the steps cached.
"""
import gzip
import json
import os
import sys
import threading
import time

# Parse trees nest up to MAX_HEIGHT (1000) deep, and the parser and the compiler walk them
# recursively as Go's do: pure Python calls, which take no C stack.
sys.setrecursionlimit(max(sys.getrecursionlimit(), 20_000))

# ------------------------------------------------------------ regexp/syntax: errors, flags, ops


class Error(ValueError):
    """What regexp.Compile returns: the pattern is not one Go compiles."""

    def __init__(self, code, expr=""):
        super().__init__(f"error parsing regexp: {code}: `{expr}`")
        self.code, self.expr = code, expr


ERR_INVALID_CHAR_CLASS = "invalid character class"
ERR_INVALID_CHAR_RANGE = "invalid character class range"
ERR_INVALID_ESCAPE = "invalid escape sequence"
ERR_INVALID_NAMED_CAPTURE = "invalid named capture"
ERR_INVALID_PERL_OP = "invalid or unsupported Perl syntax"
ERR_INVALID_REPEAT_OP = "invalid nested repetition operator"
ERR_INVALID_REPEAT_SIZE = "invalid repeat count"
# (never raised here: the one caller, testgrid_config.tab_filters, decodes each pattern from UTF-8
# strictly and refuses one that is not)
ERR_INVALID_UTF8 = "invalid UTF-8"
ERR_MISSING_BRACKET = "missing closing ]"
ERR_MISSING_PAREN = "missing closing )"
ERR_MISSING_REPEAT_ARGUMENT = "missing argument to repetition operator"
ERR_TRAILING_BACKSLASH = "trailing backslash at end of expression"
ERR_UNEXPECTED_PAREN = "unexpected )"
ERR_NESTING_DEPTH = "expression nests too deeply"
ERR_LARGE = "expression too large"

FOLD_CASE, LITERAL, CLASS_NL, DOT_NL, ONE_LINE, NON_GREEDY, PERL_X, UNICODE_GROUPS, WAS_DOLLAR, SIMPLE = \
    (1 << i for i in range(10))
PERL = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS
FLAGS_MASK = (1 << 16) - 1  # (Flags is a uint16: ^flags flips these bits only)

(OP_NO_MATCH, OP_EMPTY_MATCH, OP_LITERAL, OP_CHAR_CLASS, OP_ANY_CHAR_NOT_NL, OP_ANY_CHAR, OP_BEGIN_LINE, OP_END_LINE,
 OP_BEGIN_TEXT, OP_END_TEXT, OP_WORD_BOUNDARY, OP_NO_WORD_BOUNDARY, OP_CAPTURE, OP_STAR, OP_PLUS, OP_QUEST, OP_REPEAT,
 OP_CONCAT, OP_ALTERNATE) = range(1, 20)
OP_PSEUDO = 128
OP_LEFT_PAREN, OP_VERTICAL_BAR = OP_PSEUDO, OP_PSEUDO + 1

MAX_HEIGHT = 1000
INST_SIZE = 5 * 8
MAX_SIZE = (128 << 20) // INST_SIZE
RUNE_SIZE = 4
MAX_RUNES = (128 << 20) // RUNE_SIZE
MAX_RUNE = 0x10FFFF
MIN_FOLD, MAX_FOLD = 0x0041, 0x1E943

# ------------------------------------------------------------ unicode (Go's tables)

_UNICODE, _UNICODE_LOCK = None, threading.Lock()
UNICODE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "go_unicode.json.gz")


def unicode_tables():
    """Go's unicode tables (see unidump): loaded once, when a pattern needs them."""
    global _UNICODE
    with _UNICODE_LOCK:
        if _UNICODE is None:
            with gzip.open(UNICODE_FILE, "rt", encoding="utf-8") as f:
                data = json.load(f)
            data["simple_fold"] = {r: f for r, f in data["simple_fold"]}
            _UNICODE = data
        return _UNICODE


def simple_fold(r):
    """unicode.SimpleFold."""
    if r < 0 or r > MAX_RUNE:
        return r
    if r < 0x80 and not (0x41 <= r <= 0x5A or 0x61 <= r <= 0x7A or r == 0x4B or r == 0x53):
        return r  # (ASCII without a fold: no table needed)
    return unicode_tables()["simple_fold"].get(r, r)


ANY_TABLE = [(0, 0xFFFF, 1), (0x10000, MAX_RUNE, 1)]
ASCII_TABLE = [(0, 0x7F, 1)]
ASCII_FOLD_TABLE = [(0, 0x7F, 1), (0x017F, 0x017F, 1), (0x212A, 0x212A, 1)]


def canonical_name(name):
    """canonicalName: a leading uppercase letter, then lowercase; no underscores, spaces or hyphens."""
    out, first = [], True
    for c in name.encode("utf-8"):
        if c in (0x5F, 0x2D, 0x20):
            continue
        if first:
            if 0x61 <= c <= 0x7A:
                c -= 0x20
            first = False
        elif 0x41 <= c <= 0x5A:
            c += 0x20
        out.append(c)
    return bytes(out).decode("utf-8", "surrogateescape")


def unicode_table(name):
    """unicodeTable: (table, fold table, sign), or (None, None, 0) for no such name."""
    data = unicode_tables()
    name = canonical_name(name)
    if name == "Any":
        return ANY_TABLE, ANY_TABLE, 1
    if name == "Assigned":
        return data["cn"], data["cn"], -1
    if name == "Ascii":
        return ASCII_TABLE, ASCII_FOLD_TABLE, 1
    if name == "Lc":
        return data["categories"].get("LC"), data["fold_category"].get("LC"), 1
    if name in data["categories"]:
        return data["categories"][name], data["fold_category"].get(name), 1
    if name in data["scripts"]:
        return data["scripts"][name], data["fold_script"].get(name), 1
    aliases = {canonical_name(k): v for k, v in data["category_aliases"].items()}
    actual = aliases.get(name)
    if actual:
        return data["categories"].get(actual), data["fold_category"].get(actual), 1
    return None, None, 0


PERL_GROUPS = {"\\d": (1, [0x30, 0x39]), "\\D": (-1, [0x30, 0x39]),
               "\\s": (1, [0x9, 0xA, 0xC, 0xD, 0x20, 0x20]), "\\S": (-1, [0x9, 0xA, 0xC, 0xD, 0x20, 0x20]),
               "\\w": (1, [0x30, 0x39, 0x41, 0x5A, 0x5F, 0x5F, 0x61, 0x7A]),
               "\\W": (-1, [0x30, 0x39, 0x41, 0x5A, 0x5F, 0x5F, 0x61, 0x7A])}
_POSIX = {"alnum": [0x30, 0x39, 0x41, 0x5A, 0x61, 0x7A], "alpha": [0x41, 0x5A, 0x61, 0x7A], "ascii": [0x0, 0x7F],
          "blank": [0x9, 0x9, 0x20, 0x20], "cntrl": [0x0, 0x1F, 0x7F, 0x7F], "digit": [0x30, 0x39],
          "graph": [0x21, 0x7E], "lower": [0x61, 0x7A], "print": [0x20, 0x7E],
          "punct": [0x21, 0x2F, 0x3A, 0x40, 0x5B, 0x60, 0x7B, 0x7E], "space": [0x9, 0xD, 0x20, 0x20],
          "upper": [0x41, 0x5A], "word": [0x30, 0x39, 0x41, 0x5A, 0x5F, 0x5F, 0x61, 0x7A],
          "xdigit": [0x30, 0x39, 0x41, 0x46, 0x61, 0x66]}
POSIX_GROUPS = {**{f"[:{k}:]": (1, v) for k, v in _POSIX.items()}, **{f"[:^{k}:]": (-1, v) for k, v in _POSIX.items()}}

# ------------------------------------------------------------ regexp/syntax: the parse tree


class Regexp:
    """syntax.Regexp (the fields the parser uses)."""
    __slots__ = ("op", "flags", "sub", "rune", "min", "max", "cap", "name", "next_free")

    def __init__(self, op=0):
        self.reset()
        self.op = op

    def reset(self):
        self.op, self.flags, self.sub, self.rune, self.min, self.max, self.cap, self.name = 0, 0, [], [], 0, 0, 0, ""
        self.next_free = None


def regexp_equal(x, y):
    """Regexp.Equal."""
    if x is None or y is None:
        return x is y
    if x.op != y.op:
        return False
    if x.op == OP_END_TEXT:
        if x.flags & WAS_DOLLAR != y.flags & WAS_DOLLAR:
            return False
    elif x.op in (OP_LITERAL, OP_CHAR_CLASS):
        return x.flags & FOLD_CASE == y.flags & FOLD_CASE and x.rune == y.rune
    elif x.op in (OP_ALTERNATE, OP_CONCAT):
        if len(x.sub) != len(y.sub):
            return False
        for a, b in zip(x.sub, y.sub, strict=True):
            if not regexp_equal(a, b):
                return False
        return True
    elif x.op in (OP_STAR, OP_PLUS, OP_QUEST):
        if x.flags & NON_GREEDY != y.flags & NON_GREEDY or not regexp_equal(x.sub[0], y.sub[0]):
            return False
    elif x.op == OP_REPEAT:
        if x.flags & NON_GREEDY != y.flags & NON_GREEDY or x.min != y.min or x.max != y.max \
                or not regexp_equal(x.sub[0], y.sub[0]):
            return False
    elif x.op == OP_CAPTURE:
        if x.cap != y.cap or x.name != y.name or not regexp_equal(x.sub[0], y.sub[0]):
            return False
    return True


class _Limit(Exception):
    """The parser's panic(ErrLarge) or panic(ErrNestingDepth)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _isalnum(c):
    return "0" <= c <= "9" or "A" <= c <= "Z" or "a" <= c <= "z"


def _unhex(c):
    if "0" <= c <= "9":
        return ord(c) - 0x30
    if "a" <= c <= "f":
        return ord(c) - 0x61 + 10
    if "A" <= c <= "F":
        return ord(c) - 0x41 + 10
    return -1


def min_fold_rune(r):
    if r < MIN_FOLD or r > MAX_FOLD:
        return r
    m, r0 = r, r
    r = simple_fold(r)
    while r != r0:
        m = min(m, r)
        r = simple_fold(r)
    return m


def is_char_class(re):
    return re.op == OP_LITERAL and len(re.rune) == 1 or re.op in (OP_CHAR_CLASS, OP_ANY_CHAR_NOT_NL, OP_ANY_CHAR)


def match_rune(re, r):
    if re.op == OP_LITERAL:
        return len(re.rune) == 1 and re.rune[0] == r
    if re.op == OP_CHAR_CLASS:
        return any(re.rune[i] <= r <= re.rune[i + 1] for i in range(0, len(re.rune), 2))
    if re.op == OP_ANY_CHAR_NOT_NL:
        return r != 0x0A
    return re.op == OP_ANY_CHAR


def clean_class(r):
    """cleanClass: sorted by lo increasing, hi decreasing, abutting and overlapping ranges merged."""
    pairs = sorted(((r[i], r[i + 1]) for i in range(0, len(r), 2)), key=lambda p: (p[0], -p[1]))
    if not pairs:
        return []
    out = [pairs[0][0], pairs[0][1]]
    for lo, hi in pairs[1:]:
        if lo <= out[-1] + 1:
            if hi > out[-1]:
                out[-1] = hi
            continue
        out += [lo, hi]
    return out


def append_range(r, lo, hi):
    """appendRange: the last or next-to-last range grows when lo-hi overlaps or abuts it."""
    n = len(r)
    for i in (2, 4):
        if n >= i:
            rlo, rhi = r[n - i], r[n - i + 1]
            if lo <= rhi + 1 and rlo <= hi + 1:
                if lo < rlo:
                    r[n - i] = lo
                if hi > rhi:
                    r[n - i + 1] = hi
                return r
    r += [lo, hi]
    return r


def append_folded_range(r, lo, hi):
    if lo <= MIN_FOLD and hi >= MAX_FOLD:
        return append_range(r, lo, hi)
    if hi < MIN_FOLD or lo > MAX_FOLD:
        return append_range(r, lo, hi)
    if lo < MIN_FOLD:
        r = append_range(r, lo, MIN_FOLD - 1)
        lo = MIN_FOLD
    if hi > MAX_FOLD:
        r = append_range(r, MAX_FOLD + 1, hi)
        hi = MAX_FOLD
    for c in range(lo, hi + 1):
        r = append_range(r, c, c)
        f = simple_fold(c)
        while f != c:
            r = append_range(r, f, f)
            f = simple_fold(f)
    return r


def append_literal(r, x, flags):
    return append_folded_range(r, x, x) if flags & FOLD_CASE else append_range(r, x, x)


def append_class(r, x):
    for i in range(0, len(x), 2):
        r = append_range(r, x[i], x[i + 1])
    return r


def append_folded_class(r, x):
    for i in range(0, len(x), 2):
        r = append_folded_range(r, x[i], x[i + 1])
    return r


def append_negated_class(r, x):
    next_lo = 0
    for i in range(0, len(x), 2):
        lo, hi = x[i], x[i + 1]
        if next_lo <= lo - 1:
            r = append_range(r, next_lo, lo - 1)
        next_lo = hi + 1
    if next_lo <= MAX_RUNE:
        r = append_range(r, next_lo, MAX_RUNE)
    return r


def append_table(r, x):
    for lo, hi, stride in x:
        if stride == 1:
            r = append_range(r, lo, hi)
            continue
        for c in range(lo, hi + 1, stride):
            r = append_range(r, c, c)
    return r


def append_negated_table(r, x):
    next_lo = 0
    for lo, hi, stride in x:
        if stride == 1:
            if next_lo <= lo - 1:
                r = append_range(r, next_lo, lo - 1)
            next_lo = hi + 1
            continue
        for c in range(lo, hi + 1, stride):
            if next_lo <= c - 1:
                r = append_range(r, next_lo, c - 1)
            next_lo = c + 1
    if next_lo <= MAX_RUNE:
        r = append_range(r, next_lo, MAX_RUNE)
    return r


def negate_class(r):
    out, next_lo = [], 0
    for i in range(0, len(r), 2):
        lo, hi = r[i], r[i + 1]
        if next_lo <= lo - 1:
            out += [next_lo, lo - 1]
        next_lo = hi + 1
    if next_lo <= MAX_RUNE:
        out += [next_lo, MAX_RUNE]
    return out


def clean_alt(re):
    if re.op == OP_CHAR_CLASS:
        re.rune = clean_class(re.rune)
        if len(re.rune) == 2 and re.rune[0] == 0 and re.rune[1] == MAX_RUNE:
            re.rune, re.op = [], OP_ANY_CHAR
            return
        if len(re.rune) == 4 and re.rune == [0, 0x0A - 1, 0x0A + 1, MAX_RUNE]:
            re.rune, re.op = [], OP_ANY_CHAR_NOT_NL


def merge_char_class(dst, src):
    """dst = dst|src (dst.op >= src.op)."""
    if dst.op == OP_ANY_CHAR:
        return
    if dst.op == OP_ANY_CHAR_NOT_NL:
        if match_rune(src, 0x0A):
            dst.op = OP_ANY_CHAR
    elif dst.op == OP_CHAR_CLASS:
        if src.op == OP_LITERAL:
            dst.rune = append_literal(dst.rune, src.rune[0], src.flags)
        else:
            dst.rune = append_class(dst.rune, src.rune)
    elif dst.op == OP_LITERAL:
        if src.rune[0] == dst.rune[0] and src.flags == dst.flags:
            return
        dst.op = OP_CHAR_CLASS
        first = dst.rune[0]
        dst.rune = append_literal([], first, dst.flags)
        dst.rune = append_literal(dst.rune, src.rune[0], src.flags)


def repeat_is_valid(re, n):
    if re.op == OP_REPEAT:
        m = re.max
        if m == 0:
            return True
        if m < 0:
            m = re.min
        if m > n:
            return False
        if m > 0:
            n //= m
    for sub in re.sub:
        if not repeat_is_valid(sub, n):
            return False
    return True


class Parser:
    """regexp/syntax's parser (see parse.go)."""

    def __init__(self, flags, whole):
        self.flags, self.whole = flags, whole
        self.stack, self.free, self.num_cap, self.tmp_class = [], None, 0, []
        self.num_regexp, self.num_runes, self.repeats = 0, 0, 0
        self.height, self.size = None, None

    def new_regexp(self, op):
        re = self.free
        if re is not None:
            self.free = re.next_free
            re.reset()
        else:
            re = Regexp()
            self.num_regexp += 1
        re.op = op
        return re

    def reuse(self, re):
        if self.height is not None:
            self.height.pop(re, None)
        re.next_free = self.free
        self.free = re

    def check_limits(self, re):
        if self.num_runes > MAX_RUNES:
            raise _Limit(ERR_LARGE)
        self.check_size(re)
        self.check_height(re)

    def check_size(self, re):
        if self.size is None:
            if self.repeats == 0:
                self.repeats = 1
            if re.op == OP_REPEAT:
                n = re.max
                if n == -1:
                    n = re.min
                if n <= 0:
                    n = 1
                if n > MAX_SIZE // self.repeats:
                    self.repeats = MAX_SIZE
                else:
                    self.repeats *= n
            if self.num_regexp < MAX_SIZE // self.repeats:
                return
            self.size = {}
            for s in self.stack:
                self.check_size(s)
        if self.calc_size(re, True) > MAX_SIZE:
            raise _Limit(ERR_LARGE)

    def calc_size(self, re, force):
        if not force and re in self.size:
            return self.size[re]
        op = re.op
        if op == OP_LITERAL:
            size = len(re.rune)
        elif op in (OP_CAPTURE, OP_STAR):
            size = 2 + self.calc_size(re.sub[0], False)
        elif op in (OP_PLUS, OP_QUEST):
            size = 1 + self.calc_size(re.sub[0], False)
        elif op in (OP_CONCAT, OP_ALTERNATE):
            size = 0
            for s in re.sub:
                size += self.calc_size(s, False)
            if op == OP_ALTERNATE and len(re.sub) > 1:
                size += len(re.sub) - 1
        elif op == OP_REPEAT:
            sub = self.calc_size(re.sub[0], False)
            if re.max == -1:
                size = 2 + sub if re.min == 0 else 1 + re.min * sub
            else:
                size = re.max * sub + (re.max - re.min)
        else:
            size = 0
        size = max(1, size)
        self.size[re] = size
        return size

    def check_height(self, re):
        if self.num_regexp < MAX_HEIGHT:
            return
        if self.height is None:
            self.height = {}
            for s in self.stack:
                self.check_height(s)
        if self.calc_height(re, True) > MAX_HEIGHT:
            raise _Limit(ERR_NESTING_DEPTH)

    def calc_height(self, re, force):
        if not force and re in self.height:
            return self.height[re]
        h = 1
        for s in re.sub:
            hs = self.calc_height(s, False)
            if h < 1 + hs:
                h = 1 + hs
        self.height[re] = h
        return h

    # Parse stack manipulation.

    def push(self, re):
        self.num_runes += len(re.rune)
        r = re.rune
        if re.op == OP_CHAR_CLASS and len(r) == 2 and r[0] == r[1]:
            if self.maybe_concat(r[0], self.flags & ~FOLD_CASE):
                return None
            re.op, re.rune, re.flags = OP_LITERAL, r[:1], self.flags & ~FOLD_CASE
        elif re.op == OP_CHAR_CLASS and len(r) == 4 and r[0] == r[1] and r[2] == r[3] \
                and simple_fold(r[0]) == r[2] and simple_fold(r[2]) == r[0] \
                or re.op == OP_CHAR_CLASS and len(r) == 2 and r[0] + 1 == r[1] \
                and simple_fold(r[0]) == r[1] and simple_fold(r[1]) == r[0]:
            if self.maybe_concat(r[0], self.flags | FOLD_CASE):
                return None
            re.op, re.rune, re.flags = OP_LITERAL, r[:1], self.flags | FOLD_CASE
        else:
            self.maybe_concat(-1, 0)
        self.stack.append(re)
        self.check_limits(re)
        return re

    def maybe_concat(self, r, flags):
        n = len(self.stack)
        if n < 2:
            return False
        re1, re2 = self.stack[n - 1], self.stack[n - 2]
        if re1.op != OP_LITERAL or re2.op != OP_LITERAL or re1.flags & FOLD_CASE != re2.flags & FOLD_CASE:
            return False
        re2.rune = re2.rune + re1.rune
        if r >= 0:
            re1.rune, re1.flags = [r], flags
            return True
        self.stack.pop()
        self.reuse(re1)
        return False

    def literal(self, r):
        re = self.new_regexp(OP_LITERAL)
        re.flags = self.flags
        if self.flags & FOLD_CASE:
            r = min_fold_rune(r)
        re.rune = [r]
        self.push(re)

    def op(self, op):
        re = self.new_regexp(op)
        re.flags = self.flags
        return self.push(re)

    def repeat(self, op, lo, hi, before, after, last_repeat):
        """(after, error): before is the pattern from the operator on, after the rest after it."""
        flags = self.flags
        if self.flags & PERL_X:
            if after and after[0] == "?":
                after = after[1:]
                flags ^= NON_GREEDY
            if last_repeat:
                return "", Error(ERR_INVALID_REPEAT_OP, last_repeat[:len(last_repeat) - len(after)])
        n = len(self.stack)
        if n == 0 or self.stack[n - 1].op >= OP_PSEUDO:
            return "", Error(ERR_MISSING_REPEAT_ARGUMENT, before[:len(before) - len(after)])
        sub = self.stack[n - 1]
        re = self.new_regexp(op)
        re.min, re.max, re.flags, re.sub = lo, hi, flags, [sub]
        self.stack[n - 1] = re
        self.check_limits(re)
        if op == OP_REPEAT and (lo >= 2 or hi >= 2) and not repeat_is_valid(re, 1000):
            return "", Error(ERR_INVALID_REPEAT_SIZE, before[:len(before) - len(after)])
        return after, None

    def concat(self):
        self.maybe_concat(-1, 0)
        i = len(self.stack)
        while i > 0 and self.stack[i - 1].op < OP_PSEUDO:
            i -= 1
        subs, self.stack = self.stack[i:], self.stack[:i]
        if not subs:
            return self.push(self.new_regexp(OP_EMPTY_MATCH))
        return self.push(self.collapse(subs, OP_CONCAT))

    def alternate(self):
        i = len(self.stack)
        while i > 0 and self.stack[i - 1].op < OP_PSEUDO:
            i -= 1
        subs, self.stack = self.stack[i:], self.stack[:i]
        if subs:
            clean_alt(subs[-1])
        if not subs:
            return self.push(self.new_regexp(OP_NO_MATCH))
        return self.push(self.collapse(subs, OP_ALTERNATE))

    def collapse(self, subs, op):
        if len(subs) == 1:
            return subs[0]
        re = self.new_regexp(op)
        re.sub = []
        for sub in subs:
            if sub.op == op:
                re.sub += sub.sub
                self.reuse(sub)
            else:
                re.sub.append(sub)
        if op == OP_ALTERNATE:
            re.sub = self.factor(re.sub)
            if len(re.sub) == 1:
                old = re
                re = re.sub[0]
                self.reuse(old)
        return re

    def factor(self, sub):
        if len(sub) < 2:
            return sub
        # Round 1: common literal prefixes.
        s, strflags, start, out = [], 0, 0, []
        for i in range(len(sub) + 1):
            istr, iflags = [], 0
            if i < len(sub):
                istr, iflags = self.leading_string(sub[i])
                if iflags == strflags:
                    same = 0
                    while same < len(s) and same < len(istr) and s[same] == istr[same]:
                        same += 1
                    if same > 0:
                        s = s[:same]
                        continue
            if i == start:
                pass
            elif i == start + 1:
                out.append(sub[start])
            else:
                prefix = self.new_regexp(OP_LITERAL)
                prefix.flags, prefix.rune = strflags, list(s)
                for j in range(start, i):
                    sub[j] = self.remove_leading_string(sub[j], len(s))
                    self.check_limits(sub[j])
                suffix = self.collapse(sub[start:i], OP_ALTERNATE)
                re = self.new_regexp(OP_CONCAT)
                re.sub = [prefix, suffix]
                out.append(re)
            start, s, strflags = i, istr, iflags
        sub = out
        # Round 2: common simple prefixes (a character class, or a fixed repeat of one).
        start, out, first = 0, [], None
        for i in range(len(sub) + 1):
            ifirst = None
            if i < len(sub):
                ifirst = self.leading_regexp(sub[i])
                if first is not None and regexp_equal(first, ifirst) and (
                        is_char_class(first) or first.op == OP_REPEAT and first.min == first.max
                        and is_char_class(first.sub[0])):
                    continue
            if i == start:
                pass
            elif i == start + 1:
                out.append(sub[start])
            else:
                prefix = first
                for j in range(start, i):
                    sub[j] = self.remove_leading_regexp(sub[j], j != start)
                    self.check_limits(sub[j])
                suffix = self.collapse(sub[start:i], OP_ALTERNATE)
                re = self.new_regexp(OP_CONCAT)
                re.sub = [prefix, suffix]
                out.append(re)
            start, first = i, ifirst
        sub = out
        # Round 3: runs of single literals and classes into one class.
        start, out = 0, []
        for i in range(len(sub) + 1):
            if i < len(sub) and is_char_class(sub[i]):
                continue
            if i == start:
                pass
            elif i == start + 1:
                out.append(sub[start])
            else:
                most = start
                for j in range(start + 1, i):
                    if sub[most].op < sub[j].op or sub[most].op == sub[j].op and len(sub[most].rune) < len(sub[j].rune):
                        most = j
                sub[start], sub[most] = sub[most], sub[start]
                for j in range(start + 1, i):
                    merge_char_class(sub[start], sub[j])
                    self.reuse(sub[j])
                clean_alt(sub[start])
                out.append(sub[start])
            if i < len(sub):
                out.append(sub[i])
            start = i + 1
        sub = out
        # Round 4: runs of empty matches into one.
        return [sub[i] for i in range(len(sub))
                if not (i + 1 < len(sub) and sub[i].op == OP_EMPTY_MATCH and sub[i + 1].op == OP_EMPTY_MATCH)]

    def leading_string(self, re):
        if re.op == OP_CONCAT and re.sub:
            re = re.sub[0]
        if re.op != OP_LITERAL:
            return [], 0
        return re.rune, re.flags & FOLD_CASE

    def remove_leading_string(self, re, n):
        if re.op == OP_CONCAT and re.sub:
            sub = self.remove_leading_string(re.sub[0], n)
            re.sub[0] = sub
            if sub.op == OP_EMPTY_MATCH:
                self.reuse(sub)
                if len(re.sub) in (0, 1):
                    re.op, re.sub = OP_EMPTY_MATCH, []
                elif len(re.sub) == 2:
                    old = re
                    re = re.sub[1]
                    self.reuse(old)
                else:
                    re.sub = re.sub[1:]
            return re
        if re.op == OP_LITERAL:
            re.rune = re.rune[n:]
            if not re.rune:
                re.op = OP_EMPTY_MATCH
        return re

    def leading_regexp(self, re):
        if re.op == OP_EMPTY_MATCH:
            return None
        if re.op == OP_CONCAT and re.sub:
            sub = re.sub[0]
            return None if sub.op == OP_EMPTY_MATCH else sub
        return re

    def remove_leading_regexp(self, re, reuse):
        if re.op == OP_CONCAT and re.sub:
            if reuse:
                self.reuse(re.sub[0])
            re.sub = re.sub[1:]
            if len(re.sub) == 0:
                re.op, re.sub = OP_EMPTY_MATCH, []
            elif len(re.sub) == 1:
                old = re
                re = re.sub[0]
                self.reuse(old)
            return re
        if reuse:
            self.reuse(re)
        return self.new_regexp(OP_EMPTY_MATCH)

    # Parsing.

    def parse(self):
        p, t = self, self.whole
        last_repeat = ""
        while t:
            repeat = ""
            c = t[0]
            if c == "(":
                if p.flags & PERL_X and len(t) >= 2 and t[1] == "?":
                    t, err = p.parse_perl_flags(t)
                    if err:
                        raise err
                else:
                    p.num_cap += 1
                    p.op(OP_LEFT_PAREN).cap = p.num_cap
                    t = t[1:]
            elif c == "|":
                p.parse_vertical_bar()
                t = t[1:]
            elif c == ")":
                err = p.parse_right_paren()
                if err:
                    raise err
                t = t[1:]
            elif c == "^":
                p.op(OP_BEGIN_TEXT if p.flags & ONE_LINE else OP_BEGIN_LINE)
                t = t[1:]
            elif c == "$":
                if p.flags & ONE_LINE:
                    p.op(OP_END_TEXT).flags |= WAS_DOLLAR
                else:
                    p.op(OP_END_LINE)
                t = t[1:]
            elif c == ".":
                p.op(OP_ANY_CHAR if p.flags & DOT_NL else OP_ANY_CHAR_NOT_NL)
                t = t[1:]
            elif c == "[":
                t, err = p.parse_class(t)
                if err:
                    raise err
            elif c in "*+?":
                before, after = t, t[1:]
                op = {"*": OP_STAR, "+": OP_PLUS, "?": OP_QUEST}[c]
                after, err = p.repeat(op, 0, 0, before, after, last_repeat)
                if err:
                    raise err
                repeat, t = before, after
            elif c == "{":
                before = t
                parsed = p.parse_repeat(t)
                if parsed is None:
                    p.literal(0x7B)  # (a { that starts no repeat is itself)
                    t = t[1:]
                else:
                    lo, hi, after = parsed
                    if lo < 0 or lo > 1000 or hi > 1000 or hi >= 0 and lo > hi:
                        raise Error(ERR_INVALID_REPEAT_SIZE, before[:len(before) - len(after)])
                    after, err = p.repeat(OP_REPEAT, lo, hi, before, after, last_repeat)
                    if err:
                        raise err
                    repeat, t = before, after
            elif c == "\\":
                t = p.parse_backslash(t)
            else:
                p.literal(ord(c))
                t = t[1:]
            last_repeat = repeat
        p.concat()
        if p.swap_vertical_bar():
            p.stack.pop()
        p.alternate()
        if len(p.stack) != 1:
            raise Error(ERR_MISSING_PAREN, p.whole)
        return p.stack[0]

    def parse_backslash(self, t):
        p = self
        if p.flags & PERL_X and len(t) >= 2:
            c = t[1]
            if c == "A":
                p.op(OP_BEGIN_TEXT)
                return t[2:]
            if c == "b":
                p.op(OP_WORD_BOUNDARY)
                return t[2:]
            if c == "B":
                p.op(OP_NO_WORD_BOUNDARY)
                return t[2:]
            if c == "C":
                raise Error(ERR_INVALID_ESCAPE, t[:2])
            if c == "Q":
                lit, sep, rest = t[2:].partition("\\E")
                for ch in lit:
                    p.literal(ord(ch))
                return rest if sep else ""
            if c == "z":
                p.op(OP_END_TEXT)
                return t[2:]
        re = p.new_regexp(OP_CHAR_CLASS)
        re.flags = p.flags
        if len(t) >= 2 and t[1] in "pP":
            r, rest, err = p.parse_unicode_class(t, [])
            if err:
                raise err
            if r is not None:
                re.rune = r
                p.push(re)
                return rest
        r, rest = p.parse_perl_class_escape(t, [])
        if r is not None:
            re.rune = r
            p.push(re)
            return rest
        p.reuse(re)
        c, t, err = p.parse_escape(t)
        if err:
            raise err
        p.literal(c)
        return t

    def parse_repeat(self, s):
        """parseRepeat: (min, max, rest) for {min}, {min,} (max -1) or {min,max}; None if s is
        not of that form (min -1 when a number is too large)."""
        if not s or s[0] != "{":
            return None
        s = s[1:]
        lo, s = self.parse_int(s)
        if lo is None or not s:
            return None
        if s[0] != ",":
            hi = lo
        else:
            s = s[1:]
            if not s:
                return None
            if s[0] == "}":
                hi = -1
            else:
                hi, s = self.parse_int(s)
                if hi is None:
                    return None
                if hi < 0:
                    lo = -1
        if not s or s[0] != "}":
            return None
        return lo, hi, s[1:]

    @staticmethod
    def parse_int(s):
        """parseInt: (n, rest) (n -1 when too large), or (None, s) with no digits or a leading 0."""
        if not s or not "0" <= s[0] <= "9":
            return None, s
        if len(s) >= 2 and s[0] == "0" and "0" <= s[1] <= "9":
            return None, s
        i = 0
        while i < len(s) and "0" <= s[i] <= "9":
            i += 1
        n = 0
        for d in s[:i]:
            if n >= 100_000_000:
                n = -1
                break
            n = n * 10 + ord(d) - 0x30
        return n, s[i:]

    def parse_perl_flags(self, s):
        t = s
        starts_with_p = len(t) > 4 and t[2] == "P" and t[3] == "<"
        starts_with_name = len(t) > 3 and t[2] == "<"
        if starts_with_p or starts_with_name:
            expr_start = 3 if starts_with_name else 4
            end = t.find(">")
            if end < 0:
                return "", Error(ERR_INVALID_NAMED_CAPTURE, s)
            capture, name = t[:end + 1], t[expr_start:end]
            if not name or not all(c == "_" or _isalnum(c) for c in name):
                return "", Error(ERR_INVALID_NAMED_CAPTURE, capture)
            self.num_cap += 1
            re = self.op(OP_LEFT_PAREN)
            re.cap, re.name = self.num_cap, name
            return t[end + 1:], None
        t = t[2:]
        flags, sign, saw_flag = self.flags, 1, False
        while t:
            c, t = t[0], t[1:]
            if c == "i":
                flags |= FOLD_CASE
                saw_flag = True
            elif c == "m":
                flags &= ~ONE_LINE
                saw_flag = True
            elif c == "s":
                flags |= DOT_NL
                saw_flag = True
            elif c == "U":
                flags |= NON_GREEDY
                saw_flag = True
            elif c == "-":
                if sign < 0:
                    break
                sign, flags, saw_flag = -1, ~flags & FLAGS_MASK, False
            elif c in ":)":
                if sign < 0:
                    if not saw_flag:
                        break
                    flags = ~flags & FLAGS_MASK
                if c == ":":
                    self.op(OP_LEFT_PAREN)
                self.flags = flags
                return t, None
            else:
                break
        return "", Error(ERR_INVALID_PERL_OP, s[:len(s) - len(t)])

    def parse_vertical_bar(self):
        self.concat()
        if not self.swap_vertical_bar():
            self.op(OP_VERTICAL_BAR)

    def swap_vertical_bar(self):
        st, n = self.stack, len(self.stack)
        if n >= 3 and st[n - 2].op == OP_VERTICAL_BAR and is_char_class(st[n - 1]) and is_char_class(st[n - 3]):
            re1, re3 = st[n - 1], st[n - 3]
            if re1.op > re3.op:
                re1, re3 = re3, re1
                st[n - 3] = re3
            merge_char_class(re3, re1)
            self.reuse(re1)
            st.pop()
            return True
        if n >= 2:
            re1, re2 = st[n - 1], st[n - 2]
            if re2.op == OP_VERTICAL_BAR:
                if n >= 3:
                    clean_alt(st[n - 3])
                st[n - 2], st[n - 1] = re1, re2
                return True
        return False

    def parse_right_paren(self):
        self.concat()
        if self.swap_vertical_bar():
            self.stack.pop()
        self.alternate()
        n = len(self.stack)
        if n < 2:
            return Error(ERR_UNEXPECTED_PAREN, self.whole)
        re1, re2 = self.stack[n - 1], self.stack[n - 2]
        self.stack = self.stack[:n - 2]
        if re2.op != OP_LEFT_PAREN:
            return Error(ERR_UNEXPECTED_PAREN, self.whole)
        self.flags = re2.flags
        if re2.cap == 0:
            self.push(re1)
        else:
            re2.op, re2.sub = OP_CAPTURE, [re1]
            self.push(re2)
        return None

    def parse_escape(self, s):
        """parseEscape: (rune, rest, error)."""
        t = s[1:]
        if not t:
            return 0, "", Error(ERR_TRAILING_BACKSLASH, "")
        c, t = t[0], t[1:]
        if c in "1234567" and t and "0" <= t[0] <= "7" or c == "0":
            r = ord(c) - 0x30
            for _ in range(1, 3):
                if not t or not "0" <= t[0] <= "7":
                    break
                r = r * 8 + ord(t[0]) - 0x30
                t = t[1:]
            return r, t, None
        if c in "1234567":
            pass  # (a backreference: not supported)
        elif c == "x":
            if t:
                c, t = t[0], t[1:]
                if c == "{":
                    nhex, r = 0, 0
                    while True:
                        if not t:
                            break
                        c, t = t[0], t[1:]
                        if c == "}":
                            if nhex:
                                return r, t, None
                            break
                        v = _unhex(c)
                        if v < 0:
                            break
                        r = r * 16 + v
                        if r > MAX_RUNE:
                            break
                        nhex += 1
                else:
                    x = _unhex(c)
                    y = -1  # (at the end of s, nextRune gives RuneError and no error: no hex digit)
                    if t:
                        c, t = t[0], t[1:]
                        y = _unhex(c)
                    if x >= 0 and y >= 0:
                        return x * 16 + y, t, None
        elif c in "afnrtv":
            return {"a": 7, "f": 12, "n": 10, "r": 13, "t": 9, "v": 11}[c], t, None
        elif ord(c) < 0x80 and not _isalnum(c):
            return ord(c), t, None
        return 0, "", Error(ERR_INVALID_ESCAPE, s[:len(s) - len(t)])

    def parse_class_char(self, s, whole_class):
        if not s:
            return 0, "", Error(ERR_MISSING_BRACKET, whole_class)
        if s[0] == "\\":
            return self.parse_escape(s)
        return ord(s[0]), s[1:], None

    def parse_perl_class_escape(self, s, r):
        if not self.flags & PERL_X or len(s) < 2 or s[0] != "\\":
            return None, s
        g = PERL_GROUPS.get(s[:2])
        if g is None:
            return None, s
        return self.append_group(r, g), s[2:]

    def parse_named_class(self, s, r):
        if len(s) < 2 or s[0] != "[" or s[1] != ":":
            return None, s, None
        i = s.find(":]", 2)
        if i < 0:
            return None, s, None
        name, s = s[:i + 2], s[i + 2:]
        g = POSIX_GROUPS.get(name)
        if g is None:
            return None, "", Error(ERR_INVALID_CHAR_RANGE, name)
        return self.append_group(r, g), s, None

    def append_group(self, r, g):
        sign, cls = g
        if not self.flags & FOLD_CASE:
            return append_negated_class(r, cls) if sign < 0 else append_class(r, cls)
        tmp = clean_class(append_folded_class([], cls))
        return append_negated_class(r, tmp) if sign < 0 else append_class(r, tmp)

    def parse_unicode_class(self, s, r):
        """(class, rest, error); class None when s does not start a Unicode class."""
        if not self.flags & UNICODE_GROUPS or len(s) < 2 or s[0] != "\\" or s[1] not in "pP":
            return None, s, None
        sign = -1 if s[1] == "P" else 1
        t = s[2:]
        c, t = (t[0], t[1:]) if t else (None, "")  # (at the end of s, nextRune gives RuneError and no error)
        if c != "{":
            seq = s[:len(s) - len(t)]
            name = seq[2:]  # (at the end of s: "", which names no table)
        else:
            end = s.find("}")
            if end < 0:
                return None, "", Error(ERR_INVALID_CHAR_RANGE, s)
            seq, t, name = s[:end + 1], s[end + 1:], s[3:end]
        if name and name[0] == "^":
            sign, name = -sign, name[1:]
        tab, fold, tsign = unicode_table(name)
        if tab is None:
            return None, "", Error(ERR_INVALID_CHAR_RANGE, seq)
        if tsign < 0:
            sign = -sign
        if not self.flags & FOLD_CASE or fold is None:
            r = append_table(r, tab) if sign > 0 else append_negated_table(r, tab)
        else:
            tmp = clean_class(append_table(append_table([], tab), fold))
            r = append_class(r, tmp) if sign > 0 else append_negated_class(r, tmp)
        return r, t, None

    def parse_class(self, s):
        t = s[1:]
        re = self.new_regexp(OP_CHAR_CLASS)
        re.flags = self.flags
        cls = []
        sign = 1
        if t and t[0] == "^":
            sign, t = -1, t[1:]
            if not self.flags & CLASS_NL:
                cls += [0x0A, 0x0A]
        first = True
        while not t or t[0] != "]" or first:
            if t and t[0] == "-" and not self.flags & PERL_X and not first and (len(t) == 1 or t[1] != "]"):
                return "", Error(ERR_INVALID_CHAR_RANGE, t[:2])
            first = False
            if len(t) > 2 and t[0] == "[" and t[1] == ":":
                ncls, nt, err = self.parse_named_class(t, cls)
                if err:
                    return "", err
                if ncls is not None:
                    cls, t = ncls, nt
                    continue
            ncls, nt, err = self.parse_unicode_class(t, cls)
            if err:
                return "", err
            if ncls is not None:
                cls, t = ncls, nt
                continue
            ncls, nt = self.parse_perl_class_escape(t, cls)
            if ncls is not None:
                cls, t = ncls, nt
                continue
            rng = t
            lo, t, err = self.parse_class_char(t, s)
            if err:
                return "", err
            hi = lo
            if len(t) >= 2 and t[0] == "-" and t[1] != "]":
                t = t[1:]
                hi, t, err = self.parse_class_char(t, s)
                if err:
                    return "", err
                if hi < lo:
                    return "", Error(ERR_INVALID_CHAR_RANGE, rng[:len(rng) - len(t)])
            cls = append_folded_range(cls, lo, hi) if self.flags & FOLD_CASE else append_range(cls, lo, hi)
        t = t[1:]
        cls = clean_class(cls)
        if sign < 0:
            cls = negate_class(cls)
        re.rune = cls
        self.push(re)
        return t, None


def parse(pattern, flags=PERL):
    """syntax.Parse: the parse tree, or Error."""
    if flags & LITERAL:
        re = Regexp(OP_LITERAL)
        re.flags, re.rune = flags, [ord(c) for c in pattern]
        return re
    p = Parser(flags, pattern)
    try:
        return p.parse()
    except _Limit as e:
        raise Error(e.code, pattern) from None


# ------------------------------------------------------------ matching (RE2 semantics)

BEGIN_LINE, END_LINE, BEGIN_TEXT, END_TEXT, WORD_BOUNDARY, NOT_WORD_BOUNDARY = range(6)
_ASSERT = {OP_BEGIN_LINE: BEGIN_LINE, OP_END_LINE: END_LINE, OP_BEGIN_TEXT: BEGIN_TEXT, OP_END_TEXT: END_TEXT,
           OP_WORD_BOUNDARY: WORD_BOUNDARY, OP_NO_WORD_BOUNDARY: NOT_WORD_BOUNDARY}
MAX_STATES = 1_000_000  # an NFA larger than this is not run here (Go's own limit is about 3.3 million)
CACHE_ELEMENTS = 2_000_000  # states held in a regex's cached steps ...
CACHE_ENTRIES = 100_000  # ... and steps, before the cache is emptied


class TooLarge(Exception):
    """The pattern compiles in Go, to an automaton too large to run here."""


class TooSlow(Exception):
    """Matching ran out of its Budget."""


def is_word(c):
    """syntax.IsWordChar (ASCII)."""
    return c is not None and ("a" <= c <= "z" or "A" <= c <= "Z" or "0" <= c <= "9" or c == "_")


def fold_orbit(r):
    """r and every rune unicode.SimpleFold cycles it through."""
    out, f = [r], simple_fold(r)
    while f != r:
        out.append(f)
        f = simple_fold(f)
    return out


class Matcher:
    """A Go regexp compiled to a Thompson NFA, matched like MatchString: anywhere in the
    text, one pass over it, a set of states per position (steps cached)."""

    def __init__(self, tree):
        self.states = []  # ("set", ranges, next) | ("split", a, b) | ("assert", kind, next) | ("match",) | ("fail",)
        self.start = self._compile(tree, self._add(("match",)))
        self.cache, self.cached = {}, 0

    def _add(self, state):
        if len(self.states) >= MAX_STATES:
            raise TooLarge(f"more than {MAX_STATES} states")
        self.states.append(state)
        return len(self.states) - 1

    def _set(self, ranges, nxt):
        return self._add(("set", tuple(ranges), nxt))

    def _compile(self, re, nxt):
        op = re.op
        if op == OP_NO_MATCH:
            return self._add(("fail",))
        if op == OP_EMPTY_MATCH:
            return nxt
        if op == OP_LITERAL:
            for r in reversed(re.rune):
                runes = fold_orbit(r) if re.flags & FOLD_CASE else [r]
                nxt = self._set(sorted((x, x) for x in set(runes)), nxt)
            return nxt
        if op == OP_CHAR_CLASS:
            return self._set([(re.rune[i], re.rune[i + 1]) for i in range(0, len(re.rune), 2)], nxt)
        if op == OP_ANY_CHAR_NOT_NL:
            return self._set([(0, 0x09), (0x0B, MAX_RUNE)], nxt)
        if op == OP_ANY_CHAR:
            return self._set([(0, MAX_RUNE)], nxt)
        if op in _ASSERT:
            return self._add(("assert", _ASSERT[op], nxt))
        if op == OP_CAPTURE:
            return self._compile(re.sub[0], nxt)
        if op == OP_CONCAT:
            for s in reversed(re.sub):
                nxt = self._compile(s, nxt)
            return nxt
        if op == OP_ALTERNATE:
            starts = [self._compile(s, nxt) for s in re.sub]
            out = starts[-1]
            for s in reversed(starts[:-1]):
                out = self._add(("split", s, out))
            return out
        lo, hi = {OP_STAR: (0, -1), OP_PLUS: (1, -1), OP_QUEST: (0, 1)}.get(op, (re.min, re.max))
        sub = re.sub[0]
        if hi == -1:  # x{lo,}: lo-1 copies of x, then one in a loop (x* when lo is 0)
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

    def _step(self, current, prev, c):
        """From the states reached before this position (and the start: a match may
        begin anywhere), with prev the character before it and c the one at it (None at
        the text's ends): (whether one matches here, the states reached after c)."""
        word = is_word(prev) != is_word(c)
        holds = {BEGIN_LINE: prev is None or prev == "\n", END_LINE: c is None or c == "\n",
                 BEGIN_TEXT: prev is None, END_TEXT: c is None,
                 WORD_BOUNDARY: word, NOT_WORD_BOUNDARY: not word}
        seen, stack, reached = set(), [self.start, *current], []
        while stack:
            s = stack.pop()
            if s in seen:
                continue
            seen.add(s)
            st = self.states[s]
            kind = st[0]
            if kind == "match":
                return True, frozenset()
            if kind == "split":
                stack += (st[1], st[2])
            elif kind == "assert":
                if holds[st[1]]:
                    stack.append(st[2])
            elif kind == "set":
                reached.append(st)
        if c is None:
            return False, frozenset()
        code = ord(c)
        return False, frozenset(st[2] for st in reached if any(lo <= code <= hi for lo, hi in st[1]))

    def search(self, text, budget=None):
        """Whether the pattern matches anywhere in text. With a budget (an object with
        check(spent) and spend(spent), CPU seconds), the time it takes is spent from it."""
        start = time.thread_time()
        current, prev, found = frozenset(), None, False
        n = len(text)
        for i in range(n + 1):
            if budget is not None and i & 63 == 63:
                budget.check(time.thread_time() - start)
            c = text[i] if i < n else None
            key = (current, prev, c)
            hit = self.cache.get(key)
            if hit is None:
                hit = self._step(current, prev, c)
                if self.cached > CACHE_ELEMENTS or len(self.cache) >= CACHE_ENTRIES:
                    self.cache, self.cached = {}, 0
                self.cache[key] = hit
                self.cached += len(current) + len(hit[1]) + 1
            if hit[0]:
                found = True
                break
            current, prev = hit[1], c
        if budget is not None:
            budget.spend(time.thread_time() - start)
        return found


def compile(pattern):  # noqa: A001  (regexp.Compile)
    """regexp.Compile(pattern) for MatchString: a Matcher; Error when Go rejects the
    pattern, TooLarge when its automaton is too large to run here."""
    return Matcher(parse(pattern, PERL))
