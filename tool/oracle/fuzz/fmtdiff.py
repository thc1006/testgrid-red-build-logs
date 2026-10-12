"""Go's fmt.Sprintf with string operands (GOCHECK, its "sprintf") against gofmt.py:
fmtdiff.py SEED CASES. Formats as a test group's test_name_config may give them:
literal text, verbs with flags, widths, precisions and argument indexes, broken
ones; operands as test names, junit properties and file names may be."""
import json
import os
import random
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import gofmt  # noqa: E402

GOCHECK = os.environ.get("GOCHECK", "gocheck")
VERBS = list("svqxXdTpwfbc%!") + ["é", "Z", "V", "S"]
TEXT = ["", "[", "]", " ", "a", "é", "日本", "x.y", "\t", "%%", "{", "}", "*", ".", "0", "9", "-"]
ARGS = ["", "a", "abc", "Test [sig-node] foo", "é", "日本語", "a\"b", "a`b", "a\\b", "\t", "\n", "\x00", "\x7f",
        "­", "﻿", "​", "\U0001f600", "́", "x" * 30, "%s", "%!", "0x1"]


def piece(rng):
    out = "%" + "".join(rng.choice("#0+- ") for _ in range(rng.choice([0, 0, 1, 2, 3])))
    if rng.random() < 0.2:
        out += f"[{rng.choice([0, 1, 2, 3, 5, 99, '', 'x', '1x', '-1'])}]"
    r = rng.random()
    if r < 0.3:
        out += str(rng.choice([0, 1, 2, 5, 10, 20, 1000001, 12345678901]))
    elif r < 0.4:
        out += "*"
    if rng.random() < 0.3:
        out += "." + rng.choice(["", "0", "1", "3", "10", "*", "[2]", "1000001"])
        if rng.random() < 0.2:
            out += f"[{rng.choice([1, 2, 9])}]"
    if rng.random() < 0.95:
        out += rng.choice(VERBS)
    return out


def case(rng):
    fmt = "".join(rng.choice([lambda: rng.choice(TEXT), lambda: piece(rng)])() for _ in range(rng.randint(0, 5)))
    if rng.random() < 0.1:
        fmt = rng.choice(["%s", "%s [%s]", "%s.%s", "%s [%s] %s", "%v-%v", "%%s"])
    return fmt, [rng.choice(ARGS) for _ in range(rng.choice([0, 1, 1, 2, 2, 3, 4]))]


def check(cases):
    lines = "".join(json.dumps({"Kind": "sprintf", "S": f, "Names": a}) + "\n" for f, a in cases)
    p = subprocess.run([GOCHECK], input=lines, capture_output=True, text=True, timeout=600)
    if p.returncode:
        raise RuntimeError(p.stderr[-3000:])
    bad = []
    for (f, a), line in zip(cases, p.stdout.splitlines(), strict=True):
        want = json.loads(line)["out"]
        got = gofmt.sprintf(f, a)
        if got != want:
            bad.append((f, a, want, got))
    return bad


if __name__ == "__main__":
    seed, n = int(sys.argv[1]), int(sys.argv[2])
    rng = random.Random(seed)
    cases = [case(rng) for _ in range(n)]
    bad = check(cases)
    for f, a, want, got in bad[:10]:
        print("MISMATCH", ascii(f), ascii(a), "go:", ascii(want), "port:", ascii(got))
    print(f"seed {seed}: {n} formats, {len(bad)} mismatches")
    sys.exit(1 if bad else 0)
