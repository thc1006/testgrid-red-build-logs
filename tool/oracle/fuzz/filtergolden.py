"""Write the tab filter's golden vectors (filtergolden.py OUT.jsonl.gz): what TestGrid's
own filterGrid (GOCHECK) keeps of row names under base options, for the edge cases
and the base options of testgrid.k8s.io's tabs (filterdiff.py), cases that pin one
behaviour of Go's regexp each, and a fixed random sample. test_testgrid_config.py
replays them without Go."""
import gzip
import json
import os
import random
import sys
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import filterdiff as fd  # noqa: E402

# one behaviour each, with names on both sides of it
PINNED = {
    "a.c": ["abc", "a\nc", "ac", "aXc", "aéc"],
    "^ab": ["ab", "xab", "a-ab", "abab"],
    "ab$": ["ab", "abx", "aab", "ab\n"],
    "ab": ["aab", "aaab", "abab", "ba"],
    "aab": ["aaab", "aaaab", "aab"],
    r"\Aab": ["ab", "xab"], r"ab\z": ["ab", "abx"],
    r"\Bb": ["ab", " b", "b"], r"\bb": ["ab", " b", "b"], r"a\b": ["a", "ab", "a-"],
    r"\D": ["1", "a", ""], r"\S": [" ", "a", "\t"], r"\W": ["a", "_", "-", "9"],
    r"\w": ["_", "-", "é"], r"\b_": ["a_", " _", "_"], r"\s": ["\r", "\t", "\v", "\n", "\f", " "],
    "^a{0,2}$": ["", "a", "aa", "aaa"], "^a{2}$": ["a", "aa", "aaa"], "^a{2,}$": ["a", "aa", "aaaa"],
    "^(?:ab){1,2}$": ["ab", "abab", "ababab", "aba"],
    "[^a-c]": ["a", "b", "d", "^"], "[^a-cb-e]": ["c", "e", "f"], "[^a]": ["^", "a", "\n"],
    "[a-cx-z]": ["b", "y", "m"], "[-a]": ["-", "a", "b"], "[a\\-z]": ["-", "b"],
    "x+y": ["xy", "xxy", "y"], "x*y": ["y", "zy"], "^x?y$": ["y", "xy", "xxy"],
    "(?:a|ab)c": ["abc", "ac", "bc"], "a|b|c": ["c", "d"], "(a)|b": ["b"],
    "^$": ["", "a"], "": ["", "x"],
    # flags, named groups, quoting, Unicode and POSIX classes, octal escapes, case folding
    "(?i)k": ["K", "k", chr(0x212A), "x"], "(?i)\u017f": ["s", "S", "x"], "(?i)\u03c3": ["\u03a3", "\u03c2", "x"],
    "(?i:a)b": ["Ab", "AB"], "a(?i)b": ["aB", "AB"], "(?i)a(?-i)b": ["Ab", "AB"], "(?m)^b$": ["a\nb\nc", "ab"],
    "(?s)a.b": ["a\nb", "ab"], "(?U)a+?": ["a", "b"], "(?P<n>a)b": ["ab", "b"], "(?<n>a)b": ["ab", "b"],
    r"\Q.*\E": [".*", "ab"], r"\pL": ["\u00e9", "1"], r"\p{Greek}+": ["\u03b1\u03b2", "ab"], r"\P{Greek}": ["\u03b1", "a"],
    r"\p{Letter}": ["x", "1"], r"\p{lu}": ["A", "a"], r"(?i)\p{Lu}": ["a", "1"], r"\p{ASCII}": ["\u00e9", "e"],
    "[[:alpha:]]+": ["abc", "123"], "[[:^digit:]]": ["1", "a"], r"\101": ["A", "B"], r"\012": ["\n", "a"],
    "x{1000}": ["x" * 1000, "x" * 999], "^*a": ["a"], r"\b+a": ["a"], r"\x{D800}": ["a"],
}
TWO_FILTERS = [
    "exclude-filter-by-regex=a&exclude-filter-by-regex=b",
    "include-filter-by-regex=a&include-filter-by-regex=b",
    "include-filter-by-regex=a&exclude-filter-by-regex=b",
    "exclude-filter-by-regex=%5Ea%24&include-filter-by-regex=a",
]
QUERIES = ["%2B1", "k=a=b", "include-filter-by-regex=a%3Db", "include-filter-by-regex=a+b",
           "include-filter-by-regex=a%2Bb", "include-filter-by-regex=%E6%97%A5", "include-filter-by-regex=%E6%97",
           "include-filter-by-regex=", "&&include-filter-by-regex=x&&", "include-filter-by-regex=x;y"]


def cases(rng):
    out = [(o, fd.REAL_NAMES, False) for o in fd.REAL]
    names = sorted(set(fd.REAL_NAMES) | {"a", "b", "ab", "ba", "a b", "x", "a=b", "a+b", "a b+", "日", ""})
    for pattern, own in PINNED.items():
        for key in (fd.INC, fd.EXC):
            out.append((f"{key}={urllib.parse.quote(pattern, safe='')}", sorted(set(own) | {"zzz"}), True))
    out += [(o, names, False) for o in TWO_FILTERS + QUERIES]
    out += [(f"{key}={urllib.parse.quote(p, safe='')}", fd.REAL_NAMES, False)
            for p in fd.EDGE_PATTERNS for key in (fd.INC, fd.EXC)]
    for _ in range(1500):
        plain = rng.random() < 0.5
        options = fd.base_options(rng, plain)
        out.append((options, fd.names(rng, options), plain))
    return out


def main():
    rng = random.Random(20261012)
    all_cases = cases(rng)
    got = []
    for start in range(0, len(all_cases), 1000):
        got += fd.go_side(all_cases[start:start + 1000])
    with gzip.open(sys.argv[1], "wt", compresslevel=9) as f:
        for (options, names, plain), g in zip(all_cases, got, strict=True):
            f.write(json.dumps({"options": options, "names": names, "plain": plain, "ok": g["ok"], "kept": g["kept"]},
                               sort_keys=True) + "\n")
    print(len(all_cases), "cases written to", sys.argv[1])


if __name__ == "__main__":
    main()
