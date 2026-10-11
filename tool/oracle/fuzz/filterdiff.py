"""A tab's row filter, TestGrid's tabulator (gocheck) vs testgrid_config.py:
filterdiff.py SEED CASES (GOCHECK: the gocheck binary tool/oracle/build.sh builds).

Each case is base options and row names. Where TestGrid rejects the base options,
the port must not apply them (Unsupported); where it keeps rows, the port must keep
the same ones, or not apply the options at all. Patterns made only of the syntax
the port claims to follow must be applied. The edge cases (the filters
testgrid.k8s.io's tabs use among them) run first."""
import json
import os
import random
import subprocess
import sys
import urllib.parse

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import testgrid_config as tc  # noqa: E402

GOCHECK = os.environ.get("GOCHECK", "gocheck")
INC, EXC = tc.INCLUDE, tc.EXCLUDE

# characters names are made of: ASCII classes' edges, and their Unicode lookalikes
# (Go's \d \s \w \b are ASCII only)
NAME_CHARS = (list("abcxyzKSs_09 .-/:[](){}|*+?^$\\#&=;%,'\"") + ["\t", "\n", "\v", "\f", "\r", "\x00", "\x7f"] +
              ["é", "ß", chr(0x212A), "ſ", "Σ", "σ", "ς", chr(0x663), chr(0xA0), chr(0x2028), chr(0x3000), "日", "\U0001F600"])
LITERALS = ["Overall", "Pod", "kubetest.Test", "Monitor cluster", "sig-node", "[sig-storage]", "a", "b", "K", "é"]

REAL = [  # base options of testgrid.k8s.io's tabs (test-infra, 2026-10-11)
    "exclude-filter-by-regex=%5E(kubetest%5C.Test%7Cci-kubernetes-e2e-gce-scale-correctness%5C.Overall)%24",
    "exclude-filter-by-regex=Monitor%5Cscluster", "exclude-filter-by-regex=%5Eoperator.Run+template.*container+test%24",
    "include-filter-by-regex=Volume%7Cstorage", "exclude-filter-by-regex=Overall%24",
    "include-filter-by-regex=sig-api-machinery", "include-filter-by-regex=kubectl%7COverall",
    "include-filter-by-regex=sig-auth&width=10", "width=10", "group-by-directory=&expand-groups=&sort-by-name=",
]
REAL_NAMES = ["kubetest.Test", "kubetest.Up", "ci-kubernetes-e2e-gce-scale-correctness.Overall",
              "ci-kubernetes-e2e-gce-scale-correctness.Pod", "Overall", "job.Overall", "Monitor cluster",
              "Monitor\tcluster", "Monitor\vcluster", "Monitor" + chr(0xA0) + "cluster", "operator.Run template x container test",
              "operator.Run template x container test\n", "Volume", "storage", "kubectl", "sig-api-machinery",
              "Kubernetes e2e suite.[It] [sig-storage] Volumes", ""]

EDGE_PATTERNS = [
    "", "a", "^a", "a$", "^$", "a|", "|a", "(a)", "(?:a)", "()", "(|a)*", "a*", "a+", "a?", "a*?", "a{2}", "a{2,}",
    "a{2,3}", "a{0}", "a{1000}", "a{1001}", "a{01}", "a{,3}", "a{", "a{x}", "{", "}", "]", "a{2}{3}", "a**", "a*+",
    "a++", "a?+", "^*", "$+", r"\b*", "(?i)a", "(?s).", "(?P<n>a)", "(?<n>a)", "(?#c)", r"\d", r"\D", r"\s", r"\S",
    r"\w", r"\W", r"\b", r"\B", r"\A", r"\z", r"\Z", r"\pL", r"\p{Greek}", r"\PL", r"\Q.\E", r"\C", r"\1", r"\0",
    r"\012", r"\x41", r"\x{41}", r"\x{1F600}", r"\x{D800}", r"\x{110000}", r"\x4", r"\x{}", r"\_", r"\ ", r"\-",
    r"\#", r"\a\f\t\n\r\v", "\\", "a\\", r"\e", r"\k", "[a]", "[^a]", "[]a]", "[^]a]", "[a-]", "[-a]", "[a-c]",
    "[c-a]", r"[\d]", r"[\D]", r"[\s-a]", r"[a-\d]", r"[\b]", "[[:alpha:]]", "[[:foo:]]", "[[a]", "[a", "[^]",
    "[]", r"[\]]", r"[\\]", "[.]", "[$]", "[é-ÿ]", "(a", "a)", "((a)", "(a))", "(?:(?:a{20})b){60}",
    "(?:a{2}){3}", "(?:a*){5}", "(a{2}|b){3}", "(?:a{1}){1000}", ".", ".*", "\n", "a\nb", "a.b", "日本", "\U0001F600",
    r"[\x{41}-\x{5A}]", r"Monitor\scluster", r"^(kubetest\.Test|x\.Overall)$", "Overall$", "Volume|storage",
]


def plain_pattern(rng, depth=0):
    """A pattern made only of the syntax the port follows (that Go also accepts)."""
    def atom():
        r = rng.random()
        if r < 0.35:
            return rng.choice(["a", "b", "K", "s", "_", "0", "é", "日", " ", "x"])
        if r < 0.45:
            return rng.choice([r"\.", r"\-", r"\[", r"\]", r"\(", r"\)", r"\|", r"\*", r"\+", r"\?", r"\{", r"\}",
                               r"\^", r"\$", r"\\", r"\ ", r"\_", r"\#", r"\t", r"\n", r"\x41", r"\x{e9}"])
        if r < 0.55:
            return "."
        if r < 0.68:
            return rng.choice([r"\d", r"\D", r"\s", r"\S", r"\w", r"\W"])
        if r < 0.82:
            items = "".join(rng.choice(["a", "b-z", "0-9", r"\d", r"\s", r"\w", r"\]", r"\-", "_", "é", "K", ".",
                                        "$", "^x", " ", r"\t"]) for _ in range(rng.randint(1, 3)))
            return "[" + rng.choice(["", "^"]) + items + "]"
        if depth < 3:
            return rng.choice(["(", "(?:"]) + plain_pattern(rng, depth + 1) + ")"
        return "a"

    def piece():
        if rng.random() < 0.12:
            return rng.choice(["^", "$", r"\b", r"\B", r"\A", r"\z"])
        a = atom()
        if rng.random() < 0.3:
            q = rng.choice(["*", "+", "?", "{2}", "{0,1}", "{1,}", "{3,5}", "{2,}", "{0,}"])
            if q.startswith("{") and a.startswith("(") and "{" in a:
                q = "*"  # (Go limits counted repetitions of counted repetitions)
            a += q + ("?" if rng.random() < 0.2 else "")
        return a

    alts = ["".join(piece() for _ in range(rng.randint(0, 4))) for _ in range(rng.randint(1, 3))]
    return "|".join(alts)


def wild_pattern(rng):
    return "".join(rng.choice(list("ab.*+?{}[]()^$|\\-:,0123456789dsDSwWbBAzxpPQEC_ ?<>#=iK") + ["é", "\n"])
                   for _ in range(rng.randint(0, 12)))


def encode(rng, value):
    r = rng.random()
    if r < 0.5:
        return urllib.parse.quote(value, safe="")
    if r < 0.7:
        return urllib.parse.quote_plus(value, safe="")
    if r < 0.85:
        return "".join(c if rng.random() < 0.5 else urllib.parse.quote(c, safe="") for c in value)
    return value  # raw: "&", ";", "%", "+" and "=" as they are


def base_options(rng, plain):
    parts = []
    for _ in range(rng.randint(1, 3)):
        key = rng.choice([INC, EXC, EXC, "width", "sort-by-name"] + ([] if plain else ["unknown-option", "", "x;y"]))
        if key in (INC, EXC):
            value = plain_pattern(rng) if plain else (wild_pattern(rng) if rng.random() < 0.6 else rng.choice(EDGE_PATTERNS))
            while plain and len(value) > tc.MAX_PATTERN:  # (longer ones are not followed, by design)
                value = plain_pattern(rng)
        else:
            value = rng.choice(["", "10", "a b"])
        parts.append(f"{key}={encode(rng, value)}" if plain or rng.random() < 0.9 else key)
    if not plain and rng.random() < 0.15:
        parts.append(rng.choice(["", "%", "%zz", "a;b", "&&"]))
    return "&".join(parts)


def names(rng, options):
    out = set(REAL_NAMES[:4])
    for _ in range(rng.randint(5, 25)):
        r = rng.random()
        if r < 0.5:
            out.add("".join(rng.choice(NAME_CHARS) for _ in range(rng.randint(0, 8))))
        else:
            n = rng.choice(LITERALS)
            out.add("".join(rng.choice(NAME_CHARS) if rng.random() < 0.1 else c for c in n))
    # names close to what the options' patterns contain
    for vs in tc.go_parse_query(options)[0].values():
        for v in vs:
            text = v.decode("utf-8", "replace")
            out.add("".join(c for c in text if c not in "\\^$()|*+?{}[]"))
    return sorted(out)


def python_side(options, names_):
    try:
        includes, excludes = tc.tab_filters(options)
    except tc.Unsupported as e:
        return None, str(e)
    return [n for n in names_ if all(r.search(n) for r in includes) and not any(r.search(n) for r in excludes)], None


def go_side(cases):
    lines = "".join(json.dumps({"Kind": "filter", "S": o, "Names": ns}) + "\n" for o, ns, _ in cases)
    p = subprocess.run([GOCHECK], input=lines, capture_output=True, text=True, check=True)
    return [json.loads(line) for line in p.stdout.splitlines()]


def check(cases):
    """Mismatches, and how many cases the port did not apply although TestGrid did."""
    bad, unapplied = [], 0
    for (options, names_, plain), got in zip(cases, go_side(cases), strict=True):
        kept, why = python_side(options, names_)
        if not got["ok"]:
            if kept is not None:
                bad.append((options, f"TestGrid rejects them, the port keeps {kept}"))
            continue
        if kept is None:
            unapplied += 1
            if plain and "options not known here" not in why:
                bad.append((options, f"syntax the port follows, but it raised: {why}"))
            continue
        if kept != got["kept"]:
            bad.append((options, f"kept {[n for n in kept if n not in got['kept']]} that TestGrid drops, dropped "
                                 f"{[n for n in got['kept'] if n not in kept]} that TestGrid keeps"))
    return bad, unapplied


def main():
    seed, n = int(sys.argv[1]), int(sys.argv[2])
    rng = random.Random(seed)
    edge = [(o, REAL_NAMES, False) for o in REAL]
    edge += [(f"{key}={urllib.parse.quote(p, safe='')}", REAL_NAMES + sorted(set(names(rng, ""))), False)
             for p in EDGE_PATTERNS for key in (INC, EXC)]
    total, unapplied_total = [], 0
    for start in range(0, len(edge) + n, 1000):
        batch = edge[start:start + 1000] if start < len(edge) else []
        while len(batch) < 1000 and start + len(batch) < len(edge) + n:
            plain = rng.random() < 0.5
            options = base_options(rng, plain)
            batch.append((options, names(rng, options), plain))
        bad, unapplied = check(batch)
        total += bad
        unapplied_total += unapplied
    for options, problem in total[:20]:
        print("MISMATCH", repr(options), problem)
    print(f"seed {seed}: {len(edge)} edge cases and {n} random ones, {len(total)} mismatches, "
          f"{unapplied_total} not applied by the port (TestGrid accepts them)")
    return 1 if total else 0


if __name__ == "__main__":
    sys.exit(main())
