"""The builds TestGrid's ListBuilds takes from an entry of Prow's index (GOCHECK, its "link")
against fetch_red_logs.link_folder: linkdiff.py SEED CASES. Where TestGrid's listing fails, the
port must fail it; where TestGrid lists no build, the port must take none; where it lists one, the
port must take the same build, and read its files from exactly the folders the archive's rule
reads (see readable), where TestGrid reads started.json. The edge cases run first, then random
links and links a few characters off ones Prow writes."""
import json
import os
import random
import re
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import fetch_red_logs as frl  # noqa: E402

GOCHECK = os.environ.get("GOCHECK", "gocheck")
NBSP, IDEO, ZWSP, EM, NEL = chr(0xA0), chr(0x3000), chr(0x200B), chr(0x2003), chr(0x85)
PART = re.compile(r"[A-Za-z0-9_][A-Za-z0-9._-]*")  # (the archive's own rule for a folder's parts)

REAL = ["gs://kubernetes-ci-logs/pr-logs/pull/140698/pull-kubernetes-linter-hints/2078641291377250304",
        "gs://kubernetes-jenkins/pr-logs/pull/kubernetes_test-infra/35000/pull-test-infra-bazel/1234567890123456789",
        "gs://b/pr-logs/pull/1/j/2", "gs://b_c.d-e/p_q/1.2/x-y/99", "GS://B/P/1"]
EDGE = REAL + [
    " gs://b/pr-logs/pull/1/j/2/ \n", NBSP + "gs://b/p/1" + IDEO, "gs://b/p/1?", "gs://b/p/1#", "  ", "", "gs://b",
    "gs://b/", "gs://b//", "gs:///p/1", "gs:", "gs://", "file:///p/1", "/p/1", "p/1", "//b/p/1", "///", "*",
    "gs://b/p/1//", "gs://b//p/1", "gs://b/p//1", "gs://b/p/./1", "gs://b/p/../1", "gs://b/p/1/./", "gs://b/p/1%2F",
    "gs://b/p/a%2Fb/1", "gs://b/p/%41/1", "gs://b/p/é/1", "gs://b/p/1" + ZWSP, ZWSP, ZWSP + "p/1", "gs://b/p/-1",
    "gs://b/p/~1/2", "gs://b/p/kubernetes~x/9/j/1", "gs://b/p/1;x", "gs://b/p/1 2", "gs://b/p/1 ",
    "https://b/p/1", "gs:b/p/1", "gs://b:80/p/1", "gs://b:/p/1", "gs://u@b/p/1", "gs://@b/p/1", "gs://b@/p/1",
    "gs://b/p/1?x=1", "gs://b/p/1??", "gs://b/p/1#f", "gs://b/p/1#%zz", "gs://[::1]/p/1", "gs://[x]/p/1",
    "gs://[1.2.3.4]/p/1", "gs://b[/p/1", "gs://b]/p/1", "gs://[x/p/1", "gs://b/p/1" + chr(0x1C), "gs://b/p/1" + chr(0x7F),
    "gs://b/p/1" + chr(0), "gs://b/p/%zz/1", "gs://b/p/%4", "gs://b/p/%", ":gs://b/p/1", ":", "a:b/1", "1a:b/1",
    "_x:1/2", "gs://b c/p/1", "gs://b%41/p/1", "gs://b%25/p/1", "gs://b%e9/p/1", "gs://bé/p/1", "gs://B/p/1",
    "gs://b_c/p/1", "gs://b~c/p/1", "gs://b!c/p/1", "gs://b{c/p/1", "gs://b|c/p/1", "gs://b\\c/p/1",
    EM + "gs://b/p/1", NEL + "gs://b/p/1", chr(0x1C) + "gs://b/p/1", "g1://b/p/1", "1gs://b/p/1", "gs.x://b/p/1",
    # (bytes that are not UTF-8, which Go's encoder writes one U+FFFD each for; a NEL inside, which it leaves as is)
    "gs://b/p/%E2%82", "gs://b/p/%F0%9F%98/1", "gs://b/p/x" + NEL + "y/1", "gs://b/p/1/x" + NEL + "y",
    # (a lone surrogate, which json.dumps writes as an escape: Go's encoding/json reads U+FFFD)
    "gs://b/p/1/2" + chr(0xD800), "gs://b/p/" + chr(0xDC80) + "/1",
]
SCHEMES = ["gs", "GS", "Gs", "file", "", "http", "g1", "g+.-", "1gs", "_gs", "gs.x"]
SEPS = ["://", ":/", ":", "//", "///", ":///", "", "/"]
HOSTS = ["b", "", "b:80", "b:", "b:x", "u@b", "@b", "u:p@b", "[::1]", "[x]", "[x", "b%41", "b%2", "b%e9", "b%25",
         "b c", "b_c", "B", "b.c", "b-c", "b~c", "b!c", "b$c", "b'c", "b(c", "b*c", "b+c", "b,c", "b;c", "b=c",
         "b<c", "b\"c", "b{c", "b|c", "b\\c", "b^c", "b`c", "bé", "kubernetes-ci-logs"]
SEGS = ["p", "1", "2078641291377250304", "", ".", "..", "%41", "%2F", "%2f", "%zz", "%4", "%", "a b", "é", "~",
        "-1", "_1", "1;x", "a:b", "pr-logs", "pull", "j", "a.b", "A", "x%20y", "%e9", "+", "*", "a#b", "a?b"]
ENDS = ["", "/", "//", "?", "??", "?x", "#", "#x", "#%zz", "/?", "/#", "?#"]
SPACES = ["", " ", "\t", "\n", "\r\n", "\v", "\f", chr(0x1C), chr(0x1F), NEL, NBSP, EM, ZWSP, IDEO, chr(0x7F), chr(0)]
INSERTS = list(" \t\n/%.:@?#[]~-_+;,=*!$&'()\"<>{}|\\^`é0aZ") + [NBSP, EM, ZWSP, chr(0x1C), chr(0x7F), "%41", "%2F",
                                                                     "%2e", "%2E%2E", "%zz", "%25", "%e9", "//", "/./",
                                                                     "/../", "%20"]


def random_link(rng):
    if rng.random() < 0.05:
        return rng.choice(["", " ", "*", ":", "?", "#", "/", "//", "a:b", "gs:", "gs:x", "file:", "gs://"]) + rng.choice(ENDS)
    path = "/".join(rng.choice(SEGS) for _ in range(rng.randint(0, 5)))
    lead = rng.choice(["", "/", "//"]) if rng.random() < 0.2 else "/"
    return (rng.choice(SPACES) + rng.choice(SCHEMES) + rng.choice(SEPS) + rng.choice(HOSTS) + lead + path
            + rng.choice(ENDS) + rng.choice(SPACES))


def near_link(rng):
    """A link Prow writes, a few characters off."""
    link = rng.choice(REAL)
    for _ in range(rng.choice([0, 1, 1, 1, 2, 3])):
        op, i = rng.random(), rng.randrange(len(link) + 1)
        if op < 0.5:
            link = link[:i] + rng.choice(INSERTS) + link[i:]
        elif op < 0.75 and link:
            link = link[:i] + link[i + 1:]
        elif op < 0.9:
            link += rng.choice(["/", "//", "///", " ", "?", "#", "\n"])
        else:
            link = rng.choice([" ", "\t", NBSP, IDEO, chr(0x1C), ""]) + link
    return link


def metadata(rng, link):
    """The entry's metadata: its x-goog-meta-link, or its link, or both."""
    r = rng.random()
    if r < 0.8:
        return {"x-goog-meta-link": link}
    if r < 0.9:
        return {"link": link}
    return {"x-goog-meta-link": rng.choice(["", link]), "link": rng.choice(REAL)}


def port(md):
    try:
        return frl.link_folder(md)
    except frl.IntegrityError:
        return "fails"


def as_go(s):  # (Go's encoding/json writes each byte that is not UTF-8 as U+FFFD)
    return "".join(chr(0xFFFD) if 0xDC80 <= ord(c) <= 0xDCFF else c for c in s)


def readable(md, b):
    """The folder the archive reads for a build TestGrid lists (b), by its own rule: only one TestGrid
    reads from GCS (gs://), named by the link as written (no %XX), with a bucket and parts of
    [A-Za-z0-9_][A-Za-z0-9._-]*; else None."""
    link = md["x-goog-meta-link"] if "x-goog-meta-link" in md else md.get("link")
    folder = b["object"].removesuffix("/")
    ok = b["scheme"] == "gs" and "%" not in link and PART.fullmatch(b["bucket"]) \
        and all(PART.fullmatch(p) for p in folder.split("/"))
    return f"{b['bucket']}/{folder}" if ok else None


def check(cases):
    lines = "".join(json.dumps({"Kind": "link", "Names": [x for kv in md.items() for x in kv]}) + "\n" for md in cases)
    p = subprocess.run([GOCHECK], input=lines.encode(), capture_output=True, timeout=600)
    if p.returncode:
        raise RuntimeError(p.stderr.decode("utf-8", "replace")[-3000:])
    bad = []
    # (by "\n" only: Go's encoder leaves a U+0085 as it is, which str.splitlines() splits at)
    for md, line in zip(cases, p.stdout.decode("utf-8").removesuffix("\n").split("\n"), strict=True):
        go, got = json.loads(line), port(md)
        if not go["ok"]:
            same = got == "fails"
        elif not go["builds"]:
            same = got is None
        else:
            (b,) = go["builds"]
            folder = readable(md, b)
            same = isinstance(got, tuple) and as_go(got[0]) == b["build"] and got[1] == folder and (
                folder is None or b["resolved"] and b["started"] == f"{folder}/started.json")
        if not same:
            bad.append((md, go, got))
    return bad


if __name__ == "__main__":
    seed, n = int(sys.argv[1]), int(sys.argv[2])
    rng = random.Random(seed)
    cases = [{"x-goog-meta-link": link} for link in EDGE] + [{"link": link} for link in EDGE[:10]] + [
        {"x-goog-meta-link": "", "link": REAL[0]}, {}, {"other": REAL[0]}]
    cases += [metadata(rng, random_link(rng) if i % 2 else near_link(rng)) for i in range(n)]
    bad = check(cases)
    for md, go, got in bad[:10]:
        print("MISMATCH", ascii(md), "go:", ascii(go), "port:", ascii(got))
    read = sum(1 for md in cases if isinstance(port(md), tuple) and port(md)[1])
    print(f"seed {seed}: {len(cases)} entries ({read} of folders the port reads), {len(bad)} mismatches")
    sys.exit(1 if bad else 0)
