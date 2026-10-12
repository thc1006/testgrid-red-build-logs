"""Differential harness: TestGrid's own code (tgoracle) vs tool/testgrid_rule.py."""
import base64
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402

ORACLE = os.environ.get("TGORACLE", "tgoracle")  # tool/oracle/build.sh builds it
PREFIX = "logs/job/1/"


def b64(b):
    return None if b is None else base64.b64encode(b).decode()


def to_line(case):
    return json.dumps({"id": case["id"], "started": b64(case.get("started")), "finished": b64(case.get("finished")),
                       "podinfo": b64(case.get("podinfo")),
                       "artifacts": [{"name": a["name"], "size": a.get("size"), "data": b64(a.get("data"))}
                                     for a in case.get("artifacts", [])],
                       "stop": case["stop"], "stop_ns": case.get("stop_ns", 0),
                       "group": protojson_group(case.get("group"))})


def protojson_group(group):
    """A test group as the configurator prints it (a oneof as its Go field, null as it cannot
    set one from YAML), as protojson takes it: without those."""
    if isinstance(group, dict):
        return {k: protojson_group(v) for k, v in group.items() if not (k in ("TestResultInfo", "ComparisonValue")
                                                                       and v is None)}
    if isinstance(group, list):
        return [protojson_group(v) for v in group]
    return group


SLOW = []  # cases Go's own code takes too long on (huge quantity exponents)


def run_oracle(cases, timeout=60):
    """The oracle's answers, None for a case it cannot finish (a batch that times out is split)."""
    try:
        p = subprocess.run([ORACLE], input="".join(to_line(c) + "\n" for c in cases), capture_output=True,
                           text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        if len(cases) == 1:
            SLOW.append(cases[0])
            return [None]
        half = len(cases) // 2
        return run_oracle(cases[:half], timeout) + run_oracle(cases[half:], timeout)
    if p.returncode:
        raise RuntimeError(p.stderr[-3000:])
    return [json.loads(line) for line in p.stdout.splitlines()]


def python_side(case, now):
    objects = {}
    for name in ("started", "finished", "podinfo"):
        if case.get(name) is not None:
            objects[PREFIX + name + ".json"] = (len(case[name]), case[name])
    for a in case.get("artifacts", []):
        data = a.get("data") or b""
        objects[PREFIX + a["name"]] = (a.get("size") if a.get("size") is not None else len(data), data)
    junits = []
    for listed, opened in tr.junit_reads(sorted(n for n in objects if n.startswith(PREFIX))):
        size, data = objects.get(opened, (None, None))
        junits.append((opened, size, data, listed))
    options = tgc.cell_options(case["group"])[0] if case.get("group") is not None else None
    return tr.column("job", case.get("started"), case.get("finished"), case.get("podinfo"), junits,
                     stop_of(case), now, folder=PREFIX, options=options)


def stop_of(case):
    """The case's stop as the port takes it: Unix seconds, with the fraction "stop_ns" gives
    (a multiple of STOP_STEP nanoseconds, so the float is exact)."""
    ns = case.get("stop_ns", 0)
    assert ns % STOP_STEP == 0, ns
    return case["stop"] + ns // STOP_STEP / (10**9 // STOP_STEP)


STOP_STEP = 1953125  # nanoseconds: 1/512 of a second


def category(e):
    return e if e in ("nostart", "ancient") else e.split(":", 1)[0]


def compare(case, got, col=None):
    """What differs between the port's column (python_side's, or `col`) and TestGrid's (`got`)."""
    col = python_side(case, got["now"]) if col is None else col
    want_errors = [category(e) for e in got["errors"]]
    problems = []
    if col.errors != want_errors:
        problems.append(f"errors {col.errors} != {want_errors}")
    if sorted(col.results) != sorted(got["results"]):
        problems.append(f"results {sorted(col.results)} != {got['results']}")
    jsons = lambda m: sorted(x for x in m if x.endswith(".json") and ":" not in x)  # noqa: E731
    if jsons(col.malformed) != jsons(got["malformed"]) or len(col.malformed) != len(got["malformed"]):
        problems.append(f"malformed {col.malformed} != {got['malformed']}")
    either = getattr(col, "either", {})  # (rows of one name after a split: Go's map order picks one)
    want_cells = {row: (col.cells or {}).get(row) if status in either.get(row, ()) else status
                  for row, status in (got.get("cells") or {}).items()}
    if (col.cells or None) != (want_cells or None) and not (col.cells == {} and not want_cells):
        problems.append(f"cells {col.cells} != {got.get('cells')}")
    # The Pod row's message, when TestGrid added the row (the port does not carry the
    # messages of junit rows, so not for a junit row named like it, nor once merged).
    msg = (got.get("messages") or {}).get("job.Pod")
    if msg is not None and "runs passed" not in msg and "job.Pod" in col.notes and col.notes["job.Pod"] != msg \
            and msg != tr.MISSING_POD_INFO:
        problems.append(f"pod message {col.notes.get('job.Pod')!r} != {msg!r}")
    return problems


def check(cases, show=20):
    got = run_oracle(cases)
    bad = []
    for case, g in zip(cases, got, strict=True):
        if g is None:
            continue
        assert case["id"] == g["id"]
        try:
            problems = compare(case, g)
        except Exception as e:  # noqa: BLE001
            problems = [f"python raised {type(e).__name__}: {e}"]
        if problems:
            bad.append((case, problems))
    for case, problems in bad[:show]:
        print("MISMATCH", case["id"], problems)
    return bad
