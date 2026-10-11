"""Write the golden vectors (golden.py OUT.jsonl.gz): the edge cases and a fixed random
sample, with what TestGrid's own code (TGORACLE) made of each."""
import base64
import gzip
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge  # noqa: E402
import gen  # noqa: E402
import harness  # noqa: E402

now = int(time.time())
edge.NOW = gen.NOW = now
cases = edge.cases()
for seed in (101, 102, 103):
    rng = random.Random(seed)
    cases += [gen.case(rng, f"random-{seed}-{k}") for k in range(100)]
got = harness.run_oracle(cases)
out = []


def b64(x):
    return None if x is None else base64.b64encode(x).decode()


for c, g in zip(cases, got, strict=True):
    assert g is not None and c["id"] == g["id"]
    problems = harness.compare(c, g)
    assert not problems, (c["id"], problems)
    out.append({"id": c["id"], "now": g["now"], "stop": c["stop"], "started": b64(c.get("started")),
                "finished": b64(c.get("finished")), "podinfo": b64(c.get("podinfo")),
                "artifacts": [{"name": a["name"], "size": a.get("size"), "data": b64(a.get("data"))} for a in c["artifacts"]],
                "want": {"errors": [harness.category(e) for e in g["errors"]], "results": sorted(g["results"]),
                         "malformed": len(g["malformed"]),
                         "malformed_json": sorted(m for m in g["malformed"] if m.endswith(".json") and ":" not in m),
                         "cells": g.get("cells") or None}})
with gzip.open(sys.argv[1], "wt", compresslevel=9) as f:
    for o in out:
        f.write(json.dumps(o, sort_keys=True) + "\n")
print(len(out), "cases written to", sys.argv[1])
