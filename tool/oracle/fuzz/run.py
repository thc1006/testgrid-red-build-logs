"""Random differential fuzzing: run.py SEED CASES (TGORACLE: the oracle binary)."""
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import gen  # noqa: E402
import harness  # noqa: E402

seed, n = int(sys.argv[1]), int(sys.argv[2])
gen.NOW = int(time.time())
rng = random.Random(seed)
total_bad = 0
for batch in range(0, n, 500):
    cases = [gen.case(rng, f"{seed}-{batch + k}") for k in range(min(500, n - batch))]
    bad = harness.check(cases, show=3)
    total_bad += len(bad)
    if bad:
        with open(f"bad-{seed}.jsonl", "a") as f:
            for c, p in bad:
                f.write(json.dumps({"case": harness.to_line(c), "problems": p}) + "\n")
print(f"seed {seed}: {n} cases, {total_bad} mismatches, {len(harness.SLOW)} too slow for Go")
sys.exit(1 if total_bad else 0)
