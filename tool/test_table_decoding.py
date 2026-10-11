"""The two table decoders (the fetch's, and cross_check's written independently)
against a naive reference that expands every cell, on random tables, valid and
not: both decoders take bulk shortcuts (rows checked in C, rows with many runs
expanded as bytes), and must answer exactly as cell-by-cell decoding would."""
import os
import random
import re
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402

NOW = time.time()
RED = {9: "TIMED_OUT", 10: "CATEGORIZED_FAIL", 12: "FAIL", 14: "TOOL_FAIL"}


def reference(table):
    """(cells per column, red cells per column, informative columns) by expanding every cell,
    or what checking run by run raises: a ValueError's message, or the error of a run that is
    not an object with a value and a count (KeyError, TypeError)."""
    try:
        return expand(table)
    except (KeyError, TypeError) as e:
        return e


def expand(table):
    width = len(table["changelists"])
    per_column = [set() for _ in range(width)]
    reds, informative = {}, set()
    for row in table.get("tests", []):
        cells, col = [], 0
        for run in row["statuses"]:
            value, count = run["value"], run["count"]
            if type(count) is not int or count <= 0:
                return f"row {str(row.get('name'))[:80]!r}: bad run length {count!r}"
            if type(value) is not int or value not in cc.NAMES:
                return f"row {str(row.get('name'))[:80]!r}: bad status {value!r}"
            if col + count > width:
                return f"row {str(row.get('name'))[:80]!r} is wider than the table"
            cells += [value] * count
            col += count
        if col != width:
            return f"row {str(row.get('name'))[:80]!r} spans {col} of {width} columns"
        messages = row.get("messages") or []
        aligned = len(messages) == sum(1 for v in cells if v != 0)
        idx = 0
        for i, v in enumerate(cells):
            per_column[i].add(v)
            if v not in (0, 6):
                informative.add(i)
            if v in RED:
                reds.setdefault(i, []).append({"test": row["name"], "status": RED[v],
                                               "message": messages[idx] if aligned else None})
            if v != 0:
                idx += 1
    return per_column, reds, sorted(informative)


def random_table(rng):
    width = rng.choice([0, 1, 2, 3, 5, 8, 24, 60, 150, 400, 1000])
    stamps = [int((NOW - 3600 * (i + 1)) * 1000) for i in range(width)]
    builds = [str((stamps[i] - frl.PROW_EPOCH_MS) << 22) for i in range(width)]
    tests = []
    for r in range(rng.randint(0, 12)):
        runs, left = [], width
        dense = rng.random() < 0.3
        while left > 0:
            count = 1 if dense else rng.randint(1, max(1, left if rng.random() < 0.3 else min(left, 20)))
            count = min(count, left)
            runs.append({"value": rng.choice([0, 0, 1, 1, 1, 2, 3, 4, 6, 9, 10, 11, 12, 12, 13, 14, 15]), "count": count})
            left -= count
        if rng.random() < 0.15:  # break it
            kind = rng.randrange(9)
            if kind == 0 and runs:
                rng.choice(runs)["count"] = rng.choice([0, -1, True, 1.0, "2", None])
            elif kind == 1 and runs:
                rng.choice(runs)["value"] = rng.choice([16, -1, True, 12.0, "12", None])
            elif kind == 2:
                runs.append({"value": 1, "count": 1})  # one column too many
            elif kind == 3 and runs:
                runs.pop()  # too few
            elif kind == 4 and runs:
                del rng.choice(runs)["value"]
            elif kind == 5 and runs:
                runs[rng.randrange(len(runs))] = rng.choice([[1, 1], "x", None, 5])
            elif kind == 6 and len(runs) > 1:
                runs[0]["count"] += runs[1]["count"]  # an early run too long, a later one bad as well
                runs[1]["count"] = -1
            elif kind == 7 and runs:
                runs[-1]["count"] += 1
            elif kind == 8:
                runs.append({"count": 1})
        results = sum(run["count"] for run in runs if isinstance(run, dict) and run.get("value") != 0
                      and isinstance(run.get("count"), int) and run.get("count", 0) > 0)
        n_messages = results if rng.random() < 0.85 else max(0, results + rng.choice([-1, 1]))
        tests.append({"name": f"t{r}", "statuses": runs, "messages": [f"m{r}.{k}" for k in range(n_messages)]})
    return {"query": "bucket/logs/job", "changelists": builds, "timestamps": stamps, "tests": tests}


class DecoderTest(unittest.TestCase):
    def test_both_decoders_agree_with_cell_by_cell_decoding(self):
        rng = random.Random(20261011)
        valid = invalid = 0
        for i in range(10000):
            table = random_table(rng)
            want = reference(table)
            with self.subTest(i=i):
                if isinstance(want, (str, Exception)):  # the reference rejects it
                    invalid += 1
                    with self.assertRaises((ValueError, TypeError, KeyError)) as err:
                        list(cc.columns(table))
                    if isinstance(want, str):
                        self.assertEqual(str(err.exception), want)
                    else:  # the same kind of error as checking run by run
                        self.assertIsInstance(err.exception, type(want))
                    with self.assertRaises(ValueError):  # the fetch says so in a ValueError
                        frl.table_rows(table)
                    continue
                valid += 1
                per_column, reds, informative = want
                self.assertEqual([set(cells) for _, _, cells in cc.columns(table)], per_column)
                width, rows = frl.table_rows(table)
                self.assertEqual(frl.red_columns(table, rows), reds)
                self.assertEqual(frl.informative_columns(rows, width), informative)
        self.assertGreater(valid, 4000)
        self.assertGreater(invalid, 500)

    def test_the_fetch_says_which_run_is_bad(self):
        cases = [(["x"], "row 't': malformed run 'x'"),
                 ([{"value": 1}], "row 't': run length None is not a positive integer"),
                 ([{"value": 1, "count": True}], "row 't': run length True is not a positive integer"),
                 ([{"value": True, "count": 2}], "row 't': unknown status True"),
                 ([{"value": 16, "count": 2}], "row 't': unknown status 16"),
                 ([{"value": 1, "count": 1}], "row 't' spans 1 columns, not 2"),
                 ([{"value": 1, "count": 3}], "row 't' spans 3 columns, not 2")]
        for runs, message in cases:
            with self.subTest(runs=runs):
                table = {"changelists": ["1", "2"], "timestamps": [0, 0], "tests": [{"name": "t", "statuses": runs}]}
                with self.assertRaisesRegex(ValueError, "^" + re.escape(message) + "$"):
                    frl.table_rows(table)

if __name__ == "__main__":
    unittest.main()
