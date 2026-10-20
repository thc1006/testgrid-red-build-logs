"""A mass failure: past MAX_RED_CELLS red cells in one table, meta.json lists only the first and counts the
rest (red_cells_omitted, see red_columns). A tab TestGrid showed red with every cell of the build left out
lists no red cell, as a sighting taken from GCS before judged_from_gcs does, and the index took it for one:
it could say a backfilled build TestGrid showed red on such a tab was never seen red on TestGrid.
The index counted only the cells listed: a build with all its cells left out showed no red test, and one
with some left out could show too few, with nothing to say so.

Run: cd tool && python -m unittest test_mass_failure -v
"""
import glob
import json
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_integrity import HOUR, ArchiveWorld, obj  # noqa: E402


def cells(n):
    return [{"test": f"t{i:02d}", "status": "FAIL", "message": None} for i in range(n)]


def tab(name, red_cells, **fields):
    return dict({"dashboard": "d", "tab": name, "red_cells": red_cells}, **fields)


def meta(*tabs, **fields):
    return dict({"job": "job", "build": "1", "query": "b/logs/job", "started": None, "created": time.time() - HOUR,
                 "log": None, "podinfo": None, "tabs": list(tabs)}, **fields)


class AllLeftOut(unittest.TestCase):
    """A tab TestGrid showed red with every cell of the build left out."""

    def test_the_entry_of_a_build_taken_from_gcs_and_shown_red_so_is_not_from_gcs(self):
        m = meta(tab("g", cells(1), judged_from_gcs=True), tab("m", [], red_cells_omitted=1), backfilled="note")
        self.assertFalse(frl.index_entry("job", "1", m, time.time())["from_gcs"])  # fails: True

    def test_its_sighting_keeps_the_cells_of_one_taken_from_gcs(self):
        # (it lists no cell to take, as merge_tabs says: on that tab the index cannot tell TestGrid showed it red)
        new = [tab("m", [], red_cells_omitted=7)]
        for old in (tab("m", cells(1), judged_from_gcs=True), tab("m", [])):  # (judged, or from before that)
            with self.subTest(judged=bool(old.get("judged_from_gcs"))):
                self.assertEqual(frl.merge_tabs([old], new), ([old], False))

    def test_it_takes_a_later_sighting_that_lists_cells(self):
        first, later = tab("m", [], red_cells_omitted=7), tab("m", cells(3))
        self.assertEqual(frl.merge_tabs([first], [later]), ([later], True))
        # (not one that lists none either; and a first one TestGrid showed that lists a cell wins over any later one)
        self.assertEqual(frl.merge_tabs([first], [tab("m", [], red_cells_omitted=5)]), ([first], False))
        some = tab("m", cells(1), red_cells_omitted=7)
        self.assertEqual(frl.merge_tabs([some], [later]), ([some], False))

    def test_only_an_int_from_1_to_max_json_bytes_on_a_tab_testgrid_showed_counts(self):
        # (only a hand-edited file holds another; a tab judged from GCS counts none)
        odd = [tab("m", [], red_cells_omitted=n) for n in (0, -3, True, "7", 7.0, [7], frl.MAX_JSON_BYTES + 1)]
        odd += [tab("m", cells(1), judged_from_gcs=judged, red_cells_omitted=7) for judged in (True, 1)]
        for i, t in enumerate(odd):
            with self.subTest(i=i, n=t["red_cells_omitted"]):
                self.assertEqual(frl.cells_left_out(t), 0)  # fails: AttributeError: no cells_left_out
        for n, fields in ((1, {}), (frl.MAX_JSON_BYTES, {}), (7, {"judged_from_gcs": False})):
            with self.subTest(n=n, **fields):
                self.assertEqual(frl.cells_left_out(tab("m", [], red_cells_omitted=n, **fields)), n)  # fails: AttributeError

    def test_a_count_that_is_not_one_makes_no_sighting_on_testgrid(self):
        # (the tab is then as one from GCS that lists no red cell)
        for n in (0, -3, True, "7", 7.0, [7], frl.MAX_JSON_BYTES + 1):
            with self.subTest(n=n):
                odd = tab("m", [], red_cells_omitted=n)
                self.assertTrue(frl.index_entry("job", "1", meta(odd, backfilled="note"), time.time())["from_gcs"])
                self.assertEqual(self.sightings_kept([odd, tab("v", cells(1))]), ["g", "v"])
                # fails: (n other than 0) ['g', 'm', 'v']
        self.assertEqual(self.sightings_kept([tab("m", [], red_cells_omitted=1)]), ["g", "m"])  # (a count of 1 makes one)

    def sightings_kept(self, new):
        """The tabs of an archived build red on tab g once a later run saw it on the tabs `new`."""
        with tempfile.TemporaryDirectory() as target:
            frl.write_json(os.path.join(target, "meta.json"), meta(tab("g", cells(1)), archived_by_run="R0"))
            frl.add_sightings(target, {"tabs": new}, "R1")
            return [t["tab"] for t in frl.read_json(os.path.join(target, "meta.json"))["tabs"]]


def red_tests_cell(entry):
    """The Red tests cell of the entry's row on an index page."""
    return frl.build_rows([dict(entry, status="x")], files=False)[-1].split(" | ")[3]


class IndexEntry(unittest.TestCase):
    def test_the_cells_left_out_are_counted_apart_from_the_red_tests(self):
        m = meta(tab("a", cells(20), red_cells_omitted=4), tab("b", cells(2), red_cells_omitted=6))
        entry = frl.index_entry("job", "1", m, time.time())
        self.assertEqual((entry["red_test_count"], entry.get("red_cells_omitted")), (20, 10))  # fails: (20, None)
        self.assertEqual(red_tests_cell(entry), "t00 (+19) (and 10 red cells left out)")  # fails: 't00 (+19)'

    def test_a_tab_with_all_its_cells_left_out_says_so(self):
        entry = frl.index_entry("job", "1", meta(tab("a", [], red_cells_omitted=7)), time.time())
        self.assertEqual((entry["red_tests"], entry.get("red_cells_omitted")), ([], 7))  # fails: ([], None)
        self.assertEqual(red_tests_cell(entry), "(7 red cells left out)")  # fails: ''
        entry = frl.index_entry("job", "1", meta(tab("a", [], red_cells_omitted=1)), time.time())
        self.assertEqual(red_tests_cell(entry), "(1 red cell left out)")  # fails: ''
        m = meta(tab("g", cells(1), judged_from_gcs=True), tab("m", [], red_cells_omitted=7), backfilled="note")
        entry = frl.index_entry("job", "1", m, time.time())
        self.assertEqual(red_tests_cell(entry), "t00 (and 7 red cells left out)")  # fails: 't00'

    def test_an_entry_with_none_left_out_is_as_before(self):
        # (it has no such key, so the week pages' JSON of every other build stays as it was)
        now, base = time.time(), meta(tab("a", cells(2)), tab("b", []))
        plain = frl.index_entry("job", "1", base, now)
        self.assertNotIn("red_cells_omitted", plain)
        for n in (0, -3, True, "7", 7.0, [7], frl.MAX_JSON_BYTES + 1, 10 ** 4299):
            with self.subTest(n=str(n)[:12]):
                m = dict(base, tabs=[tab("a", cells(2), red_cells_omitted=n), tab("b", [], red_cells_omitted=n)])
                self.assertEqual(frl.index_entry("job", "1", m, now), plain)


class MassFailureIndexed(ArchiveWorld):
    def test_the_index_counts_the_cells_left_out(self):
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.show([self.build], {f"t{r}": ([12], ["boom"]) for r in range(5)})
        with mock.patch.object(frl, "MAX_RED_CELLS", 3):
            self.assertEqual(self.fetch(), 0)
        entries = []
        for path in glob.glob(os.path.join(self.root, "weeks", "*.json")):
            with open(path, encoding="utf-8") as f:
                entries += json.load(f)
        entry = next(e for e in entries if e["build"] == self.build)
        self.assertEqual((entry["red_test_count"], entry.get("red_cells_omitted")), (3, 2))  # fails: (3, None)
        with open(os.path.join(self.root, "INDEX.md"), encoding="utf-8") as f:
            self.assertIn("| t0 (+2) (and 2 red cells left out) |", f.read())  # fails: the row says only t0 (+2)


if __name__ == "__main__":
    unittest.main()
