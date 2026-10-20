"""A mass failure: past MAX_RED_CELLS red cells in one table, meta.json lists only the first and counts the
rest (red_cells_omitted, see red_columns). A tab TestGrid showed red with every cell of the build left out
lists no red cell, as a sighting taken from GCS before judged_from_gcs does, and the index took it for one:
it could say a backfilled build TestGrid showed red on such a tab was never seen red on TestGrid.

Run: cd tool && python -m unittest test_mass_failure -v
"""
import os
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_integrity import HOUR  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
