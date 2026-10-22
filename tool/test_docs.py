"""The docs, and the pages a run writes, say what the code does.

The checks that what README.md, AGENTS.md, THIRD_PARTY_NOTICES.md, tool/oracle/README.md and the pages a
run writes restate of the code still holds go here: a change to the code that makes one wrong then fails
until the text changes too. So far, INDEX.md's row of red builds given up on is checked.

Run: cd tool && python -m unittest test_docs -v
"""
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402


class Templates(unittest.TestCase):
    """INDEX.md's row of red builds given up on prints GIVE_UP_DAYS, not a 14 of its own."""

    def test_the_given_up_row_says_give_up_days(self):
        with mock.patch.object(frl, "GIVE_UP_DAYS", 9):
            rows = frl.holdings([], [], None)
        self.assertIn("| Red builds given up on (after 9 days) | 0 |", rows)  # fails: (after 14 days)
