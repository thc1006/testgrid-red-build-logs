"""Review of 2026-10-15, on cab048a: Go's regexp errors at the end of a pattern. Go's
nextRune("") returns utf8.RuneError and no error (regexp/syntax/parse.go, Go 1.26.5). So a
pattern that ends right after \\x and one more character other than { is an invalid escape
sequence (no second hex digit), and one that ends right after \\p or \\P an invalid character
class range (no class named ""). cab048a took nextRune("") for an error: it reported invalid
UTF-8 for the first, and an invalid escape sequence for the second. Each test fails on cab048a
for the reason its docstring gives, but those named test_control_*, which pass there. Every
expected error is the one Go 1.26.5's regexp.Compile gives. Both versions reject the same
patterns, so only the error a run reports changed.

Run: cd tool && python -m unittest test_review_round23 -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import goregexp  # noqa: E402

AT_THE_END = {  # pattern: regexp.Compile's error, from Go 1.26.5
    r"\p": r"invalid character class range: `\p`",
    r"\P": r"invalid character class range: `\P`",
    r"a\p": r"invalid character class range: `\p`",
    r"(\P": r"invalid character class range: `\P`",
    r"(?i)\p": r"invalid character class range: `\p`",
    r"[\p": r"invalid character class range: `\p`",
    r"[\P": r"invalid character class range: `\P`",
    r"[a\P": r"invalid character class range: `\P`",
    r"\x5": r"invalid escape sequence: `\x5`",
    r"\x$": r"invalid escape sequence: `\x$`",
    r"a\xG": r"invalid escape sequence: `\xG`",
    "\\x\u00e9": "invalid escape sequence: `\\x\u00e9`",
    "\\x\U0001F600": "invalid escape sequence: `\\x\U0001F600`",
    r"[\x5": r"invalid escape sequence: `\x5`",
    r"[a-\x5": r"invalid escape sequence: `\x5`",
    "[a-\\x\U0001F600": "invalid escape sequence: `\\x\U0001F600`",
}
AROUND = {  # (the error names the escape, and stops where Go's does)
    r"\p{": r"invalid character class range: `\p{`",
    r"\P{": r"invalid character class range: `\P{`",
    r"\pXa": r"invalid character class range: `\pX`",
    "\\P\U0001F600": "invalid character class range: `\\P\U0001F600`",
    r"\x": r"invalid escape sequence: `\x`",
    r"\x{": r"invalid escape sequence: `\x{`",
    r"\x{5": r"invalid escape sequence: `\x{5`",
    r"\x5]": r"invalid escape sequence: `\x5]`",
    r"[\x5]": r"invalid escape sequence: `\x5]`",
    r"\xG5z": r"invalid escape sequence: `\xG5`",
    r"\x5Gz": r"invalid escape sequence: `\x5G`",
    r"[\x5G-z]": r"invalid escape sequence: `\x5G`",
    r"\x5\x41": r"invalid escape sequence: `\x5\`",
    r"\8a": r"invalid escape sequence: `\8`",
    r"\qz": r"invalid escape sequence: `\q`",
}


class GoErrorsAtTheEnd(unittest.TestCase):
    def check(self, errors):
        for pattern, want in errors.items():
            with self.subTest(pattern=pattern):
                with self.assertRaises(goregexp.Error) as caught:
                    goregexp.compile(pattern)
                self.assertEqual(str(caught.exception), "error parsing regexp: " + want)

    def test_each_error_at_the_end_is_gos(self):
        """cab048a: "invalid UTF-8: ``" after \\x and one more character (not {), and "invalid
        escape sequence: `\\p`" (or `\\P`) for \\p or \\P at the end."""
        self.check(AT_THE_END)

    def test_control_each_error_around_them_is_gos(self):
        self.check(AROUND)

    def test_control_the_patterns_around_them_compile(self):
        for pattern in (r"\pL", r"\PL", r"\p{Greek}", r"[\pL]", r"\x41", r"\x{41}", r"[\x41-\x5A]"):
            with self.subTest(pattern=pattern):
                goregexp.compile(pattern)


if __name__ == "__main__":
    unittest.main()
