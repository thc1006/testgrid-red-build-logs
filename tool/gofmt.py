# Ported in part from Go's fmt, strconv and unicode (Go 1.26.5). Copyright The Go Authors; BSD 3-Clause
# license in third_party/go/LICENSE. THIRD_PARTY_NOTICES.md lists what was ported and what changed.
"""Go's fmt.Sprintf with string operands, as TestGrid renders a test name by a test
group's test_name_config (pkg/updater nameConfig.render: fmt.Sprintf(format,
parts...), every part a string), at the Go TestGrid production is built with
(tool/oracle/pinned.json): fmt's doPrintf, printArg and fmtString, with
strconv's Quote, QuoteToASCII and CanBackquote. Strings are Python str: the
config, test names, junit properties and file names are valid UTF-8, so a rune is
a code point."""
from goregexp import unicode_tables

_LETTER_CLASSES = ("L", "M", "N", "P", "S")  # unicode.IsPrint: letters, marks, numbers, punctuation, symbols
_PRINT = None


def _in(table, r):
    for lo, hi, stride in table:
        if r < lo:
            return False
        if r <= hi:
            return (r - lo) % stride == 0
    return False


def is_print(r):
    """strconv.IsPrint (unicode.IsPrint: the classes above, and the ASCII space)."""
    global _PRINT
    if r <= 0xFF:
        return 0x20 <= r <= 0x7E or (0xA1 <= r <= 0xFF and r != 0xAD)
    if _PRINT is None:
        data = unicode_tables()
        _PRINT = [sorted(data["categories"][c]) for c in _LETTER_CLASSES]
    return any(_in(t, r) for t in _PRINT)


def quote(s, ascii_only=False):
    """strconv.Quote (QuoteToASCII with ascii_only)."""
    out = ['"']
    for ch in s:
        r = ord(ch)
        if ch in ('"', "\\"):
            out.append("\\" + ch)
        elif (r < 0x80 if ascii_only else True) and is_print(r):
            out.append(ch)
        elif ch in "\a\b\f\n\r\t\v":
            out.append({"\a": "\\a", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t",
                        "\v": "\\v"}[ch])
        elif r < 0x20 or r == 0x7F:
            out.append(f"\\x{r:02x}")
        elif r < 0x10000:
            out.append(f"\\u{r:04x}")
        else:
            out.append(f"\\U{r:08x}")
    out.append('"')
    return "".join(out)


def can_backquote(s):
    """strconv.CanBackquote."""
    for ch in s:
        r = ord(ch)
        if r >= 0x80:
            if r == 0xFEFF:
                return False
            continue
        if (r < 0x20 and ch != "\t") or ch == "`" or r == 0x7F:
            return False
    return True


class _Fmt:
    """fmt's flags, width and precision for one verb."""

    def __init__(self):
        self.wid_present = self.prec_present = self.minus = self.plus = self.sharp = self.space = self.zero = False
        self.plus_v = self.sharp_v = False
        self.wid = self.prec = 0

    def padding(self, n):
        return ("0" if self.zero and not self.minus else " ") * n if n > 0 else ""

    def pad(self, s):
        if not self.wid_present or self.wid == 0:
            return s
        fill = self.padding(self.wid - len(s))
        return s + fill if self.minus else fill + s

    def truncate(self, s):
        return s[:max(self.prec, 0)] if self.prec_present else s

    def fmt_s(self, s):
        return self.pad(self.truncate(s))

    def fmt_q(self, s):
        s = self.truncate(s)
        if self.sharp and can_backquote(s):
            return self.pad("`" + s + "`")
        return self.pad(quote(s, ascii_only=self.plus))

    def fmt_sx(self, s, upper):
        b = s.encode("utf-8")
        length = len(b)
        if self.prec_present and self.prec < length:
            length = self.prec
        width = 2 * length
        if width > 0:
            if self.space:
                if self.sharp:
                    width *= 2
                width += length - 1
            elif self.sharp:
                width += 2
        else:
            return self.padding(self.wid) if self.wid_present else ""
        out = []
        if self.wid_present and self.wid > width and not self.minus:
            out.append(self.padding(self.wid - width))
        x = "X" if upper else "x"
        if self.sharp:
            out.append("0" + x)
        digits = "0123456789ABCDEF" if upper else "0123456789abcdef"
        for i in range(length):
            if self.space and i > 0:
                out.append(" ")
                if self.sharp:
                    out.append("0" + x)
            out.append(digits[b[i] >> 4] + digits[b[i] & 0xF])
        if self.wid_present and self.wid > width and self.minus:
            out.append(self.padding(self.wid - width))
        return "".join(out)


def _print_arg(f, arg, verb):
    """printArg for a string operand."""
    if verb == "T":
        return f.fmt_s("string")
    if verb == "v":
        return f.fmt_q(arg) if f.sharp_v else f.fmt_s(arg)
    if verb == "s":
        return f.fmt_s(arg)
    if verb in ("x", "X"):
        return f.fmt_sx(arg, verb == "X")
    if verb == "q":
        return f.fmt_q(arg)
    return "%!" + verb + "(string=" + _print_arg(f, arg, "v") + ")"  # (badVerb; %p too: not a pointer)


_TOO_LARGE = 1_000_000


def _parsenum(s, start, end):
    """parsenum: (number, isnum, new index)."""
    if start >= end:
        return 0, False, end
    num, isnum, i = 0, False, start
    while i < end and "0" <= s[i] <= "9":
        if num > _TOO_LARGE or num < -_TOO_LARGE:
            return 0, False, end  # (overflow: a crazy long number)
        num = num * 10 + ord(s[i]) - 0x30
        isnum = True
        i += 1
    return num, isnum, i


def _parse_arg_number(s):
    """parseArgNumber of format[i:]: (index, width consumed, ok)."""
    if len(s) < 3:
        return 0, 1, False
    for i in range(1, len(s)):
        if s[i] == "]":
            width, ok, newi = _parsenum(s, 1, i)
            if not ok or newi != i:
                return 0, i + 1, False
            return width - 1, i + 1, True
    return 0, 1, False


def sprintf(fmt, args):
    """fmt.Sprintf(fmt, args...) for string args."""
    out, end, arg_num, after_index, reordered, i = [], len(fmt), 0, False, False, 0
    n = len(args)

    def arg_number(arg_num, i):
        nonlocal reordered, good_arg_num
        if i >= end or fmt[i] != "[":
            return arg_num, i, False
        reordered = True
        index, wid, ok = _parse_arg_number(fmt[i:])
        if ok and 0 <= index < n:
            return index, i + wid, True
        good_arg_num = False
        return arg_num, i + wid, ok
    while i < end:
        good_arg_num = True
        last = i
        while i < end and fmt[i] != "%":
            i += 1
        if i > last:
            out.append(fmt[last:i])
        if i >= end:
            break
        i += 1
        f = _Fmt()
        simple = False
        while i < end:
            c = fmt[i]
            if c == "#":
                f.sharp = True
            elif c == "0":
                f.zero = True
            elif c == "+":
                f.plus = True
            elif c == "-":
                f.minus = True
            elif c == " ":
                f.space = True
            else:
                if "a" <= c <= "z" and arg_num < n:  # (the fast path: flags, then a lower case verb)
                    if c in "vw":
                        f.sharp_v, f.sharp, f.plus_v, f.plus = f.sharp, False, f.plus, False
                    out.append(_print_arg(f, args[arg_num], c))
                    arg_num += 1
                    i += 1
                    simple = True
                break
            i += 1
        if simple:
            continue
        arg_num, i, after_index = arg_number(arg_num, i)
        if i < end and fmt[i] == "*":  # (a width from an argument: never an int here)
            i += 1
            if arg_num < n:
                arg_num += 1
            out.append("%!(BADWIDTH)")
            after_index = False
        else:
            f.wid, f.wid_present, i = _parsenum(fmt, i, end)
            if after_index and f.wid_present:  # ("%[3]2d")
                good_arg_num = False
        if i + 1 < end and fmt[i] == ".":
            i += 1
            if after_index:  # ("%[3].2d")
                good_arg_num = False
            arg_num, i, after_index = arg_number(arg_num, i)
            if i < end and fmt[i] == "*":
                i += 1
                if arg_num < n:
                    arg_num += 1
                f.prec, f.prec_present = 0, False
                out.append("%!(BADPREC)")
                after_index = False
            else:
                f.prec, f.prec_present, i = _parsenum(fmt, i, end)
                if not f.prec_present:
                    f.prec, f.prec_present = 0, True
        if not after_index:
            arg_num, i, after_index = arg_number(arg_num, i)
        if i >= end:
            out.append("%!(NOVERB)")
            break
        verb = fmt[i]
        i += 1
        if verb == "%":
            out.append("%")
        elif not good_arg_num:
            out.append("%!" + verb + "(BADINDEX)")
        elif arg_num >= n:
            out.append("%!" + verb + "(MISSING)")
        else:
            if verb in "vw":
                f.sharp_v, f.sharp, f.plus_v, f.plus = f.sharp, False, f.plus, False
            out.append(_print_arg(f, args[arg_num], verb))
            arg_num += 1
    if not reordered and arg_num < n:
        out.append("%!(EXTRA " + ", ".join("string=" + _print_arg(_Fmt(), a, "v") for a in args[arg_num:]) + ")")
    return "".join(out)
