"""The docs, and the pages a run writes, say what the code does.

The tests check README.md, AGENTS.md, THIRD_PARTY_NOTICES.md and tool/oracle/README.md, as GitHub shows them (see
Page): their links and anchors, and those of the pages a run writes; HTML comments and code fences that could hide
text; the README's numbers, against the constants and workflow settings they come from; the oracle's files; the
versions the ported files come from; and the days INDEX.md says red builds are given up on after. The test
workflow runs on a change to any file they read.

Run: cd tool && uv run python -m unittest test_docs -v
"""
import email.message
import gzip
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
from html.parser import HTMLParser
from unittest import mock

import cmarkgfm
import yaml
from cmarkgfm.cmark import Options

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import report  # noqa: E402

DOCS = ["README.md", "AGENTS.md", "THIRD_PARTY_NOTICES.md", "tool/oracle/README.md"]
# (the docs, and the other files outside tool/ that the tests read)
READ = DOCS + ["LICENSE", "third_party/go/LICENSE", "third_party/unicode/LICENSE", ".gitignore",
               ".github/workflows/red-build-logs.yml", ".github/workflows/test.yml", ".github/workflows/oracle.yml"]
# (cmark-gfm's options: raw HTML, footnotes and each block's lines; cmark-gfm is GitHub's fork of CommonMark's parser)
OPTIONS = Options.CMARK_OPT_UNSAFE | Options.CMARK_OPT_FOOTNOTES | Options.CMARK_OPT_SOURCEPOS
GITHUB = os.path.join(HERE, "testdata", "github_markdown.json")
BLOCKS = {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "dt", "dd"}  # (the elements that hold text)
HEADINGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
URL = re.compile(r"(?i)[a-z][a-z0-9+.-]*:|//")  # (a link to another site)
COMMENT_END = re.compile(r"--!?>")  # (what ends an HTML comment)
RAW_UNSAFE = re.compile(r"--!?>|<[?!]|</(?![A-Za-z>])")  # (an arrow, or what a browser reads as a comment)


def read(rel):
    """A file of the repository, by its path from the root."""
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def comments(html):
    """[(start, end)] of each HTML comment of HTML, as a browser reads it: "<!-->" and "<!--->" are whole ones, "--!>"
    ends one as "-->" does, and one with no end (end None) takes the rest. A "<!--" in a raw tag's attribute value
    counts here too, though a browser reads it as part of the value. Read here, not by HTMLParser, whose reading of
    "<!-->" depends on the Python version."""
    out, i = [], html.find("<!--")
    while i >= 0:
        if html.startswith((">", "->"), i + 4):
            end = i + 5 + (html[i + 4] == "-")
        else:
            m = COMMENT_END.search(html, i + 4)
            if not m:
                return out + [(i, None)]
            end = m.end()
        out.append((i, end))
        i = html.find("<!--", end)
    return out


def uncommented(html):
    """HTML without its comments (see comments)."""
    kept, at = [], 0
    for start, end in comments(html):
        kept.append(html[at:start])
        at = len(html) if end is None else end
    return "".join(kept) + html[at:]


class Page(HTMLParser):
    """A Markdown page as GitHub shows it, read from cmark-gfm's HTML of it (see OPTIONS):
    - links: each a's href, img's src, and each srcset candidate of an img or a source, in order;
    - anchors: each heading's, slugged as GitHub does, and each element's id and name (the word characters a slug
      keeps are Python's, which, unlike GitHub's, leave out combining marks and take in numbers such as a superscript
      two);
    - blocks: [(tag, [(kind, text)])] of each paragraph, list item, heading, table cell, dt and dd, in the order they
      open. Text goes to the innermost block open, so an item's text after a nested list is the item's; a list item's
      first paragraph, with nothing before it in the item, is in the item's block, as a tight item's text is; text in
      no block is in one of its own (tag None). kind is "text", "code" (a code span), "pre" (a code block), "alt" (an
      image's alt text) or "mark" (a footnote's);
    - unsafe: the line of the first HTML comment (see comments) that is not whole on one line (one with no end takes
      the rest of the HTML, lines and all) or holds another "<!--", or of the first "-->", "--!>", "<?", "<!" or
      "</" not before a letter of raw HTML outside comments, even in an attribute value (cmark-gfm writes those of
      code and text with "&gt;" and "&lt;"); else None. GitHub may hide text after such a comment, such an arrow
      ends any comment open above it, and a browser reads the others as a comment up to the next ">". It misses a
      mistyped end before an arrow on its comment's one line: "<!-- a -> b --> c" hides " b";
    - unclosed: the first line of each fenced code block that never closes, which GitHub shows as code to the end of
      its container."""

    def __init__(self, markdown):
        super().__init__(convert_charrefs=True)
        self.lines = markdown.split("\n")
        self.links, self.ids, self.blocks, self.unclosed, self.headings = [], [], [], [], []
        self.stack, self.open, self.heading, self.loose, self.pre = [], [], None, None, None
        html = cmarkgfm.github_flavored_markdown_to_html(markdown, options=OPTIONS)
        unsafe, at = [], 0  # ((offset, HTML its Markdown line holds) of each)
        for start, end in comments(html) + [(len(html), len(html))]:  # (and the HTML after the last)
            unsafe += [(m.start(), html[html.rfind("\n", 0, m.start()) + 1:m.end()])
                       for m in RAW_UNSAFE.finditer(html, at, start)]
            if "\n" in html[start:end] or "<!--" in html[start + 4:end]:
                unsafe.append((start, html[start:end].split("\n")[0]))
            at = len(html) if end is None else end
        self.unsafe = self.line_of(html, *unsafe[0]) if unsafe else None
        self.feed(uncommented(html))
        self.close()
        self.anchors, taken, last = set(self.ids), set(), {}
        for heading in self.headings:
            slug = re.sub(r"[^\w\- ]", "", heading.lower()).replace(" ", "-")
            candidate, n = slug, last.get(slug, 0)  # (from the last -n of its slug: those before it are taken)
            while candidate in taken:
                n += 1
                candidate = f"{slug}-{n}"
            last[slug] = n
            taken.add(candidate)
            self.anchors.add(candidate)

    def line_of(self, html, at, piece):
        """The line of the Markdown that the HTML at `at` comes from: the one that holds the `piece` at `at`, counting
        each from the first line of the last block cmark-gfm made before `at` if `at` is in it, else from the line
        after it (raw HTML has no source position); else, in that block, the line of `at` in its HTML, else that line.
        A code span before it in its block can make it a line early."""
        first, base, blocks = 1, 0, list(re.finditer(r'<(\w+) data-sourcepos="(\d+):\d+-(\d+):', html[:at]))
        if blocks:
            tag, start, end = blocks[-1].groups()
            base = html.rfind(f"</{tag}>", blocks[-1].end(), at)
            first, base = (int(start), blocks[-1].start()) if base < 0 else (int(end) + 1, base)
        skip = html.count(piece, base, at)  # (the same words on earlier lines)
        for i in range(first - 1, len(self.lines)):
            skip -= self.lines[i].count(piece)
            if skip < 0:
                return i + 1
        if blocks and base == blocks[-1].start():  # (its HTML differs from its Markdown line: count the HTML's lines)
            return first + html.count("\n", base, at)
        return first

    def kind(self):
        kinds = {k for _, k in self.stack}
        return next((k for k in ("pre", "code", "mark") if k in kinds), "text")

    def run(self, kind, text):
        if self.open:
            self.blocks[self.open[-1][1]][1].append((kind, text))
        elif text.strip() or self.loose is not None:  # (text in no block: in one of its own, until a block opens)
            if self.loose is None:
                self.loose = len(self.blocks)
                self.blocks.append((None, []))
            self.blocks[self.loose][1].append((kind, text))

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "a" and a.get("href") is not None:
            self.links.append(a["href"])
        if tag == "img" and a.get("src") is not None:
            self.links.append(a["src"])
        if tag in ("img", "source") and a.get("srcset"):
            self.links += [c.split()[0] for c in a["srcset"].split(",") if c.strip()]
        self.ids += [a[k] for k in ("id", "name") if a.get(k)]
        if tag in BLOCKS:  # (self.open: (tag, its block) of each block open)
            item = self.open[-1][1] if self.open and self.open[-1][0] == "li" else None
            if tag == "p" and item == len(self.blocks) - 1 and not any(t.strip() for _, t in self.blocks[item][1]):
                self.open.append((tag, item))  # (an item's first paragraph with no text or block before it: as in a tight item)
            else:
                self.open.append((tag, len(self.blocks)))
                self.blocks.append((tag, []))
            self.loose = None
        if tag in HEADINGS:
            self.heading = []
        if tag == "pre":
            self.pre = (a.get("data-sourcepos"), [])
        if tag == "img" and a.get("alt"):
            self.run("alt", a["alt"])
        if tag not in VOID:
            mark = tag == "sup" and "footnote-ref" in (a.get("class") or "").split()
            self.stack.append((tag, "mark" if mark else tag if tag in ("pre", "code") else None))

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        if tag in HEADINGS and self.heading is not None:
            self.headings.append("".join(self.heading))
            self.heading = None
        if tag == "pre" and self.pre:
            self.fence(*self.pre)
            self.pre = None
        if tag in BLOCKS and self.open and self.open[-1][0] == tag:
            self.open.pop()
        if any(t == tag for t, _ in self.stack):
            while self.stack.pop()[0] != tag:
                pass

    def handle_data(self, data):
        if self.heading is not None:
            self.heading.append(data)
        if self.pre:
            self.pre[1].append(data)
        self.run(self.kind(), data)

    def fence(self, sourcepos, code):
        """Adds to unclosed the first line of a fenced code block whose last line (by its source position) is no fence
        of its first's character and length. (An indented code block's text holds its first line; a fenced one's does
        not.)"""
        if not sourcepos:
            return
        (l1, c1), (l2, _) = (map(int, p.split(":")) for p in sourcepos.split("-"))
        first = self.lines[l1 - 1].encode()[c1 - 1:].decode("utf-8", "replace")
        m = re.match(r"`{3,}|~{3,}", first)
        if not m or "".join(code).startswith(first):
            return
        if not (l2 > l1 and re.fullmatch(rf"[ \t>]*{re.escape(m[0][0])}{{{len(m[0])},}}[ \t]*", self.lines[l2 - 1])):
            self.unclosed.append(l1)

    def text(self, *kinds):
        """The page's text of these kinds (see blocks), each block's joined to the next by a space."""
        return " ".join("".join(t for k, t in runs if k in kinds) for _, runs in self.blocks)

    @property
    def code(self):
        """The text of each of its code spans, in order."""
        return [t for _, runs in self.blocks for k, t in runs if k == "code"]


def anchors(text):
    """The anchors of a Markdown page (see Page)."""
    return Page(text).anchors


OUTSIDE = object()  # (where a link one level out of the repository leads: a page GitHub does not serve)


def links(text):
    """The relative links of a Markdown page (see Page): inline, by reference and in HTML, but not those in code."""
    return [x for x in Page(text).links if x and not URL.match(x)]


def target(link, page, root, archive=None):
    """(the file a link of `page` leads to, its anchor), with `page` a path under `root`; a link into
    `root`/archive/ leads into `archive` instead, if given; OUTSIDE for one a level out of `root`, which is
    broken; None for one two or more levels out, which leads to a page of GitHub's site (../../issues is the
    repository's issues), not checked. The path and the anchor are percent-decoded (cmark-gfm's HTML encodes a
    space, or a letter outside ASCII, in either). The anchor of a file shown as source (?plain=1) is a line's, not
    a heading's, and is dropped."""
    path, _, anchor = link.partition("#")
    path, _, query = path.partition("?")
    path, anchor = urllib.parse.unquote(path), "" if "plain=1" in query.split("&") else urllib.parse.unquote(anchor)
    if not path:
        return page, anchor
    full = os.path.normpath(os.path.join(root, path.lstrip("/")) if path.startswith("/") else
                            os.path.join(os.path.dirname(page), path))
    out = os.path.relpath(full, root)
    if out.startswith(("../..", "..\\..")):
        return None, anchor
    if out == ".." or out.startswith(("../", "..\\")):
        return OUTSIDE, anchor
    inside = os.path.relpath(full, os.path.join(root, "archive"))
    return (os.path.join(archive, inside) if archive and not inside.startswith("..") else full), anchor


def unread(linked, root=ROOT):
    """Of the files and folders under `root` that `linked` names, outside tool/ and archive/, those not known
    to run the tests when they change: a file READ does not hold (TestWorkflowPaths checks that each one READ
    holds does), a folder with no file of READ in it."""
    out = []
    for path in sorted(linked):
        rel = os.path.relpath(path, root).replace(os.sep, "/")
        if rel.split("/")[0] in ("tool", "archive"):
            continue
        if os.path.isdir(path) and (rel == "." or any(r.startswith(rel + "/") for r in READ)):
            continue
        if rel not in READ:
            out.append(rel)
    return out


class Templates(unittest.TestCase):
    """INDEX.md's row of red builds given up on prints GIVE_UP_DAYS, not a 14 of its own."""

    def test_the_given_up_row_says_give_up_days(self):
        with mock.patch.object(frl, "GIVE_UP_DAYS", 9):
            rows = frl.holdings([], [], None)
        self.assertIn("| Red builds given up on (after 9 days) | 0 |", rows)  # fails: (after 14 days)


class ReadmeNumbers(unittest.TestCase):
    """Each number written in digits in README.md's prose (as GitHub shows it: see prose) is the code's, the
    workflow's or LICENSE's, in the words bound to it below, or one no test can bind (GitHub's, or the README's own
    advice): UNBOUND lists them, to be checked by hand. Each of those words must be in the README: a change that
    deletes or rewords the sentence that holds one changes them here too. (Numbers written as words, "step N" and a
    number right after a letter, such as md5's 5, are not checked.)"""

    NUMBER = re.compile(r"(?<![^\W_])\d+")  # (in digits, after no letter or digit)

    UNBOUND = ["GitHub shows at most 100 results", "after 60 days without activity", "a ping about every 12 hours"]

    def prose(self, text=None):
        """README.md's prose (or text's), as GitHub shows it (see Page): its text and its images' alt text, but not
        its code, footnotes' marks, bare URLs, nor "step N", "step N.M" and "steps N and M" (not "step-by-step N").
        (A list's numbers are not a page's text.)"""
        text = Page(read("README.md") if text is None else text).text("text", "alt")
        text = re.sub(r"\bhttps?://[^\s<>\"']+", "", text)
        text = re.sub(r"(?i)(?<![\w-])steps?\s+\d+(?:\.\d+)*(?:\s+and\s+\d+(?:\.\d+)*)?\b", "", text)
        return " ".join(text.split())

    def test_control_the_prose(self):
        # (what GitHub shows of each form, GitHubsReading checks; these check what prose keeps of it)
        self.assertEqual(self.prose('1. In step 3, [a](b.md) `c 5` and Step\n   4 <img alt="d 6" src="e">, steps 2 and\n   3.'),
                         "In , a and d 6, .")
        self.assertEqual(self.prose("[f][7] at https://x.example/8 [^9] g\n\n[7]: https://x.example/10\n[^9]: h 11\n\n"
                                    '<a href="https://x.example/12">i</a> j 13\n'), "f at g i j 13 h 11 \u21a9")
        self.assertEqual(self.prose("a <24 hours, b -> c, <br/>d <img src=\"e\" alt=\"f 7\">\n"), "a <24 hours, b -> c, d f 7")
        self.assertEqual(self.prose("95&nbsp;MiB, a&#8212;b, _14_ days, x step-by-step 17 days, as in _step 3_\n\n"
                                    "    an indented 5\n\n```\nfenced 6\n```\n<!-- a 7 -->\n"),
                         "95 MiB, a\u2014b, 14 days, x step-by-step 17 days, as in")
        self.assertEqual(self.prose('see "https://x.example/a", 7 days, step 4.1 and step\n4.2.\n'), 'see "", 7 days, and .')
        # (HTML blocks next to each other, and with a paragraph between them: their text in order, a space between)
        self.assertEqual(self.prose('<picture>\n<img alt="a" src="b">\n</picture>\n\n<picture>\n<img alt="30 days" '
                                    'src="c">\n</picture>\n\n<div>over the last</div>\n\nd\n\n<div>14 days</div>\n'),
                         "a 30 days over the last d 14 days")

    def hours(self, seconds):
        hours, rest = divmod(int(seconds), 3600)
        self.assertEqual(rest, 0, seconds)
        return hours

    def bound(self):
        """README.md's words as the code's and the workflow's numbers make them."""
        workflow = read(".github/workflows/red-build-logs.yml")
        every = re.search(r'cron: "\d+ \*/(\d+) \* \* \*"', workflow).group(1)
        half = self.hours(re.search(r"now / (\d+) \* \1\b", workflow).group(1))
        due = re.search(r'"\$age" -ge \$\(\((\d+) \* 3600\)\)', workflow).group(1)
        late = self.hours(re.search(r"fromJSON\(needs\.due\.outputs\.age\) > (\d+)", workflow).group(1))
        days = frl.GIVE_UP_DAYS
        license_version = re.match(r"\s*Apache License\s+Version\s+(\d+\.\d+),", read("LICENSE")).group(1)
        return [f"red builds of the last {report.HEAT_DAYS} days", f"per tab over the last {report.HEAT_DAYS} days",
                f"a log over {frl.MAX_FILE_MB} MiB after gzip", f"GitHub blocks files over {cc.GITHUB_MAX_BYTES >> 20} MiB",
                f"One still waiting {frl.HELD_DAYS} days after it was made", f"at most {days} days after that run",
                f"been unlisted for {frl.UNLISTED_DAYS} days",
                f"the part of the gap in the last {days} days and {days} days more in the run that finds it, then "
                f"{days} days more in each later run",
                f"until {frl.RECHECK_HOURS} hours after it finished", f"compared until {days} days after it was archived",
                f"failed for {days} days", f"scheduled every {every} hours", f"(00:00 to {half:02d}:00, {half:02d}:00 to 24:00)",
                f"less than {due} hours before", f"more than {late} hours old", f"Apache License {license_version}"]

    def test_each_number_bound_to_the_code_is_its(self):
        prose, bound = self.prose(), self.bound()  # fails: AttributeError: no MAX_FILE_MB
        why = ("README.md does not say it: if the code's, the workflow's or LICENSE's number changed, change the README's "
               "sentence to say the new one; if the sentence was reworded or deleted, change ReadmeNumbers' words with it")
        for words in bound + self.UNBOUND:
            with self.subTest(words):
                self.assertTrue(words in prose, why)

    def test_the_commit_steps_limit_is_the_split_size(self):
        # (the README's 95 MiB: the fetch stores larger files in parts; the commit step's limit, its messages and its
        # comment say the same size; test_review_round24's CommitStep checks that it refuses a file over it)
        workflow = read(".github/workflows/red-build-logs.yml")
        said = re.findall(r'-gt \$\(\((\d+) << 20\)\)', workflow) + re.findall(r"(?:larger than|none over) (\d+) MiB",
                                                                                 workflow)
        self.assertEqual((len(said), set(map(int, said))), (4, {frl.MAX_FILE_MB}),  # fails: AttributeError: no MAX_FILE_MB
                         "(the count, the numbers): the commit step's limit ($((N << 20))), comment (none over N MiB) and "
                         "two messages (larger than N MiB) say MAX_FILE_MB; change the 4 with a mention added or removed")
        steps = [s for job in yaml.safe_load(workflow)["jobs"].values() for s in job.get("steps", [])]
        fetch = [s["run"] for s in steps if s.get("id") == "fetch"]
        self.assertEqual(len(fetch), 1)
        self.assertIn("fetch_red_logs.py", fetch[0])
        self.assertNotIn("--max-f", fetch[0])  # (it splits at its default; argparse takes any unambiguous prefix)

    def test_control_number(self):
        self.assertEqual(self.NUMBER.findall("_14_ __15__ *3* md5 v17 2x a1 (6)"), ["14", "15", "3", "2", "6"])

    def test_no_other_number(self):
        prose, bound = self.prose(), self.bound()  # fails: AttributeError: no MAX_FILE_MB
        for words in bound + self.UNBOUND:
            prose = prose.replace(words, "")
        found = [prose[max(0, m.start() - 40):m.end() + 20] for m in self.NUMBER.finditer(prose)]
        self.assertEqual(found, [], "numbers in README.md's prose that bound() does not bind nor UNBOUND list: bind or "
                         "list each (if test_each_number_bound_to_the_code_is_its fails too, its message says what to change)")


class Links(unittest.TestCase):
    """Each relative link of the docs, and of the pages a run writes, leads to a file there is, and each
    #anchor of a Markdown page (or of a folder: its README.md, which GitHub shows there) to one of its anchors. The
    pages, and a doc's links into archive/, are checked in an archive the test writes (INDEX.md and the pages it
    links to, state.json, a run folder), so the archive in the checkout does not matter."""

    def check(self, link, page, root, archive=None):
        self.leads(link, *target(link, page, root, archive))

    def leads(self, link, file, anchor):
        """Where `link` leads, `file` and `anchor` as target gives them, is there: the file, and the anchor on it (a
        folder's: on its README.md)."""
        if file is None:
            return
        self.assertIsNot(file, OUTSIDE, link)
        self.assertTrue(os.path.exists(file), f"{file}: there is no such file; fix the link or add the file")
        page = os.path.join(file, "README.md") if os.path.isdir(file) else file
        if anchor and anchor.lower() != "top" and page.endswith(".md") and os.path.isfile(page):  # (#top: any page's)
            with open(page, encoding="utf-8") as f:
                self.assertIn(anchor, anchors(f.read()))

    def test_each_one_of_the_docs_leads_somewhere(self):
        with tempfile.TemporaryDirectory() as archive:
            frl.new_run_dir(archive, time.time())
            frl.write_json(os.path.join(archive, "state.json"), {})
            frl.write_index(archive, time.time())
            for doc in DOCS:
                for link in links(read(doc)):
                    with self.subTest(doc=doc, link=link):
                        self.check(link, os.path.join(ROOT, doc), ROOT, archive)

    def test_control_a_doc_links_into_the_archive(self):
        # (README.md reads as these links alone, the other docs as empty: a fault of the real docs fails only the test above)
        texts = {"README.md": "[s](archive/state.json) [i](archive/INDEX.md)\n"}
        with mock.patch(f"{__name__}.read", lambda rel: texts.get(rel, "")):
            self.test_each_one_of_the_docs_leads_somewhere()

    def test_each_one_of_the_pages_leads_somewhere(self):
        # (the pages are written into <tmp>/archive, so that a link out of the archive leads into the repository)
        with tempfile.TemporaryDirectory() as tmp:
            archive, now = os.path.join(tmp, "archive"), time.time()
            run_id, rel = frl.new_run_dir(archive, now)
            build = os.path.join(archive, rel, "job", "1")  # (one archived build: its week's page, its files' links)
            os.makedirs(build)
            with open(os.path.join(build, "build-log.txt.gz"), "wb") as f:
                f.write(b"x")
            frl.write_json(os.path.join(build, "meta.json"), {
                "job": "job", "build": "1", "started": now - 3600, "created": now - 3600, "query": "logs/job",
                "tabs": [{"dashboard": frl.DASHBOARDS[0], "tab": "t", "red_cells": [{"test": "job.Overall"}]}],
                "log": {"file": "build-log.txt.gz", "bytes": 1}})
            frl.write_json(os.path.join(archive, rel, "run.json"),
                           {"run": run_id, "started": now, "outcomes": {"refreshed": ["job/1"]}})
            frl.write_json(os.path.join(archive, rel, "verification.json"), {"outcome": "ok"})
            frl.write_index(archive, now)
            pages = [os.path.join(d, n) for d, _, names in os.walk(archive) for n in names if n.endswith(".md")]
            self.assertGreaterEqual(len(pages), 3, pages)  # (INDEX.md, the run's README.md, the week's page)
            with open(os.path.join(archive, "INDEX.md"), encoding="utf-8") as f:
                self.assertIn("../README.md#limits", links(f.read()))  # (why AGENTS.md keeps the heading)
            for page in pages:
                with open(page, encoding="utf-8") as f:
                    text = f.read()
                for link in links(text):
                    with self.subTest(page=os.path.relpath(page, tmp), link=link):
                        self.check_page(link, page, tmp)

    def check_page(self, link, page, tmp):
        """A link of a page written in tmp/archive/: into the archive, checked there; out of it, into the
        repository, there (and a change to what it leads to runs the tests: see unread)."""
        file, anchor = target(link, page, tmp)
        if file not in (None, OUTSIDE) and os.path.relpath(file, os.path.join(tmp, "archive")).startswith(".."):
            file = os.path.join(ROOT, os.path.relpath(file, tmp))
            self.assertEqual(unread({file}), [])
        self.leads(link, file, anchor)  # (OUTSIDE, or None for two levels out, there too)

    def test_control_a_pages_links_out_of_the_archive(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = os.path.join(tmp, "archive", "INDEX.md")
            self.check_page("../README.md#limits", index, tmp)
            for link in ("../../README.md", "../README.md#nowhere"):
                with self.subTest(link), self.assertRaises(AssertionError):
                    self.check_page(link, index, tmp)
            # (one outside tool/ and archive/ the tests are not known to run on a change to)
            with mock.patch(f"{__name__}.READ", [r for r in READ if r != ".gitignore"]), self.assertRaises(AssertionError):
                self.check_page("../.gitignore", index, tmp)

    def test_control_the_links_and_anchors_are_githubs(self):
        self.assertEqual(anchors("# A\n## When TestGrid's production manifests move\n## `tool/` and x.py\n## A\n"
                                 "## _Limits_ and [it](x)\n## `_private_`\nSetext\n---\n<a name=\"n\"></a>\n"
                                 "```\n# not a heading\n```\n~~~\n# nor this\n~~~\n<!--\n# nor this\n-->\n"),
                         {"a", "when-testgrids-production-manifests-move", "tool-and-xpy", "a-1", "limits-and-it",
                          "_private_", "setext", "n"})
        self.assertEqual(links('[a](<b c.md>) [d](e.md "title") `[f](g.md)` <!-- [h](i.md) --> [j][k]\n\n[k]: l.md\n'
                               '[^1]: a footnote\n\n<img srcset="m.svg 2x, n.svg 3x"> [o](HTTPS://p.example/q) [r](f(1).md)\n'
                               "<a href='s.md'>s</a> [t](//u.example) <img alt=x src=u.svg> <a href=v.md>v</a>\n"),
                         ["b%20c.md", "e.md", "l.md", "m.svg", "n.svg", "f(1).md", "s.md", "u.svg", "v.md"])
        self.assertEqual(anchors('## <a name="x"></a>Title\n## Title\n<a id="title-1">'), {"title", "title-1", "x"})
        # (a space at an end of a heading's text is a hyphen too, such as one a comment or a tag beside it leaves)
        self.assertEqual(anchors('## Limits <!-- a -->\n## <a name="y"></a> Title\n'), {"limits-", "-title", "y"})
        self.assertEqual(target("/tool/x.md?plain=1#L3", os.path.join(ROOT, "a", "b.md"), ROOT),
                         (os.path.join(ROOT, "tool", "x.md"), ""))
        self.assertEqual(target("x.md#y", os.path.join(ROOT, "a", "b.md"), ROOT), (os.path.join(ROOT, "a", "x.md"), "y"))
        self.assertEqual(target("b%20c.md", os.path.join(ROOT, "a", "b.md"), ROOT), (os.path.join(ROOT, "a", "b c.md"), ""))
        # (decoded once the anchor and the query are split off: a %23 or a %3F is the name's)
        self.assertEqual(target("a%23b%3Fc.md#d", os.path.join(ROOT, "a", "b.md"), ROOT),
                         (os.path.join(ROOT, "a", "a#b?c.md"), "d"))
        text = "## Caf\u00e9\n\n[a](#caf\u00e9)\n"  # (its HTML has the link percent-encoded: #caf%C3%A9)
        self.assertIn(target(links(text)[0], os.path.join(ROOT, "README.md"), ROOT)[1], anchors(text))
        self.assertEqual(target("../../issues", os.path.join(ROOT, "README.md"), ROOT)[0], None)
        self.assertEqual(target("../LICENSE", os.path.join(ROOT, "README.md"), ROOT)[0], OUTSIDE)
        self.assertEqual(target("..", os.path.join(ROOT, "README.md"), ROOT)[0], OUTSIDE)
        self.assertEqual(target("archive/state.json", os.path.join(ROOT, "README.md"), ROOT, "/a")[0],
                         os.path.join("/a", "state.json"))  # (into the archive given, whether or not the checkout has one)
        self.check("#Top", os.path.join(ROOT, "README.md"), ROOT)
        self.check("tool/oracle/#run-it", os.path.join(ROOT, "README.md"), ROOT)  # (a folder's README)
        with self.assertRaises(AssertionError):
            self.check("tool/oracle/#nowhere", os.path.join(ROOT, "README.md"), ROOT)

    def test_a_long_page_reads_in_linear_time(self):
        # (16000 headings of one text: looking for each one's -n from -1 again takes over 20 seconds)
        start = time.monotonic()
        Page("## A\n\n" * 16000)
        self.assertLess(time.monotonic() - start, 5)


class Hiding(unittest.TestCase):
    """No doc holds what could hide its text on GitHub's page: an HTML comment, an arrow, a "<?", "<!" or "</" (see Page's
    unsafe), or a code fence left open (see Page's unclosed)."""

    def test_no_comment_or_arrow_of_the_docs_can_hide_text(self):
        self.assertEqual([(doc, Page(read(doc)).unsafe) for doc in DOCS if Page(read(doc)).unsafe is not None], [],
                         '(doc, line): GitHub may hide text from here. Write each HTML comment whole on one line, with no "<!--" '
                         'in it; in raw HTML, write "-->" as "--&gt;", and "<?", "<!" or "</" not before a letter with "&lt;"')

    # (shapes the rule flags: comments that hide text, and comments of several lines and arrows in raw HTML that
    # GitHub shows whole, which the docs do without; and "<?", "<!" and "</" not before a letter, read as comments)
    UNSAFE = (
        "# Notes\n\n<!-- keep these notes\nshort ->\n\n## Section A\n\nMID-MARK: fetch --> verify.",
        "# Notes\n\n<!--\nkeep these notes short ->\n\nMID-MARK\n\nfetch --> verify.",
        "# Notes\n\n<!-- keep these notes short\n\n## Section A\n\nMID-MARK: fetch --> verify.",
        "# Notes\n\n<!-- keep these notes short\n\nMID-MARK\n\n```mermaid\ngraph LR\n  A --> B\n```",
        "# Notes\n\n<!-- keep these notes short\n\nMID-MARK: the flow is fetch -->\nverify.",
        "> - a\n>   <!-- one\n>   two ->\n>   MID-MARK more\n>   then fetch --> verify\n> - b",
        "# Notes\n\nA note <!-- keep it short\nMID-MARK: fetch --> verify.",
        "# Notes\n\n<!-- keep it short\nMID-MARK: fetch --> verify.",
        "- a <!-- keep it short\n  MID-MARK: fetch --> verify\n- b",
        "# Notes\n\n<!-- keep it short\n\n<img alt=\"fetch --> verify\" src=\"x.svg\">\n\nMID-MARK",
        "# Notes\n\n<!-- keep these\nnotes short ->\nMID-MARK: the flow is fetch -->\nverify.",
        "# Notes\n\n<!-- The old flow: fetch --> verify. -->\n\nMID-MARK",
        "# Notes\n\n<!--\nThe old flow: fetch --> verify.\n-->\n\nMID-MARK",
        "# Notes\n\nA note <!-- keep it short\nMID-MARK: fetch --> [verify](x.md)",
        "# Notes\n\n<!-- keep it short\nMID-MARK: fetch --><img src=\"x.svg\">",
        "# Notes\n\n<!-- keep it short\nMID-MARK: fetch --> <!-- c --> verify <!-- e -->",
        "<!-- a\nMID-MARK: b --> <!--> verify <!-- c -->", "<!-- a\nMID-MARK: b --> <!---> verify <!-- c -->",
        "<!-- a\nMID-MARK: b --> <!-- c --!> verify <!-- e -->",
        "A note <!-- x -->\nand <!-- keep it short\nMID-MARK: fetch --> verify.",
        "# Notes\n\n<!-- An old section.\n\nIts old text. -->\n\nMID-MARK",
        "# Notes\n\nA note <!-- a TODO that\nwraps --> and MID-MARK.", "<!-- a\nb --> MID-MARK",
        "<img alt=\"fetch --> verify\" src=\"x.svg\">\n\nMID-MARK", "<pre>\nfetch --> verify\n</pre>\n\nMID-MARK",
        "<script>\nif (a-->0) {}\n</script>\n\nMID-MARK", "<table><tr><td>fetch --> verify</td></tr></table>\n\nMID-MARK",
        "<div>\n<!--\na ->\n--></div>\n", "> - a\n>   <!-- b\n\n<div>--></div>\n", "<!-- a\n\nb -->\n", "<!-- a\n \t\nb -->\n",
        "<!--\na\n--> b\n", "<!--\na\n--> -->\n", "<!-- a --> b --> c\n", "<div>\na --> b\n</div>\n", "<!-- a\n\nb --!> c\n",
        "a <!-- b\nc --> d\n", "`a --> b`\n\n<!-- c\na --> b\n", "> - a\n>   <!-- b\n\n<div>--> c</div>\n",
        "# A\n\n- item\n  <!-- two\nlines -->\n\n## B\n", "> a\n>\n> <!-- b\n\n# C\n", "# A\n<!-- b\n",
        "> a\n> <!-- b\nc -->\n\n<!-- d -->\n", "- a\n  <!-- b\nc -->\n\n## D\n\n<!-- e -->\n",
        "`<!-- b` in code\n\n# A\n<!-- b\n", "- a\n  <!-- b\nc -->\n\n<!-- d -->\n\n- e\n  <!-- f\n",
        "# A\n\n<!-- b ->\n\nc\n\n<!-- d -->\n", "- a\n  - b\n    <!-- c\n  d <!-- e -->\n",
        "- a\n  <!-- b\n<div>\nc\n-->\n</div>\n\nd\n", "> a\n> <!-- b\n<div>\nc\n-->\n</div>\n\nd\n",
        "> a\n> <!-- b\n<blockquote>\nc -->\n</blockquote>\n", "<div>\n<!-- a\n\nb\n\n<div>--></div>\n",
        "# A\n\n- a <!-- x -->\n  <!-- b\nc -->\n\n<!-- d -->\n", "<!-- Flow: a ->\n b -->\n\ntext\n",
        "<DIV>\n<!-- a\n\n</div>\n<img alt=\"b\" src=\"c.svg\">\n-->\n",
        "<div>\n<!-- a\n\n</DIV>\n<img alt=\"b\" src=\"c.svg\">\n-->\n",
        "</blockquote>\n\n> a\n> <!-- b\n<div>\nc\n-->\n</div>\n",
        "<div>\n<!-- a -->\n<!-- b\n\n</div>\n<img alt=\"c\" src=\"d.svg\">\n-->\n",
        "<!-- Never write <!-- inside a comment. -->\n\nMID-MARK\n",
        "<? a\n\nMID-MARK\n\n## B\n", "<!X a\n\nMID-MARK\n", "<![CDATA[ a\n\nMID-MARK\n\n> q\n", "a <?php b ?> MID-MARK\n",
        "<!DOCTYPE html>\n\nMID-MARK\n", "<div>\n<? a\n</div>\n\nMID-MARK\n", "<picture>\n</ 1 MID-MARK\n</picture>\n",
    ) + tuple(f"# A\n\n<!-- b {end}\n\nc --> d\n"
              for end in ("\u2013>", "\u2014>", "--", "- >", "-- >", "-- !>", "-> c", "->", "-> \t")) + tuple(
        t for end in ("->", "- ->", "-!>", "- >", "-- >", "- !>", "-- !>", "\u2013>", "\u2014>", "--", "-> \t")
        for t in (f"<!-- a {end}\nb -->\n", f"<!-- a {end}\nb\nc -->\n", f"<!-- a\nb {end}\nc -->\n"))

    def test_control_unsafe(self):
        for text in self.UNSAFE:
            self.assertIsNotNone(Page(text).unsafe, text)

    def test_control_safe(self):
        # (whole comments on one line, comment syntax in code, and "-->", "<?" and "<!" in text)
        for text in ("<!-- a --> b <!-- c -->\n<!--> d <!---> e <!-- f --!>\n", "a `<!-- b` c\n\n```\n<!-- d\ne -->\n```\n",
                     "    <!-- f\n", "a --> b, `c --> d`\n\n```mermaid\nA --> B\n```\n", "a <? b <! c `<?d` `<!e`\n"):
            self.assertIsNone(Page(text).unsafe, text)

    def test_control_the_line(self):
        # (of the first one, in a block cmark-gfm made or in raw HTML)
        for text, line in (("# A\n\n- item\n  <!-- two\nlines -->\n\n## B\n", 4), ("> a\n>\n> <!-- b\n\n# C\n", 3),
                           ("`<!-- b` in code\n\n# A\n<!-- b\n", 4), ("x\n\n<!-- a -->\n<!-- b\nc -->\n", 4),
                           ("`a --> b`\n\n<div>\na --> b\n</div>\n", 4), ('<img alt="a --> b" src="c.svg">\n', 1),
                           ("a <!-- b --> c\n\n<div>--></div>\n\n<!-- d\n", 3), ("<!-- a <!-- b -->\n", 1),
                           ("# T\n\n<!-- note -->\n\n<!-- note\nmore -->\n", 5), ("x\n\n<div>\n<? a\n</div>\n", 4),
                           ("<div>\n<!-- a -->\n<!-- a\nb -->\n</div>\n", 3), ("<!-- a -->\n\nx\n\n<!-- a\nb -->\n", 5),
                           ('x\nsay "hi" <!X y>\n', 2)):
            self.assertEqual(Page(text).unsafe, line, text)

    def failure(self, test, texts):
        """What `test`, a test of the docs, fails with ("" if it passes) when each doc of `texts` ({doc: text}) reads as
        its text and each other doc as empty, so that the real docs, failing a test or not, change nothing there."""
        with mock.patch(f"{__name__}.read", lambda rel: texts.get(rel, "")):
            self.assertEqual([read(doc) for doc in DOCS if doc not in texts], [""] * len(set(DOCS) - set(texts)))
            try:
                test()
            except AssertionError as e:
                return str(e)
        return ""

    def test_control_the_doc_tests_read_each_doc(self):
        # (each doc in turn reads as a text of this test's own (see failure), then all of them do: the test lists each,
        # with the line, and says what to change)
        unsafe, fence = self.test_no_comment_or_arrow_of_the_docs_can_hide_text, self.test_no_fence_of_the_docs_is_unclosed
        for test, text, entry, advice in ((unsafe, "# Notes\n\n<!-- a\nb -->\n", 3, "whole on one line"),
                                          (unsafe, "# Notes\n\n<div>a --> b</div>\n", 3, 'as "--&gt;"'),
                                          (fence, "# Notes\n\n```sh\nmake\n", [3], "Close it")):
            for doc in DOCS:
                with self.subTest(doc=doc, text=text):
                    failure = self.failure(test, {doc: text})
                    self.assertIn(f"[({doc!r}, {entry})]", failure)
                    self.assertIn(advice, failure)
            failure = self.failure(test, dict.fromkeys(DOCS, text))
            for doc in DOCS:
                self.assertIn(f"({doc!r}, {entry})", failure)

    def test_control_comments(self):
        # (as HTML's tokenizer reads them: "<!-->" and "<!--->" are whole, "<!--!>" and "<!---!>" go on to the next end)
        html = "<!--> a <!---> b <!--!> c --> d <!---!> e --!> f <!-- g"
        self.assertEqual((comments(html), uncommented(html)), ([(0, 5), (8, 14), (17, 29), (32, 46), (49, None)], " a  b  d  f "))

    def test_no_fence_of_the_docs_is_unclosed(self):
        self.assertEqual([(doc, Page(read(doc)).unclosed) for doc in DOCS if Page(read(doc)).unclosed], [],
                         "(doc, lines): a fenced code block opens here and never closes, so GitHub shows the rest as code. "
                         "Close it with a line of at least as many of its fence's characters")

    def test_control_unclosed(self):
        for text, lines in (("```sh\na\n```\n", []), ("```sh\na\n", [1]), ("- ```sh\n  a\n  ```\n- b\n", []),
                            ("- ```\n  a\n- b\n", [1]), ("    ```\n    a\n", []), ("x\n\n> ~~~\n> a\n", [3]),
                            ("````\na\n```\n", [1]), ("1. ```\n   a\n   ```\n", []), ("~~~\na\n````\n", [1]),
                            ("   ```\na\n   ```\n", []), ("```\na\n``` x\n", [1]), ("> ```\n> a\n> ```\n", [])):
            self.assertEqual(Page(text).unclosed, lines, text)


class OracleReadme(unittest.TestCase):
    """tool/oracle/README.md names each file of the oracle (by its path there, its folder, or a name no other
    of its files has), and each local file it names is there."""

    UPSTREAM = {"cluster/prod/updater.yaml", "tabulator.yaml", "config/testgrids", "pkg/tabulator", "pkg/updater",
                "util/gcs", "prowjob.json"}  # (TestGrid's, test-infra's and Prow's)

    def files(self):
        oracle = os.path.join(HERE, "oracle")
        return sorted(os.path.relpath(os.path.join(d, n), oracle) for d, dirs, names in os.walk(oracle)
                      if not dirs.__setitem__(slice(None), [x for x in dirs if x != "__pycache__"])
                      for n in names if not n.endswith(".pyc"))

    def unnamed(self, files, text):
        """Of files (paths in tool/oracle/), those a code span of text (as GitHub shows it) names in none of those
        ways."""
        code, names = set(Page(text).code), [os.path.basename(f) for f in files]
        return [f for f in files if f != "README.md" and not any(
            n in code for n in (f, os.path.dirname(f), os.path.basename(f) if names.count(os.path.basename(f)) == 1
                                else "") if n not in ("", "fuzz"))]

    def test_it_names_each_file(self):
        unnamed = self.unnamed(self.files(), read("tool/oracle/README.md"))
        self.assertEqual(unnamed, [],  # fails: ['build.sh', 'fuzz/filtergolden.py', 'fuzz/fmtdiff.py']
                         "these files of the oracle are in no code span of tool/oracle/README.md: name each there")

    def test_control_unnamed(self):
        self.assertEqual(self.unnamed(["a.py", "fuzz/b.py", "c.sh", "README.md"], "x <!--> `a.py` <!-- `c.sh` --> `b.py`"),
                         ["c.sh"])

    def local_names(self, text):
        """The names in code spans of text (as GitHub shows it) that look like files or folders (with an extension, a
        slash, or a leading dot), but TestGrid's, test-infra's and Prow's."""
        return {t for t in Page(text).code
                if re.search(r"\.(py|sh|go|json|gz|ya?ml|md|proto|txt|mod|sum|bzl|toml|lock)$|/|^\.[A-Za-z]", t) and " " not in t
                and t not in self.UPSTREAM}

    def test_control_local_names(self):
        self.assertEqual(self.local_names("`.gitignore` `a.py` `b c.py` `x/y` `pkg/updater` `gocheck` `.000`"),
                         {".gitignore", "a.py", "x/y"})
        self.assertEqual(self.local_names("`a.py`\n\n```\nb.py\n```\n\n    c.py\n"), {"a.py"})  # (code blocks name none)

    def test_each_local_file_it_names_is_there(self):
        named = self.local_names(read("tool/oracle/README.md"))
        places = [os.path.join(HERE, "oracle"), os.path.join(HERE, "oracle", "fuzz"), HERE, ROOT]
        self.assertEqual(self.missing(named, places), [], "tool/oracle/README.md names these, but they are not there: "
                         "fix the name or add the file")
        # (one found only at the root: not in archive/, and one the tests are known to run on a change to)
        self.assertEqual(self.root_names_unread(self.at_root(named, places[:3])), [],
                         "tool/oracle/README.md names these at the root: name none in archive/, add the others to READ")

    def at_root(self, named, places):
        """Of named, those at the root but at none of places."""
        return sorted(n for n in named if os.path.exists(os.path.join(ROOT, n))
                      and not any(os.path.exists(os.path.join(p, n)) for p in places))

    def test_control_at_root(self):
        places = [os.path.join(HERE, "oracle"), HERE]
        self.assertEqual(self.at_root({".jsonl", "README.md", ".gitignore", "AGENTS.md", "nope.py"}, places),
                         [".gitignore", "AGENTS.md"])

    def test_control_a_comment_names_no_local_file(self):
        real = read
        with mock.patch(f"{__name__}.read",
                        lambda rel: real(rel) + ("\n<!-- `nope.py` -->\n" if rel == "tool/oracle/README.md" else "")):
            self.test_each_local_file_it_names_is_there()

    def missing(self, named, places):
        """Of named, those at none of places, but a leading-dot name without a slash that is nowhere: that is a
        file's extension (`.jsonl`), not a file."""
        return sorted(n for n in named if not any(os.path.exists(os.path.join(p, n)) for p in places)
                      and not (n.startswith(".") and "/" not in n))

    def test_control_missing(self):
        self.assertEqual(self.missing({".jsonl", "nope.py", "README.md", ".github/nope.yml"}, [ROOT]),
                         [".github/nope.yml", "nope.py"])

    def root_names_unread(self, names):
        """Of names found only at the root, those in archive/ (its files are the runs') or that unread lists."""
        return sorted(n for n in names if n.split("/")[0] == "archive" or unread({os.path.join(ROOT, n)}))

    def test_control_root_names_unread(self):
        self.assertEqual(self.root_names_unread(["archive/state.json", ".gitignore", "nope.txt", "THIRD_PARTY_NOTICES.md",
                                                 "third_party"]), ["archive/state.json", "nope.txt"])


class NoticesVersions(unittest.TestCase):
    """The versions of the ported code agree. Each ported Python file THIRD_PARTY_NOTICES.md lists names in its header
    (its first comment lines) the version of each of Go, TestGrid and apimachinery whose section of the notices lists
    it, and no other, the same in every file; the notices name the same ones; go_unicode.json.gz was dumped by that Go,
    of the Unicode version the notices name and the headers of the Unicode section's files name too. HEADERS, NOTICES
    and UNICODE are how a version is written: a TestGrid commit in the notices is one in backticks whose block names
    TestGrid last of OWNERS' projects before it. Other versions in the notices, such as GCS's client's, are not checked.
    The ported files are listed below too, so a new one fails until it is added."""

    V = r"(\d+(?:\.\d+)+)"
    HEADERS = {"Go": rf"\b[Gg]o\s*{V}|(?<![\w.]){V};\s+Copyright\s+The\s+Go\s+Authors",
               "TestGrid": r"(?i)\btestgrid\b(?:(?!\b(?:test-infra|prow)\b)[^;()])*?[\s(]([0-9a-f]{7,40});\s+Copyright",
               "apimachinery": rf"k8s\.io/apimachinery\s+\((v{V[1:]};"}
    UNICODE = rf"\bUnicode\s+{V}|(?<![\w.]){V};\s+Copyright\s+Unicode"
    SECTIONS = {"Go": "Go", "TestGrid": "TestGrid", "Kubernetes": "apimachinery"}  # (the notices' sections)
    NOTICES = {"Go": HEADERS["Go"], "apimachinery": rf"k8s\.io/apimachinery\s+(?:and\s+k8s\.io/api\s+)?(v{V[1:]}"}
    # (a commit in the notices is that of the project of OWNERS named last before it in its block)
    OWNERS = re.compile(r"(?i)\b(testgrid|test-infra|prow)\b")

    def header(self, name):
        with open(os.path.join(HERE, name), encoding="utf-8") as f:
            lines = f.read().split("\n")
        return " ".join(x.lstrip("#").strip() for x in lines[:next(i for i, x in enumerate(lines) if not x.startswith("#"))])

    def test_control_a_headers_go_in_any_words(self):
        for header in ("(Go 1.26.5)", "Go 1.26.5's fmt", "go1.26.5", "(go 1.26.5)", "(1.26.5; Copyright The Go Authors)",
                       "(and 1.26.5; Copyright The Go Authors)"):
            self.assertEqual({next(filter(None, m.groups())) for m in re.finditer(self.HEADERS["Go"], header)},
                             {"1.26.5"}, header)
        # (a Go module's own version is not Go's)
        self.assertFalse(re.search(self.HEADERS["Go"], "golang.org/x/text (v0.14.0; Copyright The Go Authors)"))

    def test_each_version(self):
        page, ports = Page(read("THIRD_PARTY_NOTICES.md")), {}
        notices, sections = page.text("text", "code"), self.sections(page)
        for title, blocks in sections.items():
            for tag, runs in blocks:
                first = next(((k, t) for k, t in runs if t.strip()), ("", ""))
                name = re.fullmatch(r"tool/(\w+\.py)", first[1]) if tag == "li" and first[0] == "code" else None
                if name:  # (a list item that opens with the file's path)
                    ports.setdefault(name[1], []).extend([self.SECTIONS[title]] if title in self.SECTIONS else [])
        self.assertEqual(sorted(ports), ["fetch_red_logs.py", "gofmt.py", "goregexp.py", "testgrid_config.py",
                                         "testgrid_rule.py"])
        said = {}
        for name, families in ports.items():
            header = self.header(name)
            for family in self.HEADERS:
                with self.subTest(name=name, family=family):
                    found = {next(filter(None, m.groups())) for m in re.finditer(self.HEADERS[family], header)}
                    self.assertEqual(len(found), int(family in families), found)
                    said.setdefault(family, set()).update(found)
        for family, versions in said.items():
            with self.subTest(family):
                self.assertEqual(len(versions), 1, versions)
                found = (set(self.commits_after_testgrid(read("THIRD_PARTY_NOTICES.md"))) if family == "TestGrid" else
                         {next(filter(None, m.groups())) for m in re.finditer(self.NOTICES[family], notices)})
                self.assertEqual(found, versions)
        with gzip.open(os.path.join(HERE, "go_unicode.json.gz"), "rt", encoding="utf-8") as f:
            tables = json.load(f)
        self.assertEqual({"go" + v for v in said["Go"]}, {tables["go"]})
        self.assertEqual(set(re.findall(rf"\bUnicode\s+{self.V}", notices)), {tables["unicode"]})
        # (the files the notices' Unicode section names list its characters: their headers name its version)
        listing = {m[1] for _, runs in sections["Unicode"] for k, t in runs if k == "code"
                   for m in [re.fullmatch(r"tool/(\w+\.py)", t)] if m}
        self.assertEqual(listing, {"fetch_red_logs.py", "testgrid_rule.py"})
        self.assertEqual(self.unicode_problems(listing, ports, self.header, tables["unicode"]), [])

    def sections(self, page):
        """{title: its blocks} of each ## section of a Page."""
        out, title = {}, None
        for tag, runs in page.blocks:
            if tag == "h2":
                title = "".join(t for _, t in runs).strip()
                out[title] = []
            elif title is not None:
                out[title].append((tag, runs))
        return out

    def commits_after_testgrid(self, text):
        """The commits in code spans of Markdown text whose paragraph, list item or heading names TestGrid before them,
        with no other project of OWNERS named in between (in text or in code), in order."""
        out = []
        for _, runs in Page(text).blocks:
            owner = None
            for kind, t in runs:
                for m in self.OWNERS.finditer(t):
                    owner = m[1].lower()
                if kind == "code" and re.fullmatch(r"[0-9a-f]{7,40}", t) and owner == "testgrid":
                    out.append(t)
        return out

    def test_control_a_version_in_code(self):
        real = read
        self.assertIn("k8s.io/apimachinery and k8s.io/api v", real("THIRD_PARTY_NOTICES.md"))
        with mock.patch(f"{__name__}.read", lambda rel: real(rel).replace(
                "k8s.io/apimachinery and k8s.io/api v", "`k8s.io/apimachinery` and `k8s.io/api` v")
                if rel == "THIRD_PARTY_NOTICES.md" else real(rel)):
            self.test_each_version()

    def test_control_a_comment_names_no_version(self):
        real = read
        with mock.patch(f"{__name__}.read", lambda rel: real(rel) + (
                "\n<!-- TestGrid at `abc1234`, Go 1.2.3 -->\n" if rel == "THIRD_PARTY_NOTICES.md" else "")):
            self.test_each_version()

    def test_control_a_loose_list(self):
        # (a blank line between two items makes the list loose: each item's text is in a paragraph of its own)
        real, item = read, "\n- `tool/gofmt.py`"
        self.assertIn(item, real("THIRD_PARTY_NOTICES.md"))
        with mock.patch(f"{__name__}.read", lambda rel: real(rel).replace(item, "\n" + item)
                        if rel == "THIRD_PARTY_NOTICES.md" else real(rel)):
            self.test_each_version()

    def test_control_items(self):
        # (a list item's block holds its text, in a loose list as in a tight one, and the end tag of another block
        # does not end it; a paragraph in another block is a block of its own)
        def blocks(text):
            return [(tag, [(k, t) for k, t in runs if t.strip()]) for tag, runs in Page(text).blocks]
        for text in ("- `a` b</p>c\n- d\n", "- `a` b</p>c\n\n- d\n"):
            self.assertEqual(blocks(text), [("li", [("code", "a"), ("text", " b"), ("text", "c")]), ("li", [("text", "d")])],
                             text)
        self.assertEqual(blocks("<dl><dd>\n<p>a</p>\n</dd></dl>\n"), [("dd", []), ("p", [("text", "a")])])
        # (a folded paragraph's end tag ends it, so the item's ends the item: what follows is in no block)
        self.assertEqual(blocks("- a\n\n- b\n\n<div>c 7</div>\n"), [("li", [("text", "a")]), ("li", [("text", "b")]),
                                                                    (None, [("text", "c 7")])])
        # (a paragraph after another block of its item, such as a nested list, a heading or a table, is one of its own:
        # the text stays in order)
        for text, after in (("- - a\n\n  b\n", [("li", [("text", "a")])]), ("- ## a\n\n  b\n", [("h2", [("text", "a")])]),
                            ("- | a |\n  |---|\n  | c |\n\n  b\n", [("th", [("text", "a")]), ("td", [("text", "c")])])):
            self.assertEqual(blocks(text), [("li", [])] + after + [("p", [("text", "b")])], text)

    def unicode_problems(self, listing, ports, header, version):
        """[(file, the Unicode versions its header names)] for each file of ports or listing whose header names a
        Unicode version though the notices' Unicode section does not list it, or, listed, does not name `version`
        alone."""
        out = []
        for name in sorted(set(ports) | set(listing)):
            found = {next(filter(None, m.groups())) for m in re.finditer(self.UNICODE, header(name))}
            if found != ({version} if name in listing else set()):
                out.append((name, found))
        return out

    def test_control_unicode_problems(self):
        headers = {"a.py": "x (15.0.0; Copyright Unicode, Inc.)", "b.py": "y", "c.py": "z Unicode 14.0.0", "d.py": "w",
                   "e.py": "tables of 16.0.0; Copyright Unicode, Inc.", "f.py": "an emoji set (v44.1; Copyright Unicode, Inc.)",
                   "g.py": "Unicode 15.0.0", "h.py": "(15.0.0; Copyright Unicode) and Unicode 16.0.0",
                   "i.py": "a module's data (v1.44.1; Copyright Unicode, Inc.)"}
        self.assertEqual(self.unicode_problems({"a.py", "d.py", "h.py"}, set(headers) - {"d.py", "h.py"}, headers.get, "15.0.0"),
                         [("c.py", {"14.0.0"}), ("d.py", set()), ("e.py", {"16.0.0"}), ("g.py", {"15.0.0"}),
                          ("h.py", {"15.0.0", "16.0.0"})])

    def test_control_a_headers_testgrid_commit(self):
        for header in ("TestGrid (f15bfa4808a4; Copyright The TestGrid Authors)", "TestGrid at f15bfa4808a4; Copyright The",
                       "TestGrid's pkg/updater (f15bfa4808a4; Copyright The TestGrid Authors) and Prow (a1b2c3d4e5f6; Copyright",
                       "github.com/GoogleCloudPlatform/testgrid (f15bfa4808a4; Copyright The TestGrid Authors)",
                       "TestGrid (f15bfa4808a4; Copyright The TestGrid Authors) and k8s.io/utils (a1b2c3d4e5f6; Copyright X)"):
            self.assertEqual(re.findall(self.HEADERS["TestGrid"], header), ["f15bfa4808a4"], header)
        # (another project's commit, or a pseudo-version's, is not TestGrid's)
        for header in ("Prow (a1b2c3d4e5f6; Copyright The Kubernetes Authors)",
                       "k8s.io/utils (v0.0.0-20230726121419-3b25d923346b; Copyright The Kubernetes Authors)",
                       "TestGrid's code; its port at a1b2c3d4e5f6; Copyright X",
                       "TestGrid and Prow (a1b2c3d4e5f6; Copyright The Kubernetes Authors)",
                       "tool/testgrid_rule.py at a1b2c3d4e5f6; Copyright X", "notestgrid (f15bfa4808a4; Copyright X)",
                       "TestGrid's tests (with k8s.io/utils at a1b2c3d4e5f6; Copyright X)",
                       "TestGrid's configurator, in test-infra (a1b2c3d4e5f6; Copyright The Kubernetes Authors)",
                       "TestGrid (v0.0.177-0.20260902201319-f15bfa4808a4; Copyright The TestGrid Authors)",
                       "TestGrid at v0.0.177-0.20260902201319-f15bfa4808a4; Copyright The TestGrid Authors",
                       "TestGrid (f15bfa4808a4; see below)", "TestGrid (f15bfa; Copyright X)",
                       f"TestGrid ({'a' * 41}; Copyright X)"):
            self.assertEqual(re.findall(self.HEADERS["TestGrid"], header), [], header)
        notices = "\n\n".join([
            "TestGrid `f15bfa4808a4`, and github.com/GoogleCloudPlatform/testgrid at `f15bfa4808a4`; test-infra at "
            "`a1b2c3d4e5f6`",
            "TestGrid at `f15bfa4808a4` (once `abc1234`); its `pkg/updater` at\n`f15bfa4808a4`",  # (a paragraph of two lines)
            "`1234567`, of no project named in its paragraph; `tool/testgrid_rule.py` at `7654321`",
            "`GoogleCloudPlatform/testgrid` at `f15bfa4808a4`; TestGrid's configurator, in test-infra at `a1b2c3d4e5f6`",
            "TestGrid at `f15bfa4808a4`; Prow at `a1b2c3d4e5f6`",
            "- TestGrid's item at `f15bfa4808a4`\n- another item, at `2345678`\n## TestGrid\n`3456789`",
            "1. TestGrid at `f15bfa4808a4`\n2. another, at `2345678`", "the notices name\nTestGrid at `f15bfa4808a4`",
            "TestGrid in an issue\n#123 at `f15bfa4808a4`", "TestGrid at `f15bfa4808a4`\n   \n`2345678`",
            "TestGrid at `f15bfa4808a4`\n  - a nested item at `2345678`\n* another item at `3456789`",
            " ## TestGrid\n`4567890`", "TestGrid\n    # not a heading, at `f15bfa4808a4`",
            "####### TestGrid at\n`f15bfa4808a4`", "#123 moved TestGrid to\n`f15bfa4808a4`",
            "TestGrid at `f15bfa4808a4`\n#\n`2345678`", "TestGrid at `f15bfa4808a4`\n1) another at `2345678`",
            "TestGrid at `f15bfa4808a4`\n+ a third at `3456789`",
            "- TestGrid at `f15bfa4808a4`\n\n  its second paragraph, at `2345678`"])  # (a loose item's second paragraph)
        self.assertEqual(self.commits_after_testgrid(notices), ["f15bfa4808a4"] * 3 + ["abc1234"] + ["f15bfa4808a4"] * 16)
        self.assertEqual(self.commits_after_testgrid("TestGrid at `f15bfa4808a4` <!-- was `abc1234` -->"),
                         ["f15bfa4808a4"])  # (a commit in a comment is not on the page)


def tool_files():
    """The files under tool/, by their paths from the root (but the virtualenv and caches): the tests read
    their code and data."""
    out = []
    for d, dirs, names in os.walk(HERE):
        dirs[:] = [x for x in dirs if x not in (".venv", "__pycache__", ".ruff_cache", ".pytest_cache")]
        out += [os.path.relpath(os.path.join(d, n), ROOT).replace(os.sep, "/") for n in names if not n.endswith(".pyc")]
    return sorted(out)


def events(on):
    """A workflow's on: as {event: its filters, or None}, whether it is a name, a list or a map."""
    return {e: None for e in ([on] if isinstance(on, str) else on)} if isinstance(on, (str, list)) else on


def github_paths(patterns, path):
    """Whether a workflow's paths filter takes a change to `path`, as GitHub's filter patterns match: * does
    not match /, ** does (**/ also nothing), ? and + are of the character before them, [] is a class, and the
    last pattern that matches decides (a ! one excludes)."""
    def regex(pattern):
        out, i = "", 0
        while i < len(pattern):
            if pattern.startswith("**/", i):
                out, i = out + "(?:.*/)?", i + 3
            elif pattern.startswith("**", i):
                out, i = out + ".*", i + 2
            elif pattern[i] == "[" and (j := pattern.find("]", i + 1)) > i:
                out, i = out + pattern[i:j + 1], j + 1
            else:
                out, i = out + ({"*": "[^/]*", "?": "?", "+": "+"}.get(pattern[i]) or re.escape(pattern[i])), i + 1
        return re.compile(out + r"\Z")
    taken = False
    for p in patterns:
        if regex(p.lstrip("!")).match(path):
            taken = not p.startswith("!")
    return taken


class TestWorkflowPaths(unittest.TestCase):
    """The test workflow runs on a change to each file of tool/, and to each file outside it that these tests
    read (READ). Each file outside tool/ and archive/ that the docs link to is one of READ, and each such
    folder holds one (Links checks the same of the pages a run writes)."""

    READ = READ

    def test_each_file_read_here_triggers_it(self):
        workflow = yaml.safe_load(read(".github/workflows/test.yml"))
        on = events(workflow.get(True, workflow.get("on")))  # (YAML 1.1 reads a bare on as true)
        for event in ("push", "pull_request"):
            self.assertIn(event, on, "the test workflow does not run on it")
            patterns, ignored = (on[event] or {}).get("paths"), (on[event] or {}).get("paths-ignore") or []
            files = self.READ + tool_files()
            self.assertLessEqual({"tool/fetch_red_logs.py", "tool/go_unicode.json.gz"}, set(files))
            for rel in files:
                with self.subTest(event=event, file=rel):
                    self.assertTrue((patterns is None or github_paths(patterns, rel)) and not github_paths(ignored, rel),
                                    f"{rel}: test.yml does not run on a change to it: add it to its paths, or take it "
                                    "out of paths-ignore")  # fails: AGENTS.md, the notices and the licenses

    def test_each_file_the_docs_link_to_is_read_here(self):
        # (so READ above holds each, and a change to one runs the tests)
        linked = {f for doc in DOCS for link in links(read(doc))
                  for f, _ in [target(link, os.path.join(ROOT, doc), ROOT)] if f not in (None, OUTSIDE)}
        self.assertEqual(unread(linked), [], "the docs link to these files (or folders) outside tool/ and archive/, which "
                         "READ does not hold: add each to READ (a folder: a file in it) and to test.yml's paths")

    def test_control_unread(self):
        with tempfile.TemporaryDirectory() as tmp:
            for folder in ("docs", "third_party", "tool"):
                os.makedirs(os.path.join(tmp, folder))
            linked = {tmp, os.path.join(tmp, "docs"), os.path.join(tmp, "third_party"), os.path.join(tmp, "tool"),
                      os.path.join(tmp, "README.md"), os.path.join(tmp, "tools.md"), os.path.join(tmp, "tool", "x.py"),
                      os.path.join(tmp, "archive", "runs")}
            self.assertEqual(unread(linked, tmp), ["docs", "tools.md"])

    def test_control_on_in_each_form(self):
        self.assertEqual((events("push"), events(["push", "pull_request"]), events({"push": {"paths": ["x"]}})),
                         ({"push": None}, {"push": None, "pull_request": None}, {"push": {"paths": ["x"]}}))

    def test_control_the_patterns_are_githubs(self):
        self.assertTrue(github_paths(["third_party/**"], "third_party/go/LICENSE"))
        self.assertFalse(github_paths(["third_party/*"], "third_party/go/LICENSE"))
        self.assertFalse(github_paths(["**", "!*.md"], "README.md"))
        self.assertTrue(github_paths(["**", "!*.md"], "tool/a.md"))  # (* stops at /)
        self.assertTrue(github_paths(["LICENSE?"], "LICENSE"))
        # (examples of GitHub's filter pattern cheat sheet)
        for pattern, path in (("docs/**/*.md", "docs/README.md"), ("**/docs/**", "docs/hello.md"),
                              ("**/README.md", "README.md"), ("**/*src/**", "my-src/code/js/app.js"),
                              ("**/*-post.md", "my-post.md"), ("**/migrate-*.sql", "migrate-10909.sql"),
                              ("*.jsx?", "page.js"), ("[CB]at", "Cat"), ("[1-2]00", "200")):
            self.assertTrue(github_paths([pattern], path), (pattern, path))
        self.assertFalse(github_paths(["[1-2]00"], "-00"))


KINDS = ("text", "code", "pre", "alt", "mark")  # (all a page shows, see Page)


class GitHubPage(HTMLParser):
    """A page as GitHub's renderer showed it (its HTML in GITHUB), read as Page reads cmark-gfm's: its links, anchors
    and text. GitHub's sanitizer drops comments and the tags it does not allow; GitHub links each image to itself (not
    counted: that link is the image's) and puts each heading's anchor in an element of its own, with "user-content-"
    before it, as before each id and name. It also links a bare URL in raw HTML, and shows an image of another site
    through its camo proxy, so a case with either compares no links (Links reads no link to another site)."""

    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.links, self.images, self.anchors, self.parts, self.in_anchor = [], [], set(), [], 0
        self.feed(html)
        self.close()
        for src in self.images:
            if src in self.links:
                self.links.remove(src)
        self.links += self.images
        self.text = "".join(self.parts)

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        heading_anchor = tag == "a" and "anchor" in (a.get("class") or "").split()
        if heading_anchor:
            self.in_anchor += 1
        elif tag == "a" and a.get("href") is not None:
            self.links.append(a["href"])
        self.anchors |= {a[k].removeprefix("user-content-") for k in ("id", "name") if a.get(k)}
        if tag == "img" and a.get("src") is not None:
            self.images.append(a["src"])
        if tag == "img" and a.get("alt"):
            self.parts.append(a["alt"])
        if tag in ("img", "source") and a.get("srcset"):
            self.images += [c.split()[0] for c in a["srcset"].split(",") if c.strip()]

    def handle_endtag(self, tag):
        if tag == "a" and self.in_anchor:
            self.in_anchor -= 1

    def handle_data(self, data):
        if not self.in_anchor:
            self.parts.append(data)


class GitHubsReading(unittest.TestCase):
    """Page reads each case of GITHUB as GitHub's renderer showed it when the case was recorded (its REST API's POST
    /markdown, mode markdown): the same marks hidden (END-MARK, its last line, and MID-MARK), links, anchors and text
    (a case where GitHub shows an emoji, or leaves a task list's boxes as written, compares no text); and Page's unsafe
    flags each case where GitHub hid a mark. The cases are the forms of Markdown the checks rely on, and the ones
    reviews found read otherwise than GitHub. They are the renders of the day GITHUB's "recorded" gives, which no test
    renews; that API leaves alerts, math and task lists as written where github.com renders them. To record them
    again (it needs GITHUB_TOKEN): cd tool && uv run python test_docs.py --record"""

    def test_each_case_is_read_as_github_did(self):
        with open(GITHUB, encoding="utf-8") as f:
            cases = json.load(f)["cases"]
        self.assertGreaterEqual(len(cases), 106)
        for case in cases:
            page, github = Page(case["markdown"]), GitHubPage(case["html"])
            marks = [m for m in ("MID-MARK", "END-MARK") if m in case["markdown"]]
            mine = {"hidden": [m for m in marks if m not in page.text(*KINDS)], "links": sorted(page.links),
                    "anchors": sorted(page.anchors), "text": " ".join(page.text(*KINDS).split())}
            theirs = {"hidden": [m for m in marks if m not in github.text], "links": sorted(github.links),
                      "anchors": sorted(github.anchors), "text": " ".join(github.text.split())}
            keys = case.get("compare", list(mine))
            with self.subTest(case=case["name"]):
                self.assertTrue(case["markdown"].endswith("\nEND-MARK\n"))
                self.assertEqual({k: mine[k] for k in keys}, {k: theirs[k] for k in keys})

    def test_each_hide_is_unsafe(self):
        with open(GITHUB, encoding="utf-8") as f:
            cases = json.load(f)["cases"]
        hides = [c for c in cases if any(m in c["markdown"] and m not in GitHubPage(c["html"]).text
                                         for m in ("MID-MARK", "END-MARK"))]
        self.assertGreaterEqual(len(hides), 22)
        self.assertEqual([c["name"] for c in hides if Page(c["markdown"]).unsafe is None], [])

    def test_control_github_pages(self):
        page = GitHubPage('<div class="markdown-heading"><h2 class="heading-element">A b</h2><a id="user-content-a-b" '
                          'class="anchor" aria-label="Permalink: A b" href="#a-b"><span></span></a></div>\n<p><a '
                          'target="_blank" rel="noopener noreferrer" href="x.svg"><img src="x.svg" alt="c 1"></a> <a '
                          'href="y.md">d</a> <a name="user-content-n"></a></p>')
        self.assertEqual((page.links, page.anchors, " ".join(page.text.split())), (["y.md", "x.svg"], {"a-b", "n"}, "A b c 1 d"))


class Recording(unittest.TestCase):
    """record() sends each case's markdown to GitHub's API, in mode markdown, and keeps the HTML it returns as that
    case's, sends GITHUB_TOKEN to the API alone, and writes GITHUB whole or not at all, with its mode, by a file on
    disk before it replaces GITHUB; this file run as a script runs the tests, and with --record, record()."""

    API = "https://api.github.com/markdown"
    # (a proxy at a port of this host that nothing listens on: it refuses each request that would leave the process)
    OFFLINE = {"http_proxy": "http://127.0.0.1:9", "https_proxy": "http://127.0.0.1:9", "no_proxy": ""}
    # (each name and markdown its own: a name sent for its case's markdown, or a render kept for another case, shows)
    CASES = [{"name": "a", "markdown": "x\n"}, {"name": "b", "markdown": "*y*\n"}]

    def recorded(self, answer, replace=os.replace):
        """What record() does with GITHUB holding CASES (mode 0o640) on 2001-09-09 (UTC): (GITHUB after, the names in
        its folder, its os.stat, each request as (URL, Authorization, Accept, X-GitHub-Api-Version, JSON sent), what
        record() raised, and in order each os.fsync as ("fsync", inode, size) and os.replace as ("replace", inode, in
        GITHUB's folder, onto GITHUB, JSON held)). `answer(url)` gives each response's (status, Location), its body
        "<p>" + text sent + "</p>"; `replace` stands for os.replace. No request leaves the process: https_open is
        replaced, and OFFLINE's proxy refuses one that would."""
        sent, done, real_fsync = [], [], os.fsync

        def https_open(handler, request):
            body = json.loads(request.data)
            sent.append((request.full_url, request.get_header("Authorization"), request.get_header("Accept"),
                         request.get_header("X-github-api-version"), body))  # (urllib capitalizes a header's name)
            status, location = answer(request.full_url)
            headers = email.message.Message()
            if location:
                headers["Location"] = location
            response = urllib.response.addinfourl(io.BytesIO(f"<p>{body['text']}</p>".encode()), headers,
                                                  request.full_url, status)
            response.msg = "a reason"
            return response

        def fsync(fd):
            done.append(("fsync", os.fstat(fd).st_ino, os.fstat(fd).st_size))
            real_fsync(fd)

        def moving(src, dst):
            with open(src, encoding="utf-8") as f:
                done.append(("replace", os.stat(src).st_ino, os.path.dirname(src) == tmp, dst == path, json.load(f)))
            return replace(src, dst)

        raised = None
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "github_markdown.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"cases": self.CASES}, f)
            os.chmod(path, 0o640)
            with mock.patch.object(urllib.request.HTTPSHandler, "https_open", https_open), \
                    mock.patch(f"{__name__}.GITHUB", path), mock.patch.dict(os.environ, GITHUB_TOKEN="t", **self.OFFLINE), \
                    mock.patch("os.fsync", fsync), mock.patch("os.replace", moving), \
                    mock.patch("time.gmtime", return_value=time.gmtime(1_000_000_000)):
                try:
                    record()
                except Exception as e:  # (what it raised is the result)
                    raised = e
            with open(path, encoding="utf-8") as f:
                return json.load(f), sorted(os.listdir(tmp)), os.stat(path), sent, raised, done

    def test_it_keeps_each_render(self):
        data, files, stat, sent, raised, done = self.recorded(lambda url: (200, None))
        self.assertIsNone(raised)
        # (each case's markdown sent, in mode markdown, and the HTML returned for it kept as that case's)
        self.assertEqual(sent, [(self.API, "Bearer t", "application/vnd.github+json", "2022-11-28",
                                 {"text": case["markdown"], "mode": "markdown"}) for case in self.CASES])
        self.assertEqual((data["cases"], data["recorded"]),
                         ([dict(case, html=f"<p>{case['markdown']}</p>") for case in self.CASES], "2001-09-09"))
        self.assertEqual((files, stat.st_mode & 0o777), (["github_markdown.json"], 0o640))
        # (the file that replaces GITHUB, of its folder: on disk, whole, before it does)
        self.assertEqual(done, [("fsync", stat.st_ino, stat.st_size), ("replace", stat.st_ino, True, True, data)])

    def test_it_follows_no_redirect(self):
        # (urllib would send each header but the content's on, GITHUB_TOKEN's too, even to another host)
        data, files, stat, sent, raised, done = self.recorded(
            lambda url: (302, "https://elsewhere.example/x") if url == self.API else (200, None))
        self.assertIsInstance(raised, urllib.error.HTTPError)
        raised.close()
        self.assertEqual(([s[:2] for s in sent], [c.get("html") for c in data["cases"]], done),
                         ([(self.API, "Bearer t")], [None, None], []))

    def test_it_writes_github_whole_or_not_at_all(self):
        data, files, stat, sent, raised, done = self.recorded(lambda url: (200, None), mock.Mock(side_effect=OSError("full")))
        self.assertIsInstance(raised, OSError)
        self.assertEqual(([c.get("html") for c in data["cases"]], files), ([None, None], ["github_markdown.json"]))

    def test_run_as_a_script(self):
        # (with no token, record() stops before its first request; OFFLINE's proxy refuses one it would make)
        env = dict({k: v for k, v in os.environ.items() if k != "GITHUB_TOKEN"}, **self.OFFLINE)
        script = [sys.executable, "-B", os.path.abspath(__file__)]
        tests = subprocess.run(script + ["GitHubsReading.test_control_github_pages"], env=env, capture_output=True,
                               text=True, timeout=300)
        self.assertEqual((tests.returncode, tests.stderr.split()[-1:]), (0, ["OK"]), tests.stderr)
        self.assertIn("\nRan 1 test in ", tests.stderr)
        recording = subprocess.run(script + ["--record"], env=env, capture_output=True, text=True, timeout=300)
        self.assertEqual((recording.returncode, recording.stderr), (1, "record: GITHUB_TOKEN is not set\n"))


class NoRedirects(urllib.request.HTTPRedirectHandler):
    """Follows no redirect: urllib would send each header but the content's on, GITHUB_TOKEN's too, to where it
    leads, another host or not."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, f"{msg}; not followed to {newurl}", headers, fp)


def record():
    """Render each case of GITHUB again with GitHub's REST API (it needs GITHUB_TOKEN, and sends it there alone: see
    NoRedirects), and keep the HTML it returns: GITHUB is written whole or not at all, by a file of its folder, with
    its mode, on disk before it replaces GITHUB, so that a power loss leaves the old file or the new one, whole."""
    token = os.environ.get("GITHUB_TOKEN") or sys.exit("record: GITHUB_TOKEN is not set")
    with open(GITHUB, encoding="utf-8") as f:
        data = json.load(f)
    opener = urllib.request.build_opener(NoRedirects)
    for case in data["cases"]:
        request = urllib.request.Request(
            "https://api.github.com/markdown", data=json.dumps({"text": case["markdown"], "mode": "markdown"}).encode(),
            headers={"Accept": "application/vnd.github+json", "Authorization": f"Bearer {token}",
                     "X-GitHub-Api-Version": "2022-11-28"})
        with opener.open(request, timeout=60) as r:
            case["html"] = r.read().decode("utf-8")
    data["recorded"] = time.strftime("%Y-%m-%d", time.gmtime())
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=os.path.dirname(GITHUB), delete_on_close=False) as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
        f.write("\n")
        shutil.copymode(GITHUB, f.name)
        f.flush()
        os.fsync(f.fileno())
        f.close()
        os.replace(f.name, GITHUB)  # (an exception before this removes the file; a kill or a power loss before this leaves it)


if __name__ == "__main__":
    if sys.argv[1:] == ["--record"]:
        record()
    else:
        unittest.main()
