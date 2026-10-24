"""The docs, and the pages a run writes, say what the code does.

The checks that what README.md, AGENTS.md, THIRD_PARTY_NOTICES.md, tool/oracle/README.md and the pages a
run writes restate of the code still holds go here: a change to the code that makes one wrong then fails
until the text changes too. So far, INDEX.md's row of red builds given up on is checked. Page reads a
Markdown page as GitHub shows it, for the checks of the docs to come.

Run: cd tool && uv run python -m unittest test_docs -v
"""
import email.message
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
import urllib.request
import urllib.response
from html.parser import HTMLParser
from unittest import mock

import cmarkgfm
from cmarkgfm.cmark import Options

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402

# (cmark-gfm's options: raw HTML and footnotes; cmark-gfm is GitHub's fork of CommonMark's parser)
OPTIONS = Options.CMARK_OPT_UNSAFE | Options.CMARK_OPT_FOOTNOTES
GITHUB = os.path.join(HERE, "testdata", "github_markdown.json")
BLOCKS = {"p", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "dt", "dd"}  # (the elements that hold text)
HEADINGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}
COMMENT_END = re.compile(r"--!?>")  # (what ends an HTML comment)


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
      image's alt text) or "mark" (a footnote's)."""

    def __init__(self, markdown):
        super().__init__(convert_charrefs=True)
        self.links, self.ids, self.blocks, self.headings = [], [], [], []
        self.stack, self.open, self.heading, self.loose = [], [], None, None
        html = cmarkgfm.github_flavored_markdown_to_html(markdown, options=OPTIONS)
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
        if tag in BLOCKS and self.open and self.open[-1][0] == tag:
            self.open.pop()
        if any(t == tag for t, _ in self.stack):
            while self.stack.pop()[0] != tag:
                pass

    def handle_data(self, data):
        if self.heading is not None:
            self.heading.append(data)
        self.run(self.kind(), data)

    def text(self, *kinds):
        """The page's text of these kinds (see blocks), each block's joined to the next by a space."""
        return " ".join("".join(t for k, t in runs if k in kinds) for _, runs in self.blocks)

    @property
    def code(self):
        """The text of each of its code spans, in order."""
        return [t for _, runs in self.blocks for k, t in runs if k == "code"]


class Templates(unittest.TestCase):
    """INDEX.md's row of red builds given up on prints GIVE_UP_DAYS, not a 14 of its own."""

    def test_the_given_up_row_says_give_up_days(self):
        with mock.patch.object(frl, "GIVE_UP_DAYS", 9):
            rows = frl.holdings([], [], None)
        self.assertIn("| Red builds given up on (after 9 days) | 0 |", rows)  # fails: (after 14 days)


KINDS = ("text", "code", "pre", "alt", "mark")  # (all a page shows, see Page)


class GitHubPage(HTMLParser):
    """A page as GitHub's renderer showed it (its HTML in GITHUB), read as Page reads cmark-gfm's: its links, anchors
    and text. GitHub's sanitizer drops comments and the tags it does not allow; GitHub links each image to itself (not
    counted: that link is the image's) and puts each heading's anchor in an element of its own, with "user-content-"
    before it, as before each id and name. It also links a bare URL in raw HTML, and shows an image of another site
    through its camo proxy, so a case with either compares no links."""

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
    /markdown, mode markdown): the same marks hidden (END-MARK, its last line, and MID-MARK), links, anchors and text (a
    case where GitHub shows an emoji, or leaves a task list's boxes as written, compares no text). The cases are the
    forms of Markdown the checks rely on, and the ones reviews found read otherwise than GitHub. They are the renders of
    the day GITHUB's "recorded" gives, which no test renews; that API leaves alerts, math and task lists as written
    where github.com renders them. To record them again (it needs GITHUB_TOKEN):
    cd tool && uv run python test_docs.py --record"""

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
