"""Builds' lifecycles, generated: Hypothesis drives TestGrid and GCS through
sequences such as a red cell showing up, its log appearing before its
finished.json, Prow replacing the log, the build scrolling off TestGrid, the
archived copy getting damaged and GCS failing while it is repaired, running the
fetch after each step (storing logs gzipped or not, whole or in parts), and
checks what the archive holds against a simple model."""
import contextlib
import gzip
import io
import os
import shutil
import sys
import tempfile
import time
import unittest
import zlib

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, precondition, rule

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import FakeServer, gcs_object, table  # noqa: E402

HOUR = 3600
# a red build still uploading, a red one from more than a day ago, and one that passed
AGES = {"red": 3, "old-red": 30, "passed": 10}
NAMES = tuple(AGES)
TEXTS = (b"log v1\n", b"log v2, longer\n", b"x")
PART_BYTES = 8  # with parts on, every log but the shortest is stored in parts


class Lifecycle(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        frl.BACKOFF = 0
        self.srv = FakeServer()
        self.root = tempfile.mkdtemp()
        now = time.time()
        self.builds = {n: str((int((now - h * HOUR) * 1000) - frl.PROW_EPOCH_MS) << 22) for n, h in AGES.items()}
        self.shown, self.logs, self.outage, self.gzip, self.parts = set(), {}, False, False, False
        self.srv.summary("dash", {"t": "FAILING"})
        self.srv.gcs_listing("bucket", [])
        for b in self.builds.values():
            self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": frl.created(b)}))
        self.finish("passed")
        self.publish()

    def teardown(self):
        self.srv.close()
        shutil.rmtree(self.root, ignore_errors=True)

    # --- the world

    @staticmethod
    def red(name):
        return name != "passed"

    def publish(self):
        """TestGrid's table: the builds shown, newest first, the failing ones red."""
        shown = sorted(self.shown, key=lambda n: int(self.builds[n]), reverse=True)
        cells = [12 if self.red(n) else 1 for n in shown]
        t = table("bucket/logs/job", [self.builds[n] for n in shown],
                  {"job.Overall": (cells, ["boom" if c == 12 else "" for c in cells])})
        t["timestamps"] = [int(frl.created(self.builds[n]) * 1000) for n in shown]
        self.srv.table("dash", "t", t)

    def finish(self, name):
        self.srv.route(f"/bucket/logs/job/{self.builds[name]}/finished.json",
                       (200, {}, {"timestamp": int(time.time()), "passed": not self.red(name),
                                  "result": "FAILURE" if self.red(name) else "SUCCESS"}))

    def serve_log(self, name):
        path = f"/bucket/logs/job/{self.builds[name]}/build-log.txt"
        if name not in self.logs:
            self.srv.httpd.routes.pop(path, None)
        else:
            self.srv.route(path, (503, {}, b"") if self.outage else gcs_object(self.logs[name]))

    def target(self, name):
        rel = frl.archived_builds(self.root)[0].get(f"job/{self.builds[name]}")
        return os.path.join(self.root, rel) if rel else None

    def stored_files(self, name):
        target = self.target(name)
        return sorted(f for f in os.listdir(target) if f.startswith("build-log.txt")) if target else []

    def stored_log(self, target, meta):
        """The archived log's content, read here without the fetch's own helpers."""
        info = meta.get("log")
        if not info:
            return None
        data = b""
        for n in [p["file"] for p in info.get("parts") or []] or [info["file"]]:
            with open(os.path.join(target, n), "rb") as fh:
                data += fh.read()
        if info["file"].endswith(".gz"):
            try:
                data = gzip.decompress(data)
            except (OSError, EOFError, zlib.error):
                return b"<not gzip>"
        return data

    def fetch(self):
        clean = not self.outage
        shown_red = {n for n in self.shown if self.red(n)}
        args = ["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url, "--gcs", self.srv.url]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            frl.main(args + (["--gzip"] if self.gzip else [])
                     + (["--max-file-mb", str(PART_BYTES / (1 << 20))] if self.parts else []))
        archived = {n for n in NAMES if self.target(n)}
        # never a build that is not red, whatever TestGrid showed or GCS held
        assert "passed" not in archived, archived
        for name in archived:
            target = self.target(name)
            meta = frl.read_json(os.path.join(target, "meta.json"))
            assert frl.valid_meta(meta), meta
            if clean and name in self.logs:
                # GCS has a log and could be read: the copy is that log, byte for byte
                assert self.stored_log(target, meta) == self.logs[name], (name, meta)
            # nothing too large for a part, and nothing meta.json does not list
            info = meta.get("log") or {}
            listed = [p["file"] for p in info.get("parts") or []] or ([info["file"]] if info else [])
            assert self.stored_files(name) == sorted(listed), (name, self.stored_files(name), meta)
            if self.parts:
                assert all(os.path.getsize(os.path.join(target, n)) <= PART_BYTES for n in listed), meta
        if clean:
            # a red build TestGrid showed, whose log GCS has, is archived
            for name in shown_red:
                assert name not in self.logs or name in archived, (name, archived)
        state = frl.read_json(os.path.join(self.root, "state.json"))
        assert all(isinstance(state.get(k, {}), dict) for k in ("tabs", "unresolved", "given_up", "watch")), state

    # --- what happens (each followed by a fetch)

    @initialize(gzip=st.booleans(), parts=st.booleans())
    def storage(self, gzip, parts):
        self.gzip, self.parts = gzip, parts

    @rule(name=st.sampled_from(NAMES))
    def show(self, name):
        self.shown.add(name)
        self.publish()
        self.fetch()

    @rule(name=st.sampled_from(NAMES))
    def scroll_off(self, name):
        self.shown.discard(name)
        self.publish()
        self.fetch()

    @rule(name=st.sampled_from(NAMES), text=st.sampled_from(TEXTS))
    def upload_log(self, name, text):
        self.logs[name] = text + name.encode()
        self.serve_log(name)
        self.fetch()

    @rule(name=st.sampled_from(["red", "old-red"]))
    def finish_build(self, name):
        self.finish(name)
        self.fetch()

    @rule(on=st.booleans())
    def gcs_outage(self, on):
        self.outage = on
        for name in NAMES:
            self.serve_log(name)
        self.fetch()

    def archived_with_log(self):
        return [n for n in NAMES if self.stored_files(n)]

    @precondition(lambda self: self.archived_with_log())
    @rule(data=st.data())
    def damage_copy(self, data):
        """Flip a byte of an archived log, keeping its size."""
        name = data.draw(st.sampled_from(self.archived_with_log()))
        path = os.path.join(self.target(name), self.stored_files(name)[0])
        with open(path, "r+b") as fh:
            first = fh.read(1)
            fh.seek(0)
            fh.write(bytes([first[0] ^ 0xFF]))
        self.fetch()

    @precondition(lambda self: any(n in self.logs for n in self.archived_with_log()))
    @rule(data=st.data())
    def replace_archived_log(self, data):
        """Prow uploads the log of an archived build again, with other content."""
        name = data.draw(st.sampled_from([n for n in self.archived_with_log() if n in self.logs]))
        text = data.draw(st.sampled_from([t for t in TEXTS if t + name.encode() != self.logs[name]]))
        self.logs[name] = text + name.encode()
        self.serve_log(name)
        self.fetch()


Lifecycle.TestCase.settings = settings(max_examples=40, stateful_step_count=12, deadline=None,
                                       suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])


class LifecycleTest(Lifecycle.TestCase):
    pass


if __name__ == "__main__":
    unittest.main()
