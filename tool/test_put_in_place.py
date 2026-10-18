"""A build's files go in place with the meta.json that lists them. A run takes each file from GCS into temp
files first (whole, or in parts), and puts it in place with its new meta.json by renames, under the write lock:
a run abandoned at its deadline, or interrupted, waits for them, and a disk error before them stops it before
anything changes. Before, each file went in place as soon as it was taken, and its meta.json after.

Run: cd tool && python -m unittest test_put_in_place -v
"""
import gzip
import hashlib
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import FakeServer  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj  # noqa: E402
from test_podinfo import POD  # noqa: E402


class CutOffAsAFileGoesInPlace(ArchiveWorld):
    """The run reaches its deadline just as a worker puts a file it took from GCS in place."""

    def fetch_cut_off_as(self, name, *extra):
        """Run the fetch, its deadline passing as a worker puts the first file of name (build-log.txt or
        podinfo.json, whole or a part) in place: the run then abandons its workers. On the code before, the
        file went in place on its own, and the run was abandoned before the meta.json that lists it was
        written (temp_path waits for that, not to race the main thread)."""
        real_replace, real_temp, put = frl.replace, frl.temp_path, []

        def replace(src, dst):
            done = real_replace(src, dst)
            if not put and threading.current_thread() is not threading.main_thread() \
                    and os.path.basename(dst).startswith(name):
                put.append(dst)
                time.sleep(max(0.0, frl.context().deadline - time.monotonic()) + 0.3)
            return done

        def temp_path(path):
            if put and os.path.basename(path) == "meta.json":
                frl.context().abandoned.wait(5)
            return real_temp(path)
        with mock.patch.object(frl, "replace", replace), mock.patch.object(frl, "temp_path", temp_path):
            code = self.fetch("--run-timeout-minutes", "0.03", *extra)  # (1.8 s)
        self.assertTrue(put, "no file was put in place")
        return code

    def test_a_repair(self):
        # (a damaged log GCS has a new copy of: taken again, it goes in place with the meta.json that says so)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"x")  # (no longer the size meta.json records)
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
        self.fetch_cut_off_as("build-log.txt", "--gzip")
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["errors"]), ({"repaired": [self.key]}, []))
        # fails: TimeoutError: run timeout reached before this build was done
        self.assertTrue(frl.archived_intact(self.bdir(), deep=True))
        self.assertEqual(gzip.decompress(self.read("build-log.txt.gz")), b"log, uploaded again\n")
        self.assertEqual(self.check()[0], 0)


    def test_a_refresh(self):
        # (a file GCS has a new copy of, or a podinfo.json it has now, taken: it goes in place with the meta.json
        # that says so, and the run says it did not compare the build to the end)
        self.finished(ago=3 * HOUR)  # (not final: each run compares it with GCS)
        limit = ("--max-file-mb", "0.00005")  # (52-byte parts)
        for name, extra, first, again in (("build-log.txt", (), POD, b"log, uploaded again\n"),
                                          ("podinfo.json", (), POD, POD.replace(b"OOMKilled", b"Error")),
                                          ("build-log.txt", limit, POD, b"log, longer than a part\n" * 4),
                                          ("podinfo.json", (), None, POD)):
            with self.subTest(name=name, extra=extra, added=first is None):
                shutil.rmtree(os.path.join(self.root, "runs"), ignore_errors=True)
                self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
                self.srv.route(self.base + "/podinfo.json", obj(first) if first else (404, {}, b""))
                self.assertEqual(self.fetch(*extra), 0)
                self.srv.route(f"{self.base}/{name}", obj(again, "2"))
                self.fetch_cut_off_as(name, *extra)
                run = self.runs()[-1]
                self.assertEqual((run["outcomes"], run["errors"], run["gcs_compared"]),
                                 ({"refreshed": [self.key]}, [], False))
                # fails: TimeoutError: run timeout reached before this build was done (a podinfo.json added: not-checked)
                self.assertEqual(self.meta()["refreshed"], [name])
                self.assertTrue(frl.archived_intact(self.bdir(), deep=True))
                # (the copies it no longer lists went with it, not for the end of the run to remove)
                self.assertEqual([w for w in run["warnings"] if "does not list it" in w], [])
                code, out = self.check()  # (its files are GCS's: the one problem is the run's warning)
                self.assertIn(": 0 differ, 0 not settled yet", out)
                self.assertEqual([line.strip() for line in out.splitlines() if "PROBLEM" in line], [
                    f"PROBLEM: the run warned: {run['serious_warnings'][0]}"])


class DiskErrorAsAFileGoesInPlace(ArchiveWorld):
    """A file a run takes again goes in place with the meta.json that lists it: a disk error before the renames
    changes nothing."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * HOUR)  # (not final: each run compares it with GCS)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)  # (without --gzip)
        self.folder = self.bdir()
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))

    def assert_as_it_was(self):
        self.assertEqual((self.read("build-log.txt"), sorted(os.listdir(self.folder))), (b"log\n", ["build-log.txt",
                                                                                                    "meta.json"]))
        self.assertTrue(frl.archived_intact(self.folder, deep=True))

    def test_one_whose_meta_json_cannot_be_written(self):
        # (a full disk: the copy taken does not go in place, and the next run takes it again)
        real = frl.temp_path

        def full(path):
            if path == os.path.join(self.folder, "meta.json"):
                raise OSError(28, "No space left on device")
            return real(path)
        with mock.patch.object(frl, "temp_path", full):
            self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed: LocalIOError", run["errors"][0]["error"])
        # fails: OSError (taken for GCS's: one not final 14 days after it was archived is then made final)
        self.assertEqual((self.read("build-log.txt"), run["gcs_compared"]), (b"log\n", False))  # fails: the new log
        self.assert_as_it_was()
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("build-log.txt"), b"log, uploaded again\n")

    def test_one_in_another_form_whose_meta_json_cannot_be_put_in_place(self):
        # (as a run killed outright between the two leaves it: the copy as it came stays with the meta.json that
        # lists it, and the end of the run removes the one gzipped)
        real = os.replace

        def failing(src, dst):
            if dst == os.path.join(self.folder, "meta.json"):
                raise OSError(5, "Input/output error")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", failing):
            self.assertEqual(self.fetch("--gzip"), 1)
        self.assertTrue(frl.archived_intact(self.folder, deep=True))
        # fails: its copy as it came removed before (and the end of the run removes the one gzipped: no log is left)
        self.assert_as_it_was()
        self.assertTrue(any(w.endswith("/build-log.txt.gz: removed, as its meta.json does not list it (a copy cut off "
                                       "part-way)") for w in self.runs()[-1]["warnings"]))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.folder)), ["build-log.txt.gz", "meta.json"])


    def test_control_one_in_the_same_form_whose_meta_json_cannot_be_put_in_place(self):
        # (as before: the disk failing once the log is renamed over its copy leaves it under the old meta.json,
        # and the next run repairs it from GCS)
        real = os.replace

        def failing(src, dst):
            if dst == os.path.join(self.folder, "meta.json"):
                raise OSError(5, "Input/output error")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", failing):
            self.assertEqual(self.fetch(), 1)
        self.assertFalse(frl.archived_intact(self.folder, deep=True))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.read("build-log.txt")),
                         ({"repaired": [self.key]}, b"log, uploaded again\n"))
        self.assertTrue(frl.archived_intact(self.folder, deep=True))

    def test_one_whose_file_fails_to_go_in_place_otherwise(self):
        # (not the disk's doing: taken for GCS's, so one not final 14 days after it was archived is made final,
        # and the meta.json the run writes for it then lists the copy in place, not the one taken)
        frl.write_json(os.path.join(self.folder, "meta.json"), dict(self.meta(), archived_at=time.time() - 15 * DAY))

        def failing(target, meta, copies=()):  # (as put_in_place fails: its temp files go)
            for c in copies:
                c.discard()
            raise RuntimeError("not the disk")
        with mock.patch.object(frl, "put_in_place", failing):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.meta()["final"]), ({"stopped-comparing": [self.key]}, True))
        self.assert_as_it_was()


class PutInPlace(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d)

    def copy(self, data, max_bytes=None):
        """A copy of build-log.txt holding data, staged in temp files of the folder."""
        tmp = frl.temp_path(os.path.join(self.d, "build-log.txt"))
        with open(tmp, "wb") as f:
            f.write(data)
        temps, parts = frl.split_parts(tmp, os.path.join(self.d, "build-log.txt"), max_bytes)
        info = {"file": "build-log.txt", "bytes": len(data), "file_bytes": len(data), "md5": hashlib.md5(data).hexdigest()}
        return frl.Staged("build-log.txt", dict(info, parts=parts) if parts else info, temps)

    def listing(self):
        return sorted(os.listdir(self.d))

    def test_its_files_and_meta_json_all_or_none_when_the_run_is_abandoned(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        for i in range(3):
            with open(os.path.join(self.d, f"build-log.txt.{i:03d}"), "wb") as f:
                f.write(b"O" * 10)
        copy = self.copy(b"N" * 25, 10)
        real, started = frl.replace, threading.Event()

        def slow_replace(src, dst):
            done = real(src, dst)
            if dst.endswith(".000"):
                started.set()
                time.sleep(0.3)  # abandon() is called now, from another thread
            return done
        result = {}

        def worker():
            frl.bind_context(ctx)
            try:
                frl.put_in_place(self.d, {"log": copy.info}, [copy])
            except Exception as e:  # noqa: BLE001
                result["error"] = e
        with mock.patch.object(frl, "replace", slow_replace):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            started.wait(5)
            ctx.abandon()
            t.join(5)
        self.assertEqual((result, self.listing()), ({}, ["build-log.txt.000", "build-log.txt.001", "build-log.txt.002",
                                                         "meta.json"]))
        self.assertEqual(frl.read_json(os.path.join(self.d, "meta.json")), {"log": copy.info})
        self.assertTrue(frl.intact(self.d, {"log": copy.info}, deep=True))

    def test_nothing_once_the_run_is_abandoned(self):
        ctx = frl.RunContext(frl.AdaptiveLimit(1, 1))
        copy = self.copy(b"N" * 25, 10)
        ctx.abandon()
        result = {}

        def worker():
            frl.bind_context(ctx)
            try:
                frl.put_in_place(self.d, {"log": copy.info}, [copy])
            except frl.Abandoned as e:
                result["error"] = e
        t = threading.Thread(target=worker)
        t.start()
        t.join(5)
        self.assertIn("error", result)
        self.assertEqual(self.listing(), [])  # (nor a temp file)

    def test_nothing_when_its_meta_json_cannot_be_written(self):
        frl.write_json(os.path.join(self.d, "meta.json"), {"log": None})
        copy = self.copy(b"N" * 25, 10)
        real = frl.temp_path

        def full(path):
            if path == os.path.join(self.d, "meta.json"):
                raise OSError(28, "No space left on device")
            return real(path)
        with mock.patch.object(frl, "temp_path", full), self.assertRaises(frl.LocalIOError):
            frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual(self.listing(), ["meta.json"])
        self.assertEqual(frl.read_json(os.path.join(self.d, "meta.json")), {"log": None})

    def test_the_copies_meta_json_no_longer_lists_go_after_it(self):
        old = ["build-log.txt", "build-log.txt.gz", "build-log.txt.gz.000", "build-log.txt.003", "podinfo.json",
               "podinfo.json.gz.000"]
        for n in old:
            with open(os.path.join(self.d, n), "wb") as f:
                f.write(b"old")
        new = ["build-log.txt.000", "build-log.txt.001", "build-log.txt.002"]
        real = os.replace

        def failing(src, dst):  # (as a run killed outright between the two would leave it)
            if os.path.basename(dst) == "meta.json":
                raise OSError(5, "Input/output error")
            return real(src, dst)
        copy = self.copy(b"N" * 25, 10)
        with mock.patch.object(frl.os, "replace", failing), self.assertRaises(frl.LocalIOError):
            frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual(self.listing(), sorted(old + new))
        copy = self.copy(b"N" * 25, 10)
        frl.put_in_place(self.d, {"log": copy.info}, [copy])
        # (podinfo.json's are not this copy's: the cleanup removes a stray one, and says so)
        self.assertEqual(self.listing(), new + ["meta.json", "podinfo.json", "podinfo.json.gz.000"])

    def test_one_that_cannot_be_removed_stays(self):
        # (meta.json is in place: the cleanup reports it, as one that is not a file a download writes)
        os.mkdir(os.path.join(self.d, "build-log.txt.gz"))
        copy = self.copy(b"N" * 5)
        frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual(self.listing(), ["build-log.txt", "build-log.txt.gz", "meta.json"])
        self.assertTrue(frl.intact(self.d, frl.read_json(os.path.join(self.d, "meta.json"))))


    def test_a_full_disk_refuses_a_new_name_before_any_file_is_renamed_over(self):
        # (a log of 2 parts, now of 3: the new name goes first, and its refusal changes no part there)
        for i in range(2):
            with open(os.path.join(self.d, f"build-log.txt.{i:03d}"), "wb") as f:
                f.write(b"O" * 10)
        real = os.replace

        def full(src, dst):
            if os.path.dirname(dst) == self.d and not os.path.lexists(dst):
                raise OSError(28, "No space left on device")
            return real(src, dst)
        copy = self.copy(b"N" * 25, 10)
        with mock.patch.object(frl.os, "replace", full), self.assertRaises(frl.LocalIOError):
            frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual(self.listing(), ["build-log.txt.000", "build-log.txt.001"])
        for i in range(2):
            with open(os.path.join(self.d, f"build-log.txt.{i:03d}"), "rb") as f:
                self.assertEqual(f.read(), b"O" * 10)

    def test_a_temp_file_that_cannot_be_removed_after_a_failure(self):
        # (it is the next run's to sweep: the failure is still the disk's, not an OSError of the cleanup)
        real_replace, real_remove = os.replace, os.remove

        def failing_replace(src, dst):
            if os.path.basename(dst) == "build-log.txt":
                raise OSError(5, "Input/output error")
            return real_replace(src, dst)

        def failing_remove(path, *args, **kwargs):
            if os.path.basename(path).startswith(".build-log.txt."):
                raise OSError(5, "Input/output error")
            return real_remove(path, *args, **kwargs)
        copy = self.copy(b"N" * 5)
        with mock.patch.object(frl.os, "replace", failing_replace), mock.patch.object(frl.os, "remove", failing_remove), \
                self.assertRaises(frl.LocalIOError):
            frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual([n for n in self.listing() if not n.startswith(".build-log.txt.")], [])

    def test_a_folder_that_cannot_be_listed_after_the_renames(self):
        # (meta.json is in place: the copies it no longer lists are strays, for the cleanup)
        copy = self.copy(b"N" * 5)
        with mock.patch.object(frl, "listdir", side_effect=OSError(5, "Input/output error")):
            frl.put_in_place(self.d, {"log": copy.info}, [copy])
        self.assertEqual(self.listing(), ["build-log.txt", "meta.json"])


class TakenAgain(unittest.TestCase):
    """A request tried again after its attempt made a copy (an attempt can still fail once its body is read,
    as when its connection cannot be put back in its pool): the copy goes, and only the last one is left."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d)
        self.srv.route("/bucket/x/build-log.txt", obj(b"log, longer than a part\n" * 4))

    def download(self, failures):
        """Download the log in 52-byte parts, the first `failures` attempts failing once their copy is made."""
        real, failed = frl.settle, []

        def settle(raw, reuse):
            real(raw, reuse)
            if reuse and len(failed) < failures:
                failed.append(1)
                raise OSError(104, "Connection reset by peer")
        with mock.patch.object(frl, "settle", settle), mock.patch.object(frl, "BACKOFF", 0):
            return frl.download(self.srv.url + "/bucket/x/build-log.txt", os.path.join(self.d, "build-log.txt"),
                                max_bytes=52)

    def test_the_copy_of_an_attempt_tried_again(self):
        copy = self.download(1)
        self.assertEqual(self.srv.hits("/bucket/x/build-log.txt"), 2)
        self.assertEqual(sorted(os.listdir(self.d)), sorted(os.path.basename(t) for t, _ in copy.temps))
        self.assertEqual([name for _, name in copy.temps], [os.path.join(self.d, f"build-log.txt.00{i}") for i in (0, 1)])
        copy.discard()
        self.assertEqual(os.listdir(self.d), [])

    def test_the_copy_of_the_last_attempt_when_it_fails(self):
        with self.assertRaises(OSError):
            self.download(frl.RETRIES)
        self.assertEqual((self.srv.hits("/bucket/x/build-log.txt"), os.listdir(self.d)), (frl.RETRIES, []))


if __name__ == "__main__":
    unittest.main()
