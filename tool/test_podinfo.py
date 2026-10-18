"""podinfo.json, Prow's record of a build's pod (how each of its containers ended, and the node it ran
on), kept for every archived build GCS has one for. Before, only a build GCS had no log for kept it.

Run: cd tool && python -m unittest test_podinfo -v
"""
import contextlib
import gzip
import hashlib
import json
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
from test_integrity import DAY, HOUR, ArchiveWorld, obj  # noqa: E402

POD = json.dumps({"pod": {"status": {"phase": "Failed", "containerStatuses": [
    {"name": "test", "state": {"terminated": {"reason": "OOMKilled", "exitCode": 2}}}]}}}).encode()


class PodInfo(ArchiveWorld):
    """A red build with a log in GCS, which finished 3 hours ago: not final, so each run compares
    its files with GCS again."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))

    def pod(self, data=POD, generation="1"):
        self.srv.route(self.base + "/podinfo.json", obj(data, generation))

    def test_a_build_with_a_log_keeps_its_podinfo(self):
        """How a build's containers ended (an OOMKilled, say) and its node were in the archive only
        for a build without a log."""
        self.pod()
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt", "meta.json", "podinfo.json"])  # fails: none
        self.assertEqual(self.read("podinfo.json"), POD)
        self.assertEqual((self.meta()["podinfo"]["md5"], self.meta()["podinfo"]["bytes"]),
                         (hashlib.md5(POD).hexdigest(), len(POD)))
        self.assertEqual(self.check()[0], 0)

    def test_one_gcs_has_later_is_added_by_the_next_run(self):
        # (Prow writes it up to minutes after finished.json)
        self.assertEqual(self.fetch(), 0)
        self.assertIsNone(self.meta()["podinfo"])
        self.pod()
        self.assertEqual(self.fetch(), 0)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertEqual((self.read("podinfo.json"), self.meta()["refreshed"]), (POD, ["podinfo.json"]))
        self.assertEqual(self.check()[0], 0)

    def test_one_that_cannot_be_read_keeps_the_log_in(self):
        """Reading podinfo.json failing (here, GCS refuses it) is no reason to leave the build's log
        out: the next run looks again."""
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.meta()["podinfo"]), ({"archived": [self.key]}, None))
        self.pod()
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("podinfo.json"), POD)  # fails: no such file

    def test_one_that_cannot_be_read_when_a_later_run_looks_fails_that_comparison(self):
        """As one of its log: the run says so, and the next run compares it again (not one that is
        final only in the run whose sevenths hold it, days later)."""
        self.assertEqual(self.fetch(), 0)  # (none in GCS yet)
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 1)  # fails: 0
        run = self.runs()[-1]
        self.assertEqual((run["errors"], run["gcs_compared"]), ([{"build": self.key, "error": "comparing the archived "
                                                                  "copy with GCS failed: HTTPError: HTTP Error 403: "
                                                                  "Forbidden"}], False))
        self.pod()
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("podinfo.json"), POD)

    def test_an_old_build_s_that_cannot_be_read_is_read_again_by_the_next_run(self):
        # (the build is archived with its log, and not final until it is read)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 0)
        self.assertFalse(self.meta()["final"])  # fails: True
        self.pod()
        with mock.patch.object(frl, "gcs_sevenths", return_value=set()):  # (not in the run's sevenths)
            self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.read("podinfo.json"), self.meta()["final"]), (POD, True))

    def test_a_comparison_that_fails_after_it_added_one_says_so_first(self):
        # (meta.json lists the file on disk, though the rest of the comparison then fails: the end of the
        # run does not remove it, as a copy cut off part-way)
        self.assertEqual(self.fetch(), 0)  # (none in GCS yet)
        self.pod()
        with mock.patch.object(frl, "settle_flags", side_effect=RuntimeError("a failure after it")):
            self.assertEqual(self.fetch(), 1)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt", "meta.json", "podinfo.json"])  # fails: none
        self.assertEqual((self.meta()["podinfo"]["md5"], self.runs()[-1]["warnings"]), (hashlib.md5(POD).hexdigest(), []))

    def test_one_the_deadline_cuts_off_after_a_log_is_taken_again_is_not_compared_to_the_end(self):
        # (its podinfo.json, compared after the log: the run says it did not compare all it was due to, as
        # for one it did not update; stopped: its worker, answered once the run stops it, ends before the run
        # looks at it, stopped at its next write)
        real_abandon = frl.RunContext.abandon
        for outcome, first, stopped in [(o, f, s) for o, f in (("refreshed", obj(b"log\n")), ("log-recovered", (404, {}, b"")))
                                        for s in (False, True)]:
            with self.subTest(outcome, stopped=stopped):
                shutil.rmtree(os.path.join(self.root, "runs"), ignore_errors=True)
                self.srv.route(self.base + "/build-log.txt", first)
                self.pod()
                self.assertEqual(self.fetch(), 0)
                self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
                answer = threading.Event()

                def slow(h, answer=answer):
                    answer.wait(10)
                    with contextlib.suppress(OSError):
                        obj(POD)(h)

                def abandon(ctx, answer=answer):
                    real_abandon(ctx)
                    answer.set()
                    time.sleep(0.5)
                self.srv.route(self.base + "/podinfo.json", slow)
                try:
                    with mock.patch.object(frl.RunContext, "abandon", abandon if stopped else real_abandon):
                        self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), 1)  # (1.8 s)  # fails: 0
                finally:
                    answer.set()
                run = self.runs()[-1]
                self.assertEqual((run["outcomes"], run["gcs_compared"]), ({outcome: [self.key]}, False))  # fails: True
                self.assertEqual(run["serious_warnings"], [
                    f"1 archived builds were not compared with GCS before the run reached its deadline ({self.key}): "
                    "the next run compares them"])

    def test_one_whose_worker_ends_just_after_the_deadline_is_compared(self):
        # (before the run stopped its workers: it ended as one in time does, and is not counted as cut off;
        # nor is one whose comparison failed then, for GCS: its error is the run's)
        self.pod()
        self.assertEqual(self.fetch(), 0)
        real_unchanged, real_abandon = frl.unchanged_in_gcs, frl.RunContext.abandon
        for failing in (False, True):
            with self.subTest(failing=failing):
                def unchanged(url, info, failing=failing):
                    same = real_unchanged(url, info)
                    if url.endswith("/podinfo.json"):  # (its last request: it ends just after the deadline)
                        time.sleep(max(0.0, frl.context().deadline - time.monotonic()) + 0.2)
                        if failing:
                            raise frl.IntegrityError("GCS's answer was not what it said")
                    return same

                def abandon(ctx):
                    time.sleep(1)  # (then the run stops its workers)
                    real_abandon(ctx)
                with mock.patch.object(frl, "unchanged_in_gcs", unchanged), \
                        mock.patch.object(frl.RunContext, "abandon", abandon):
                    self.assertEqual(self.fetch("--run-timeout-minutes", "0.03"), int(failing))  # (1.8 s)  # fails: (ended) 1
                run = self.runs()[-1]
                self.assertEqual((run["outcomes"], run["gcs_compared"], [e["error"] for e in run["errors"]]),
                                 ({}, False, ["comparing the archived copy with GCS failed: IntegrityError: GCS's answer was "
                                              "not what it said"]) if failing else
                                 ({"already-archived": [self.key]}, True, []))
                # fails: not-checked, False (and no error)

    def test_the_same_one_under_a_new_generation_is_not_refreshed(self):
        self.pod()
        self.assertEqual(self.fetch(), 0)
        self.pod(POD, "7")
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertNotIn("refreshed", self.meta())

    def test_the_same_one_taken_again_is_not_refreshed(self):
        # (taken again as GCS's identity of it changed, as one served gzipped does for an older record)
        self.pod()
        self.assertEqual(self.fetch(), 0)
        with mock.patch.object(frl, "unchanged_in_gcs", return_value=False):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertNotIn("refreshed", self.meta())

    def test_a_disk_error_on_it_is_one_of_the_build(self):
        # (a local error, not GCS's: the build is not archived without it, but retried)
        self.pod()
        real = os.replace

        def full(src, dst):
            if "podinfo" in dst:
                raise OSError(28, "No space left on device")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", side_effect=full):
            self.assertEqual(self.fetch(), 1)  # fails: 0
        self.assertIn("LocalIOError", self.runs()[-1]["errors"][0]["error"])
        self.assertIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("podinfo.json"), POD)

    def test_one_gcs_changed_is_reported_then_fetched_again(self):
        self.pod()
        self.assertEqual(self.fetch(), 0)
        changed = POD.replace(b"OOMKilled", b"Error")
        self.pod(changed, "2")
        code, out = self.check()
        self.assertEqual(code, 1)  # fails: 0, not compared
        self.assertIn(f"{self.key} podinfo.json: GCS changed since it was archived", out)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.read("podinfo.json"), self.meta()["refreshed"]), (changed, ["podinfo.json"]))
        self.assertEqual(self.check()[0], 0)


class PodInfoGzipped(ArchiveWorld):
    """With --gzip, as the workflow runs: podinfo.json is kept gzipped, as the log is."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * HOUR)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(POD))

    def without_a_log(self):
        del self.srv.httpd.routes[self.base + "/build-log.txt"]

    def test_kept_gzipped(self):
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json", "podinfo.json.gz"])
        # fails: none
        self.assertEqual(gzip.decompress(self.read("podinfo.json.gz")), POD)
        self.assertEqual((self.meta()["podinfo"]["file"], self.meta()["podinfo"]["md5"]),
                         ("podinfo.json.gz", hashlib.md5(POD).hexdigest()))
        self.assertEqual(self.check()[0], 0)

    def test_also_without_a_log(self):
        # (code before kept it as it came, with --gzip too)
        self.without_a_log()
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["meta.json", "podinfo.json.gz"])  # fails: podinfo.json
        self.assertEqual(self.check()[0], 0)

    def test_one_kept_as_it_came_is_gzipped_once_gcs_changes_it(self):
        # (as code before kept that of a build without a log)
        self.without_a_log()
        self.assertEqual(self.fetch(), 0)  # (without --gzip)
        changed = POD.replace(b"OOMKilled", b"Error")
        self.srv.route(self.base + "/podinfo.json", obj(changed, "2"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["meta.json", "podinfo.json.gz"])  # fails: podinfo.json
        self.assertEqual(gzip.decompress(self.read("podinfo.json.gz")), changed)
        # (not left for the end of the run, which removes it as a copy cut off part-way, and says so)
        self.assertEqual(self.runs()[-1]["warnings"], [])
        self.assertEqual(self.check()[0], 0)

    def test_one_a_later_run_adds_is_gzipped(self):
        del self.srv.httpd.routes[self.base + "/podinfo.json"]
        self.assertEqual(self.fetch("--gzip"), 0)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json", "podinfo.json.gz"])
        # fails: none
        self.assertEqual(gzip.decompress(self.read("podinfo.json.gz")), POD)
        self.assertEqual(self.check()[0], 0)

    def test_one_too_large_for_one_file_is_kept_in_parts(self):
        # (as a log is, over --max-file-mb after gzip; a part a cut-off copy left is found)
        self.without_a_log()
        self.assertEqual(self.fetch("--gzip", "--max-file-mb", "0.00005"), 0)  # (52 bytes)
        parts = self.meta()["podinfo"]["parts"]
        self.assertEqual([p["file"] for p in parts][:2], ["podinfo.json.gz.000", "podinfo.json.gz.001"])
        # fails: podinfo.json.000, ...
        self.assertEqual(gzip.decompress(b"".join(self.read(p["file"]) for p in parts)), POD)
        self.assertEqual(self.check()[0], 0)
        stray = f"podinfo.json.gz.{len(parts):03d}"
        with open(os.path.join(self.bdir(), stray), "wb") as f:
            f.write(b"x")
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"{self.key} {stray}: not listed in its meta.json", out)

    def test_one_kept_in_parts_leaves_no_part_once_taken_whole(self):
        # (in parts as it came, then gzipped once GCS changes it: the parts go with the copy they were)
        self.without_a_log()
        self.assertEqual(self.fetch("--max-file-mb", "0.00005"), 0)  # (without --gzip: podinfo.json.000, ...)
        self.assertTrue(self.meta()["podinfo"]["parts"])
        self.srv.route(self.base + "/podinfo.json", obj(POD.replace(b"OOMKilled", b"Error"), "2"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["meta.json", "podinfo.json.gz"])  # fails: podinfo.json
        self.assertEqual(self.runs()[-1]["warnings"], [])  # (not left for the end of the run to remove)

    def test_a_comparison_that_fails_after_it_took_one_again_says_so_first(self):
        # (as for a log: meta.json names the file on disk, though the rest of the comparison then fails)
        self.without_a_log()
        self.assertEqual(self.fetch(), 0)  # (podinfo.json, as it came)
        self.srv.route(self.base + "/podinfo.json", obj(POD.replace(b"OOMKilled", b"Error"), "2"))
        with mock.patch.object(frl, "settle_flags", side_effect=RuntimeError("a failure after it")):
            self.assertEqual(self.fetch("--gzip"), 1)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["meta.json", "podinfo.json.gz"])  # fails: podinfo.json
        self.assertEqual(self.meta()["podinfo"]["file"], "podinfo.json.gz")
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))

    def test_a_run_that_adds_one_counts_only_it(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"x" * 100_000))
        del self.srv.httpd.routes[self.base + "/podinfo.json"]
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["bytes_downloaded"], 100_000)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertEqual(self.runs()[-1]["bytes_downloaded"], len(POD))


class PodInfoOfAnOldBuild(ArchiveWorld):
    """A final red build, archived by code that kept podinfo.json only without a log (here, before
    GCS had one): the run whose sevenths hold it compares it with GCS, and adds its podinfo.json."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.srv.route(self.base + "/podinfo.json", obj(POD))

    def sevenths(self, which):
        return mock.patch.object(frl, "gcs_sevenths", return_value=which)

    def test_control_not_in_the_runs_sevenths(self):
        with self.sevenths(set()):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.meta()["podinfo"]), ({"already-archived": [self.key]}, None))

    def test_in_the_runs_sevenths(self):
        with self.sevenths(set(range(7))):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertEqual(self.read("podinfo.json"), POD)
        self.assertEqual(self.check()[0], 0)


class PodInfoOfARepair(ArchiveWorld):
    """A final build whose archived log is damaged, and whose podinfo.json.gz is intact: the next run takes
    the log again from GCS (a repair), and not its podinfo.json, whose copy stays (GCS may have none now,
    as it deletes old builds' files)."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"x")  # (no longer the size meta.json records)

    def test_its_podinfo_stays_unasked(self):
        # (not removed at the end of the run "as its meta.json does not list it (a copy cut off part-way)")
        self.srv.route(self.base + "/podinfo.json", (404, {}, b""))
        asked = self.srv.hits(self.base + "/podinfo.json")
        self.assertEqual(self.fetch("--gzip"), 0)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["warnings"]), ({"repaired": [self.key]}, []))
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json", "podinfo.json.gz"])
        # fails: no podinfo.json.gz
        self.assertEqual((gzip.decompress(self.read("podinfo.json.gz")), self.srv.hits(self.base + "/podinfo.json")),
                         (POD, asked))
        self.assertEqual((self.meta()["repaired"], run["bytes_downloaded"]), (["build-log.txt"], len(b"log\n")))
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))
        self.assertEqual(self.check()[0], 0)

    def test_not_a_damaged_copy_gcs_no_longer_has(self):
        # (as for a damaged log GCS no longer has: an error, the copy left as it is for a person to look at;
        # one of the size meta.json records, its content changed)
        path = os.path.join(self.bdir(), "podinfo.json.gz")
        data = bytearray(self.read("podinfo.json.gz"))  # fails: no such file
        data[len(data) // 2] ^= 0xFF
        with open(path, "wb") as f:
            f.write(data)
        self.srv.route(self.base + "/podinfo.json", (404, {}, b""))
        self.assertEqual(self.fetch("--gzip"), 1)
        self.assertIn("the archived podinfo.json is damaged and GCS no longer has it",
                      self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json", "podinfo.json.gz"])



class PodInfoDamaged(ArchiveWorld):
    """A final build whose archived podinfo.json.gz is damaged, and whose log is intact: the next run takes
    its podinfo.json again from GCS (a repair), and not its log, whose copy stays."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        with open(os.path.join(self.bdir(), "podinfo.json.gz"), "ab") as f:
            f.write(b"x")  # (no longer the size meta.json records)

    def test_its_log_stays_unasked(self):
        # (GCS may have none now: not "the archived log is damaged and GCS no longer has it", which it is not)
        self.srv.route(self.base + "/build-log.txt", (404, {}, b""))
        asked = self.srv.hits(self.base + "/build-log.txt")
        self.assertEqual(self.fetch("--gzip"), 0)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["errors"]), ({"repaired": [self.key]}, []))  # fails: already-archived
        self.assertEqual((gzip.decompress(self.read("build-log.txt.gz")), gzip.decompress(self.read("podinfo.json.gz")),
                          self.srv.hits(self.base + "/build-log.txt")), (b"log\n", POD, asked))
        self.assertEqual((self.meta()["repaired"], run["bytes_downloaded"]), (["podinfo.json"], len(POD)))
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))

    def test_not_when_gcs_no_longer_has_its_podinfo(self):
        # (an error, as for a damaged log GCS no longer has; nor is its log taken again, run after run)
        self.srv.route(self.base + "/podinfo.json", (404, {}, b""))
        asked = self.srv.hits(self.base + "/build-log.txt")
        for _ in range(2):
            self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
            self.assertIn("the archived podinfo.json is damaged and GCS no longer has it",
                          self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.srv.hits(self.base + "/build-log.txt"), asked)


class PodInfoInOlderRunReadmes(ArchiveWorld):
    """A build a run archived 3.8 days ago, past the runs whose READMEs every run writes again (those of
    the last RECHECK_HOURS and a day): a later run that adds its podinfo.json, or takes it again in
    another form, writes that README again too, so that the build's row links the files it has."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3.9 * DAY)

    def archived(self, *extra):
        real = time.time
        with mock.patch("time.time", lambda: real() - 3.8 * DAY):
            self.assertEqual(self.fetch(*extra), 0)
        return frl.run_dirs(self.root)[0]

    def readme(self, rel):
        with open(os.path.join(self.root, rel, "README.md")) as f:
            return f.read()

    def test_one_a_later_run_adds(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        rel = self.archived("--gzip")
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertIn(f"[podinfo.json.gz](../../../{rel}/job/{self.build}/podinfo.json.gz)", self.readme(rel))

    def test_not_one_that_is_a_link(self):
        # (a link in the archive is never followed, and stays as a person left it, as linking() leaves it)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        rel = self.archived("--gzip")
        outside = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, outside)
        with open(os.path.join(outside, "page.md"), "w") as f:
            f.write("a person's page\n")
        os.remove(os.path.join(self.root, rel, "README.md"))
        os.symlink(os.path.join(outside, "page.md"), os.path.join(self.root, rel, "README.md"))
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"refreshed": [self.key]})  # fails: already-archived
        self.assertTrue(os.path.islink(os.path.join(self.root, rel, "README.md")))
        with open(os.path.join(outside, "page.md")) as f:
            self.assertEqual(f.read(), "a person's page\n")

    def test_nor_one_that_cannot_be_read(self):
        # (it stays as it is, and the run goes on)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        rel = self.archived("--gzip")
        path = os.path.join(self.root, rel, "README.md")
        os.chmod(path, 0)  # (tearDown removes it all the same: its folder can be written)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.runs()[-1]["errors"]), ({"refreshed": [self.key]}, []))
        # fails: already-archived

    def test_one_taken_again_in_another_form(self):
        # (podinfo.json as code before kept it, for a build without a log; gzipped once GCS changes it)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        rel = self.archived()
        self.assertIn(f"](../../../{rel}/job/{self.build}/podinfo.json)", self.readme(rel))
        self.srv.route(self.base + "/podinfo.json", obj(POD.replace(b"OOMKilled", b"Error"), "2"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], sorted(os.listdir(self.bdir()))),
                         ({"refreshed": [self.key]}, ["meta.json", "podinfo.json.gz"]))  # fails: podinfo.json
        self.assertIn(f"[podinfo.json.gz](../../../{rel}/job/{self.build}/podinfo.json.gz)", self.readme(rel))
        self.assertNotIn("/podinfo.json)", self.readme(rel))  # (a link to a file no longer there)


class PodInfoWithoutALog(ArchiveWorld):
    """A red build whose pod never ran: GCS has podinfo.json and no log."""

    def test_control_it_keeps_its_podinfo_as_before(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived-without-log": [self.key]})
        self.assertEqual(sorted(os.listdir(self.bdir())), ["meta.json", "podinfo.json"])

    def test_control_a_repair_keeps_its_note(self):
        # (archived without a log or finished.json, with a note saying so; then its podinfo.json is damaged)
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertTrue(self.meta()["note"].startswith("GCS had neither finished.json nor build-log.txt "))
        with open(os.path.join(self.bdir(), self.meta()["podinfo"]["file"]), "ab") as f:
            f.write(b"x")  # (no longer the size meta.json records)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertTrue(self.meta()["note"].startswith("GCS had neither finished.json nor build-log.txt "))

    def test_control_one_that_cannot_be_read_is_retried(self):
        # (its only file: the build is archived once it can be read)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 1)
        self.assertIsNone(self.bdir())
        self.assertIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_control_one_archived_with_no_file_compares_it_when_it_comes(self):
        # (archived when GCS had nothing; podinfo.json then appears, and cannot be read: the comparison
        # of its only file fails, and the next run compares it again)
        self.finished(ago=1.5 * DAY)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual((self.meta()["log"], self.meta()["podinfo"]), (None, None))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch(), 1)
        run = self.runs()[-1]
        self.assertEqual((run["errors"], run["gcs_compared"]), ([{"build": self.key, "error": "comparing the archived "
                                                                  "copy with GCS failed: HTTPError: HTTP Error 403: "
                                                                  "Forbidden"}], False))
        self.srv.route(self.base + "/podinfo.json", obj(POD))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("podinfo.json"), POD)


if __name__ == "__main__":
    unittest.main()
