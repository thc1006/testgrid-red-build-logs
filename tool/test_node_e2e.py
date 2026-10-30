"""The red builds of node e2e jobs, told by their prowjob.json (see fetch_red_logs.node_e2e_job), and the asset of
each one's artifacts/: packed by the fetch, uploaded by upload_assets.py, recorded in its meta.json.

Run: cd tool && python -m unittest test_node_e2e -v
"""
import contextlib
import glob
import hashlib
import inspect
import io
import json
import math
import os
import shutil
import sys
import tarfile
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
import upload_assets as ua  # noqa: E402
from test_fetch_red_logs import FakeServer, gcs_object, gzip_stored, md5_b64, table  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj, snowflake  # noqa: E402
from test_upload_assets import ENV, REPO, FakeGitHub  # noqa: E402

# (the args of real jobs' prowjob.json, cut short)
KUBETEST_NODE = ["--deployment=node", "--gcp-zone=us-central1-b", "--node-tests=true", "--provider=gce", "--timeout=80m"]
KUBETEST2_NODE = ["kubetest2", "noop", "--test=node", "--", "--repo-root=.", "--gcp-zone=us-central1-b",
                  "--parallelism=8", "--timeout=180m"]
KUBETEST2_DRA = ["kubetest2", "noop", "--test=node", "--", "--use-binaries-from-package",
                 "--label-filter=DRA && Feature: isSubsetOf { DynamicResourceAllocation } && !Flaky", "--timeout=60m"]
KUBETEST_GCE = ["--check-leaked-resources", "--extract=ci/fast/latest-fast", "--gcp-node-image=gci", "--provider=gce",
                "--timeout=120m"]
KIND_DRA = ["/bin/bash", "-xce", "set -o pipefail\nkind create cluster ...\nkubetest2 noop --test=node"]


def prowjob(args, command=("runner.sh",)):
    return {"kind": "ProwJob", "spec": {"job": "job", "pod_spec": {"containers": [
        {"image": "kubekins", "command": list(command), "args": args}]}}}


class NodeE2eJob(unittest.TestCase):
    def test_the_node_e2e_jobs(self):
        for args in (KUBETEST_NODE, KUBETEST2_NODE, KUBETEST2_DRA):
            with self.subTest(args[:3]):
                self.assertIs(frl.node_e2e_job(prowjob(args, ("runner.sh", "/workspace/scenarios/kubernetes_e2e.py"))),
                              True)  # fails: no node_e2e_job
        self.assertIs(frl.node_e2e_job(prowjob([], ["/usr/local/bin/kubetest2", "noop", "--test=node"])),
                      True)  # fails: no node_e2e_job
        self.assertIs(frl.node_e2e_job(prowjob(["kubetest2", "gce", "--up", "--test=node"])), True)  # (any deployer)

    def test_not_others(self):
        for args in (KUBETEST_GCE, KIND_DRA, ["kubetest2", "gce", "--test=ginkgo"], ["kubetest2", "noop", "--test=ginkgo"],
                     ["kubetest2", "noop", "--", "--test=node"], ["kubetest", "--test=node"], ["--deployment=nodes"],
                     ["kubetest2", "noop"], []):
            with self.subTest(args):
                self.assertIs(frl.node_e2e_job(prowjob(args)), False)  # fails: no node_e2e_job

    def test_none_of_what_is_not_a_prowjob(self):
        for value in (None, [], "x", {}, {"spec": {}}, {"spec": {"pod_spec": {"containers": {}}}},
                      {"spec": {"pod_spec": {"containers": ["x"]}}}, {"spec": []}):
            with self.subTest(value):
                self.assertIsNone(frl.node_e2e_job(value))  # fails: no node_e2e_job
        odd = prowjob([7, None, "--deployment=node"])  # (a word not a string is none)
        self.assertIs(frl.node_e2e_job(odd), True)  # fails: no node_e2e_job


class NodeE2eBuild(ArchiveWorld):
    """A red build whose prowjob.json says what its job runs."""

    def route(self, value):
        self.srv.route(self.base + "/prowjob.json", (200, {}, value))

    def test_store_records_it(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.route(prowjob(KUBETEST2_NODE))
        self.assertEqual(self.fetch(), 0)
        self.assertIs(self.meta().get("node_e2e"), True)  # fails: None
        self.assertEqual(self.check()[0], 0)

    def test_and_that_it_is_not(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.route(prowjob(KUBETEST_GCE))
        self.assertEqual(self.fetch(), 0)
        self.assertIs(self.meta().get("node_e2e"), False)  # fails: None

    def test_one_it_cannot_read_is_left_out_and_read_by_the_next_comparison(self):
        self.finished(ago=3 * HOUR)  # (not final: compared again by the next run)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        for value in (None, b"not json"):
            with self.subTest(value):
                if value is not None:
                    self.route(value)
                self.assertEqual(self.fetch(), 0)
                self.assertNotIn("node_e2e", self.meta())
        self.route(prowjob(KUBETEST_NODE))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertIs(self.meta().get("node_e2e"), True)  # fails: None
        self.route(prowjob(KUBETEST_GCE))  # (read once: what it said stays)
        self.assertEqual(self.fetch(), 0)
        self.assertIs(self.meta()["node_e2e"], True)

    def test_a_final_build_in_its_seventh(self):
        # (one archived before the runs read it: compared, and so read, within a week)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.route(prowjob(KUBETEST_NODE))
        before = self.read("meta.json")
        with mock.patch.object(frl, "gcs_sevenths", return_value=set()):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("meta.json"), before)  # (not in its seventh)
        with mock.patch.object(frl, "gcs_sevenths", return_value=set(range(7))):
            self.assertEqual(self.fetch(), 0)
        self.assertIs(self.meta().get("node_e2e"), True)  # fails: None

    def test_a_repair_keeps_what_it_recorded(self):
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.route(prowjob(KUBETEST_NODE))
        self.assertEqual(self.fetch(), 0)
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        self.route(prowjob(KUBETEST_GCE))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertIs(self.meta()["node_e2e"], True)  # fails: KeyError


HOST = "tmp-node-e2e-1a2b3c4d-cos-129-19506-448-53"
LONG = f"{HOST}/pods/kube-system_some-pod-with-a-long-name-for-its-test_0f0e0d0c-0b0a-0908-0706-050403020100/c/0.log"


class PackArtifacts(unittest.TestCase):
    """A build's artifacts/ in GCS, packed into one tar.xz."""

    FILES = {f"artifacts/{HOST}/kubelet.log": b"kubelet\n" * 1000, f"artifacts/{HOST}-system.log": b"system\n",
             f"artifacts/{HOST}/serial-1.log": b"", "artifacts/junit_runner.xml": b"<testsuites/>\n",
             f"artifacts/{LONG}": b"a pod\n"}

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)
        self.work = os.path.join(self.dir, "work")
        os.mkdir(self.work)
        self.base = "/bucket/logs/job/1"
        for path, data in self.FILES.items():
            self.srv.route(f"{self.base}/{path}", obj(data))

    def pack(self, name="a.tar.xz", members=None):
        out = os.path.join(self.dir, name)
        return out, frl.pack_artifacts(self.srv.url + self.base, list(members or self.FILES), self.work, out,
                                       1761800000)  # fails: no pack_artifacts

    def test_one_tar_xz_of_them_all(self):
        out, record = self.pack()
        with open(out, "rb") as f:
            data = f.read()
        self.assertEqual((record["bytes"], record["sha256"]), (len(data), hashlib.sha256(data).hexdigest()))
        self.assertEqual(data[:6], b"\xfd7zXZ\x00")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:xz") as tar:
            entries = tar.getmembers()
            self.assertEqual([e.name for e in entries], sorted(p.removeprefix("artifacts/") for p in self.FILES))
            for e in entries:
                self.assertEqual((e.type, e.mode, e.uid, e.gid, e.uname, e.gname, e.mtime),
                                 (tarfile.REGTYPE, 0o644, 0, 0, "", "", 1761800000), e.name)
                self.assertEqual(tar.extractfile(e).read(), self.FILES["artifacts/" + e.name])
        self.assertGreater(len(LONG), 100)  # (more than ustar takes: a PAX header holds it)
        self.assertEqual(record["members"], [
            {"path": p, "bytes": len(self.FILES[p]), "md5": hashlib.md5(self.FILES[p]).hexdigest(),
             "gcs": {"generation": "1", "md5": hashlib.md5(self.FILES[p]).hexdigest(), "crc32c": "00000000",
                     "bytes": len(self.FILES[p]), "encoding": "identity"}} for p in sorted(self.FILES)])
        self.assertEqual(os.listdir(self.work), [])

    def test_the_same_files_give_the_same_bytes(self):
        a, first = self.pack("a.tar.xz")
        b, second = self.pack("b.tar.xz", members=reversed(list(self.FILES)))
        self.assertEqual(first, second)
        with open(a, "rb") as f, open(b, "rb") as g:
            self.assertEqual(f.read(), g.read())

    def test_a_member_stored_gzipped_is_packed_as_its_content(self):
        self.srv.route(f"{self.base}/artifacts/{HOST}/kubelet.log", gzip_stored(b"kubelet\n" * 1000))
        out, record = self.pack()
        with tarfile.open(out, mode="r:xz") as tar:
            self.assertEqual(tar.extractfile(f"{HOST}/kubelet.log").read(), b"kubelet\n" * 1000)

    def test_one_gcs_serves_wrong_is_refused(self):
        path = f"artifacts/{HOST}/kubelet.log"
        self.srv.route(f"{self.base}/{path}", gcs_object(b"other", md5=md5_b64(b"kubelet")))
        with self.assertRaises(frl.IntegrityError):
            self.pack()
        self.assertEqual(os.listdir(self.dir), ["work"])
        self.assertEqual(os.listdir(self.work), [])

    def test_one_gcs_no_longer_has(self):
        with self.assertRaises(frl.NotFound):
            self.pack(members=list(self.FILES) + ["artifacts/gone.log"])
        self.assertEqual(os.listdir(self.dir), ["work"])

    def test_a_name_that_is_not_a_plain_one_below_artifacts(self):
        for path in ("artifacts/../x", "artifacts//x", "artifacts/a/./b", "artifacts/a/", "logs/x", "artifacts/a\nb",
                     "artifacts/", None):
            with self.subTest(path):
                with self.assertRaises(frl.AssetRefused):  # fails: no AssetRefused
                    self.pack(members=[path])
                self.assertEqual(os.listdir(self.dir), ["work"])
        with mock.patch.object(frl, "download") as download, self.assertRaises(frl.AssetRefused):  # fails: no AssetRefused
            self.pack(members=list(self.FILES) + ["artifacts/zz/../x"])
        self.assertEqual(download.call_count, 0)  # (refused before any file is taken)

    def test_files_that_reach_the_cap_once_unpacked(self):
        # (GCS lists a gzipped file's stored bytes: a few can unpack to many)
        self.srv.route(f"{self.base}/artifacts/{HOST}/kubelet.log", gzip_stored(self.FILES[f"artifacts/{HOST}/kubelet.log"]))
        files = sum(map(len, self.FILES.values()))
        with mock.patch.object(frl, "ASSET_FILES_BYTES", files + 1):  # fails: no ASSET_FILES_BYTES
            self.pack("a.tar.xz")
        with mock.patch.object(frl, "ASSET_FILES_BYTES", files), self.assertRaises(frl.AssetTooLarge) as raised:
            self.pack("b.tar.xz")
        self.assertEqual(raised.exception.files, files)
        self.assertEqual(sorted(os.listdir(self.dir)), ["a.tar.xz", "work"])
        self.assertEqual(os.listdir(self.work), [])

    def test_a_member_is_taken_only_while_it_fits(self):
        path = f"artifacts/{HOST}/kubelet.log"
        self.srv.route(f"{self.base}/{path}", gzip_stored(b"k" * 5000))
        for step in (1000, 5000):  # (the limit reached first, or with MAX_DOWNLOAD_BYTES)
            sizes = mock.patch.multiple(frl, MAX_DOWNLOAD_BYTES=4000, ASSET_FILES_BYTES=2000, GUNZIP_STEP=step)
            with sizes, self.assertRaises(frl.AssetTooLarge) as raised:  # fails: no ASSET_FILES_BYTES
                self.pack(members=[path])
            self.assertEqual(raised.exception.files, 2000)
        self.assertEqual((self.srv.hits(f"{self.base}/{path}"), os.listdir(self.work)), (2, []))  # (not tried again)

    def test_a_download_stops_at_its_limit(self):
        url = f"{self.srv.url}{self.base}/artifacts/{HOST}/kubelet.log"
        frl.download(url, os.path.join(self.work, "member"), limit=8000).discard()  # fails: no limit
        with self.assertRaises(frl.OverLimit):
            frl.download(url, os.path.join(self.work, "member"), limit=7999)
        with mock.patch.object(frl, "MAX_DOWNLOAD_BYTES", 7999), self.assertRaises(frl.OverLimit):  # (by its length)
            frl.download(url, os.path.join(self.work, "member"), limit=8000)
        self.assertEqual((self.srv.hits(f"{self.base}/artifacts/{HOST}/kubelet.log"), os.listdir(self.work)), (3, []))

    def test_a_disk_error_is_not_gcss(self):
        full = OSError(28, "No space left on device")
        for method in ("addfile", "close"):
            with self.subTest(method), self.assertRaises(frl.LocalIOError), \
                    mock.patch.object(frl.tarfile.TarFile, method, side_effect=full):  # fails: no tarfile
                self.pack()
            self.assertEqual(os.listdir(self.dir), ["work"])
        for patch in (mock.patch.object(frl.tempfile, "mkstemp", side_effect=full),
                      mock.patch.object(frl.os, "replace", side_effect=full)):
            with self.subTest(patch.attribute), self.assertRaises(frl.LocalIOError), patch:
                self.pack()
            self.assertEqual(os.listdir(self.dir), ["work"])

    def run_context(self, deadline):
        ctx = frl.RunContext(frl.AdaptiveLimit(8, 8), deadline)
        self.addCleanup(frl.bind_context, frl.context())
        frl.bind_context(ctx)
        return ctx

    def test_none_past_the_runs_deadline(self):
        self.run_context(time.monotonic() - 1)
        with mock.patch.object(frl, "download", side_effect=frl.download) as download, \
                self.assertRaisesRegex(TimeoutError, "run timeout reached while packing"):
            self.pack()
        self.assertEqual((os.listdir(self.dir), download.call_count), (["work"], 0))  # (nor took a file)

    def test_nor_once_the_run_is_stopped_while_a_member_is_packed(self):
        ctx, download = self.run_context(math.inf), frl.download

        def then_stopped(*args, **kwargs):  # (after the download: tarfile reads it, and xz packs it)
            staged = download(*args, **kwargs)
            ctx.abandoned.set()
            return staged
        with mock.patch.object(frl, "download", side_effect=then_stopped), self.assertRaises(frl.Abandoned):
            self.pack(members=[f"artifacts/{HOST}/kubelet.log"])
        self.assertEqual(os.listdir(self.dir), ["work"])
        self.assertEqual(os.listdir(self.work), [])

    def test_nor_once_its_deadline_passes_while_a_member_is_packed(self):
        ctx, download = self.run_context(math.inf), frl.download

        def then_late(*args, **kwargs):
            staged = download(*args, **kwargs)
            ctx.deadline = -math.inf
            return staged
        with mock.patch.object(frl, "download", side_effect=then_late), \
                self.assertRaisesRegex(TimeoutError, "run timeout reached while packing"):
            self.pack(members=[f"artifacts/{HOST}/kubelet.log"])
        self.assertEqual(os.listdir(self.dir), ["work"])

    def test_one_too_large_to_upload(self):
        size = self.pack("a.tar.xz")[1]["bytes"]
        with mock.patch.object(frl, "MAX_ASSET_BYTES", size):
            self.pack("b.tar.xz")
        with mock.patch.object(frl, "MAX_ASSET_BYTES", size - 1):
            with self.assertRaises(frl.AssetTooLarge):
                self.pack("c.tar.xz")
        self.assertEqual(sorted(os.listdir(self.dir)), ["a.tar.xz", "b.tar.xz", "work"])


class AssetWorld(ArchiveWorld):
    """A red build of a node e2e job, finished three days ago, whose artifacts/ GCS has; the runs pack them into
    --assets."""

    FILES = {f"artifacts/{HOST}/kubelet.log": b"kubelet\n" * 100, f"artifacts/{HOST}/serial-1.log": b"booting\n",
             f"artifacts/{HOST}-system.log": b"system\n", "artifacts/junit_runner.xml": b"<testsuites/>\n"}

    def setUp(self):
        super().setUp()
        self.assets = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.assets)
        self.node_e2e(self.base, self.FILES)

    def node_e2e(self, base, files, ago=3 * DAY, args=KUBETEST2_NODE):
        self.srv.route(base + "/finished.json", (200, {}, {"result": "FAILURE", "timestamp": int(time.time() - ago)}))
        self.srv.route(base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(base + "/prowjob.json", (200, {}, prowjob(args)))
        for path, data in files.items():
            self.srv.route(f"{base}/{path}", gcs_object(data))

    def manifest(self):
        return frl.read_json(os.path.join(self.assets, "manifest.json"))

    def asset(self, name):
        with open(os.path.join(self.assets, name), "rb") as f:
            return f.read()


class Assets(AssetWorld):
    def test_a_pending_asset_of_its_artifacts(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        run, meta = self.runs()[-1], self.meta()
        record = meta.get("artifacts_asset")
        self.assertIsNotNone(record)
        name = f"job_{self.build}.tar.xz"
        data = self.asset(name)
        self.assertEqual(record, {"name": name, "month": run["run"][:7], "bytes": len(data),
                                  "sha256": hashlib.sha256(data).hexdigest(), "members": record["members"],
                                  "built_by_run": run["run"], "pending_since": run["started"]})
        self.assertEqual([(m["path"], m["bytes"], m["md5"]) for m in record["members"]],
                         [(p, len(d), hashlib.md5(d).hexdigest()) for p, d in sorted(self.FILES.items())])
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:xz") as tar:
            self.assertEqual(sorted(tar.getnames()), sorted(p.removeprefix("artifacts/") for p in self.FILES))
        self.assertEqual(self.manifest(), {"run": run["run"], "assets": [
            {"name": name, "month": record["month"], "bytes": len(data), "sha256": record["sha256"]}],
            "delete": []})  # fails: no "delete"
        self.assertEqual(run["assets_built"], [name])
        self.assertEqual(run["bytes_downloaded"], len(b"log\n") + sum(map(len, self.FILES.values())))  # (packing too)
        self.assertIn("assets", run["timings"])
        # (not a file of the build folder: what intact() and the cleanup read stays as it was)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt", "meta.json"])
        self.assertEqual(frl.file_records(meta), frl.file_records({k: v for k, v in meta.items() if k != "artifacts_asset"}))
        self.assertTrue(frl.intact(self.bdir(), meta, deep=True))
        self.assertEqual(frl.stray_files(self.bdir(), meta), [])
        self.assertEqual(sorted(os.listdir(self.assets)), sorted([name, "manifest.json"]))  # (the work folder goes)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_a_run_packs_a_pending_one_again_the_same(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        first, data = self.meta()["artifacts_asset"], self.asset(f"job_{self.build}.tar.xz")
        self.assets = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.assets)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        again = self.meta()["artifacts_asset"]
        self.assertEqual(again, dict(first, built_by_run=self.runs()[-1]["run"]))
        self.assertEqual(self.asset(again["name"]), data)
        self.assertEqual([a["sha256"] for a in self.manifest()["assets"]], [first["sha256"]])

    def test_one_archived_before_the_runs_read_its_prowjob_json(self):
        # (final, and packed in the run whose sevenths it is in: so each is packed within a week)
        self.assertEqual(self.fetch(), 0)
        meta = self.meta()
        del meta["node_e2e"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        with mock.patch.object(frl, "gcs_sevenths", return_value=set(range(7))):
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.meta()["artifacts_asset"]["name"], f"job_{self.build}.tar.xz")  # fails: KeyError: 'artifacts_asset'
        self.assertEqual([a["name"] for a in self.manifest()["assets"]], [f"job_{self.build}.tar.xz"])
        self.assertTrue(self.meta()["node_e2e"])

    def test_its_month_is_that_of_the_run_that_archived_it(self):
        # (a run of another month packs it into the release of the month it was archived in, again too)
        self.assertEqual(self.fetch(), 0)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_by_run="2026-09-30T230000Z"))
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertEqual(self.meta()["artifacts_asset"]["month"], "2026-09")
        meta = self.meta()
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, archived_by_run="2026-08-01T000000Z"))
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.meta()["artifacts_asset"]["month"], "2026-09")

    def test_but_not_one_uploaded(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        meta = self.meta()
        meta["artifacts_asset"]["uploaded"] = {"release": "node-logs-2026-10"}
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.meta(), meta)
        self.assertEqual(self.manifest()["assets"], [])

    def test_a_repair_keeps_its_record(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        meta = self.meta()
        meta["artifacts_asset"]["uploaded"] = {"release": "node-logs-2026-10", "url": "https://x/y"}
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual(self.meta()["artifacts_asset"], meta["artifacts_asset"])
        self.assertEqual(self.manifest()["assets"], [])

    def test_one_whose_node_never_existed(self):
        for path in list(self.FILES):
            self.srv.httpd.routes.pop(f"{self.base}/{path}")
        self.srv.route(f"{self.base}/artifacts/junit_runner.xml", gcs_object(b"<testsuites/>\n"))
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertEqual(self.meta()["artifacts_asset"], {"members": []})
        self.assertEqual(self.manifest()["assets"], [])
        self.srv.route(f"{self.base}/artifacts/{HOST}/serial-1.log", gcs_object(b"late\n"))
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.meta()["artifacts_asset"], {"members": []})  # (looked at once)

    def test_a_folders_placeholder_is_no_member(self):
        # (an empty object named <folder>/, as the GCS console makes one)
        self.srv.route(f"{self.base}/artifacts/{HOST}/", gcs_object(b""))
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertEqual(len(self.meta()["artifacts_asset"]["members"]), len(self.FILES))

    def test_one_not_settled_yet_waits(self):
        self.finished(ago=30 * 60)
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertNotIn("artifacts_asset", self.meta())
        self.assertEqual(self.manifest()["assets"], [])
        self.finished(ago=3 * HOUR)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.meta()["artifacts_asset"]["name"], f"job_{self.build}.tar.xz")

    def test_one_that_never_settles_once_final(self):
        # (no finished.json: final GIVE_UP_DAYS after it was archived, never settled)
        self.srv.httpd.routes.pop(self.base + "/finished.json")
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertNotIn("artifacts_asset", self.meta())
        meta = dict(self.meta(), archived_at=time.time() - 15 * DAY)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual((self.meta()["settled"], self.meta()["final"]), (False, True))
        self.assertIn("name", self.meta()["artifacts_asset"])

    def test_a_member_gcs_no_longer_has_loses_a_pending_one(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        path = f"artifacts/{HOST}/serial-1.log"
        self.srv.httpd.routes.pop(f"{self.base}/{path}")
        self.assets = tempfile.mkdtemp()  # (each run's: on GitHub's runner, in $RUNNER_TEMP)
        self.addCleanup(shutil.rmtree, self.assets)
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn(f"GCS no longer has {self.srv.url}{self.base}/{path}", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.meta()["artifacts_asset"]["lost"], path)
        self.assertEqual(self.manifest()["assets"], [])
        self.assertEqual(sorted(os.listdir(self.assets)), ["manifest.json"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (not tried again)

    def test_a_listing_gcs_changed_since_is_an_error_of_this_run_only(self):
        listed = frl.list_objects

        def stale(gcs, bucket, folder):
            return dict(listed(gcs, bucket, folder), **{f"{folder}{HOST}/gone.log": (1, False, None)})
        with mock.patch.object(frl, "list_objects", side_effect=stale):
            self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
        self.assertIn("packing its artifacts/ failed: NotFound", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.meta()["artifacts_asset"], {"failing_since": self.runs()[-1]["started"]})
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(len(self.meta()["artifacts_asset"]["members"]), len(self.FILES))
        self.assertNotIn("failing_since", self.meta()["artifacts_asset"])

    def test_what_is_not_gcss_doing_starts_no_failures(self):
        self.assertEqual(self.fetch(), 0)
        ctx = frl.RunContext(frl.AdaptiveLimit(8, 8), time.monotonic() + 60)
        self.addCleanup(frl.bind_context, frl.context())
        frl.bind_context(ctx)
        for error in (frl.LocalIOError("disk full"), TypeError("a bug"), frl.Abandoned("stopped"), "deadline"):
            if error == "deadline":  # (a pack stopped at the run's deadline: see still_packing)
                ctx.deadline, error = time.monotonic() - 1, TimeoutError("run timeout reached while packing")
            with self.subTest(error), mock.patch.object(frl, "pack_artifacts", side_effect=error), \
                    self.assertRaises(type(error)):
                frl.take_asset(self.srv.url, self.bdir(), self.meta(), self.assets, "run", time.time())  # fails: no take_asset
            self.assertNotIn("artifacts_asset", self.meta())

    def test_one_gcs_keeps_serving_wrong_is_given_up_on(self):
        self.srv.route(f"{self.base}/artifacts/{HOST}/kubelet.log", gcs_object(b"other", md5=md5_b64(b"kubelet")))
        self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
        self.assertIn("packing its artifacts/ failed: IntegrityError", self.runs()[-1]["errors"][0]["error"])
        since = self.runs()[-1]["started"]
        self.assertEqual(self.fetch("--assets", self.assets), 1)  # (an error of each run, for GIVE_UP_DAYS)
        self.assertEqual(self.meta()["artifacts_asset"], {"failing_since": since})
        frl.write_json(os.path.join(self.bdir(), "meta.json"),
                       dict(self.meta(), artifacts_asset={"failing_since": since - frl.GIVE_UP_DAYS * DAY - 1}))
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn(f"failed for {frl.GIVE_UP_DAYS} days, given up: IntegrityError", self.runs()[-1]["errors"][0]["error"])
        self.assertIn("IntegrityError", self.meta()["artifacts_asset"]["given_up"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (said once)
        self.assertEqual(self.manifest()["assets"], [])

    def test_a_pending_one_keeps_its_record_as_it_fails(self):
        # (a given-up rebuild's bytes and sha256 still say which asset GitHub holds: see record_uploads)
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        pending = self.meta()["artifacts_asset"]
        self.srv.route(f"{self.base}/artifacts/{HOST}/kubelet.log", gcs_object(b"other", md5=md5_b64(b"kubelet")))
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertEqual(self.meta()["artifacts_asset"], dict(pending, failing_since=self.runs()[-1]["started"]))
        since = self.runs()[-1]["started"] - frl.GIVE_UP_DAYS * DAY - 1
        frl.write_json(os.path.join(self.bdir(), "meta.json"),
                       dict(self.meta(), artifacts_asset=dict(pending, failing_since=since)))
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        record = self.meta()["artifacts_asset"]
        self.assertIn("IntegrityError", record.pop("given_up"))
        self.assertEqual(record, dict(pending, failing_since=since))

    def test_a_listing_that_keeps_failing_is_given_up_on(self):
        self.assertEqual(self.fetch(), 0)
        with mock.patch.object(frl, "list_objects", side_effect=frl.IntegrityError("an unexpected listing")):
            self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
            self.assertIn("packing its artifacts/ failed: IntegrityError", self.runs()[-1]["errors"][0]["error"])
            self.assertEqual(self.meta()["artifacts_asset"], {"failing_since": self.runs()[-1]["started"]})
            frl.write_json(os.path.join(self.bdir(), "meta.json"),
                           dict(self.meta(), artifacts_asset={"failing_since": time.time() + 2 * DAY}))
            self.assertEqual(self.fetch("--assets", self.assets), 1)  # (one not plausible starts again)
            since = self.runs()[-1]["started"]
            self.assertEqual(self.meta()["artifacts_asset"], {"failing_since": since})
            frl.write_json(os.path.join(self.bdir(), "meta.json"),
                           dict(self.meta(), artifacts_asset={"failing_since": since - frl.GIVE_UP_DAYS * DAY - 1}))
            self.assertEqual(self.fetch("--assets", self.assets), 1)
            self.assertIn(f"failed for {frl.GIVE_UP_DAYS} days, given up", self.runs()[-1]["errors"][0]["error"])
            self.assertEqual(self.fetch("--assets", self.assets), 0)

    def test_a_name_it_refuses_is_reported_once(self):
        self.srv.route(f"{self.base}/artifacts/{HOST}//kubelet.log", gcs_object(b"x\n"))
        self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
        self.assertIn("not a plain name below artifacts/: never packed", self.runs()[-1]["errors"][0]["error"])
        self.assertIn(f"{HOST}//kubelet.log", self.meta()["artifacts_asset"]["refused"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["assets"], [])

    def test_one_whose_files_reach_the_cap_once_unpacked(self):
        path = f"artifacts/{HOST}/kubelet.log"
        self.srv.route(f"{self.base}/{path}", gzip_stored(self.FILES[path]))
        name = f"{self.base.removeprefix('/bucket/')}/{path}"
        self.srv.gcs_listing("bucket", [name], sizes={name: 30})  # (GCS lists the bytes it stores, fewer than it holds)
        files = sum(map(len, self.FILES.values()))
        with mock.patch.object(frl, "ASSET_FILES_BYTES", files):
            self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
            self.assertIn(f"its artifacts/ hold {files} bytes of files or more", self.runs()[-1]["errors"][0]["error"])
            self.assertEqual(self.meta()["artifacts_asset"], {"too_large": files})
            self.assertEqual(self.fetch("--assets", self.assets), 0)

    def test_one_too_large_is_reported_once(self):
        files = sum(map(len, self.FILES.values()))
        with mock.patch.object(frl, "ASSET_FILES_BYTES", files):
            self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 2 (no --assets)
            self.assertIn(f"its artifacts/ hold {files} bytes", self.runs()[-1]["errors"][0]["error"])
            self.assertEqual(self.meta()["artifacts_asset"], {"too_large": files})
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        with mock.patch.object(frl, "ASSET_FILES_BYTES", files + 1), mock.patch.object(frl, "MAX_ASSET_BYTES", 1):
            self.assertEqual(self.fetch("--assets", self.assets), 0)  # (recorded: never packed)
            self.assertEqual(self.manifest()["assets"], [])
        meta = self.meta()
        del meta["artifacts_asset"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (pending)
        pending = self.meta()["artifacts_asset"]
        self.assets = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.assets)
        with mock.patch.object(frl, "MAX_ASSET_BYTES", 1):  # (packed again: an asset too large to upload)
            self.assertEqual(self.fetch("--assets", self.assets), 1)
            self.assertEqual(self.meta()["artifacts_asset"], dict(pending, too_large=files))
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(sorted(os.listdir(self.assets)), ["manifest.json"])

    def test_one_of_too_many_files_is_reported_once(self):
        files, n = sum(map(len, self.FILES.values())), len(self.FILES)
        with mock.patch.object(frl, "MAX_ASSET_MEMBERS", n - 1):  # fails: no MAX_ASSET_MEMBERS
            self.assertEqual(self.fetch("--assets", self.assets), 1)
            self.assertIn(f"its artifacts/ hold {n} files, a run packs at most {n - 1}",
                          self.runs()[-1]["errors"][0]["error"])
            self.assertEqual(self.meta()["artifacts_asset"], {"too_large": files})
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        meta = self.meta()
        del meta["artifacts_asset"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        with mock.patch.object(frl, "MAX_ASSET_MEMBERS", n):
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(len(self.meta()["artifacts_asset"]["members"]), n)

    def test_a_member_that_is_not_one(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        meta = self.meta()
        meta["artifacts_asset"]["members"][1] = "artifacts/x"
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn("IntegrityError", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.meta(), meta)
        meta["artifacts_asset"]["members"][1] = {"path": "artifacts/../x"}  # (one a pack refuses: said once)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        record = self.meta()["artifacts_asset"]
        self.assertIn("artifacts/../x", record.pop("refused"))
        self.assertEqual(record, meta["artifacts_asset"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)

    def test_the_runs_deadline_stops_the_packs(self):
        self.assertEqual(self.fetch(), 0)
        ctx, started, done = frl.RunContext(frl.AdaptiveLimit(8, 8)), [], frl.threading.Event()
        self.addCleanup(frl.bind_context, frl.context())
        frl.bind_context(ctx)

        def packing(*args):  # (as pack_artifacts stops at its next read, see still_packing, and its read may wait)
            started.append(args)
            ctx.abandoned.wait(60)
            done.wait(10)
            raise frl.Abandoned("packing")
        with mock.patch.object(frl, "take_asset", side_effect=packing):  # fails: no take_asset
            begun = time.monotonic()
            got = frl.take_assets(self.root, self.srv.url, self.assets, "run", time.time(), begun + 0.5)
            waited = time.monotonic() - begun
            done.set()
        self.assertEqual((got, len(started)), (([], {}, 1, 0, False), 1))
        self.assertTrue(ctx.abandoned.is_set())
        self.assertLess(waited, 5)  # (not waiting for the pack it stopped)

    def test_none_starts_past_the_runs_deadline(self):
        self.assertEqual(self.fetch(), 0)
        ctx = frl.RunContext(frl.AdaptiveLimit(8, 8))
        self.addCleanup(frl.bind_context, frl.context())
        frl.bind_context(ctx)
        with mock.patch.object(frl, "take_asset", return_value=({"members": []}, 0)) as take:  # fails: no take_asset
            got = frl.take_assets(self.root, self.srv.url, self.assets, "run", time.time(), time.monotonic() - 1)
        self.assertEqual((got, take.call_count, ctx.abandoned.is_set()), (([], {}, 1, 0, False), 0, False))

    def test_four_at_a_time_and_the_next_once_one_is_done(self):
        builds = [snowflake(self.start - 600 * i) for i in range(6)]  # (newest first: self.build is the first)
        self.show(builds, {"job.Overall": ([12] * 6, ["boom"] * 6)})
        for build in builds[1:]:
            self.node_e2e(f"/bucket/logs/job/{build}", {f"artifacts/{HOST}/kubelet.log": b"k\n"})
        self.assertEqual(self.fetch(), 0)
        lock, now, peak, newest, waited = threading.Lock(), [0], [0], threading.Event(), []

        def packing(gcs, target, meta, *rest):
            with lock:
                now[0] += 1
                peak[0] = max(peak[0], now[0])
            if meta["build"] == builds[0]:  # (packed last)
                newest.set()
            elif meta["build"] == builds[-1]:  # (packed first: done once the last has started)
                waited.append(newest.wait(5))
            else:
                time.sleep(0.2)
            with lock:
                now[0] -= 1
            return {"members": []}, 0
        with mock.patch.object(frl, "cpu_count", lambda: 8):
            with mock.patch.object(frl, "take_asset", side_effect=packing):  # fails: no take_asset
                got = frl.take_assets(self.root, self.srv.url, self.assets, "run", time.time(), time.monotonic() + 60)
        self.assertEqual((got, peak[0], waited), (([], {}, 0, 0, False), 4, [True]))

    def test_the_oldest_first_whatever_its_job(self):
        older = snowflake(self.start - 600)
        self.srv.summary("dash", {"t": "FAILING", "u": "FAILING"})
        self.srv.table("dash", "u", table("bucket/logs/zjob", [older], {"zjob.Overall": ([12], ["boom"])},
                                          start=(self.start - 600) * 1000))
        self.node_e2e(f"/bucket/logs/zjob/{older}", {f"artifacts/{HOST}/kubelet.log": b"older\n"})
        with mock.patch.object(frl, "ASSET_BYTES", 1), mock.patch.object(frl, "cpu_count", lambda: 1):  # fails: no ASSET_BYTES
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.runs()[-1]["assets_built"], [f"zjob_{older}.tar.xz"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (and listed so: one left pending is uploaded first)
        self.assertEqual([a["name"] for a in self.manifest()["assets"]], [f"zjob_{older}.tar.xz", f"job_{self.build}.tar.xz"])

    def test_a_cap_leaves_the_rest_for_later(self):
        older = snowflake(self.start - 600)
        base = f"/bucket/logs/job/{older}"
        self.show([self.build, older], {"job.Overall": ([12, 12], ["boom", "boom"])})
        self.node_e2e(base, {f"artifacts/{HOST}/kubelet.log": b"older\n"})
        with mock.patch.object(frl, "ASSET_BYTES", 1), mock.patch.object(frl, "cpu_count", lambda: 1):  # fails: no ASSET_BYTES
            self.assertEqual(self.fetch("--assets", self.assets), 0)  # (one at a time: the first goes past it)
        run = self.runs()[-1]
        self.assertEqual(run["assets_built"], [f"job_{older}.tar.xz"])
        self.assertEqual(run["left_for_later"], {"artifacts_assets": 1})
        self.assertNotIn("artifacts_asset", self.meta())
        with mock.patch.object(frl, "ASSET_SECONDS", 0):
            self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.runs()[-1]["left_for_later"], {"artifacts_assets": 2})
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(sorted(self.runs()[-1]["assets_built"]), [f"job_{older}.tar.xz", f"job_{self.build}.tar.xz"])

    def test_an_interrupt_while_packing_still_reports_the_run(self):
        wait = frl.cf.wait

        def interrupted(fs, timeout=None, return_when=frl.cf.ALL_COMPLETED):
            if next(f.function for f in inspect.stack() if f.filename == frl.__file__) == "take_assets":
                raise KeyboardInterrupt
            return wait(fs, timeout, return_when)
        with mock.patch.object(frl.cf, "wait", side_effect=interrupted):
            self.assertEqual(self.fetch("--assets", self.assets), 130)  # fails: 2 (no --assets)
        run = self.runs()[-1]
        self.assertTrue(run["interrupted"])
        self.assertEqual((run["left_for_later"], run["assets_built"]), ({"artifacts_assets": 1}, []))
        self.assertEqual(self.manifest()["assets"], [])

    def test_none_after_the_downloads_were_interrupted(self):
        self.assertEqual(self.fetch(), 0)
        wait = frl.cf.wait

        def interrupted(fs, timeout=None, return_when=frl.cf.ALL_COMPLETED):
            done = wait(fs, timeout, return_when)
            if next(f.function for f in inspect.stack() if f.filename == frl.__file__) == "run":  # (the downloads')
                raise KeyboardInterrupt  # (once they are all done: none is left to stop)
            return done
        with mock.patch.object(frl.cf, "wait", side_effect=interrupted):
            self.assertEqual(self.fetch("--assets", self.assets), 130)  # fails: 2 (no --assets)
        self.assertEqual(self.runs()[-1]["errors"], [])
        self.assertEqual(self.manifest()["assets"], [])
        self.assertEqual(self.runs()[-1]["left_for_later"], {"artifacts_assets": 1})  # (due all the same)

    def test_an_interrupt_while_it_reads_the_records_still_reports_the_run(self):
        with mock.patch.object(frl, "asset_due", side_effect=KeyboardInterrupt):  # fails: no asset_due
            self.assertEqual(self.fetch("--assets", self.assets), 130)
        self.assertTrue(self.runs()[-1]["interrupted"])
        self.assertTrue(os.path.exists(os.path.join(self.root, "state.json")))
        self.assertEqual(self.manifest()["assets"], [])

    def test_without_assets_nothing_is_packed(self):
        self.assertEqual(self.fetch(), 0)
        self.assertIs(self.meta()["node_e2e"], True)
        self.assertNotIn("artifacts_asset", self.meta())
        self.assertNotIn("assets_built", self.runs()[-1])
        self.assertEqual(os.listdir(self.assets), [])
        self.assertEqual(self.fetch("--list-only", "--assets", self.assets), 0)
        self.assertEqual(os.listdir(self.assets), [])

    def test_nor_for_a_build_that_is_not_node_e2e(self):
        self.srv.route(self.base + "/prowjob.json", (200, {}, prowjob(KUBETEST_GCE)))
        self.assertEqual(self.fetch(), 0)
        before = self.read("meta.json")
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # fails: 2 (no --assets)
        self.assertEqual(self.read("meta.json"), before)
        self.assertEqual(self.manifest()["assets"], [])

    def test_assets_in_the_archive_are_refused(self):
        self.assertEqual(self.fetch("--assets", os.path.join(self.root, "assets")), 1)  # fails: 2 (no --assets)
        self.assertFalse(os.path.exists(os.path.join(self.root, "assets")))


class UploadWorld(AssetWorld):
    """The workflow's steps on a red node e2e build: the fetch packs its artifacts/, the upload step uploads them to a
    fake GitHub, and --record-uploads records that in meta.json."""

    def setUp(self):
        super().setUp()
        self.gh = FakeGitHub()
        self.addCleanup(self.gh.close)
        for name in ("BACKOFF", "GAP"):
            patcher = mock.patch.object(ua, name, 0)
            patcher.start()
            self.addCleanup(patcher.stop)

    def upload(self):
        with mock.patch("sys.stdout"), mock.patch("sys.stderr"):
            return ua.main([self.assets, "--api", self.gh.url, "--uploads", self.gh.url], ENV)

    def record(self, path=None):
        with mock.patch("sys.stdout") as out, mock.patch("sys.stderr") as err:
            code = frl.main(["--archive", self.root, "--record-uploads",
                             path or os.path.join(self.assets, "uploaded.json")])  # fails: SystemExit 2
        self.warnings = "".join(c.args[0] for c in err.write.call_args_list)
        self.said = "".join(c.args[0] for c in out.write.call_args_list)
        return code

    def uploaded_json(self, uploaded):
        frl.write_json(os.path.join(self.assets, "uploaded.json"), {"run": "x", "uploaded": uploaded})

    def new_assets(self):
        self.assets = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.assets)

    def take_out(self):
        frl.write_json(os.path.join(self.root, "excluded.json"), {self.key: {"reason": "a log that must not stay public"}})

    def state(self):
        return frl.read_json(os.path.join(self.root, "state.json"))


class Uploads(UploadWorld):

    def test_fetch_upload_record(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        self.assertEqual(self.upload(), 0)
        tag = "node-logs-" + record["month"]
        self.assertEqual(self.gh.held(tag), {record["name"]: "sha256:" + record["sha256"]})
        self.assertEqual(self.record(), 0, self.warnings)
        url = f"https://github.com/{REPO}/releases/download/{tag}/{record['name']}"
        self.assertEqual(self.meta()["artifacts_asset"], dict(record, uploaded={"release": tag, "url": url}))
        self.assertEqual(self.warnings, "")
        before = self.read("meta.json")
        self.assertEqual(self.record(), 0)  # (again: nothing changes)
        self.assertEqual(self.read("meta.json"), before)
        self.assertEqual(self.said, "meta.json records 0 more uploaded assets\n")
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["assets"], [])  # (not packed again)
        self.assertEqual(self.check()[0], 0)

    def test_a_run_whose_record_was_lost_finds_it_uploaded(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.upload(), 0)
        self.new_assets()  # (the record step failed, or the push: the next run's folder is a new one)
        self.assertEqual(self.record(), 0)
        self.assertIn("no ", self.warnings)
        self.assertNotIn("uploaded", self.meta()["artifacts_asset"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (packs it again: the same bytes)
        self.assertEqual(self.upload(), 0)
        self.assertEqual(sum(1 for m, p, _ in self.gh.requests if m == "POST" and "/assets?" in p), 1)
        self.assertEqual(self.record(), 0)
        self.assertEqual(self.meta()["artifacts_asset"]["uploaded"]["release"], "node-logs-" + self.runs()[0]["run"][:7])

    def test_an_upload_that_is_not_the_records(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        meta = self.meta()
        record = meta["artifacts_asset"]
        tag = "node-logs-" + record["month"]
        good = {"release": tag, "size": record["bytes"], "digest": "sha256:" + record["sha256"], "url": "https://x/y"}
        for up in (dict(good, digest="sha256:" + "0" * 64), dict(good, size=1), dict(good, release="v1"),
                   dict(good, url=None), "x"):
            with self.subTest(up):
                frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
                self.uploaded_json({record["name"]: up})
                self.assertEqual(self.record(), 0)
                self.assertNotIn("uploaded", self.meta()["artifacts_asset"])
                self.assertIn(record["name"], self.warnings)
        self.uploaded_json({"job_1.tar.xz": good, "other_2.tar.xz": good, "x.tar.xz": good})
        self.assertEqual(self.record(), 0)
        self.assertEqual(self.warnings.count("no archived build's meta.json records it"), 3)
        self.uploaded_json({record["name"]: good})
        self.assertEqual(self.record(), 0, self.warnings)
        self.assertEqual(self.meta()["artifacts_asset"]["uploaded"], {"release": tag, "url": "https://x/y"})

    def test_one_a_cut_off_step_uploaded_is_recorded_from_releases_json(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        tag = "node-logs-" + record["month"]
        self.gh.release(tag, [("other_1.tar.xz", b"x")])  # (one no archived build records: no warning)
        self.assertEqual(self.upload(), 0)
        self.uploaded_json({})  # (cut off before it wrote that it uploaded it)
        self.assertEqual(self.record(), 0, self.warnings)
        url = f"https://github.com/{REPO}/releases/download/{tag}/{record['name']}"
        self.assertEqual(self.meta()["artifacts_asset"], dict(record, uploaded={"release": tag, "url": url}))
        self.assertEqual((self.said, self.warnings), ("meta.json records 1 more uploaded asset\n", ""))

    def test_a_lost_one_github_holds_whole_is_recorded(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.upload(), 0)
        self.new_assets()  # (its record was lost)
        self.srv.httpd.routes.pop(f"{self.base}/artifacts/{HOST}/serial-1.log")  # (GCS keeps a build's files 90 days)
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn("lost", self.meta()["artifacts_asset"])
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.record(), 0, self.warnings)
        self.assertEqual(self.meta()["artifacts_asset"]["uploaded"]["release"], "node-logs-" + self.runs()[0]["run"][:7])

    def test_one_github_says_no_digest_of_keeps_the_note(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.gh.hide_digest = 99
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.record(), 0, self.warnings)
        self.assertIn("this is the sha256 of the file sent", self.meta()["artifacts_asset"]["uploaded"]["note"])
        before = self.read("meta.json")
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["assets"], [])  # (not packed, and replaced, again)
        self.assertEqual((self.upload(), self.record()), (0, 0))  # (releases.json lists it with its digest now)
        self.assertEqual(self.read("meta.json"), before)

    def test_the_asset_of_a_build_taken_out_is_deleted(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        self.assertEqual((self.upload(), self.record()), (0, 0))
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertIsNone(self.bdir())
        self.assertEqual(self.state().get("assets_to_delete"), [record["name"]])  # fails: None
        self.assertEqual(self.manifest()["delete"], [record["name"]])
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.gh.held("node-logs-" + record["month"]), {})
        self.assertEqual(self.record(), 0, self.warnings)
        self.assertNotIn("assets_to_delete", self.state())
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["delete"], [record["name"]])  # (asked for while excluded.json lists it)

    def test_a_delete_that_failed_is_asked_for_again(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]  # (pending: a run whose commit was lost may have uploaded it)
        self.assertEqual(self.upload(), 0)
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.gh.delete_faults = [500] * ua.RETRIES * 2  # (tried in both listings)
        self.assertEqual(self.upload(), 1)  # fails: 0
        self.assertEqual(self.record(), 0)
        self.assertEqual(self.state()["assets_to_delete"], [record["name"]])
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["delete"], [record["name"]])
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.record(), 0)
        self.assertNotIn("assets_to_delete", self.state())

    def test_one_never_uploaded_is_asked_for_once(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (pending, and the upload step never ran)
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.record(), 0)
        self.assertNotIn("assets_to_delete", self.state())

    def test_a_malformed_name_to_delete_is_dropped(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        frl.write_json(os.path.join(self.root, "state.json"),
                       dict(self.state(), assets_to_delete=["job_1.tar.xz", "../x_1.tar.xz", 7]))
        # (a serious warning, as for any of state.json)
        self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: (kept) 0
        self.assertIn("state.json: dropped a malformed assets_to_delete record", self.runs()[-1]["warnings"])
        self.assertEqual(self.manifest()["delete"], ["job_1.tar.xz"])

    def test_names_kept_before_and_malformed_ones_as_one_is_taken_out(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        name = self.meta()["artifacts_asset"]["name"]
        frl.write_json(os.path.join(self.root, "state.json"),
                       dict(self.state(), assets_to_delete=[7, {}, "x", "job_1.tar.xz"]))
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 1)  # fails: 0
        self.assertIn("state.json: dropped a malformed assets_to_delete record", self.runs()[-1]["warnings"])
        self.assertEqual(self.state()["assets_to_delete"], sorted(["job_1.tar.xz", name]))
        self.assertEqual(self.manifest()["delete"], sorted(["job_1.tar.xz", name]))

    def test_the_name_is_kept_before_the_folder_goes(self):
        # (a run killed as it takes the build out: the next one still asks for the asset)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        frl.write_json(os.path.join(self.root, "state.json"), dict(self.state(), assets_to_delete=["job_1.tar.xz"]))
        self.take_out()
        with mock.patch.object(frl, "remove_build", side_effect=KeyboardInterrupt):
            self.assertEqual(self.fetch("--assets", self.assets), 130)
        self.assertEqual(self.state()["assets_to_delete"], sorted(["job_1.tar.xz", record["name"]]))  # fails: ['job_1.tar.xz']

    def test_deleted_though_the_fetch_stops_after_the_take_out(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        self.assertEqual((self.upload(), self.record()), (0, 0))
        tags, reason = self.gh.tags(), {"reason": "a log that must not stay public"}
        frl.write_json(os.path.join(self.root, "excluded.json"), {self.key: reason, "job/1": reason})  # (job/1: not archived)
        self.new_assets()
        with mock.patch.object(frl, "gcs_sevenths", side_effect=KeyboardInterrupt):
            self.assertEqual(self.fetch("--assets", self.assets), 130)
        deletes = sorted(["job_1.tar.xz", record["name"]])
        self.assertEqual(self.manifest(), {"run": None, "assets": [], "delete": deletes})  # fails: None
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.gh.held("node-logs-" + record["month"]), {})
        self.assertEqual(self.gh.tags(), tags)
        self.assertEqual(frl.read_json(os.path.join(self.assets, "uploaded.json")),
                         {"run": None, "uploaded": {}, "deleted": {record["name"]: "deleted", "job_1.tar.xz": "absent"}})
        self.assertEqual(self.record(), 0)
        self.assertNotIn("assets_to_delete", self.state())

    def test_a_record_step_drops_what_is_not_a_name_to_delete(self):
        frl.write_json(os.path.join(self.root, "state.json"), {"assets_to_delete": [{}, "job_1.tar.xz", "job_2.tar.xz"]})
        frl.write_json(os.path.join(self.assets, "uploaded.json"),
                       {"run": "x", "uploaded": {}, "deleted": {"job_1.tar.xz": "deleted"}})
        self.assertEqual(self.record(), 0)
        self.assertEqual(self.state()["assets_to_delete"], ["job_2.tar.xz"])  # fails: [{}, 'job_1.tar.xz', 'job_2.tar.xz']

    def test_one_whose_folder_stays_is_not_packed_again(self):
        # (its removal failed: the asset is deleted all the same, and the folder is not packed and uploaded again)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        name = self.meta()["artifacts_asset"]["name"]
        self.assertEqual((self.upload(), self.record()), (0, 0))
        meta = self.meta()
        del meta["artifacts_asset"]["uploaded"]  # (pending again, as one GCS changed is)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.take_out()
        with mock.patch.object(frl, "remove_build", side_effect=PermissionError("denied")):
            self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn("taking it out (excluded.json) failed", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual((self.manifest()["assets"], self.manifest()["delete"]), ([], [name]))  # fails: KeyError
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.gh.held("node-logs-" + meta["artifacts_asset"]["month"]), {})

    def test_one_uploaded_without_a_record_is_deleted_by_its_name(self):
        # (a run whose commit was lost uploaded it: no meta.json records it)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        self.assertEqual(self.upload(), 0)
        meta = self.meta()
        del meta["artifacts_asset"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["delete"], [record["name"]])  # fails: KeyError
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.gh.held("node-logs-" + record["month"]), {})

    def test_one_a_run_whose_push_failed_uploaded_is_deleted(self):
        # (taken out before a run archived the build again: neither its meta.json nor state.json names the asset)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        record = self.meta()["artifacts_asset"]
        self.assertEqual(self.upload(), 0)
        self.addCleanup(shutil.rmtree, self.root)
        self.root = tempfile.mkdtemp()
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.manifest()["delete"], [record["name"]])  # fails: KeyError: 'delete'
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.gh.held("node-logs-" + record["month"]), {})

    def test_a_record_step_without_uploads(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        before = self.read("meta.json")
        for text in ("{", "[]", '{"uploaded": []}'):
            with self.subTest(text):
                with open(os.path.join(self.assets, "uploaded.json"), "w") as f:
                    f.write(text)
                self.assertEqual(self.record(), 0)
                self.assertEqual(self.read("meta.json"), before)


class Pages(UploadWorld):
    """INDEX.md, the week's page and the run folder's README on a red node e2e build: where its artifacts/ are."""

    def pages(self):
        found = [os.path.join(self.root, "INDEX.md"), *glob.glob(os.path.join(self.root, "weeks", "*.md")),
                 os.path.join(self.bdir(), "..", "..", "README.md")]
        out = {}
        for path in found:
            with open(path, encoding="utf-8") as f:
                out[os.path.relpath(path, self.root)] = f.read()
        return out

    def row(self, text):
        return next(line for line in text.splitlines() if f"[job {self.build}](" in line)

    def test_its_row_says_where_they_are(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        before = self.pages()
        self.assertEqual(len(before), 3)
        for name, text in before.items():
            self.assertTrue(self.row(text).endswith(f" {frl.PENDING_UPLOAD} |"), name)  # fails: AttributeError
        self.assertIn("| … whose artifacts/ are pending upload | 1 |", before["INDEX.md"])
        self.assertEqual((self.upload(), self.record()), (0, 0))
        url = self.meta()["artifacts_asset"]["uploaded"]["url"]
        after = self.pages()
        for name, text in after.items():
            self.assertTrue(self.row(text).endswith(f") [artifacts/ as .tar.xz]({url}) |"), name)
        # (written again as the fetch wrote it, from meta.json as it is now)
        changed = [a for a, b in zip(before["INDEX.md"].splitlines(), after["INDEX.md"].splitlines(), strict=True)
                   if a != b and not a.startswith("Updated ")]
        self.assertEqual(changed, [self.row(before["INDEX.md"]), "| … with its artifacts/ in a release (a node e2e build) | 0 |",
                                   "| … whose artifacts/ are pending upload | 1 |"])
        self.assertIn("| … with its artifacts/ in a release (a node e2e build) | 1 |", after["INDEX.md"])

    def test_none_for_a_build_that_is_not_node_e2e(self):
        self.srv.route(self.base + "/prowjob.json", (200, {}, prowjob(KUBETEST_GCE)))
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        for name, text in self.pages().items():
            self.assertNotIn("artifacts/", self.row(text), name)
        self.assertIn("| … whose artifacts/ are pending upload | 0 |", self.pages()["INDEX.md"])  # fails: not there

    def test_nor_a_link_of_a_url_a_link_cannot_hold(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        meta = self.meta()
        meta["artifacts_asset"]["uploaded"] = {"release": "node-logs-2026-10", "url": "https://x.example/a)b"}
        self.assertIsNone(frl.asset_entry(meta))  # fails: no asset_entry
        meta["artifacts_asset"]["uploaded"]["url"] = "https://x.example/a"
        self.assertEqual(frl.asset_entry(meta), {"url": "https://x.example/a"})

    def test_the_pages_of_another_run_are_left_as_they_are(self):
        # (the record step of a run whose fetch was cut off before it made its folder)
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.upload(), 0)
        before = self.pages()
        frl.write_json(os.path.join(self.assets, "uploaded.json"), dict(frl.read_json(
            os.path.join(self.assets, "uploaded.json")), run="2026-10-01T000000Z"))
        self.assertEqual(self.record(), 0)
        self.assertIn("uploaded", self.meta()["artifacts_asset"])
        self.assertEqual(self.pages(), before)

    def test_its_errors_are_said_once(self):
        # (an unreadable meta.json: the run's report has it already)
        self.assertEqual(self.fetch(), 0)
        os.makedirs(os.path.join(self.root, frl.run_dirs(self.root)[-1], "job", "1"))
        with open(os.path.join(self.root, frl.run_dirs(self.root)[-1], "job", "1", "meta.json"), "w") as f:
            f.write("{")
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        readme = os.path.join(self.root, frl.run_dirs(self.root)[-1], "README.md")

        def said():
            with open(readme, encoding="utf-8") as f:
                return [self.pages()["INDEX.md"].count("unreadable"), f.read().count("unreadable")]
        before = said()
        self.assertEqual((self.upload(), self.record()), (0, 0))
        self.assertIn("uploaded", self.meta()["artifacts_asset"])
        self.assertEqual(said(), before)

    def test_and_the_builds_not_archived_yet(self):
        other = snowflake(self.start - 3600)  # (red, and GCS fails to serve it)
        self.show([self.build, other], {"job.Overall": ([12, 12], ["boom", "boom"])})
        for name in ("finished.json", "build-log.txt", "prowjob.json"):
            self.srv.route(f"/bucket/logs/job/{other}/{name}", (500, {}, b""))
        with mock.patch.object(frl, "BACKOFF", 0):
            self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertIn(f"[job {other}](", self.pages()["INDEX.md"].split("## Not archived yet")[1])
        self.assertEqual((self.upload(), self.record()), (0, 0))
        self.assertIn(f"[job {other}](", self.pages()["INDEX.md"].split("## Not archived yet")[1])


class Verified(UploadWorld):
    """cross_check.py on the assets, as the workflow runs it after the record step: against what releases.json says
    the branch's releases hold, and each asset of the window's builds downloaded from its URL (on the fake GitHub)."""

    def setUp(self):
        super().setUp()
        self.gh.download = self.gh.url
        for patcher in (mock.patch.object(frl, "BACKOFF", 0),
                        mock.patch.object(cc, "ASSET_URLS", ("https://", self.gh.url), create=True)):  # (the fake's http://)
            patcher.start()
            self.addCleanup(patcher.stop)

    def verify(self, hours="200"):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url, "--hours", hours,
                            "--releases", os.path.join(self.assets, "releases.json")])  # fails: SystemExit 2
        self.out = out.getvalue()
        return code

    def uploaded_one(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual((self.upload(), self.record()), (0, 0))
        return self.meta()["artifacts_asset"]

    def held(self):
        record = self.meta()["artifacts_asset"]
        return next(a for assets in self.gh.assets.values() for a in assets.values() if a["name"] == record["name"])

    def edit_releases(self, tag, **changes):
        path = os.path.join(self.assets, "releases.json")
        held = frl.read_json(path)
        held["releases"][tag]["assets"][0].update(changes)
        frl.write_json(path, held)

    def test_an_uploaded_asset_is_checked_whole(self):
        self.uploaded_one()
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn("assets: 1 uploaded (1 of the window's downloaded and checked whole), 0 pending", self.out)
        self.assertEqual([p for _, p, _ in self.gh.requests if p.startswith("/blob/")], [f"/blob/{self.held()['id']}"])
        self.assertNotIn("no archived build's meta.json records it", self.out)
        before = time.strftime("%Y-%m-%dT%H%M%SZ", time.gmtime(time.time() - DAY))
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), artifacts_asset=dict(
            self.meta()["artifacts_asset"], built_by_run=before)))  # (nor packed in the window)
        with mock.patch.object(frl, "gcs_sevenths", return_value=set()):  # (nor compared with GCS out of its seventh)
            self.assertEqual(self.verify(hours="1"), 0, self.out)  # (one older than the window is not downloaded)
        self.assertIn("(0 of the window's downloaded", self.out)
        self.assertIn("the artifacts/ of 0 compared with GCS", self.out)

    def test_a_draft_is_a_problem(self):
        self.uploaded_one()
        path = os.path.join(self.assets, "releases.json")
        held = frl.read_json(path)
        tag = self.meta()["artifacts_asset"]["uploaded"]["release"]
        held["releases"][tag]["draft"] = True  # (as when its tag is deleted)
        frl.write_json(path, held)
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"{tag} is a draft", self.out)

    def test_bytes_that_are_not_its_asset(self):
        record = self.uploaded_one()
        data = self.gh.data[str(self.held()["id"])]
        for served, says in ((b"x" * len(data), "no asset of its members: LZMAError"),
                             (data + b"\0", "more bytes than meta.json records"),
                             (data[:-1], f"serves {record['bytes'] - 1} bytes")):
            with self.subTest(says):
                self.gh.data[str(self.held()["id"])] = served
                self.assertEqual(self.verify(), 1)
                self.assertIn(says, self.out)
        self.gh.data[str(self.held()["id"])] = data
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), artifacts_asset=dict(
            record, sha256="0" * 64)))  # (the bytes as packed, of another sha256 than its record's)
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"serves {record['bytes']} bytes of sha256:{record['sha256']}, not", self.out)

    def test_a_member_not_as_recorded(self):
        record = self.uploaded_one()
        meta = self.meta()
        for i, member in enumerate(record["members"]):
            with self.subTest(member["path"]):
                members = [dict(m, md5="0" * 32) if j == i else m for j, m in enumerate(record["members"])]
                frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=dict(record, members=members)))
                self.assertEqual(self.verify(), 1)
                self.assertIn(f"has md5 {member['md5']}, not the {'0' * 32} meta.json records", self.out)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=dict(
            record, members=record["members"][:-1])))
        self.assertEqual(self.verify(), 1)
        self.assertIn("is not the member meta.json records next", self.out)
        renamed = [dict(record["members"][0], path=record["members"][0]["path"] + ".old")] + record["members"][1:]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=dict(record, members=renamed)))
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"its {record['members'][0]['path'].removeprefix('artifacts/')!r} of", self.out)
        extra = dict(record["members"][0], path="artifacts/zz.log")
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=dict(
            record, members=record["members"] + [extra])))
        self.assertEqual(self.verify(), 1)
        self.assertIn("it lacks artifacts/zz.log", self.out)

    def test_one_its_release_no_longer_holds(self):
        record = self.uploaded_one()
        for assets in self.gh.assets.values():
            assets.pop(record["name"], None)
        self.assertEqual((self.fetch("--assets", self.assets), self.upload()), (0, 0))  # (nothing to upload)
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"node-logs-{record['month']} does not hold its asset {record['name']}", self.out)
        self.assertIn("HTTP Error 404", self.out)
        self.assertEqual(sum(1 for _, p, _ in self.gh.requests if "/releases/download/" in p), 1)  # (a 404 is not tried again)

    def test_one_github_holds_otherwise(self):
        record = self.uploaded_one()
        for key, value in (("digest", "sha256:" + "0" * 64), ("size", 1), ("state", "starter")):
            with self.subTest(key):
                self.edit_releases(f"node-logs-{record['month']}", **{key: value})
                self.assertEqual(self.verify(), 1)
                self.assertIn(f"holds {record['name']} of ", self.out)
                self.assertEqual((self.upload(), self.record()), (0, 0))  # (as it was)
        self.edit_releases(f"node-logs-{record['month']}", digest=None)  # (GitHub's digest is "string or null")
        self.assertEqual(self.verify(), 0, self.out)

    def test_what_gcs_changed_since_is_only_noted(self):
        self.uploaded_one()
        self.srv.route(f"{self.base}/artifacts/{HOST}/kubelet.log", gcs_object(b"uploaded again\n"))
        self.srv.route(f"{self.base}/artifacts/{HOST}/late.log", gcs_object(b"late\n"))
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn(f"artifacts/{HOST}/kubelet.log: GCS changed since it was archived", self.out)
        self.assertIn(f"artifacts/{HOST}/late.log: GCS has it, its asset does not (only noted", self.out)

    def test_a_pending_one_is_noted_then_a_problem(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (the upload step never ran)
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn(f"{self.key}: its node-log asset is pending upload", self.out)
        self.assertIn("releases.json: the upload step failed", self.out)
        meta = self.meta()
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, archived_at=time.time() - 15 * DAY))
        self.assertEqual(self.verify(), 0, self.out)  # (packed now)
        meta["artifacts_asset"]["pending_since"] = time.time() - 15 * DAY
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, archived_at=time.time() - 15 * DAY))
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"{self.key}: its node-log asset is still pending 15 days after its build was archived", self.out)

    def test_a_pending_one_that_fails_counts_from_its_first_pack(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        meta = self.meta()
        meta["artifacts_asset"].update(pending_since=time.time() - 15 * DAY, failing_since=time.time() - DAY)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, archived_at=time.time() - 15 * DAY))
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"{self.key}: its node-log asset is still pending 15 days after its build was archived", self.out)

    def test_one_whose_first_pack_fails_counts_from_that_try(self):
        self.assertEqual(self.fetch(), 0)  # (backfilled: packed long after it was archived)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 20 * DAY))
        self.srv.route(f"{self.base}/artifacts/{HOST}/serial-1.log", gcs_object(b"booting\n", md5=md5_b64(b"other")))
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertEqual(list(self.meta()["artifacts_asset"]), ["failing_since"])
        self.assertEqual(self.upload(), 0)
        self.assertEqual(self.verify(), 1)  # (the run's error)
        self.assertIn(f"{self.key}: its node-log asset is pending upload", self.out)

    def test_one_no_run_packs_is_noted(self):
        meta = self.meta() if self.fetch("--assets", self.assets) == 0 else None
        for key, value in (("lost", f"artifacts/{HOST}/serial-1.log"), ("too_large", 1 << 31), ("refused", "x"),
                           ("given_up", "y")):
            with self.subTest(key):
                frl.write_json(os.path.join(self.bdir(), "meta.json"),
                               dict(meta, archived_at=time.time() - 30 * DAY, artifacts_asset={key: value}))
                self.assertEqual(self.verify(), 0, self.out)
                self.assertIn(f"{self.key}: its artifacts/ are not kept ({key}: {value})", self.out)

    def test_a_lost_one_github_holds_whole_is_checked_as_uploaded(self):
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.assertEqual(self.upload(), 0)
        self.new_assets()  # (its record was lost)
        self.srv.httpd.routes.pop(f"{self.base}/artifacts/{HOST}/serial-1.log")
        self.assertEqual(self.fetch("--assets", self.assets), 1)
        self.assertEqual((self.upload(), self.record()), (0, 0))
        self.assertEqual(sorted(self.meta()["artifacts_asset"]), ["built_by_run", "bytes", "lost", "members", "month",
                                                                  "name", "pending_since", "sha256", "uploaded"])
        self.assertEqual(self.fetch("--assets", self.assets), 0)  # (a run without the error of the lost one)
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn("assets: 1 uploaded (1 of the window's downloaded and checked whole), 0 pending upload, 0 not kept",
                      self.out)

    def test_a_build_taken_out_whose_asset_a_release_still_holds(self):
        record = self.uploaded_one()
        self.take_out()
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        self.gh.delete_faults = [500] * ua.RETRIES * 2
        self.assertEqual(self.upload(), 1)
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"{record['name']}: of a build taken out (excluded.json), but node-logs-{record['month']} still "
                      "holds it", self.out)

    def test_an_asset_no_record_names_is_noted(self):
        self.gh.release("node-logs-" + time.strftime("%Y-%m", time.gmtime()), [("other_1.tar.xz", b"x")])
        self.uploaded_one()
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn("other_1.tar.xz: node-logs-", self.out)
        self.assertIn("holds it, but no archived build's meta.json records it", self.out)

    def test_another_branchs_records_are_not_compared_with_its_releases(self):
        record = self.uploaded_one()
        for assets in self.gh.assets.values():
            assets.pop(record["name"], None)  # (gone from the default branch's release: not what this branch checks)
        self.gh.release(ua.branch_prefix("test", "main") + record["month"], prerelease=True)
        with mock.patch("sys.stdout"), mock.patch("sys.stderr"):
            self.assertEqual(ua.main([self.assets, "--api", self.gh.url, "--uploads", self.gh.url],
                                     dict(ENV, GITHUB_REF_NAME="test")), 0)
        self.assertEqual(self.verify(), 1)
        self.assertNotIn("does not hold its asset", self.out)
        self.assertIn("HTTP Error 404", self.out)  # (downloaded all the same)

    def test_a_releases_json_not_the_upload_steps(self):
        self.uploaded_one()
        for value in ([], {"releases": {}}, {"prefix": "node-logs-", "releases": []}, "{"):
            with self.subTest(value):
                with open(os.path.join(self.assets, "releases.json"), "w") as f:
                    f.write(value if isinstance(value, str) else json.dumps(value))
                self.assertEqual(self.verify(), 1)
                self.assertIn("releases.json is not the upload step's list of the releases", self.out)

    def test_one_packed_in_the_window_is_downloaded(self):
        # (a backfilled build's, or one uploaded late: its build started before the window)
        record = self.uploaded_one()
        self.gh.data[str(self.held()["id"])] = b"x" * record["bytes"]
        self.assertEqual(self.verify(hours="48"), 1)
        self.assertIn("(1 of the window's downloaded", self.out)
        self.assertIn("serves no asset of its members", self.out)

    def test_a_failed_download_is_tried_again(self):
        self.uploaded_one()
        urlopen, data = cc.urllib.request.urlopen, self.gh.data[str(self.held()["id"])]

        class Cut(io.BytesIO):
            length = 1  # (http.client: the bytes of its Content-Length not read yet)
        failed = [Cut(data[:len(data) // 2]), cc.urllib.error.HTTPError("x", 503, "Service Unavailable", {}, None)]

        def opening(req, *args, **kwargs):
            if "/releases/download/" in req.full_url and failed:
                if isinstance(failed[-1], Exception):
                    raise failed.pop()
                return failed.pop()
            return urlopen(req, *args, **kwargs)
        with mock.patch.object(cc.urllib.request, "urlopen", side_effect=opening):
            self.assertEqual(self.verify(), 0, self.out)
        self.assertEqual(failed, [])

    def test_a_tar_of_more_bytes_than_its_members_take(self):
        # (a PAX header of any size, which tarfile reads whole)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:xz", format=tarfile.PAX_FORMAT) as tar:
            entry = tarfile.TarInfo("a")
            entry.size, entry.pax_headers = 1, {"comment": "x" * 100_000}
            tar.addfile(entry, io.BytesIO(b"x"))
        data = buf.getvalue()
        record = {"members": [{"path": "artifacts/a", "bytes": 1, "md5": hashlib.md5(b"x").hexdigest()}]}
        with self.assertRaisesRegex(ValueError, "more bytes than its members take"):
            cc.asset_members(cc.Hashed(io.BytesIO(data), len(data)), record)  # fails: no asset_members

    def test_but_one_of_what_its_members_take_is_read_whole(self):
        # (one small member: the end of the tar; many of long names: each one's PAX header)
        for members in ([("a", b"x")], [(f"{i:03}" + "n" * 150, b"x") for i in range(30)]):
            with self.subTest(len(members)):
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w:xz", format=tarfile.PAX_FORMAT) as tar:
                    for name, data in members:
                        entry = tarfile.TarInfo(name)
                        entry.size = len(data)
                        tar.addfile(entry, io.BytesIO(data))
                record = {"members": [{"path": "artifacts/" + name, "bytes": len(data), "md5": hashlib.md5(data).hexdigest()}
                                      for name, data in members]}
                got = cc.Hashed(io.BytesIO(buf.getvalue()), len(buf.getvalue()))  # fails: no Hashed
                self.assertEqual(cc.asset_members(got, record), [])
                self.assertEqual(got.left, 0)

    def test_a_failed_comparison_of_its_members_with_gcs_is_noted(self):
        # (the fetch never compares them, so never gives up on one GCS keeps refusing)
        self.uploaded_one()
        listing = frl.list_objects
        for error in (frl.IntegrityError("GCS answers 503 (fake)"),
                      urllib.error.HTTPError("https://gcs", 403, "Forbidden (fake)", {}, None)):
            with self.subTest(error):
                def failing(gcs, bucket, prefix, *args, error=error, **kwargs):
                    if prefix.endswith("/artifacts/"):
                        raise error
                    return listing(gcs, bucket, prefix, *args, **kwargs)
                with mock.patch.object(frl, "list_objects", side_effect=failing):
                    self.assertEqual(self.verify(), 0, self.out)
                self.assertIn(f"{self.key}: cannot compare the members of its asset with GCS: {type(error).__name__}",
                              self.out)
        m, member = self.meta(), self.meta()["artifacts_asset"]["members"][0]
        path = urllib.parse.urlsplit(frl.node_log_url(m["log_url"].rsplit("/", 1)[0], member["path"])).path
        served = self.srv.httpd.routes.get(path)
        for status in (403, 503):  # (as GCS answers a member's HEAD)
            with self.subTest(status):
                self.srv.route(path, (status, {}, b""))
                self.assertEqual(self.verify(), 0, self.out)
                self.assertIn(f"{self.key}: cannot compare the members of its asset with GCS: HTTPError", self.out)
                self.assertGreater(self.srv.hits(path), 0)
        record = m["artifacts_asset"]
        self.edit_releases(record["uploaded"]["release"], digest="sha256:" + "0" * 64)
        self.assertEqual(self.verify(), 1)  # (what the release holds is still checked)
        self.assertIn(f"holds {record['name']} of {record['bytes']} bytes, sha256:{'0' * 64}", self.out)
        self.edit_releases(record["uploaded"]["release"], digest="sha256:" + record["sha256"])
        self.srv.route(path, served)
        self.assertEqual(self.verify(), 0, self.out)
        with mock.patch.object(frl, "list_objects", side_effect=KeyError("path")):  # (not GCS's doing)
            self.assertEqual(self.verify(), 1)
        self.assertIn(f"{self.key}: cannot compare the members of its asset with GCS: KeyError", self.out)

    def test_one_archived_long_before_it_was_first_packed(self):
        # (a backfilled build: GIVE_UP_DAYS count from its first pack)
        self.assertEqual(self.fetch(), 0)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 20 * DAY))
        self.assertEqual(self.fetch("--assets", self.assets), 0)
        with mock.patch.object(ua, "put", side_effect=ua.Later("left for the next run")):
            self.assertEqual(self.upload(), 0)
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn(f"{self.key}: its node-log asset is pending upload", self.out)
        meta = self.meta()
        del meta["archived_at"], meta["artifacts_asset"]["pending_since"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, created=int(time.time() - 20 * DAY)))
        self.assertEqual(self.verify(), 1)  # (without those, from when the build was made)
        self.assertIn(f"{self.key}: its node-log asset is still pending 20 days after", self.out)

    def test_one_not_packed_yet_is_noted(self):
        self.assertEqual(self.fetch(), 0)  # (a run that packed none: the next packs it)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), archived_at=time.time() - 30 * DAY))
        self.assertEqual(self.verify(), 0, self.out)
        self.assertIn(f"{self.key}: its artifacts/ are not packed yet", self.out)
        self.assertIn("0 uploaded (0 of the window's downloaded and checked whole), 1 pending upload", self.out)

    def test_one_that_is_not_where_github_has_it(self):
        record = self.uploaded_one()
        meta = self.meta()
        meta["artifacts_asset"]["uploaded"]["url"] = "file:///etc/hostname"
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"{self.key} meta.json: its asset's \"uploaded\" does not say where it is", self.out)
        self.assertIn("(0 of the window's downloaded", self.out)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=record))
        self.edit_releases(f"node-logs-{record['month']}", url="https://example.com/x")
        self.assertEqual(self.verify(), 1)
        self.assertIn(f"serves {record['name']} at https://example.com/x, not at the URL meta.json records", self.out)

    def test_a_record_that_is_not_one(self):
        self.uploaded_one()
        meta = self.meta()
        for value in ([], {"members": "x"}, {"name": "x", "members": [{"path": 1}]}):
            with self.subTest(value):
                frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts_asset=value))
                self.assertEqual(self.verify(), 1)
                self.assertIn(f"{self.key} meta.json: its artifacts_asset is not an asset's record", self.out)


if __name__ == "__main__":
    unittest.main()
