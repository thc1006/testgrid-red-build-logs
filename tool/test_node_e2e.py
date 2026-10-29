"""The red builds of node e2e jobs, told by their prowjob.json (see fetch_red_logs.node_e2e_job).

Run: cd tool && python -m unittest test_node_e2e -v
"""
import hashlib
import io
import math
import os
import shutil
import sys
import tarfile
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import FakeServer, gcs_object, gzip_stored, md5_b64  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj  # noqa: E402

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

    def test_but_not_for_a_final_build(self):
        # (one archived before the runs read it: taken in a later change, not one of every final build at once)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.route(prowjob(KUBETEST_NODE))
        before = self.read("meta.json")
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("meta.json"), before)

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


if __name__ == "__main__":
    unittest.main()
