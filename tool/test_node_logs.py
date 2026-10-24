"""The logs of its cluster's nodes a build folder can store too, after build-log.txt and podinfo.json (see
fetch_red_logs.NODE_LOGS): <node>.<file> in the folder, listed in meta.json's "artifacts" with their path in GCS.

Run: cd tool && python -m unittest test_node_logs -v
"""
import glob
import gzip
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import gcs_object, send  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj  # noqa: E402

MASTER = "artifacts/e2e-master/kube-master-installation.log"
NODE = "artifacts/cluster-logs/kt2-a1-minion-group-ab12/kube-node-installation.log"


def name_of(path):
    """<node>.<file>: the name in a build folder of the node log GCS has at path."""
    return ".".join(path.split("/")[-2:])


def add_node_logs(world, logs, compress=True, max_bytes=None):
    """List logs ({path in the build's folder in GCS: content}) in the meta.json of the build world archived,
    each taken from GCS into its folder as a run takes a file (see frl.download, frl.put_in_place)."""
    folder, meta = world.bdir(), world.meta()
    copies, records = [], []
    for path, data in sorted(logs.items()):
        world.srv.route(f"{world.base}/{path}", obj(data))
        copies.append(frl.download(f"{world.srv.url}{world.base}/{path}", os.path.join(folder, name_of(path)), compress,
                                   max_bytes))
        records.append(dict({"path": path}, **copies[-1].info))
    frl.put_in_place(folder, dict(meta, artifacts=records), copies)
    return records


class NodeLogsListed(ArchiveWorld):
    """A red build archived with its log, final, whose meta.json then lists logs of a control plane and a node."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.records = add_node_logs(self, {MASTER: b"installing\n", NODE: b"a node\n"})
        self.names = [name_of(MASTER) + ".gz", name_of(NODE) + ".gz"]

    def index(self):
        with open(os.path.join(self.root, "INDEX.md"), encoding="utf-8") as f:
            return f.read()

    def test_they_are_the_builds_files(self):
        # (the next run leaves them as they are, cross_check checks them, and the pages link and count them)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz"] + self.names + ["meta.json"])
        self.assertEqual(self.meta()["artifacts"], self.records)
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn("\n  inventory: 1 build: 1 with a log, 1 with control-plane or node logs\n", out)  # fails: not there
        index = self.index()
        self.assertIn("| … with control-plane or node logs (its cluster did not come up) | 1 |", index)
        for name in self.names:
            self.assertIn(f"[{name}](", index)
        with open(glob.glob(os.path.join(self.root, "weeks", "*.json"))[0], encoding="utf-8") as f:
            entry = json.load(f)[0]
        self.assertEqual((entry["node_logs"], entry["bytes"]), (True, len(b"log\ninstalling\na node\n")))

    def test_a_damaged_one_is_found(self):
        with open(os.path.join(self.bdir(), self.names[0]), "r+b") as f:  # (same size, other bytes)
            data = f.read()
            f.seek(0)
            f.write(data[:-9] + bytes([data[-9] ^ 1]) + data[-8:])
        self.assertFalse(frl.intact(self.bdir(), self.meta(), deep=True))  # fails: True
        code, out = self.check()
        self.assertEqual(code, 1)
        self.assertIn(f"{self.key} {self.names[0]}: cannot verify", out)
        os.remove(os.path.join(self.bdir(), self.names[1]))
        self.assertFalse(frl.intact(self.bdir(), self.meta()))

    def test_one_it_does_not_list_is_a_stray(self):
        stray = name_of(MASTER).replace("kube-master-installation", "etcd") + ".gz.000"
        with open(os.path.join(self.bdir(), stray), "wb") as f:
            f.write(b"x")
        code, out = self.check()
        self.assertEqual(code, 1)  # fails: 0
        self.assertIn(f"{self.key} {stray}: not listed in its meta.json", out)
        self.assertEqual(self.fetch("--gzip"), 0)  # (the cleanup at its start removes it)
        self.assertNotIn(stray, os.listdir(self.bdir()))
        self.assertEqual(self.meta()["artifacts"], self.records)

    def test_but_none_of_a_meta_json_that_does_not_list_them_in_a_list(self):
        # (one edited, or damaged as valid JSON: it cannot say which copies are its own, so the cleanup leaves them)
        for value in ({}, "x", 7):
            with self.subTest(value):
                frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), artifacts=value))
                self.assertEqual(frl.stray_files(self.bdir(), self.meta()), [])
                self.assertEqual(self.fetch("--gzip"), 0)
                self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz"] + self.names + ["meta.json"])
                self.assertEqual(self.runs()[-1]["warnings"], [])

    def test_but_cross_check_reports_such_a_meta_json(self):
        # (no check takes in the copies it leaves then)
        for value in ({}, "x", 7):
            with self.subTest(value):
                frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), artifacts=value))
                code, out = self.check()
                self.assertEqual(code, 1, out)  # fails: 0
                self.assertIn(f'PROBLEM: {self.key} meta.json: its "artifacts" is not a list\n', out)

    def test_a_record_whose_path_is_not_a_node_logs(self):
        # (another file, a folder no GCE instance has, or one out of the build's folder: none is a stored file)
        for path in ("artifacts/e2e-master/kube-proxy.log", "artifacts/E2E-master/kube-master-installation.log",
                     "artifacts/e2e.master/kube-master-installation.log", "../e2e-master/kube-master-installation.log",
                     "artifacts/e2e-master/x/kube-master-installation.log", None):
            with self.subTest(path):
                meta = self.meta()
                meta["artifacts"] = [dict(self.records[0], path=path), self.records[1]]
                self.assertFalse(frl.intact(self.bdir(), meta))  # fails: True
                self.assertEqual(frl.stray_files(self.bdir(), meta), [])  # (it cannot say which are its own)

    def test_one_in_parts(self):
        meta = self.meta()
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(meta, artifacts=[]))
        for name in self.names:
            os.remove(os.path.join(self.bdir(), name))
        data = os.urandom(300)  # (does not compress: 3 parts of 128 bytes or less)
        record, = add_node_logs(self, {MASTER: data}, max_bytes=128)
        self.assertEqual([p["file"] for p in record["parts"]], [f"{self.names[0]}.{i:03d}" for i in range(3)])
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn(", 1 with a file stored in parts,", out)  # fails: not there
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertIn(f"[{self.names[0]}.002](", self.index())


class NodeLogsWithoutALog(ArchiveWorld):
    """A red build archived without a log (GCS had none), whose meta.json then lists logs of a control plane and a
    node: it is neither one with only podinfo.json (its pod ran), nor one without a file."""

    def counted(self, podinfo):
        self.finished(ago=3 * DAY)
        if podinfo:
            self.srv.route(self.base + "/podinfo.json", obj(b"{}\n"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived-without-log": [self.key]})
        add_node_logs(self, {MASTER: b"installing\n", NODE: b"a node\n"})
        self.assertEqual(self.fetch("--gzip"), 0)
        with open(os.path.join(self.root, "INDEX.md"), encoding="utf-8") as f:
            index = f.read()
        for row, n in (("with only podinfo.json (the pod never ran)", 0), ("without a file (GCS had none)", 0),
                       ("with control-plane or node logs (its cluster did not come up)", 1)):
            self.assertIn(f"| … {row} | {n} |", index)  # fails: | 1 | (one with only podinfo.json, or without a file)
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn("\n  inventory: 1 build: 1 with control-plane or node logs, 1 still compared with GCS on every run\n",
                      out)

    def test_with_podinfo(self):
        self.counted(podinfo=True)

    def test_without_it(self):
        self.counted(podinfo=False)


class NodeLogNames(unittest.TestCase):
    def test_what_is_a_copy_of_a_node_log(self):
        for n, of in [("e2e-master.etcd.log", "e2e-master.etcd.log"), ("e2e-master.etcd.log.gz", "e2e-master.etcd.log"),
                      ("e2e-master.etcd.log.gz.007", "e2e-master.etcd.log"), ("e2e-master.etcd.log.000", "e2e-master.etcd.log"),
                      ("a.serial-1.log", "a.serial-1.log"), ("x" * 63 + ".etcd.log", "x" * 63 + ".etcd.log"),
                      ("build-log.txt.gz.001", "build-log.txt"), ("e2e-master.kube-proxy.log", None),
                      ("E2E-master.etcd.log", None), ("e2e-.etcd.log", None), ("x" * 64 + ".etcd.log", None),
                      ("e2e-master.etcd.log.gz.0001", None), ("e2e-master.etcd.log.000.gz", None), (".e2e.etcd.log", None),
                      ("e2e-master.etcd.log.part", None), ("cluster-logs.etcd.log", None)]:
            with self.subTest(n):
                self.assertEqual(frl.copy_of(n), of)  # fails: None

    def test_its_name_by_its_path(self):
        self.assertEqual(frl.node_log_name(MASTER), name_of(MASTER))  # fails: AttributeError: no node_log_name
        self.assertEqual(frl.node_log_name(NODE), name_of(NODE))
        for path in ("artifacts/cluster-logs/kube-master-installation.log", "artifacts/e2e-master/kube-proxy.log",
                     "artifacts/a/b/etcd.log", "artifacts/cluster-logs/cluster-logs/etcd.log", "e2e-master/etcd.log",
                     "artifacts/e2e-master/etcd.log/", 7):
            self.assertIsNone(frl.node_log_name(path), path)

    def test_the_bytes_a_run_took_of_them(self):
        def record(n):
            return {"bytes": n, "md5": "x", "file": "f"}
        meta = {"log": record(1), "podinfo": record(2), "artifacts": [dict(record(4), path=MASTER), dict(record(8), path=NODE)],
                "refreshed_by_run": "r", "refreshed": [name_of(MASTER)]}
        self.assertEqual(frl.bytes_taken("archived", meta, "r"), 15)  # fails: 3
        self.assertEqual(frl.bytes_taken("refreshed", meta, "r"), 4)
        self.assertEqual(frl.bytes_taken("repaired", dict(meta, repaired=["build-log.txt", name_of(NODE)]), "x"), 9)


INIT_FAILED = b"\x1b[0;31mCluster failed to initialize within 300 seconds.\x1b[0m\n"  # (as kube-up writes it)
LISTING = "/storage/v1/b/bucket/o?"


class LogSays(unittest.TestCase):
    """Whether a build's log says its cluster failed to initialize: its copy in the build folder, or one staged."""

    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.d)

    def copy(self, data, compress, max_bytes):
        """A copy of build-log.txt holding data, as a run stores it: (its record, its parts staged in temp files)."""
        final = os.path.join(self.d, "build-log.txt" + (".gz" if compress else ""))
        tmp = frl.temp_path(final)
        with open(tmp, "wb") as f:
            f.write(gzip.compress(data, mtime=0) if compress else data)
        temps, parts = frl.split_parts(tmp, final, max_bytes)
        info = {"file": os.path.basename(final), **({"parts": parts} if parts else {})}
        return info, frl.Staged("build-log.txt", info, temps)

    def test_whole_or_in_parts_gzipped_or_not_staged_or_in_place(self):
        data = os.urandom(140).hex().encode() + INIT_FAILED + b"log\n"  # (in parts of 100 bytes, across two)
        for compress in (False, True):
            for max_bytes in (None, 100):
                with self.subTest(compress=compress, max_bytes=max_bytes):
                    info, copy = self.copy(data, compress, max_bytes)
                    self.assertTrue(frl.log_says(self.d, info, copy))  # fails: AttributeError: no log_says
                    self.assertFalse(frl.log_says(self.d, info, copy, b"no such text"))
                    frl.put_in_place(self.d, {}, [copy])  # (as a run puts it in place)
                    self.assertTrue(frl.log_says(self.d, info))
                    self.assertFalse(frl.log_says(self.d, info, text=b"no such text"))

    def test_a_copy_that_does_not_gunzip_is_a_disk_error(self):
        # (one changed on disk since it was taken: not GCS's doing, nor a log that does not say so)
        with open(os.path.join(self.d, "build-log.txt.gz"), "wb") as f:
            f.write(gzip.compress(b"log\n", mtime=0) + b"damage")
        with self.assertRaises(frl.LocalIOError):
            frl.log_says(self.d, {"file": "build-log.txt.gz"})  # fails: AttributeError: no log_says

    def test_not_an_older_record_of_one_too_large_to_store(self):
        self.assertFalse(frl.log_says(self.d, {"omitted": True, "file": "build-log.txt"}))  # fails: AttributeError


class UpFailedWorld(ArchiveWorld):
    """A red build whose cluster did not come up (kubetest.Up red), finished 3 hours ago, with its log in GCS and,
    under artifacts/, what kube-up's log dump gave of its control plane (e2e-master) and of a node."""

    LOGS = {"e2e-master/kube-master-installation.log": b"installing\n", "e2e-master/kube-master-configuration.log":
            b"configuring\n", "e2e-master/etcd.log": b"etcd\n", "e2e-master/containerd.log": b"containerd\n",
            "e2e-master/serial-1.log": b"serial\n", "e2e-minion-group-ab12/kube-node-installation.log": b"node\n",
            "e2e-minion-group-ab12/kube-node-configuration.log": b"node configuring\n"}
    OTHERS = {"e2e-master/kube-apiserver.log": b"apiserver\n", "e2e-master/kube-scheduler.log": b"scheduler\n",
              "e2e-minion-group-ab12/containerd.log": b"a node's\n", "e2e-minion-group-ab12/serial-1.log": b"a node's\n",
              "e2e-master/etcd-events.log": b"events\n", "junit_runner.xml": b"<testsuite/>",
              "E2E-master/etcd.log": b"no GCE instance's\n", "e2e-minion-group-cd34/kube-node-installation.log": b""}

    def setUp(self):
        super().setUp()
        self.show([self.build], {"job.Overall": ([12], ["boom"]), "kubetest.Up": ([12], ["e2e-up.sh: exit status 2"])})
        self.finished(ago=3 * HOUR)
        self.log(b"log\n")
        for path, data in {**self.LOGS, **self.OTHERS}.items():
            self.put(path, data)

    def log(self, data):
        self.srv.route(self.base + "/build-log.txt", obj(data))

    def put(self, path, data):
        self.srv.route(f"{self.base}/artifacts/{path}", gcs_object(data))

    def listed(self, *paths):
        """List these too (their sizes as if GCS held 5 bytes of each), though GCS serves none of them."""
        names = [f"logs/job/{self.build}/artifacts/{p}" for p in paths]
        self.srv.gcs_listing("bucket", names, sizes=dict.fromkeys(names, 5))

    def refuse_listing(self):
        """GCS refuses a listing of the build's artifacts (and only that)."""
        listing = self.srv.httpd.prefix_routes[LISTING]

        def refuse(h):
            if "artifacts" in h.path:
                return send(h, 503, {}, b"")
            return listing(h)
        self.srv.httpd.prefix_routes[LISTING] = refuse

    def stored(self, name):
        return gzip.decompress(self.read(name + ".gz"))

    def files(self):
        """{name: content} of each file in the build's folder."""
        return {n: self.read(n) for n in os.listdir(self.bdir())}

    def names(self, paths):
        return sorted(name_of(p) for p in paths)

    def paths(self, paths):
        return sorted(f"artifacts/{p}" for p in paths)

    def bug(self, path=""):
        """A bug of this code as it takes a log of a node (that at artifacts/<path>, if path is given)."""
        real = frl.download

        def buggy(url, *args):
            if f"/artifacts/{path}" in url:
                raise TypeError("a bug")
            return real(url, *args)
        return mock.patch.object(frl, "download", buggy)


class UpFailed(UpFailedWorld):
    def test_its_nodes_logs_are_kept(self):
        self.assertEqual(self.fetch("--gzip"), 0)
        names = self.names(self.LOGS)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz"] + [n + ".gz" for n in names] + ["meta.json"])
        # fails: no node's
        for path, data in self.LOGS.items():
            self.assertEqual(self.stored(name_of(path)), data)
        m = self.meta()
        self.assertEqual([r["path"] for r in m["artifacts"]], self.paths(self.LOGS))
        self.assertEqual(list(m["artifacts"][0]), ["path", "bytes", "md5", "verified", "file", "file_bytes", "gcs"])
        self.assertEqual(list(m)[list(m).index("podinfo"):][:3], ["podinfo", "artifacts", "archived_by_run"])
        self.assertEqual((m["settled"], m["final"]), (True, False))  # (not final for 48 hours, as any build)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["errors"]), ({"archived": [self.key]}, []))
        self.assertEqual(run["bytes_downloaded"], len(b"log\n") + sum(len(d) for d in self.LOGS.values()))
        code, out = self.check()
        self.assertEqual(code, 0, out)
        self.assertIn("1 with control-plane or node logs", out)

    def test_the_apiserver_log_too_when_the_cluster_failed_to_initialize(self):
        # (as a log that says so, read in parts: a part ends in the middle of it)
        self.log(b"x" * 80 + INIT_FAILED + b"log\n")
        self.assertEqual(self.fetch("--max-file-mb", "0.0001"), 0)  # (104 bytes)
        self.assertGreater(len(self.meta()["log"]["parts"]), 1)
        self.assertEqual(self.read("e2e-master.kube-apiserver.log"), b"apiserver\n")  # fails: no such file
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]],
                         self.paths([*self.LOGS, "e2e-master/kube-apiserver.log"]))

    def test_control_not_for_a_build_whose_up_did_not_fail(self):
        # (its meta.json is as before: no "artifacts", and GCS is not asked)
        self.show([self.build], {"job.Overall": ([12], ["boom"]), "kubetest.Up": ([1], [""])})
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json"])
        self.assertNotIn("artifacts", self.meta())
        self.assertFalse([p for p in self.srv.httpd.hits if "artifacts" in p])

    def test_kubetest2s_folders(self):
        for path in [*self.LOGS, *self.OTHERS]:
            del self.srv.httpd.routes[f"{self.base}/artifacts/{path}"]
        self.put("cluster-logs/kt2-a1-master/etcd.log", b"etcd\n")
        self.put("cluster-logs/kt2-a1-minion-group-ab12/kube-node-installation.log", b"node\n")
        self.put("cluster-logs/cluster-info.log", b"info\n")
        self.show([self.build], {"job.Overall": ([12], ["boom"]), "kubetest2.Up": ([12], [""])})
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]],  # fails: KeyError: 'artifacts'
                         self.paths(["cluster-logs/kt2-a1-master/etcd.log",
                                     "cluster-logs/kt2-a1-minion-group-ab12/kube-node-installation.log"]))
        self.assertEqual(self.stored("kt2-a1-master.etcd.log"), b"etcd\n")

    def test_of_two_of_one_name_the_first_by_path(self):
        # (from kube-up's folder and kubetest2's, both: kubetest2's sorts first here)
        self.put("cluster-logs/e2e-master/etcd.log", b"kubetest2's\n")
        self.assertEqual(self.fetch("--gzip"), 0)
        record, = (r for r in self.meta()["artifacts"] if name_of(r["path"]) == "e2e-master.etcd.log")  # fails: KeyError: ...
        self.assertEqual((record["path"], self.stored("e2e-master.etcd.log")),
                         ("artifacts/cluster-logs/e2e-master/etcd.log", b"kubetest2's\n"))

    def test_in_parts(self):
        self.put("e2e-master/serial-1.log", os.urandom(300))
        self.assertEqual(self.fetch("--gzip", "--max-file-mb", "0.0001"), 0)  # (104 bytes)
        record, = (r for r in self.meta()["artifacts"] if r["path"].endswith("serial-1.log"))  # fails: KeyError: ...
        self.assertEqual([p["file"] for p in record["parts"]][:2],
                         ["e2e-master.serial-1.log.gz.000", "e2e-master.serial-1.log.gz.001"])
        self.assertEqual(self.check()[0], 0)

    def test_one_gcs_cannot_list_leaves_them_for_the_next_run(self):
        self.refuse_listing()
        self.finished(ago=3 * DAY)  # (final, but for that)
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual((self.runs()[-1]["outcomes"], m["artifacts"], m["final"]), ({"archived": [self.key]}, [], False))
        # fails: KeyError: 'artifacts'
        self.assertEqual(sorted(os.listdir(self.bdir())), ["build-log.txt.gz", "meta.json"])

    def test_nor_does_a_repair_of_its_log_drop_those_it_has(self):
        # (their copies are intact: they stay, and the build is not final, so the next run lists them again)
        self.finished(ago=3 * DAY)
        self.assertEqual(self.fetch("--gzip"), 0)
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        self.refuse_listing()
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual([r["path"] for r in m["artifacts"]], self.paths(self.LOGS))  # fails: KeyError: 'artifacts'
        self.assertEqual((self.runs()[-1]["outcomes"], m["repaired"], m["final"]),
                         ({"repaired": [self.key]}, ["build-log.txt"], False))

    def test_nor_one_gcs_cannot_serve(self):
        # (but the others are kept)
        self.srv.route(f"{self.base}/artifacts/e2e-master/serial-1.log", (403, {}, b""))
        self.listed("e2e-master/serial-1.log")
        self.finished(ago=3 * DAY)
        self.assertEqual(self.fetch("--gzip"), 0)
        others = [p for p in self.LOGS if p != "e2e-master/serial-1.log"]
        m = self.meta()
        self.assertEqual(([r["path"] for r in m["artifacts"]], m["final"]), (self.paths(others), False))
        # fails: KeyError: 'artifacts'
        self.assertEqual(sorted(os.listdir(self.bdir())),
                         ["build-log.txt.gz"] + [n + ".gz" for n in self.names(others)] + ["meta.json"])
        self.assertEqual((self.runs()[-1]["outcomes"], self.runs()[-1]["errors"]), ({"archived": [self.key]}, []))

    def test_one_gone_since_it_was_listed_is_as_none(self):
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        self.listed("e2e-master/etcd.log")
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual([r["path"] for r in m["artifacts"]],  # fails: KeyError: 'artifacts'
                         self.paths(p for p in self.LOGS if p != "e2e-master/etcd.log"))
        self.assertEqual(self.runs()[-1]["errors"], [])

    def test_a_bug_taking_them_keeps_its_log_in(self):
        # (as one taking its podinfo.json: the next run reads them again; a repair fails, though)
        with self.bug():
            self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual((self.meta()["artifacts"], self.meta()["final"]), ([], False))  # fails: KeyError: 'artifacts'


class UpFailedRepaired(UpFailedWorld):
    """As UpFailed, archived: a copy of the logs of one of its nodes is then damaged."""

    def setUp(self):
        super().setUp()
        self.assertEqual(self.fetch("--gzip"), 0)
        self.damaged = "e2e-master.etcd.log"
        with open(os.path.join(self.bdir(), self.damaged + ".gz"), "ab") as f:
            f.write(b"damage")

    def test_a_repair_takes_again_only_the_damaged_one(self):
        hits = dict(self.srv.httpd.hits)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})  # fails: {'already-archived': [...]}
        self.assertEqual(self.meta()["repaired"], [self.damaged])
        self.assertEqual(self.stored(self.damaged), b"etcd\n")
        again = sorted(p for p, n in self.srv.httpd.hits.items() if n != hits.get(p, 0) and "/artifacts/" in p)
        self.assertEqual(again, [f"{self.base}/artifacts/e2e-master/etcd.log"])
        self.assertEqual(self.check()[0], 0)

    def test_an_intact_one_gcs_no_longer_has_stays(self):
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/serial-1.log"]
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})  # fails: {'already-archived': [...]}
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]], self.paths(self.LOGS))
        self.assertEqual(self.stored("e2e-master.serial-1.log"), b"serial\n")

    def test_a_repair_that_keeps_some_keeps_what_the_runs_know_of_failed_comparisons(self):
        # (as one that keeps another file: those are not compared with GCS now)
        since = time.time() - 2 * DAY
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(self.meta(), comparison_failing_since=since))
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual(m.get("comparison_failing_since"), since)  # fails: None
        self.assertEqual((self.runs()[-1]["outcomes"], m["repaired"]),
                         ({"repaired": [self.key]}, ["build-log.txt", self.damaged]))

    def test_not_one_gcs_no_longer_has(self):
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn(f"the archived {self.damaged} is damaged and GCS no longer has it",
                      self.runs()[-1]["errors"][0]["error"])

    def test_nor_one_gcs_cannot_list_now(self):
        # (the folder stays as it is, and the next run takes it again)
        listing, before = self.srv.httpd.prefix_routes[LISTING], self.files()
        self.refuse_listing()
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn("HTTPError: HTTP Error 503", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.files(), before)
        self.srv.httpd.prefix_routes[LISTING] = listing
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.meta()["repaired"]), ({"repaired": [self.key]}, [self.damaged]))
        self.assertEqual(self.stored(self.damaged), b"etcd\n")

    def test_nor_one_gcs_cannot_serve_now(self):
        # (the error is GCS's failure on it, not that on a log the folder holds no copy of, which GCS refuses first; the
        # folder stays as it is)
        before = self.files()
        self.srv.route(f"{self.base}/artifacts/e2e-master/etcd.log", (503, {}, b""))
        self.srv.route(f"{self.base}/artifacts/a-master/etcd.log", (403, {}, b""))
        self.listed("e2e-master/etcd.log", "a-master/etcd.log")
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertEqual(self.runs()[-1]["errors"][0]["error"], "HTTPError: HTTP Error 503: Service Unavailable")
        self.assertEqual(self.files(), before)

    def test_nor_one_gcs_no_longer_has_when_gcs_cannot_list_them(self):
        # (it is not lost, unseen: the next run that can list them says GCS no longer has it)
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        listing, before = self.srv.httpd.prefix_routes[LISTING], self.files()
        self.refuse_listing()
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertEqual(self.files(), before)
        self.srv.httpd.prefix_routes[LISTING] = listing
        self.assertEqual(self.fetch("--gzip"), 1)
        self.assertIn(f"the archived {self.damaged} is damaged and GCS no longer has it",
                      self.runs()[-1]["errors"][0]["error"])
        self.assertEqual(self.files(), before)

    def test_a_bug_taking_them_fails_a_repair(self):
        # (one taking a log it has no copy of, too: it is not one GCS could not serve)
        self.put("e2e-minion-group-ef56/kube-node-installation.log", b"a node that came up late\n")
        with self.bug("e2e-minion-group-ef56/"):
            self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn("TypeError: a bug", self.runs()[-1]["errors"][0]["error"])

    def log_damaged_once_checked(self):
        """The copy of the build's log damaged (bytes after its gzip member) once the repair has found it intact, as
        the disk could damage it, before the repair reads it to tell whether to keep kube-apiserver.log."""
        real = frl.intact

        def damaging(target, meta, deep=False):
            ok = real(target, meta, deep)
            if ok and set(meta) == {"log"}:  # (the repair's check of the log alone)
                with open(os.path.join(target, "build-log.txt.gz"), "ab") as f:
                    f.write(b"damage")
            return ok
        return mock.patch.object(frl, "intact", damaging)

    def test_a_log_it_cannot_read_is_a_disk_error(self):
        # (not GCS's failure on the copy to take again: the folder stays as it is, and the next run takes both again)
        before = dict(self.files())
        before["build-log.txt.gz"] += b"damage"
        with self.log_damaged_once_checked():
            self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        error = self.runs()[-1]["errors"][0]["error"]
        self.assertTrue(error.startswith("LocalIOError: ") and "build-log.txt.gz: bad gzip data" in error, error)
        self.assertEqual(self.files(), before)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.meta()["repaired"], ["build-log.txt", self.damaged])
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))

    def test_nor_in_a_repair_with_none_to_take_again(self):
        # (its podinfo.json damaged instead, its nodes' logs intact: the failure is not taken for GCS failing to list
        # them, which would put in place a meta.json the copy of the log no longer matches)
        self.srv.route(self.base + "/podinfo.json", obj(b"{}\n"))
        self.assertEqual(self.fetch("--gzip"), 0)  # (the repair takes the damaged one again, and podinfo.json)
        with open(os.path.join(self.bdir(), "podinfo.json.gz"), "ab") as f:
            f.write(b"damage")
        before = dict(self.files())
        before["build-log.txt.gz"] += b"damage"
        with self.log_damaged_once_checked():
            self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertTrue(self.runs()[-1]["errors"][0]["error"].startswith("LocalIOError: "))
        self.assertEqual(self.files(), before)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual(self.meta()["repaired"], ["build-log.txt", "podinfo.json"])
        self.assertTrue(frl.intact(self.bdir(), self.meta(), deep=True))

    def unreadable(self):
        """Its meta.json no longer read as its own; the copy damaged is whole again."""
        with open(os.path.join(self.bdir(), "meta.json"), "w") as f:
            f.write("{not json")
        with open(os.path.join(self.bdir(), self.damaged + ".gz"), "wb") as f:
            f.write(gzip.compress(b"etcd\n"))

    def test_one_of_a_folder_whose_meta_json_cannot_be_read_stays_if_gcs_no_longer_has_it(self):
        # (as its log, or podinfo.json: the copy and the meta.json are left as they are)
        self.unreadable()
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn(f"GCS no longer has {self.damaged}, of which its folder holds a copy",
                      self.runs()[-1]["errors"][0]["error"])
        self.assertIn(self.damaged + ".gz", os.listdir(self.bdir()))

    def test_one_gcs_still_has_is_taken_again_though_not_one_to_keep_now(self):
        # (kube-apiserver.log, as of a log that said the cluster failed to initialize)
        self.unreadable()
        with open(os.path.join(self.bdir(), "e2e-master.kube-apiserver.log.gz"), "wb") as f:
            f.write(gzip.compress(b"an older copy\n"))
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]],  # fails: KeyError: 'artifacts'
                         self.paths([*self.LOGS, "e2e-master/kube-apiserver.log"]))
        self.assertEqual(self.stored("e2e-master.kube-apiserver.log"), b"apiserver\n")

    def test_so_does_one_of_a_build_whose_tabs_no_longer_show_its_up_failed(self):
        # (TestGrid shows it red, but not on kubetest.Up now: the copies its folder holds are still its)
        self.unreadable()
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        self.show([self.build], {"job.Overall": ([12], ["boom"])})
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn(f"GCS no longer has {self.damaged}, of which its folder holds a copy",
                      self.runs()[-1]["errors"][0]["error"])

    def test_nor_does_a_later_repair_of_its_log_drop_them(self):
        # (its meta.json lists them, as recheck() reads it: a repair keeps them, and one GCS no longer has stays)
        self.unreadable()
        self.show([self.build], {"job.Overall": ([12], ["boom"])})
        self.assertEqual(self.fetch("--gzip"), 0)  # (its copies taken again; its tabs show no Up failure now)
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/serial-1.log"]
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]], self.paths(self.LOGS))  # fails: KeyError: 'artifacts'
        self.assertEqual(self.stored("e2e-master.serial-1.log"), b"serial\n")

    def test_nor_does_the_end_of_that_run_remove_their_copies(self):
        # (as copies cut off part-way, its meta.json listing none: one GCS no longer has would be lost, with no error;
        # the next run compares them, as any)
        self.unreadable()
        self.show([self.build], {"job.Overall": ([12], ["boom"])})
        self.assertEqual(self.fetch("--gzip"), 0)
        copies = sorted(n for n in os.listdir(self.bdir()) if n.startswith("e2e-"))
        self.assertEqual(copies, [n + ".gz" for n in self.names(self.LOGS)])  # fails: []
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/etcd.log"]
        self.assertEqual(self.fetch("--gzip"), 0)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["errors"], run["warnings"]), ({"repaired": [self.key]}, [], []))
        self.assertEqual(sorted(n for n in os.listdir(self.bdir()) if n.startswith("e2e-")), copies)
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.stored(self.damaged)), ({"already-archived": [self.key]}, b"etcd\n"))

    def test_nor_does_one_judged_from_gcs_that_testgrid_then_shows_without_its_up_failure(self):
        # (judged red on kubetest.Up by TestGrid's rule, then shown by TestGrid without it: its tabs take TestGrid's
        # cells, see merge_tabs, and show no Up failure now; its meta.json lists them)
        m = self.meta()
        frl.write_json(os.path.join(self.bdir(), "meta.json"), dict(m, tabs=[dict(t, judged_from_gcs=True) for t in m["tabs"]]))
        self.show([self.build], {"job.Overall": ([12], ["boom"])})
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        del self.srv.httpd.routes[f"{self.base}/artifacts/e2e-master/serial-1.log"]
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual([c["test"] for t in m["tabs"] for c in t["red_cells"]], ["job.Overall"])
        self.assertEqual([r["path"] for r in m["artifacts"]], self.paths(self.LOGS))  # fails: KeyError: 'artifacts'
        self.assertEqual((m["repaired"], self.stored(self.damaged), self.stored("e2e-master.serial-1.log")),
                         (["build-log.txt", self.damaged], b"etcd\n", b"serial\n"))

    def test_and_one_whose_list_of_them_is_empty_takes_them(self):  # (kills a mutant)
        # (as one archived when GCS could not list them: its meta.json lists none, but it lists them, so the repair
        # lists GCS again and takes them, as a comparison would)
        m = self.meta()
        frl.write_json(os.path.join(self.bdir(), "meta.json"),
                       dict(m, artifacts=[], tabs=[dict(t, judged_from_gcs=True) for t in m["tabs"]]))
        self.show([self.build], {"job.Overall": ([12], ["boom"])})
        with open(os.path.join(self.bdir(), "build-log.txt.gz"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch("--gzip"), 0)
        self.assertEqual([r["path"] for r in self.meta()["artifacts"]], self.paths(self.LOGS))  # fails: KeyError: 'artifacts'

    def test_and_those_when_gcs_cannot_list_them(self):
        self.unreadable()
        self.refuse_listing()
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn("GCS cannot serve (HTTPError: HTTP Error 503", self.runs()[-1]["errors"][0]["error"])
        self.assertIn(self.damaged + ".gz", os.listdir(self.bdir()))

    def test_the_error_is_that_of_the_one_gcs_cannot_serve(self):
        # (the folder holds copies of two logs GCS lists: one GCS no longer has, and one it cannot serve now; GCS first
        # refuses a log the folder holds no copy of: the error says GCS's failure on the second, of its copy; once GCS
        # serves that one, the next run says GCS no longer has the first)
        node, first = "e2e-minion-group-ab12/kube-node-installation.log", "e2e-minion-group-0000/kube-node-installation.log"
        self.unreadable()
        self.srv.route(f"{self.base}/artifacts/e2e-master/etcd.log", (404, {}, b""))
        self.srv.route(f"{self.base}/artifacts/{node}", (503, {}, b""))
        self.srv.route(f"{self.base}/artifacts/{first}", (403, {}, b""))
        self.listed("e2e-master/etcd.log", node, first)
        self.assertEqual(self.fetch("--gzip"), 1)  # fails: 0
        self.assertIn(f"GCS cannot serve (HTTPError: HTTP Error 503: Service Unavailable) {name_of(node)}, of which its "
                      f"folder holds a copy", self.runs()[-1]["errors"][0]["error"])
        self.put(node, b"node\n")
        self.assertEqual(self.fetch("--gzip"), 1)
        self.assertIn(f"GCS no longer has {self.damaged}, of which its folder holds a copy",
                      self.runs()[-1]["errors"][0]["error"])
        self.assertIn(self.damaged + ".gz", os.listdir(self.bdir()))

    def test_but_not_one_it_holds_no_copy_of_that_gcs_cannot_serve(self):
        # (as podinfo.json: the build is archived with those it holds, taken again, and not final)
        self.unreadable()
        self.srv.route(f"{self.base}/artifacts/e2e-minion-group-ef56/kube-node-installation.log", (403, {}, b""))
        self.listed("e2e-minion-group-ef56/kube-node-installation.log")
        self.finished(ago=3 * DAY)
        self.assertEqual(self.fetch("--gzip"), 0)
        m = self.meta()
        self.assertEqual(([r["path"] for r in m["artifacts"]], m["final"]), (self.paths(self.LOGS), False))
        # fails: KeyError: 'artifacts'
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})


if __name__ == "__main__":
    unittest.main()
