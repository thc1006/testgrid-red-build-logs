"""The logs of its cluster's nodes a build folder can store too, after build-log.txt and podinfo.json (see
fetch_red_logs.NODE_LOGS): <node>.<file> in the folder, listed in meta.json's "artifacts" with their path in GCS.

Run: cd tool && python -m unittest test_node_logs -v
"""
import glob
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_integrity import DAY, ArchiveWorld, obj  # noqa: E402

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


if __name__ == "__main__":
    unittest.main()
