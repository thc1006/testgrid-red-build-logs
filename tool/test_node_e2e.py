"""The red builds of node e2e jobs, told by their prowjob.json (see fetch_red_logs.node_e2e_job).

Run: cd tool && python -m unittest test_node_e2e -v
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
