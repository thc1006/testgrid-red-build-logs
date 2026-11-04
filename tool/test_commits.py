"""The commits a red build checked out, from its clone-records.json (see fetch_red_logs.commits_of), recorded in its
meta.json as "commits".

Run: cd tool && python -m unittest test_commits -v
"""
import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import fetch_red_logs as frl  # noqa: E402
from test_integrity import DAY, HOUR, ArchiveWorld, obj  # noqa: E402

K, T = "aa54d42eb56fe26378272b5f7cb9d6f5c010b1a8", "f3e9f609b6397dc92791f0f42f2a69ed4162ebf4"


def record(org, repo, sha, **more):
    return {"refs": {"org": org, "repo": repo, "base_ref": "master", "path_alias": f"k8s.io/{repo}"},
            "commands": [{"command": "git fetch", "output": "..."}], "final_sha": sha, "duration": 26482597750,
            **more}


# (as Prow writes it: a first record of the empty refs it starts from, then one per repository)
RECORDS = [{"refs": {"org": "", "repo": ""}}, record("kubernetes", "test-infra", T),
           record("kubernetes", "kubernetes", K)]


class CommitsOf(unittest.TestCase):
    def test_each_repository_it_cloned(self):
        self.assertEqual(frl.commits_of(RECORDS),
                         {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})  # fails: no commits_of

    def test_a_sha256_commit(self):
        sha = "0123456789abcdef" * 4
        self.assertEqual(frl.commits_of([record("org", "repo", sha)]), {"org/repo": sha})  # fails: no commits_of

    def test_none_of_a_record_that_does_not_name_one(self):
        for r in (record("kubernetes", "kubernetes", K, failed=True), record("kubernetes", "kubernetes", ""),
                  record("kubernetes", "kubernetes", K[:12]), record("kubernetes", "kubernetes", K.upper()),
                  record("kubernetes", "kubernetes", K + "\n"), record("kubernetes", "kubernetes", None),
                  record("", "kubernetes", K), record("a/b", "kubernetes", K), record("kubernetes", "a/b", K),
                  record("kubernetes", None, K),
                  {"refs": None, "final_sha": K}, {"final_sha": K}, "x", None, 7):
            with self.subTest(r):
                self.assertEqual(frl.commits_of([r]), {})  # fails: no commits_of

    def test_none_of_what_is_not_a_list(self):
        for value in (None, {}, "x", {"refs": {"org": "kubernetes", "repo": "kubernetes"}, "final_sha": K}):
            with self.subTest(value):
                self.assertIsNone(frl.commits_of(value))  # fails: no commits_of
        self.assertEqual(frl.commits_of([]), {})  # fails: no commits_of


class BuildCommits(ArchiveWorld):
    """A red build whose clone-records.json says what it checked out."""

    def route(self, value):
        self.srv.route(self.base + "/clone-records.json", (200, {}, value))

    def stored(self, ago=3 * DAY):
        self.finished(ago=ago)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))

    def test_store_records_them(self):
        self.stored()
        self.route(RECORDS)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("commits"),
                         {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})  # fails: None
        self.assertEqual(self.check()[0], 0)

    def test_none_for_a_build_gcs_has_no_record_of(self):
        self.stored()  # (a job that cloned nothing, as one that downloads the build it tests)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("commits"), {})  # fails: None

    def test_none_left_out_while_gcs_has_no_finished_json_either(self):
        # (GCS may have deleted the build's files, or not have written finished.json yet: no record is no news)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn("commits", self.meta())  # fails: {} is recorded
        self.finished(ago=3 * HOUR)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("commits"), {})

    def test_a_final_build_gcs_no_longer_has(self):
        self.stored()
        self.assertEqual(self.fetch(), 0)
        meta = self.meta()
        del meta["commits"]  # (archived before the runs read them)
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        for name in ("finished.json", "build-log.txt"):  # (GCS deleted the build's files)
            self.srv.route(f"{self.base}/{name}", (404, {}, b""))
        with mock.patch.object(frl, "gcs_sevenths", return_value=set(range(7))):
            self.assertEqual(self.fetch(), 0)
        self.assertNotIn("commits", self.meta())  # fails: {} is recorded

    def test_one_gcs_cannot_serve_is_left_out_and_read_by_the_next_comparison(self):
        self.stored(ago=3 * HOUR)  # (not final: compared again by the next run)
        for route in ((403, {}, b"denied"), (200, {}, b"not json")):
            with self.subTest(route[0]):
                self.srv.route(self.base + "/clone-records.json", route)
                self.assertEqual(self.fetch(), 0)
                self.assertNotIn("commits", self.meta())
        self.route(RECORDS)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual(self.meta().get("commits"),
                         {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})  # fails: None
        self.route([record("kubernetes", "kubernetes", T)])  # (read once: what it said stays)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta()["commits"], {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})

    def test_a_final_build_in_its_seventh(self):
        # (one archived before the runs read them: compared, and so read, within a week)
        self.stored()
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        meta = self.meta()
        del meta["commits"]
        frl.write_json(os.path.join(self.bdir(), "meta.json"), meta)
        self.route(RECORDS)
        before = self.read("meta.json")
        with mock.patch.object(frl, "gcs_sevenths", return_value=set()):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.read("meta.json"), before)  # (not in its seventh)
        with mock.patch.object(frl, "gcs_sevenths", return_value=set(range(7))):
            self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("commits"),
                         {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})  # fails: None

    def test_a_repair_keeps_what_it_recorded(self):
        self.stored()
        self.route(RECORDS)
        self.assertEqual(self.fetch(), 0)
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        self.route([record("kubernetes", "kubernetes", T)])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual(self.meta()["commits"],
                         {"kubernetes/test-infra": T, "kubernetes/kubernetes": K})  # fails: KeyError


V = "v1.38.0-alpha.3.406+58d9a3d9bb7203"


class ReadJobVersion(unittest.TestCase):
    def read(self, finished):
        with mock.patch.object(frl, "get_json", return_value=finished):
            return frl.read_finished("base")["job_version"]

    def test_the_job_version_its_job_wrote(self):
        self.assertEqual(self.read({"result": "FAILURE", "metadata": {"job-version": V, "revision": V}}),
                         V)  # fails: KeyError

    def test_none_of_one_that_is_not_a_version(self):
        for metadata in ({"job-version": ""}, {"job-version": "master"}, {"job-version": V + "\n"},
                         {"job-version": "v1.38 0"}, {"job-version": "v" + "1" * 129}, {"job-version": 1.38},
                         {"revision": V}, {}, None, [V], V):
            with self.subTest(metadata):
                self.assertIsNone(self.read({"result": "FAILURE", "metadata": metadata}))  # fails: KeyError
        self.assertIsNone(self.read({"result": "FAILURE"}))  # fails: KeyError


class BuildJobVersion(ArchiveWorld):
    """A red build whose finished.json says which version of Kubernetes its job tested."""

    def finished_with(self, ago, version):
        self.srv.route(self.base + "/finished.json", (200, {}, {
            "result": "FAILURE", "timestamp": int(frl.time.time() - ago),
            **({"metadata": {"job-version": version}} if version else {})}))

    def test_store_records_it(self):
        self.finished_with(3 * DAY, V)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.meta().get("job_version"), V)  # fails: None
        self.assertEqual(self.check()[0], 0)

    def test_none_for_a_job_that_wrote_none(self):
        self.finished_with(3 * DAY, None)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn("job_version", self.meta())

    def test_a_comparison_records_one_written_later(self):
        self.finished_with(3 * HOUR, None)  # (not final: compared again by the next run)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertNotIn("job_version", self.meta())
        self.finished_with(3 * HOUR, V)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"already-archived": [self.key]})
        self.assertEqual(self.meta().get("job_version"), V)  # fails: None

    def test_a_repair_keeps_what_it_recorded(self):
        self.finished_with(3 * DAY, V)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        os.remove(os.path.join(self.bdir(), "build-log.txt"))
        self.finished_with(3 * DAY, None)
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual(self.meta().get("job_version"), V)  # fails: None


if __name__ == "__main__":
    unittest.main()
