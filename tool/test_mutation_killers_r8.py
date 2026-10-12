"""Mutation-testing gaps of review W4c (round 8 of the mutation killers): TestGrid's reads of a
build's column over time (possible_reads, Timeline, gcs_column), TestGrid's config over time
(ConfigHistory, tab_eras, held_by, run(), cross_check) and the upload job's runs
(testgrid_config: gcs_get, upload_runs, plan, load, main). Each test is tagged with the mutants
of the W4c run (on 76191a2) it killed that the suite did not: F, C, X and R ones are mechanical
mutants of fetch_red_logs.py, testgrid_config.py, cross_check.py and testgrid_rule.py, H ones
hand-made; the tests follow the code as it is now."""
import concurrent.futures as cf
import contextlib
import datetime
import hashlib
import io
import json
import math
import os
import random
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module imports: their tests are not collected again here)
import test_review_round7 as r7  # noqa: E402
import test_review_round9 as r9  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
import testgrid_rule as tr  # noqa: E402
from test_fetch_red_logs import DAY, FakeServer, send  # noqa: E402
from test_fetch_red_logs import table as ttc_table  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

MINUTE = 60


# ------------------------------------------------------------ testgrid_config: reading the upload job's runs

class GcsGet(unittest.TestCase):
    """testgrid_config.gcs_get: an object's bytes, None for none, retried as a server asks."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        sleep = mock.patch.object(tgc.time, "sleep")
        self.slept = sleep.start()
        self.addCleanup(sleep.stop)

    def sequence(self, path, *statuses):
        """Serve `path` with these statuses in turn (the last one from then on), body b"ok"."""
        left = list(statuses)

        def serve(h):
            status = left.pop(0) if len(left) > 1 else left[0]
            send(h, status, {}, b"ok" if status == 200 else b"")
        self.srv.route(path, serve)

    def test_a_server_error_is_retried_with_backoff(self):
        # C061, C184, C186, C229, C230, C261, C262, C288, C289, C294, C295, HC10, HQ09
        self.sequence("/b/x", 503, 429, 200)
        self.assertEqual(tgc.gcs_get(f"{self.srv.url}/b/x"), b"ok")
        self.assertEqual(self.srv.hits("/b/x"), 3)
        self.assertEqual([c.args[0] for c in self.slept.call_args_list], [1, 2])

    def test_the_last_attempt_raises(self):
        # C009, C010, C061, C184, C186, C187, C225, C227, C229, C230, C261, C262, C276, C277, C294, C295, C298, C299, HC09, HC10,
        # HQ09, HQ10
        self.sequence("/b/x", 503)
        with self.assertRaises(urllib.error.HTTPError):
            tgc.gcs_get(f"{self.srv.url}/b/x")
        self.assertEqual(self.srv.hits("/b/x"), 4)
        self.assertEqual([c.args[0] for c in self.slept.call_args_list], [1, 2, 4])

    def test_a_client_error_is_not_retried(self):
        # C184, C186, C187, C225, C226, C261, C262, C286, C287, C290-C293, C296, C297, HQ09
        for status in (400, 403, 410):
            self.sequence(f"/b/{status}", status)
            with self.assertRaises(urllib.error.HTTPError):
                tgc.gcs_get(f"{self.srv.url}/b/{status}")
            self.assertEqual(self.srv.hits(f"/b/{status}"), 1, status)
        for status in (408, 500, 502, 504):  # (each of the codes a server asks a retry with)
            self.sequence(f"/b/{status}", status, 200)
            self.assertEqual(tgc.gcs_get(f"{self.srv.url}/b/{status}"), b"ok", status)

    def test_a_network_error_is_retried_then_raised(self):
        # C009, C010, C061, C188, C189, C228, C263, C264, C278, C279, HC09, HC10, HQ14
        closed = socket.socket()  # (a port held, nothing listening: connection refused)
        self.addCleanup(closed.close)
        closed.bind(("127.0.0.1", 0))
        with self.assertRaises(urllib.error.URLError):
            tgc.gcs_get(f"http://127.0.0.1:{closed.getsockname()[1]}/b/x")
        self.assertEqual(len(self.slept.call_args_list), 3)

    def test_a_broken_response_is_retried(self):  # C188, C228, HQ15
        def script(n, s, read_request):
            if read_request() is not None:
                s.sendall(b"garbage\r\n\r\n" if n == 1 else r7.response(200, b"ok"))
        srv = r7.RawServer(script)
        self.addCleanup(srv.close)
        self.assertEqual(tgc.gcs_get(f"{srv.url}/b/x"), b"ok")
        self.assertEqual(srv.connections, 2)

    def test_an_object_larger_than_a_mebibyte_is_refused(self):  # C037-C039, C119, C182, C257, C258, C274, HC07, HC08, HQ06
        self.srv.route("/b/limit", (200, {}, b"x" * (1 << 20)))
        self.srv.route("/b/over", (200, {}, b"x" * ((1 << 20) + 1)))
        self.assertEqual(len(tgc.gcs_get(f"{self.srv.url}/b/limit")), 1 << 20)
        with self.assertRaisesRegex(ValueError, "more than"):
            tgc.gcs_get(f"{self.srv.url}/b/over")

    def test_each_request_has_a_timeout(self):  # C272, C273, HQ16
        seen = []

        def urlopen(url, timeout=None):
            seen.append(timeout)
            raise urllib.error.HTTPError(url, 404, "nope", {}, None)
        with mock.patch.object(tgc.urllib.request, "urlopen", urlopen):
            self.assertIsNone(tgc.gcs_get("http://x/y"))
        self.assertEqual(seen, [60])


class UploadJobRuns(unittest.TestCase):
    """testgrid_config.upload_runs, plan and main's uploads and merge commands, against a GCS of
    the upload job's runs (as test_config_history.Uploads serves them)."""

    def setUp(self):
        self.srv = FakeServer()
        self.addCleanup(self.srv.close)
        self.objects = []
        gcs = mock.patch.object(tgc, "GCS", self.srv.url)
        gcs.start()
        self.addCleanup(gcs.stop)
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir)

    def upload(self, at, finished=True, commit="a" * 40, page=None):
        b = str(snowflake(at))
        base = f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{b}"
        self.srv.route(f"{base}/started.json", (200, {}, {"timestamp": int(at), "repo-commit": commit}))
        if finished is not None:
            body = finished if not isinstance(finished, bool) else {"timestamp": int(at) + 5, "passed": finished}
            self.srv.route(f"{base}/finished.json", (200, {}, body))
        self.objects.append(f"logs/{tgc.UPLOAD_JOB}/{b}/prowjob.json")
        self.srv.gcs_listing("kubernetes-ci-logs", self.objects, page=page)
        return b

    def main(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = tgc.main(list(argv))
        self.assertEqual(code, 0)
        return json.loads(out.getvalue())

    def write(self, name, data):
        path = os.path.join(self.dir, name)
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_every_page_of_the_listing_is_read(self):  # C071, C074, C075, C131, HQ25
        t = int(time.time()) - DAY
        builds = [self.upload(t + 60 * i, page=1) for i in range(3)]
        self.assertEqual([r["build"] for r in tgc.upload_runs(t - 1)], builds)

    def test_a_listing_that_does_not_end_or_is_not_one_is_an_error(self):  # C073-C075, C129, C130, HQ25
        page = json.dumps({"prefixes": [], "nextPageToken": "more"}).encode()
        with mock.patch.object(tgc, "gcs_get", return_value=page) as get:
            with self.assertRaisesRegex(ValueError, "did not end"):
                tgc.upload_runs(time.time() - DAY)
        self.assertEqual(get.call_count, 1000)
        for bad in (b"[]", b'{"prefixes": "x"}', b"not json"):
            with mock.patch.object(tgc, "gcs_get", return_value=bad):
                with self.assertRaisesRegex(ValueError, "unexpected GCS listing", msg=bad):
                    tgc.upload_runs(time.time() - DAY)

    def test_the_listing_starts_at_the_first_build_id_made_after(self):
        # C126-C128, C193, C194, C265, C280, C281, HC13, HC14, HQ21
        urls = []

        def get(url):
            urls.append(url)
            return b'{"prefixes": []}'
        after = time.time() - DAY
        with mock.patch.object(tgc, "gcs_get", get):
            tgc.upload_runs(after)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(urls[0]).query)
        self.assertEqual(query["startOffset"], [f"logs/{tgc.UPLOAD_JOB}/{snowflake(after)}"])
        self.assertEqual((query["prefix"], query["delimiter"]), ([f"logs/{tgc.UPLOAD_JOB}/"], ["/"]))

    def test_a_run_whose_finished_json_is_not_json_failed(self):  # C140, HQ27
        t = int(time.time()) - DAY
        self.upload(t, finished=b"{not json")
        self.assertEqual(tgc.upload_runs(t - 1), [])

    def test_a_start_without_its_time_uploaded_nothing(self):  # C202, HQ26
        t = int(time.time()) - DAY
        b = self.upload(t)
        self.srv.route(f"/kubernetes-ci-logs/logs/{tgc.UPLOAD_JOB}/{b}/started.json", (200, {}, {"repo-commit": "a" * 40}))
        self.assertEqual(tgc.upload_runs(t - 1), [])

    def test_the_plan_lists_from_the_last_finished_run_the_archive_knows(self):  # C144, C206, HQ32
        t = int(time.time()) - DAY
        done, between, running = self.upload(t), self.upload(t + 60), self.upload(t + 120, finished=None)
        state = {"config_uploads": [
            {"build": done, "commit": "a" * 40, "started": t, "finished": t + 5, "tabs": {}, "base": True},
            {"build": running, "commit": "a" * 40, "started": t + 120, "finished": None, "tabs": None}]}
        self.assertEqual([r["build"] for r in tgc.plan(state, time.time())["runs"]], [between, running])

    def test_uploads_reads_the_archives_state(self):  # C027, C031, C088-C090, C092, C151, C153, C154, C206, C214, HC01, HQ47
        t = int(time.time()) - DAY
        known, new = self.upload(t, commit="a" * 40), self.upload(t + 60, commit="b" * 40)
        state = self.write("state.json", {"config_uploads": [
            {"build": known, "commit": "a" * 40, "started": t, "finished": t + 5, "tabs": {}, "base": True}]})
        got = self.main("uploads", "--state", state)
        # (a, known, is built too: how b changed the test groups is told by a's whole config)
        self.assertEqual(([r["build"] for r in got["runs"]], [b["commit"] for b in got["build"]]),
                         ([new], ["b" * 40, "a" * 40]))
        # (a state file not there yet: the last 18 days)
        self.upload(t - 18 * DAY, commit="c" * 40)
        got = self.main("uploads", "--state", os.path.join(self.dir, "missing.json"))
        self.assertEqual([r["build"] for r in got["runs"]], [known, new])

    def test_uploads_without_a_state_lists_the_last_days(self):
        # C007, C008, C027, C031, C088, C090, C092, C151, C152, C154, C267, HC01, HC02, HQ47
        now = int(time.time())
        self.upload(now - 18.5 * DAY)
        recent = self.upload(now - 17.5 * DAY)
        self.assertEqual([r["build"] for r in self.main("uploads")["runs"]], [recent])

    def test_merge_writes_the_runs_and_the_configs_load_reads(self):
        # C028, C029, C033, C058, C094, C096, C097, C113, C155, C156, HQ49, HQ50, HQ52
        t = int(time.time()) - DAY
        runs = [{"build": str(snowflake(t)), "commit": "a" * 40, "started": t, "finished": t + 5},
                {"build": str(snowflake(t + 60)), "commit": "b" * 40, "started": t + 60, "finished": None}]
        recipes = [tgc.recipe_id(r["commit"], None) for r in runs]
        uploads = self.write("uploads.json", {
            "after": t - 1, "runs": [dict(r, job=None, recipe=rid) for r, rid in zip(runs, recipes, strict=True)],
            "build": [{"recipe": rid, "commit": r["commit"], "job": None} for r, rid in zip(runs, recipes, strict=True)],
            "compare": [{"build": runs[0]["build"], "recipe": recipes[0], "prev": None},
                        {"build": runs[1]["build"], "recipe": recipes[1], "prev": recipes[0]}]})
        import yaml
        config = os.path.join(self.dir, "config.yaml")
        with open(config, "w") as f:
            yaml.safe_dump({"test_groups": [ttc.GROUP], "dashboards": [{"name": "dash", "dashboard_tab": [
                {"name": "t", "test_group_name": "job"}]}]}, f)
        extracts = []
        for (commit, digest), rid in zip((("a" * 40, "sha256:" + "1" * 64), ("b" * 40, None)), recipes, strict=True):
            args = ["extract", config, "--recipe", rid, "--commit", commit, "--committed", str(t - 100), "--image",
                    "img:v1", "--dashboard", "dash"] + (["--digest", digest] if digest else [])
            extracts.append(self.write(f"{commit[0]}.json", self.main(*args)))
        merged = self.write("merged.json", self.main("merge", uploads, *extracts))
        got = tgc.load(merged)
        self.assertEqual(got["runs"], [dict(r, recipe=rid) for r, rid in zip(runs, recipes, strict=True)])
        self.assertEqual(sorted(got["configs"]), sorted(recipes))
        tabs, source = got["configs"][recipes[0]]
        self.assertEqual(sorted(tabs), ["dash#t"])
        self.assertEqual(source, {"test_infra": "a" * 40, "committed": t - 100, "image": "img:v1",
                                  "digest": "sha256:" + "1" * 64})
        self.assertNotIn("digest", got["configs"][recipes[1]][1])
        self.assertEqual(got["group_changes"], {runs[0]["build"]: {}, runs[1]["build"]: {}})

    def test_load_refuses_a_dashboard_that_is_not_an_object(self):  # C109
        # (that config is not taken, as one that could not be built: the rest of the file is)
        built = {"commit": "c" * 40, "committed": 1_789_990_000, "image": "img", "dashboards": {"dash": ["t"]}}
        path = self.write("c.json", {"format": tgc.FORMAT, "after": 1, "runs": [], "configs": {ttc.recipe_of("c" * 40): built},
                                     "group_changes": {}, "failed": {}})
        loaded = tgc.load(path)
        self.assertEqual(loaded["configs"], {})
        self.assertIn("dash: not an object", loaded["failed"][ttc.recipe_of("c" * 40)])

    def test_odd_entries_of_a_listing_are_skipped(self):  # C196, C235-C237
        prefix = f"logs/{tgc.UPLOAD_JOB}/"
        b, elsewhere = str(snowflake(time.time() - DAY)), str(snowflake(time.time() - DAY + 60))
        other = prefix[:-2] + "x/"  # (another folder whose name is as long)
        page = json.dumps({"prefixes": [5, None, f"{prefix}latest/", f"{other}{elsewhere}/", f"{prefix}{b}/"]}).encode()
        started = json.dumps({"timestamp": int(time.time() - DAY), "repo-commit": "a" * 40}).encode()
        finished = json.dumps({"timestamp": int(time.time() - DAY) + 5, "passed": True}).encode()

        def get(url):
            return page if "/storage/v1/" in url else started if url.endswith("started.json") else finished
        with mock.patch.object(tgc, "gcs_get", get):
            self.assertEqual([r["build"] for r in tgc.upload_runs(time.time() - 2 * DAY)], [b])

    def test_a_finished_json_that_is_not_an_object_failed(self):  # C140
        t = int(time.time()) - DAY
        self.upload(t, finished=[1])
        self.assertEqual(tgc.upload_runs(t - 1), [])

    def test_the_plan_skips_what_its_state_does_not_hold_well(self):  # C148, C212, C241-C244, HQ38, HQ39
        t = int(time.time()) - DAY
        new = self.upload(t, commit="b" * 40)
        state = {"config_uploads": ["x", {"build": 5}, {"build": "x"}, {"commit": "c" * 40},
                                    {"build": str(snowflake(t - 600)), "commit": 5, "started": t - 600,
                                     "finished": t - 595, "tabs": None},
                                    {"build": str(snowflake(t - 300)), "commit": "x", "started": t - 300,
                                     "finished": t - 295, "tabs": None}]}
        out = tgc.plan(state, time.time())
        self.assertEqual(([r["build"] for r in out["runs"]], [b["commit"] for b in out["build"]]), ([new], ["b" * 40]))

    def test_the_plan_lists_again_a_run_still_running_before_the_last_one_that_ended(self):  # C144, C209, C268, C269, C282, HQ34
        t = int(time.time()) - DAY
        early = self.upload(t, commit="a" * 40)  # (it ran long: it finished after the next one)
        done = self.upload(t + 60, commit="b" * 40)
        late = self.upload(t + 120, finished=None, commit="c" * 40)
        state = {"config_uploads": [
            {"build": early, "commit": "a" * 40, "started": t, "finished": None, "tabs": None},
            {"build": done, "commit": "b" * 40, "started": t + 60, "finished": t + 65, "tabs": {}, "base": True},
            {"build": late, "commit": "c" * 40, "started": t + 120, "finished": None, "tabs": None}]}
        self.assertEqual([r["build"] for r in tgc.plan(state, time.time())["runs"]], [early, done, late])


class LoadsWhatMergeWrote(unittest.TestCase):
    """testgrid_config.load (format 3) and valid_entry: what they accept and refuse."""

    RECIPE = ttc.recipe_of("c" * 40)
    RUN = {"build": "2109000000000000000", "commit": "c" * 40, "started": 1_790_000_000, "finished": 1_790_000_005,
           "recipe": RECIPE}
    BUILT = {"commit": "c" * 40, "committed": 1_789_990_000, "image": "img", "dashboards": {"dash": {"t": ttc.entry("")}}}

    def load(self, data):
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        with open(path, "w") as f:
            json.dump(dict({"format": tgc.FORMAT, "after": 1, "group_changes": {}, "failed": {}}, **data), f)
        return tgc.load(path)

    def test_a_run_that_ends_as_it_starts_is_one(self):  # C270
        got = self.load({"runs": [dict(self.RUN, finished=self.RUN["started"])], "configs": {}})
        self.assertEqual(got["runs"][0]["finished"], self.RUN["started"])

    def test_an_empty_digest_is_none(self):  # C112
        got = self.load({"runs": [], "configs": {self.RECIPE: dict(self.BUILT, digest="")}})
        self.assertNotIn("digest", got["configs"][self.RECIPE][1])

    def test_wrong_types_are_value_errors(self):  # C163, C165, C167, C173, C174, C176, C253, HC20
        bad = [{"runs": [r], "configs": {}} for r in (
            ["build", "commit", "started", "finished"], dict(self.RUN, build=21090000), dict(self.RUN, commit=5),
            dict(self.RUN, finished="x"), dict(self.RUN, commit="c" * 41), dict(self.RUN, recipe=5))]
        bad += [{"runs": [], "configs": {c: b}} for c, b in (
            (self.RECIPE, "x"), (self.RECIPE, dict(self.BUILT, dashboards=["dash"])),
            (self.RECIPE, dict(self.BUILT, image=5)), ("c" * 41, self.BUILT), (self.RECIPE, dict(self.BUILT, commit=5)))]
        for data in bad:
            with self.assertRaises(ValueError, msg=data):
                self.load(data)

    def test_load_raises_only_value_errors(self):  # C109, C163, C165, C167, C173, C174, C176, C253
        rng = random.Random(9)
        scalars = [None, True, 0, -1, 1.5, "", "x", [], {}, [1], {"a": 1}, "c" * 40, 1_790_000_000, self.RECIPE]
        loaded = 0
        for _ in range(1500):
            run = dict(self.RUN)
            if rng.random() < 0.7:
                run[rng.choice(list(run) + ["extra"])] = rng.choice(scalars)
            built = json.loads(json.dumps(self.BUILT))
            part = rng.random()
            if part < 0.25:
                built[rng.choice(["commit", "committed", "image", "dashboards", "digest"])] = rng.choice(scalars)
            elif part < 0.5:
                built["dashboards"]["dash"] = rng.choice(scalars)
            elif part < 0.75:
                built["dashboards"]["dash"]["t"][rng.choice(["test_group", "base_options", "group", "group_digest",
                                                            "duplicate"])] = rng.choice(scalars)
            data = {"runs": rng.choice([[run], [run, self.RUN], rng.choice(scalars), [rng.choice(scalars)]]),
                    "configs": rng.choice([{rng.choice([self.RECIPE, "x"]): rng.choice([built, built, rng.choice(scalars)])},
                                           rng.choice(scalars)])}
            try:
                got = self.load(data)
            except ValueError:
                continue
            loaded += 1
            for r in got["runs"]:
                self.assertTrue(isinstance(r["build"], str) and tgc.is_time(r["started"]), r)
            for tabs, source in got["configs"].values():
                self.assertTrue(isinstance(source["image"], str) and tgc.is_time(source["committed"]), source)
                for entry in tabs.values():
                    tgc.TabRule(entry)
        self.assertGreater(loaded, 10)  # (the loop does reach the files load accepts)

    def test_an_entry_with_every_field(self):  # C157, C160, HC19
        self.assertTrue(tgc.valid_entry(dict(ttc.entry(""), duplicate=True)))  # (as extract writes a second tab of a name)
        for digest in (5, ["x"], "1" * 63, "1" * 40, "1" * 65, "G" * 64):
            self.assertFalse(tgc.valid_entry(dict(ttc.entry(""), group_digest=digest)), digest)

    def test_times_are_positive_and_below_two_to_the_forty(self):  # C122, C124, C191, C232-C234
        self.assertEqual([tgc.is_time(v) for v in (0, 1, (1 << 40) - 1, 1 << 40, True, 1.5)],
                         [False, True, True, False, False, False])


# ------------------------------------------------------------ fetch_red_logs: TestGrid's config over time

NOW = 1_790_000_000
E1, E2, E3 = ttc.entry(""), ttc.entry(ttc.ConfigWorld.HIDE), ttc.entry("", days_of_results=1)


def upload_run(at, commit, finished=True):
    """A run of the upload job that began at `at` (test_config_history.run)."""
    return {"build": str(snowflake(at)), "commit": commit * 40, "started": at, "finished": at + 5 if finished else None}


def built(**tabs_of):
    return {c * 40: (tabs, {"test_infra": c * 40}) for c, tabs in tabs_of.items()}


def merge(h, runs, configs, after=None):
    """h.merge of the upload job's `runs` listed after `after` (by default, just before the
    first) and the configs built ({commit: (tabs, source)})."""
    h.merge(tch.loaded(runs, configs, after=after))


class HistoryKept(unittest.TestCase):
    """ConfigHistory: what it keeps, as state.json holds it, and what it makes of it."""

    def history(self, runs, configs):
        h = frl.ConfigHistory()
        merge(h, runs, configs)
        return h

    def again(self, h, warnings=None):
        uploads, entries = h.encode()
        return frl.ConfigHistory(json.loads(json.dumps(uploads)), json.loads(json.dumps(entries)), warnings)

    def test_a_run_that_changed_no_tab_is_kept(self):  # F700
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b"), upload_run(NOW + 200, "c")],
                         built(a={"d#t": E1}, b={"d#t": E1}, c={"d#t": E2}))
        uploads, _ = h.encode()
        self.assertEqual(uploads[1]["tabs"], {})
        warnings = []
        self.assertEqual([r["tabs"] for r in self.again(h, warnings).runs], [{"d#t": E1}, {"d#t": E1}, {"d#t": E2}])
        self.assertEqual(warnings, [])

    def test_a_run_still_running_or_ending_as_it_starts_is_kept(self):  # F505, F698
        runs = [upload_run(NOW, "a"), dict(upload_run(NOW + 100, "b"), finished=NOW + 100), upload_run(NOW + 200, "c", False)]
        h = self.history(runs, built(a={"d#t": E1}, b={"d#t": E2}, c={"d#t": E3}))
        warnings = []
        got = self.again(h, warnings)
        self.assertEqual([(r["finished"], r["tabs"]) for r in got.runs],
                         [(NOW + 5, {"d#t": E1}), (NOW + 100, {"d#t": E2}), (None, {"d#t": E3})])
        self.assertEqual(warnings, [])

    def test_malformed_runs_are_dropped_not_a_crash(self):  # F144, F390-F392, F616, F619, F745, HH01
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b")], built(a={"d#t": E1}, b={"d#t": E2}))
        uploads, entries = h.encode()
        bad_entry = dict(E1, test_group=5)
        for bad, ents in ((dict(uploads[1], build=5), entries), (dict(uploads[1], build="x"), entries),
                          (dict(uploads[1], commit=5), entries), (dict(uploads[1], finished="x"), entries),
                          (dict(uploads[1], tabs=["d#t"]), entries), (dict(uploads[1], tabs={"d#t": ["x"]}), entries),
                          (dict(uploads[1], tabs={"d#t": "bad"}), dict(entries, bad=bad_entry))):
            warnings = []
            got = frl.ConfigHistory([uploads[0], bad], ents, warnings)
            self.assertEqual([r["tabs"] for r in got.runs], [{"d#t": E1}], bad)
            self.assertIn("dropped malformed runs", " ".join(warnings), bad)
        for not_entries in (None, [], "x"):  # (state.json without its entries, or with something else)
            warnings = []
            self.assertEqual(frl.ConfigHistory(uploads, not_entries, warnings).runs, [], not_entries)
            self.assertIn("dropped malformed runs", " ".join(warnings))

    def test_without_a_warnings_list_malformed_runs_are_dropped_quietly(self):  # F256
        self.assertEqual(frl.ConfigHistory(["x"], {}).runs, [])

    def test_kept_from_the_last_run_that_began_77_days_ago(self):  # F001, F002, F078, F079, F526, F730, HH10, HK28, HK29, HK32
        day = 86400
        runs = [upload_run(NOW - 80 * day, "a"), upload_run(NOW - 77 * day, "b"), upload_run(NOW - 77 * day + 3600, "c"),
                upload_run(NOW - int(76.5 * day), "d"), upload_run(NOW - 50 * day, "e")]
        h = self.history(runs, built(a={}, b={}, c={}, d={}, e={}))
        h.prune(NOW)
        self.assertEqual([r["commit"][0] for r in h.runs], ["b", "c", "d", "e"])
        h.prune(NOW + 3600)  # (c began 77 days before that)
        self.assertEqual([r["commit"][0] for r in h.runs], ["c", "d", "e"])

    def test_merge_keeps_a_run_that_ended(self):  # F410
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b")], built(a={"d#t": E1}, b={"d#t": E2}))
        merge(h, [upload_run(NOW + 50, "c")], {})  # (b not listed again: it ended before)
        self.assertEqual([r["commit"][0] for r in h.runs], ["a", "c", "b"])

    def test_merge_keeps_what_it_knows_of_a_run_listed_again(self):  # F261, F413, HH07
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b", finished=False)],
                         built(a={"d#t": E1}, b={"d#t": E2}))
        merge(h, [upload_run(NOW + 100, "b", finished=False), upload_run(NOW + 200, "c")], {})  # (b's config not built now)
        self.assertEqual([r["tabs"] for r in h.runs], [{"d#t": E1}, {"d#t": E2}, None])

    def test_eras_change_a_minute_after_a_run_ends(self):  # F008, F009, HK03, HK04, HK15-HK17
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b")], built(a={"d#t": E1}, b={"d#t": E2}))
        pick = frl.PICKUP
        self.assertEqual(h.eras("d#t"), [(-math.inf, NOW + 5 + pick, frl.UNKNOWN), (NOW, NOW + 105 + pick, E1),
                                         (NOW + 100, math.inf, E2)])
        late = frl.PICKUP + frl.READ_GAP - frl.UPDATE_SPAN  # (PICKUP, and the update after it may begin a gap later)
        changed = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b")],
                               built(a={"d#t": E1}, b={"d#t": E3}))
        self.assertEqual(changed.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, "changed")])

    def test_a_first_run_still_running_is_not_a_crash(self):  # F634
        h = self.history([upload_run(NOW, "a", finished=False)], built(a={"d#t": E1}))
        self.assertEqual(h.eras("d#t"), [(-math.inf, math.inf, frl.UNKNOWN), (NOW, math.inf, E1)])

    def test_each_run_not_known_is_an_event(self):  # F008, F009, F433, F545, HH22, HH23, HK03, HK04, HK15-HK17
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b"), upload_run(NOW + 200, "c"),
                          upload_run(NOW + 300, "d")], built(a={"d#u": E1}, d={"d#u": E1}))
        late = frl.PICKUP + frl.READ_GAP - frl.UPDATE_SPAN
        # (b, c: not known; a and d have no such tab: d's makes no read)
        self.assertEqual(h.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, "unknown"),
                                                       (NOW + 200, NOW + 205 + late, "unknown")])
        undigested = {k: v for k, v in E1.items() if k != "group_digest"}  # (a config from before the digest)
        h = self.history([upload_run(NOW, "a"), upload_run(NOW + 100, "b")],
                         built(a={"d#t": undigested}, b={"d#t": undigested}))
        self.assertEqual(h.events("d#t", NOW + 1000), [(NOW + 100, NOW + 105 + late, "unknown")])


class ConfigsABuildWasShownUnder(unittest.TestCase):
    """tab_eras, build_days, tab_verdict and held_by: the configs a tab had while TestGrid may have
    shown a build (it started at most 6 hours after it was made, and an update takes up to 20
    minutes; each config by its own days of results)."""

    @staticmethod
    def red_column():
        col = tr.Column([], [], {"FAIL"}, {"job.Overall": "FAIL"}, {"job.Overall": "it did not pass"})
        col.open, col.partial = False, None
        return col

    def test_the_eras_of_the_days_a_build_may_be_shown(self):
        # F010, F085, F086, F135, F136, F374, F375, F379, F498-F500, F502, HE01-HE03, HE05, HK18, HK20, HK21
        two = [tgc.TabRule(ttc.entry("", days_of_results=2)) for _ in range(4)]
        end2 = NOW + 6 * 3600 + 2 * 86400 + 20 * 60
        rule = {"days": 3, "eras": [(-math.inf, NOW - 1, two[0]), (-math.inf, NOW, two[1]), (end2, math.inf, two[2]),
                                    (end2 + 1, math.inf, two[3])]}
        self.assertEqual([two.index(r) for _, _, r in frl.tab_eras(rule, NOW)], [1, 2])
        end3 = NOW + 6 * 3600 + 3 * 86400 + 20 * 60  # (a config not known, or no such tab: by the tab's days)
        for era in (frl.UNKNOWN, None):
            self.assertEqual(frl.tab_eras({"days": 3, "eras": [(end3, math.inf, era)]}, NOW), [(end3, math.inf, era)])
            self.assertEqual(frl.tab_eras({"days": 3, "eras": [(end3 + 1, math.inf, era)]}, NOW), [])

    def test_a_build_is_read_with_the_most_days_of_its_configs(self):  # F241, F381, HE06-HE08
        rule = {"days": 3, "eras": [(-math.inf, math.inf, tgc.TabRule(ttc.entry("", days_of_results=30))),
                                    (NOW - 10, math.inf, tgc.TabRule(ttc.entry("", days_of_results=2)))]}
        self.assertEqual(frl.build_days(rule, NOW), 30)
        self.assertEqual(frl.build_days({"days": 3, "eras": [(-math.inf, math.inf, frl.UNKNOWN)]}, NOW), 3)

    def test_each_era_is_judged_by_its_own_problem(self):  # HE15
        held = tgc.TabRule(ttc.entry("", build_override_configuration_value="x"))
        rule = {"rows": None, "problem": None, "eras": [(-math.inf, math.inf, held)]}
        self.assertEqual(frl.tab_verdict(self.red_column(), "job", rule, NOW)[0], "undecided")
        rule = {"rows": None, "problem": "now held", "eras": [(-math.inf, math.inf, tgc.TabRule(ttc.entry("")))]}
        self.assertEqual(frl.tab_verdict(self.red_column(), "job", rule, NOW)[0], "red")

    def test_held_by_the_problem_of_a_config_the_build_was_shown_under(self):  # F286, HA05, HA06
        rec = {"tabs": [{"dashboard": "d", "tab": "t"}, {"dashboard": "d", "tab": "gone"}], "created": NOW}
        held = tgc.TabRule(ttc.entry("", build_override_configuration_value="x"))
        self.assertIsNone(frl.held_by(rec, {}))
        self.assertIn("build_override_configuration_value", frl.held_by(rec, {"d#t": {"eras": [(-math.inf, math.inf, held)]}}))
        self.assertEqual(frl.held_by(rec, {"d#t": {"problem": "p", "eras": [(-math.inf, math.inf, frl.UNKNOWN)]}}), "p")
        self.assertIsNone(frl.held_by(rec, {"d#t": {"problem": "p", "eras": [(-math.inf, math.inf, tgc.TabRule(E1))]}}))

    def test_where_a_config_came_from(self):  # F059, HA01
        self.assertEqual(frl.config_about({"test_infra": "a" * 40, "committed": "x", "uploaded": None}),
                         "test-infra aaaaaaaaaaaa")
        self.assertEqual(frl.config_about({"test_infra": "a" * 40, "committed": NOW, "uploaded": NOW + 60}),
                         f"test-infra aaaaaaaaaaaa of {frl.iso(NOW)}, uploaded {frl.iso(NOW + 60)}")


# ------------------------------------------------------------ fetch_red_logs: TestGrid's reads of a build's column

FAILING, PASSING = r9.FAILING, r9.PASSING
GAP = frl.READ_GAP


class Reads(r9.JobWorld):
    """round 9's world (a job's builds in GCS, each file with when it was written), at a whole
    second, so reads can be put exactly where a rule says they end."""

    def setUp(self):
        super().setUp()
        self.now = float(int(self.now))

    def short(self, made_ago, junits=(), **parts):
        """A build that started a minute after it was made and was done 6 minutes after: TestGrid
        reads it until a gap after its first 20 minutes (21 minutes after it was made)."""
        return self.build(made_ago, finished=5 * MINUTE, podinfo=6 * MINUTE, junits=junits, **parts)

    def at(self, b, seconds):
        """The time `seconds` after build b was made."""
        return frl.created(b) + seconds

    def column(self, b, days=15, timeline=None, events=()):
        return frl.gcs_column(self.srv.url, "bucket/logs/job", b, days, self.now, timeline, events)

    def judged(self, b, days=15, shows=None, **kw):
        return frl.verdict(self.column(b, days, **kw), "job", shows=shows)[0]


class ReadEdges(Reads):
    """The edges of a build's own reads: TestGrid reads its column again within READ_GAP after the
    last read that had a reason to (the moment that window ends included): while it runs; and at
    updates that begin less than 20 minutes after it started, within their own 20 minutes."""

    REACH = 6 * MINUTE + GAP  # (its Pod row waits until podinfo.json, 6 minutes after it was made; a gap after)

    def test_a_file_written_as_the_last_read_may_happen_is_in_it(self):  # F082, F491, F572, F576, HK04
        b = self.short(10 * HOUR, junits=[("junit_01.xml", self.REACH, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_one_written_half_a_minute_later_is_not(self):  # F081, HK03
        b = self.short(10 * HOUR, junits=[("junit_01.xml", self.REACH + 30, FAILING)])
        self.assertEqual(self.judged(b), "not-red")

    def test_a_file_written_at_the_moment_of_now_is_read_now(self):  # F452, F456, F491, F570
        b = self.short(HOUR, junits=[("junit_01.xml", HOUR, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_reads_that_end_now_are_over(self):  # F081, F233, HK03
        b = self.short(self.REACH)
        col = self.column(b)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")

    def test_a_build_too_old_for_the_tab_is_read_until_then(self):  # F607, F608
        # (finished 40 minutes before its deadline, its Pod row still waiting for podinfo.json; on a
        # tab of one day it is too old 24 h 20 min after it started, ten minutes from now)
        # (finished.json written 40 minutes before its deadline: no read whose column is made after the
        # deadline misses it)
        b = self.build(24 * HOUR + 11 * MINUTE, finished=MINUTE + 23 * HOUR + 20 * MINUTE)
        col = self.column(b, days=1)
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")

    def test_an_ancient_start_is_no_start(self):  # F218, F220, HG01
        # (started two days before its ID was made: too old for a tab of one day at every read, but
        # TestGrid may still read it, by its ID, a gap after it was made)
        b = self.build(HOUR, started=-2 * DAY)
        col = self.column(b, days=1)
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")

    def test_the_junit_files_of_a_build_without_a_start_are_not_listed(self):  # F027
        b = self.build(10 * HOUR, started=None, finished=5 * MINUTE)
        listing = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def serve(h):  # (its folder cannot be listed)
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            return send(h, 503, {}, b"") if q["prefix"][0] == f"logs/job/{b}/" else listing(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        col = self.column(b)
        self.assertIsNone(col.partial)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")

    def test_the_inputs_are_what_gcs_holds(self):  # F038, F609, F610
        b = self.short(5 * HOUR, junits=[("junit_01.xml", 2 * MINUTE, FAILING)])
        started, finished, podinfo, junits = self.column(b).inputs
        self.assertEqual(json.loads(started), {"timestamp": int(frl.created(b)) + MINUTE})
        self.assertEqual(json.loads(finished)["timestamp"], int(frl.created(b)) + 5 * MINUTE)
        self.assertEqual(json.loads(podinfo), r9.POD_OK)
        self.assertEqual(junits, [(f"logs/job/{b}/artifacts/junit_01.xml", len(FAILING), FAILING)])

    def test_the_red_rows_are_the_last_reads_when_it_is_red(self):  # HS09
        # (t failed in junit_01, then passed in junit_02: flaky from then on, as TestGrid shows it now)
        b = self.build(10 * HOUR, finished=HOUR, passed=False, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 30 * MINUTE, FAILING), ("junit_02.xml", 40 * MINUTE, PASSING)])
        state, cells = frl.verdict(self.column(b), "job")
        self.assertEqual((state, [c["test"] for c in cells]), ("red", ["job.Overall"]))

    def test_a_junit_file_that_cannot_be_parsed_says_why(self):  # HP03
        b = self.short(5 * HOUR, junits=[("junit_01.xml", 2 * MINUTE, b"<testsuite>")])
        with self.assertRaises(tr.Malformed) as parse:
            tr.parse_junit(b"<testsuite>")
        why = str(parse.exception)
        state, cells = frl.verdict(self.column(b), "job")
        self.assertEqual(state, "red")
        self.assertIn(f"junit_01.xml: parse: {why}", cells[0]["message"])


class RereadAfterAConfigChange(Reads):
    """An update after the build's test group's config changed reads its column again (seeds:
    gcs_column's events), for a build that started less than 7 days before, until 20 minutes
    after the update could begin; not before it, not after."""

    def changed(self, b, frm, to):
        return [(self.at(b, frm), self.at(b, to), "changed")]

    def test_a_change_six_and_a_half_days_after_the_start_reads_it_again(self):  # F007, F363, HK10
        b = self.short(10 * DAY, junits=[("junit_01.xml", 6.4 * DAY, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 6.5 * DAY, 6.5 * DAY + HOUR)), "red")

    def test_one_seven_and_a_half_days_after_does_not(self):  # F006, F362, HG13, HK09
        b = self.short(10 * DAY, junits=[("junit_01.xml", 7.4 * DAY, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 7.5 * DAY, 7.5 * DAY + HOUR)), "not-red")

    def test_seven_days_count_from_the_start(self):  # F006, F007, F225, F362, F363, F366, HG12, HG13, HG15, HK09, HK10
        # (its pod pending for two hours: seven days after its start is 7 days 2 hours after it was made)
        b = self.build(10 * DAY, started=2 * HOUR, finished=2 * HOUR + 5 * MINUTE, podinfo=2 * HOUR + 6 * MINUTE,
                       junits=[("junit_01.xml", 7 * DAY + 50 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 7 * DAY + HOUR, 7 * DAY + 3 * HOUR)), "red")
        b = self.short(10 * DAY, junits=[("junit_01.xml", 7 * DAY + MINUTE - 10 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 7 * DAY + MINUTE, 7 * DAY + HOUR)), "not-red")

    def test_an_update_that_begins_after_seven_days_does_not_count(self):  # F006, F362, HG13, HG14, HK09
        b = self.short(10 * DAY, junits=[("junit_01.xml", 7.2 * DAY, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 6.9 * DAY, 7.5 * DAY)), "not-red")

    def test_reads_go_on_twenty_minutes_after_an_update_could_begin(self):  # F364, F365
        b = self.short(5 * DAY, junits=[("junit_01.xml", DAY + HOUR + 10 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, DAY, DAY + HOUR)), "red")

    def test_a_file_written_as_the_update_may_last_begin_is_read(self):  # F364, F365, F491, F667, F668, HM23
        b = self.short(5 * DAY, junits=[("junit_01.xml", DAY + HOUR + 20 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b, events=self.changed(b, DAY, DAY + HOUR)), "red")

    def test_a_file_written_as_the_update_may_first_begin_is_read_with_it(self):  # F312, F491, F667, F671, F719, F720
        # (t passed in junit_02, written just as the update after the change may begin: the update
        # reads both, flaky)
        b = self.short(5 * DAY, junits=[("junit_01.xml", DAY, FAILING), ("junit_02.xml", 2 * DAY, PASSING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 2 * DAY, 2 * DAY + HOUR)), "not-red")

    def test_nor_before_it(self):  # F312, F667, F671, F720
        # (t failed in junit_01, then passed in junit_02, after the build's own reads: the update
        # after the change reads it flaky)
        b = self.short(5 * DAY, junits=[("junit_01.xml", DAY, FAILING), ("junit_02.xml", DAY + HOUR, PASSING)])
        self.assertEqual(self.judged(b, events=self.changed(b, 2 * DAY, 2 * DAY + HOUR)), "not-red")

    def test_a_build_an_update_may_still_read_again_is_open(self):  # F093, HM18, HM20
        b = self.short(DAY)
        col = self.column(b, events=[(self.now - HOUR, self.now + HOUR, "changed")])
        self.assertTrue(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "undecided")


class OlderColumnsHold(Reads):
    """An update reads a column again while an older column of its job runs (older by start; for
    a build without a start, by ID), and one update longer while a column without a start that
    appeared then runs (Timeline.older_end)."""

    def late(self, made_ago, ago, **parts):
        """A build made made_ago ago that finished in an hour, its failing junit file written `ago`."""
        made = frl.created(snowflake(self.now - made_ago))
        return self.build(made_ago, finished=HOUR, podinfo=HOUR + MINUTE,
                          junits=[("junit_01.xml", self.now - ago - made, FAILING)], **parts)

    def test_the_older_column_that_runs_longest_holds(self):  # F209
        self.build(30 * HOUR)  # (never finished: runs until 24 h after it started, 6 h ago)
        self.build(29 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        self.assertEqual(self.judged(self.late(28 * HOUR, 7 * HOUR)), "red")

    def test_an_older_build_that_started_before_its_id_was_made(self):  # F201, HB03
        self.build(30 * HOUR, started=-5 * MINUTE)  # (its clock five minutes behind)
        self.assertEqual(self.judged(self.late(28 * HOUR, 7 * HOUR)), "red")

    def test_an_older_build_that_finished_after_its_reads_ended(self):  # HB04
        # (its finished.json written 30 h after it started, after the read after its deadline: it ran
        # until its deadline, 16 h ago, and holds until a gap after that)
        self.build(40 * HOUR, finished=MINUTE + 30 * HOUR)
        self.assertEqual(self.judged(self.late(38 * HOUR, 10 * HOUR)), "not-red")
        self.assertEqual(self.judged(self.late(38 * HOUR, 14 * HOUR)), "red")

    def test_a_column_older_by_a_day_and_more_holds(self):  # F303, HK23, HT09
        # (made 28 h before the build, its pod pending 5 h: it runs until an hour after the build was made)
        self.build(38 * HOUR, started=5 * HOUR)
        b = self.short(10 * HOUR, junits=[("junit_01.xml", 3 * HOUR, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_a_column_older_by_31_hours_still_holds(self):  # F010, F011, F183, F303, HK23, HK27, HT09
        # (pending 5 h 55 min, past its deadline when it finished, 2 h later, its Pod row waits an hour
        # after that: it runs until 1 h 25 min after the build was made)
        self.build(41 * HOUR + 30 * MINUTE, started=5 * HOUR + 55 * MINUTE, finished=31 * HOUR + 55 * MINUTE)
        b = self.short(10 * HOUR, junits=[("junit_01.xml", 3 * HOUR + 20 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_a_column_without_a_start_holds_up_to_35_hours_40_after_an_older_one_was_made(self):
        # F010-F012, F086-F088, F183, F184, F303, HK21, HK23, HK25-HK27, HT09
        # (pending 5 h 59 min; finished 2 h 19 min after its deadline, read then: it runs until 33 h 18 min
        # after it was made; the one without a start, made while it ran, holds a gap longer: until 35 h
        # 38 min after, 38 minutes after the build was made, which is read until a gap after that)
        self.build(45 * HOUR, started=5 * HOUR + 59 * MINUTE, finished=32 * HOUR + 18 * MINUTE)
        self.build(12 * HOUR, started=None, finished=MINUTE)
        b = self.short(10 * HOUR, junits=[("junit_01.xml", 2 * HOUR + 50 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_a_column_made_after_the_build_that_started_before_it(self):  # F010, F086, F682, HK21, HT08
        # (the build's pod was pending 5 h 50 min; one made 5 h 30 min after it started before it and hung)
        b = self.build(30 * HOUR, started=5 * HOUR + 50 * MINUTE, finished=5 * HOUR + 55 * MINUTE,
                       podinfo=5 * HOUR + 56 * MINUTE, junits=[("junit_01.xml", 12 * HOUR, FAILING)])
        self.build(30 * HOUR - 5 * HOUR - 30 * MINUTE)
        self.assertEqual(self.judged(b), "red")

    def test_a_newer_column_does_not_hold_a_build_without_a_start(self):  # F479, HT12
        b = self.build(10 * HOUR, started=None, finished=5 * MINUTE)
        self.build(10 * HOUR - 10 * MINUTE)  # (made after it, still running)
        self.assertEqual(self.judged(b), "not-red")

    def test_only_columns_without_a_start_hold_one_update_longer(self):  # F480, F481, HB02
        self.build(30 * HOUR)  # (hung: runs until 6 h ago; the build's reads go on until a gap after)
        self.build(29 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)  # (started while it ran)
        u = self.build(28 * HOUR + 30 * MINUTE, started=None, finished=MINUTE)
        self.srv.route(f"/bucket/logs/job/{u}/started.json", (200, {}, b"{"))  # (unreadable: not one without a start)
        self.assertEqual(self.judged(self.late(28 * HOUR, 2 * HOUR + 30 * MINUTE)), "not-red")

    def test_a_column_without_a_start_is_held_only_by_older_ones(self):  # HT16
        t = 30 * HOUR
        self.build(t, started=None, finished=MINUTE)
        b = self.short(t - 5 * MINUTE, junits=[("junit_01.xml", 25 * HOUR, FAILING)])
        self.build(t - 10 * MINUTE)  # (made after both, started after the build, hung)
        self.assertEqual(self.judged(b), "not-red")

    def test_a_newer_column_without_a_start_holds_nothing(self):  # F480, F482
        t = 30 * HOUR
        b = self.short(t, junits=[("junit_01.xml", 25 * HOUR, FAILING)])
        self.build(t - 10 * MINUTE)  # (made after the build, started after it, hung)
        self.build(t - 20 * MINUTE, started=None, finished=MINUTE)  # (made while that one ran)
        self.assertEqual(self.judged(b), "not-red")

    def test_a_column_without_a_start_holds_while_an_older_one_is_new(self):  # F346, F683, F684, HT15
        # (an older build done in 4 minutes is still in its first 20 minutes when the one without a start appears)
        t = 30 * HOUR
        self.build(t + HOUR, finished=3 * MINUTE, podinfo=4 * MINUTE)  # (done long before: it holds nothing)
        self.build(t, finished=3 * MINUTE, podinfo=4 * MINUTE)
        self.build(t - 10 * MINUTE, started=None, finished=MINUTE)
        b = self.short(t - 15 * MINUTE, junits=[("junit_01.xml", 4 * HOUR - 15 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_the_longest_hold_counts(self):  # F209, HT17
        t = 30 * HOUR
        self.build(t, finished=3 * MINUTE, podinfo=4 * MINUTE)  # (done in 4 minutes)
        self.build(t - 10 * MINUTE, started=None, finished=MINUTE)  # (holds a gap after its first 20 minutes)
        self.build(t - 15 * MINUTE)  # (started before the build, hung: runs 24 hours)
        b = self.short(t - 20 * MINUTE, junits=[("junit_01.xml", 10 * HOUR - 20 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_a_column_without_a_start_holds_from_the_moment_it_appears(self):  # F082, F347, F683, F684, HT15
        # (an older build in its first 20 minutes until exactly when the one without a start was made)
        t = 30 * HOUR
        self.build(t, finished=2 * MINUTE, podinfo=3 * MINUTE)
        self.build(t - MINUTE - 20 * MINUTE, started=None, finished=MINUTE)
        b = self.short(t - 25 * MINUTE, junits=[("junit_01.xml", 4 * HOUR + 10 * MINUTE - 25 * MINUTE, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_the_build_itself_holds_nothing(self):  # HT07
        # (the run's timeline, as of when it began, saw the build running; it is done since)
        b = self.build(8 * HOUR, finished=2 * HOUR, podinfo=2 * HOUR + MINUTE)
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.at(b, HOUR))
        col = self.column(b, timeline=timeline)
        self.assertFalse(col.open)
        self.assertEqual(frl.verdict(col, "job")[0], "not-red")

    def test_a_watched_build_is_judged_with_the_runs_timeline(self):  # F468, HA08
        # (the run's timeline, as of 8 h ago, saw a hung older build running: it may hold the build)
        self.build(30 * HOUR)
        b = self.build(28 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        rec = {"job": "job", "build": b, "query": "bucket/logs/job", "created": frl.created(b),
               "tabs": [{"dashboard": "dash", "tab": "t"}]}
        state, _, _ = frl.judge_in_tabs(self.srv.url, rec, {}, frl.Timelines(self.now - 8 * HOUR))
        self.assertEqual(state, "undecided")

    def test_a_gap_build_is_judged_with_the_runs_timeline(self):  # F468, F652, F724, F726, HA11
        self.build(30 * HOUR)
        b = self.build(28 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE)
        gap = ("dash#t", {"query": "bucket/logs/job", "dashboard": "dash", "tab": "t", "oldest": self.now - HOUR},
               self.now - 40 * HOUR, None, None)
        with cf.ThreadPoolExecutor(4) as pool:
            out = frl.backfill_gaps([gap], self.srv.url, pool, time.monotonic() + 60, {}, set(),
                                    timelines=frl.Timelines(self.now - 8 * HOUR))
        self.assertEqual([w["build"] for w in out["watch"]], [b])

    def test_a_timeline_judges_as_of_its_own_now(self):  # F468
        self.build(30 * HOUR)  # (hung: it ran until 6 h ago, running 8 h ago)
        b = self.late(28 * HOUR, 7 * HOUR)
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now - 8 * HOUR)
        holds, _ = timeline.grid(b, frl.created(b) + MINUTE)
        self.assertGreater(max(hi for _, hi in holds), self.now)  # (it may run on: until it is too old to run)
        holds, _ = frl.Timeline(self.srv.url, "bucket/logs/job", self.now).grid(b, frl.created(b) + MINUTE)
        self.assertLess(max(hi for _, hi in holds), self.now)


class OneTimelinePerJob(Reads):
    """A run's Timeline of a job serves every build of it the run judges: its listings and reads
    are kept, and each build is judged by the job's builds of its own window."""

    def hung_since(self, made_ago):
        return self.build(made_ago)  # (never finished: runs until 24 h after it started)

    def test_a_newer_builds_window_is_listed_again(self):  # F209, F339, F471, F473, F590
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        first = self.short(30 * HOUR)
        self.hung_since(22 * HOUR)  # (after the first build's window: it runs on now)
        second = self.short(20 * HOUR)
        self.assertEqual(self.judged(first, timeline=timeline), "not-red")
        self.assertEqual(self.judged(second, timeline=timeline), "undecided")

    def test_an_older_builds_window_is_listed_again(self):  # F303, F339, F471, F472, F588, HK23, HT09
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        self.build(58 * HOUR, started=5 * HOUR)  # (before the newer build's window; it ran until 29 h ago)
        older = self.short(30 * HOUR, junits=[("junit_01.xml", 3 * HOUR, FAILING)])
        newer = self.short(20 * HOUR)
        self.assertEqual(self.judged(newer, timeline=timeline), "not-red")
        self.assertEqual(self.judged(older, timeline=timeline), "red")

    def test_a_primed_listing_is_cut_to_each_builds_window(self):  # F724, F726
        self.hung_since(30 * HOUR)
        b = self.build(28 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", 21 * HOUR, FAILING)])  # (written 7 h ago)
        timeline = frl.Timeline(self.srv.url, "bucket/logs/job", self.now)
        with cf.ThreadPoolExecutor(4) as pool:
            cf.wait(timeline.prime(pool, [frl.created(b)]))
        self.assertEqual(self.judged(b, timeline=timeline), "red")

    def test_priming_stops_waiting_at_the_deadline(self):  # F216, F600-F603
        b = self.short(20 * HOUR)
        release = threading.Event()
        self.addCleanup(release.set)
        listing = self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"]

        def serve(h):  # (the job's builds are listed only after a long wait)
            q = urllib.parse.parse_qs(urllib.parse.urlsplit(h.path).query)
            if "delimiter" in q:
                release.wait(20)
            return listing(h)
        self.srv.httpd.prefix_routes["/storage/v1/b/bucket/o?"] = serve
        timelines = frl.Timelines(self.now)
        with cf.ThreadPoolExecutor(4) as pool:
            start = time.monotonic()
            timelines.prime(pool, self.srv.url, [("bucket/logs/job", b)], deadline=time.monotonic() + 0.5)
            took = time.monotonic() - start
            release.set()
        self.assertLess(took, 10)

    def test_a_listing_abandoned_does_not_stop_priming(self):  # F602, F604, F687
        b = self.short(20 * HOUR)
        with mock.patch.object(frl, "list_build_ids", side_effect=frl.Abandoned("the run timed out")):
            with cf.ThreadPoolExecutor(4) as pool:
                frl.Timelines(self.now).prime(pool, self.srv.url, [("bucket/logs/job", b)])


class Small(unittest.TestCase):
    """Small parts of the new code, with what they promise."""

    def test_the_events_of_every_tab_of_a_build(self):  # HE09
        rules = {"d#a": {"events": [(1, 2), (5, 6)]}, "d#b": {"events": [(3, 4), (1, 2)]}, "d#c": {}}
        self.assertEqual(frl.tab_events(rules, ["d#a", "d#b", "d#c", "d#gone"]), [(1, 2), (3, 4), (5, 6)])

    def test_a_groups_digest_is_of_the_group_as_json(self):  # HQ02, HQ03
        group = {"name": "é", "days_of_results": 3, "since": datetime.date(2026, 1, 2)}
        text = '{"days_of_results":3,"name":"é","since":"2026-01-02"}'
        self.assertEqual(tgc.group_digest(group), hashlib.sha256(text.encode()).hexdigest())

    def test_a_build_id_tells_its_millisecond(self):  # HC13, HC14, HQ05
        b = str(snowflake(1_790_000_000.5))
        self.assertEqual(tgc.made(b), 1_790_000_000.5)
        self.assertEqual(tgc.made(b), frl.created(b) + 0.5)

    def test_the_upload_jobs_runs_are_listed_from_gcs(self):  # HC17
        urls = []

        def get(url):
            urls.append(url)
            return b'{"prefixes": []}'
        with mock.patch.object(tgc, "gcs_get", get):
            tgc.upload_runs(time.time() - DAY)
        self.assertTrue(urls[0].startswith("https://storage.googleapis.com/storage/v1/b/kubernetes-ci-logs/o?"), urls)

    def test_a_build_id_has_at_most_20_digits(self):  # HC21
        d = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, d)
        path = os.path.join(d, "c.json")
        run = {"commit": "c" * 40, "started": 1_790_000_000, "finished": 1_790_000_005, "recipe": ttc.recipe_of("c" * 40)}
        for build, ok in (("9" * 20, True), ("9" * 21, False)):
            with open(path, "w") as f:
                json.dump({"format": tgc.FORMAT, "after": 1, "runs": [dict(run, build=build)], "configs": {},
                           "group_changes": {}, "failed": {}}, f)
            if ok:
                self.assertEqual(tgc.load(path)["runs"][0]["build"], build)
            else:
                with self.assertRaises(ValueError):
                    tgc.load(path)


# ------------------------------------------------------------ the fetch and cross_check, with TestGrid's config over time

class OverTime(ttc.ConfigWorld):
    """Runs of the fetch (and cross_check) with TestGrid's config over time in the archive's state
    (test_config_history.ConfigOverTime's history: each (when, dashboards) a run of the upload job,
    its config not known when dashboards is None; the file the fetch reads holds the last again)."""

    def history(self, *configs):
        return tch.ConfigOverTime.history(self, *configs)

    def tab(self, entry, **more):
        return {"dash": {"t": entry, **more}}

    def no_gap(self, **parts):
        """The tab scanned a quarter of an hour ago: no history gap to search."""
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR / 4, "oldest": self.now - HOUR / 2}}, **parts)

    def watch(self, b, tabs=("t",)):
        rec = dict(self.watched(b), tabs=[{"dashboard": "dash", "tab": t, "tab_status": "FAILING", "red_cells": []}
                                          for t in tabs])
        return {f"job/{b}": {"rec": rec, "first_seen": self.now - HOUR}}

    def fetched(self, path):
        self.fetch_with(path)
        return self.runs()[-1]

    def older_running_and_newer_done(self):
        """A build made 10 h ago still running, and one made 9 h ago that passed in an hour: TestGrid
        reads the newer one's column again while the older one runs."""
        hung, held = snowflake(self.now - 10 * HOUR), snowflake(self.now - 9 * HOUR)
        self.srv.route(f"/bucket/logs/job/{hung}/started.json", (200, {}, {"timestamp": int(self.now - 10 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{held}/started.json", (200, {}, {"timestamp": int(self.now - 9 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{held}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 8 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{held}/podinfo.json", (200, {}, r9.POD_OK))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, hung, held)])
        return hung, held

    def started_before_made(self, b, days):
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(frl.created(b) - days * DAY)}))


class ConfigReadNow(OverTime):
    """Which config the run takes as read now, and what it says of the runs not known yet."""

    def test_only_runs_not_known_yet(self):  # F438, F439, F560, HR17
        self.no_gap()
        run_ = self.fetched(self.history((self.now - 30 * DAY, None), (self.now - 20 * DAY, None)))
        warning = next(w for w in run_["warnings"] if "runs of its upload job" in w)
        self.assertIn(f"of 2 runs of its upload job, from {frl.iso(int(self.now - 30 * DAY) - 5)} to "
                      f"{frl.iso(int(self.now - 20 * DAY) - 5)}", warning)
        self.assertIsNone(run_["testgrid_config"])

    def test_a_later_run_not_known_keeps_the_config_from_being_read_now(self):  # F293, F438, F440, F562, HR01, HR15
        self.no_gap()
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, None)))
        self.assertIsNone(run_["testgrid_config"])
        self.assertNotIn("testgrid_config", self.state())

    def test_an_earlier_run_not_known_does_not(self):  # F560, F562, HR02, HR04
        self.no_gap()
        run_ = self.fetched(self.history((self.now - 30 * DAY, None), (self.now - 20 * DAY, self.tab(ttc.entry("")))))
        source = run_["testgrid_config"]["source"]
        self.assertEqual((source["upload"], source["uploaded"]), (str(snowflake(self.now - 20 * DAY)), int(self.now - 20 * DAY)))
        self.assertEqual(run_["testgrid_config"]["read"], run_["started"])

    def test_the_latest_config_known_is_the_tabs_config(self):  # HR05
        # (the state's own record of the tab's config hides kubetest.Test, which the table shows; the
        # latest config known shows it: the tab agrees with its config)
        self.put_state(tabs={"dash#t": {"scan": self.now - HOUR / 4, "oldest": self.now - HOUR / 2}},
                       tab_configs={"dash#t": {"config": ttc.entry(self.HIDE), "read": self.now - HOUR}})
        self.show([self.visible], {"job.Pod": ([1], [""]), "kubetest.Test": ([1], [""])}, start=int(self.now - HOUR))
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, None)))
        self.assertNotIn("does not match the tab", " ".join(run_["warnings"]))

    def test_the_history_is_pruned(self):  # F070, HK28
        self.no_gap()
        self.fetched(self.history((self.now - 100 * DAY, self.tab(ttc.entry(""))), (self.now - 90 * DAY, self.tab(E2)),
                                  (self.now - 20 * DAY, self.tab(E3))))
        self.assertEqual([u["build"] for u in self.state()["config_uploads"]],
                         [str(snowflake(self.now - 90 * DAY)), str(snowflake(self.now - 20 * DAY))])


class JudgedOverTime(OverTime):
    """Builds judged by every config their tab had while TestGrid may have shown them."""

    def test_a_watched_build_is_read_with_the_most_days_of_its_configs(self):  # F241, F381, HA07, HE06, HE08
        # (it started two days before it was made: too old for one day of results, not for 15)
        self.started_before_made(self.b, 2)
        self.no_gap(watch=self.watch(self.b))
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                                         (self.now - 10 * HOUR, self.tab(ttc.entry("", days_of_results=1)))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_gap_build_is_read_with_the_most_days_of_its_configs(self):  # F241, F381, HA10, HE06, HE08
        self.started_before_made(self.b, 2)
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                                         (self.now - 10 * HOUR, self.tab(ttc.entry("", days_of_results=1)))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_watched_build_is_read_again_after_its_groups_config_changed(self):  # HA09
        tch.ConfigOverTime.late_junit(self, 20 * HOUR)
        self.no_gap(watch=self.watch(self.b))
        changed = dict(ttc.entry(""), group_digest="1" * 64)
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                                         (self.now - 10 * HOUR, self.tab(changed))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_watched_build_on_a_tab_its_config_no_longer_has(self):  # HR07
        # (the tab, gone from the config 10 hours ago, hid the build's red rows while it was shown)
        self.no_gap(watch=self.watch(self.b, tabs=("gone",)))
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""), gone=ttc.entry(self.HIDE))),
                                         (self.now - 10 * HOUR, self.tab(ttc.entry("")))))
        self.assertEqual(run_["outcomes"], {"not-red": [f"job/{self.b}"]})

    def test_a_watched_build_on_two_tabs_is_read_with_the_longer_days(self):  # F283
        self.started_before_made(self.b, 2)
        self.no_gap(watch=self.watch(self.b, tabs=("t", "t2")))
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry("", days_of_results=1),
                                                                         t2=ttc.entry("", days_of_results=15)))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_gap_build_is_read_again_after_the_config_of_either_of_its_tabs_changed(self):  # HA15
        tch.ConfigOverTime.late_junit(self, 20 * HOUR)
        self.srv.summary("dash", {"t": "FAILING", "t2": "PASSING"})
        self.srv.table("dash", "t2", ttc_table("bucket/logs/job", [self.visible], {"job.Pod": ([1], [""]),
                                                                                  "kubetest.Up": ([1], [""])},
                                               start=int(self.now - HOUR) * 1000))
        self.put_state(tabs={key: {"scan": self.now - 40 * HOUR, "oldest": self.now - 60 * HOUR}
                             for key in ("dash#t", "dash#t2")})
        changed = dict(ttc.entry(""), group_digest="1" * 64)  # (t's test group changed; t2's did not)
        run_ = self.fetched(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""), t2=ttc.entry("", name="job2"))),
                                         (self.now - 10 * HOUR, self.tab(changed, t2=ttc.entry("", name="job2")))))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_gap_builds_column_is_held_by_its_jobs_older_column(self):  # F652, F724, F726
        hung, held = self.older_running_and_newer_done()
        run_ = self.fetched(self.config(""))
        self.assertIn(f"job/{held}", run_["outcomes"].get("watching", []))


    def test_a_watched_build_is_judged_with_the_runs_timelines(self):  # F468, F724, F726, HA08, HR13
        # (the run's timelines, made as of 8 h before it began, saw a hung older build still running)
        hung, b = snowflake(self.now - 31 * HOUR), snowflake(self.now - 29 * HOUR)
        self.srv.route(f"/bucket/logs/job/{hung}/started.json", (200, {}, {"timestamp": int(self.now - 31 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/started.json", (200, {}, {"timestamp": int(self.now - 29 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                       (200, {}, {"result": "SUCCESS", "passed": True, "timestamp": int(self.now - 28 * HOUR)}))
        self.srv.route(f"/bucket/logs/job/{b}/podinfo.json", (200, {}, r9.POD_OK))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, hung, b)])
        self.no_gap(watch=self.watch(b))

        class Earlier(frl.Timelines):
            def __init__(self, now):
                super().__init__(now - 8 * HOUR)
        with mock.patch.object(frl, "Timelines", Earlier):
            run_ = self.fetched(self.config(""))
        self.assertEqual(run_["outcomes"], {"watching": [f"job/{b}"]})

class StateRecords(OverTime):
    """state.json's records the new code checks, and a damaged run report."""

    def test_a_mismatch_that_opened_and_closed_at_once_is_kept(self):  # F711
        self.no_gap(tab_configs={"dash#t": {"config": ttc.entry(""), "read": self.now - HOUR, "mismatch": {
            "detail": "d", "since": self.now - DAY, "until": self.now - DAY}}})
        self.fetch()
        self.assertNotIn("dropped a malformed config mismatch record", " ".join(self.runs()[-1]["warnings"]))

    def test_a_config_record_uploaded_at_no_time_is_dropped(self):  # HL02
        self.no_gap(testgrid_config={"read": self.now - HOUR, "source": {"test_infra": "a" * 40, "uploaded": "yesterday"}})
        self.fetch()
        self.assertIn("dropped a malformed testgrid_config record", " ".join(self.runs()[-1]["warnings"]))

    def test_a_run_report_whose_config_was_read_at_no_time(self):  # HW01
        d = self.earlier_run(self.now - HOUR)
        report = frl.read_json(os.path.join(d, "run.json"))
        report["testgrid_config"] = {"source": {"test_infra": "a" * 40}, "read": None}
        frl.write_json(os.path.join(d, "run.json"), report)
        self.no_gap()
        self.fetch()
        self.assertEqual(self.runs()[-1]["errors"], [])


class CrossCheckedOverTime(OverTime):
    """cross_check judges builds TestGrid no longer lists by the config over time in state.json,
    as the fetch does."""

    def setUp(self):
        super().setUp()
        self.earlier_run(self.now - 3 * DAY)  # (the archive began before the builds: cross_check judges them)

    def without_history(self):
        state = self.state()
        for k in ("config_uploads", "config_entries"):
            state.pop(k, None)
        frl.write_json(os.path.join(self.root, "state.json"), state)

    def without_run_configs(self):
        run_path = os.path.join(self.root, self.runs()[-1]["dir"], "run.json")
        report = frl.read_json(run_path)
        for t in report["tabs"]:
            t.pop("config", None)
        frl.write_json(run_path, report)

    def test_a_state_json_cross_check_cannot_read(self):  # HX01, X013
        self.assertEqual(self.fetch_with(self.config(self.HIDE)), 0)
        for damaged in (b"{", b"[]"):
            with open(os.path.join(self.root, "state.json"), "wb") as f:
                f.write(damaged)
            code, out = self.check()
            self.assertEqual(code, 0, out)

    def test_a_tab_its_config_did_not_have_once(self):  # X021
        path = self.history((self.now - 30 * DAY, {"dash": {"u": ttc.entry("")}}),
                            (self.now - 20 * DAY, self.tab(ttc.entry(self.HIDE))))
        self.fetch_with(path)
        code, out = self.check()
        self.assertEqual(code, 0, out)

    def test_the_days_of_a_config_without_its_history(self):  # HE07, X027, X028, X030
        self.started_before_made(self.b, 2)
        self.fetch_with(self.config("", days_of_results=1))
        self.assertIsNone(self.bdir(self.b))
        self.without_history()
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)

    def test_the_days_of_the_history_without_the_runs_config(self):  # F381, HE06, HE08, HX05, X027, X029, X031
        self.started_before_made(self.b, 2)
        self.fetch_with(self.config("", days_of_results=1))
        self.without_run_configs()
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)

    def test_the_most_days_of_the_configs_it_was_shown_under(self):  # F241, F381, HA10, HE06, HE08, HX05
        self.started_before_made(self.b, 2)
        self.fetch_with(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))),
                                     (self.now - 10 * HOUR, self.tab(ttc.entry("", days_of_results=1)))))
        shutil.rmtree(self.bdir(self.b))
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_a_change_of_its_test_groups_config_reads_it_again(self):  # HX03, HX07, X039
        tch.ConfigOverTime.late_junit(self, 20 * HOUR)
        changed = dict(ttc.entry(""), group_digest="1" * 64)
        self.fetch_with(self.history((self.now - 30 * DAY, self.tab(ttc.entry(""))), (self.now - 10 * HOUR, self.tab(changed))))
        shutil.rmtree(self.bdir(self.b))
        code, out = self.check()
        self.assertIn(f"MISSED red build job/{self.b}", out)

    def test_an_older_column_of_its_job_holds_it(self):  # F724, F726, X038
        hung, held = self.older_running_and_newer_done()
        self.no_gap()
        self.fetch_with(self.config(""))
        code, out = self.check()
        self.assertIn(f"job/{held}: TestGrid no longer lists it and it is not decided yet", out)

    def test_only_the_jobs_own_tabs_count(self):  # HX11
        # (another job's tab of 15 days of results does not make the build's own one-day tab longer)
        self.started_before_made(self.b, 2)
        other = snowflake(self.now - HOUR)
        self.srv.summary("dash", {"t": "FAILING", "u": "PASSING"})
        self.srv.table("dash", "u", ttc_table("bucket/logs/other", [other], {"other.Overall": ([1], [""])},
                                              start=int(self.now - HOUR) * 1000))
        self.no_gap()
        self.fetch_with(self.config_of({"dash": {"t": ttc.entry("", days_of_results=1),
                                                 "u": ttc.entry("", name="other", gcs_prefix="bucket/logs/other")}}))
        code, out = self.check()
        self.assertNotIn(f"MISSED red build job/{self.b}", out)


class MoreEdges(Reads):
    """Edges of the other builds' reads, and of the history kept."""

    def test_an_older_build_finished_as_its_last_read_may_happen(self):  # F572, F576, F583
        # (hung past its deadline, 16 h ago; finished.json written exactly a read gap after it, as the
        # read after the deadline may still happen: read then, its Pod row waits an hour more)
        self.build(40 * HOUR, finished=MINUTE + DAY + GAP)
        made = frl.created(snowflake(self.now - 38 * HOUR))
        b = self.build(38 * HOUR, finished=HOUR, podinfo=HOUR + MINUTE,
                       junits=[("junit_01.xml", self.now - 12 * HOUR - made, FAILING)])
        self.assertEqual(self.judged(b), "red")

    def test_merging_an_empty_listing_is_not_a_crash(self):  # F411
        h = frl.ConfigHistory()
        merge(h, [upload_run(NOW, "a"), upload_run(NOW + 100, "b", finished=False)], built(a={"d#t": E1}))
        merge(h, [], {}, after=NOW + 50)  # (nothing listed now: b, which still ran before, failed)
        self.assertEqual([r["tabs"] for r in h.runs], [{"d#t": E1}])

    def test_every_entry_of_a_history_is_told_apart(self):  # HH33
        entries = {f"d#t{i}": ttc.entry(f"width={i}") for i in range(40)}
        h = frl.ConfigHistory()
        merge(h, [upload_run(NOW, "a")], {"a" * 40: (entries, {})})
        uploads, kept = h.encode()
        self.assertEqual(frl.ConfigHistory(uploads, kept).runs[0]["tabs"], entries)
