"""An archived build whose comparisons with GCS keep failing: GCS refuses a file, or serves one that cannot be
verified. Each failed comparison is an error of the run, and meta.json keeps when the first run that failed began.
A run that fails once 14 days have passed since gives up on the build, with one error: the build is final, and
meta.json keeps that run's failure. It is still compared at least once a week, but a failure on GCS's side there
is no error, and cross_check.py only notes what GCS holds of it. A failure of the disk, or of a bug in the tool, is
an error that neither starts nor ends the failures. Before, a final build that kept failing failed every
run, and cross_check.py with it, so no run was done with its sevenths; and one not final was made final on
its first error 14 days after it was archived, whatever came before.

Run: cd tool && python -m unittest test_give_up_comparing -v
"""
import contextlib
import http.client
import io
import os
import sys
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import cross_check as cc  # noqa: E402
import fetch_red_logs as frl  # noqa: E402
from test_fetch_red_logs import md5_b64, send  # noqa: E402
from test_http_pool import UNPARSABLE, unparsable  # noqa: E402
from test_integrity import DAY, ArchiveWorld, obj, slurp, snowflake  # noqa: E402

ALL = set(range(7))
SINCE = "comparison_failing_since"
GIVEN_UP = "comparison_given_up"
ERR403 = "HTTPError: HTTP Error 403: Forbidden"


def with_header(data, name, value):
    """A GCS object served with one more header."""
    def serve(h):
        send(h, 200, {"x-goog-stored-content-encoding": "identity", "x-goog-stored-content-length": str(len(data)),
                      "x-goog-hash": f"crc32c=AAAAAA==,md5={md5_b64(data)}", "x-goog-generation": "1",
                      "Content-Length": str(len(data)), name: value}, data)
    return serve


def shown_not_served(data, generation):
    """A GCS object that a HEAD request shows, but a GET is refused (403)."""
    def serve(h):
        if h.command == "HEAD":
            return obj(data, generation)(h)
        send(h, 403, {}, b"")
    return serve


class World(ArchiveWorld):
    """A build final since its first run (it finished 3 days before), whose log GCS then refuses."""

    def setUp(self):
        super().setUp()
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.srv.route(self.base + "/build-log.txt", (403, {}, b""))

    def fetch_in(self, sevenths=ALL):
        with mock.patch.object(frl, "gcs_sevenths", return_value=sevenths):
            return self.fetch()

    def verify(self, sevenths=ALL):
        """cross_check.py on the last run, as the workflow runs it: (exit code, its verification.json)."""
        self.out = io.StringIO()
        with mock.patch.object(frl, "gcs_sevenths", return_value=sevenths), contextlib.redirect_stdout(self.out):
            code = cc.main(["--archive", self.root, "--testgrid", self.srv.url, "--gcs", self.srv.url,
                            "--hours", "48", "--record"])
        return code, frl.read_json(os.path.join(self.root, frl.run_dirs(self.root)[-1], "verification.json"))

    def edit(self, **fields):
        path = os.path.join(self.bdir(), "meta.json")
        frl.write_json(path, dict(frl.read_json(path), **fields))

    def failing_since(self, days):
        """meta.json as a run whose comparison failed `days` ago leaves it (the build made before): that time."""
        since = time.time() - days * DAY
        self.edit(**{SINCE: since}, created=min(self.meta()["created"], since - DAY))
        return since

    def given_up(self, days=15):
        """meta.json as the run that gave up on it, its comparisons failing since `days` ago, leaves it: that time."""
        since = self.failing_since(days)
        self.edit(final=True, **{GIVEN_UP: ERR403})
        return since

    def index(self):
        return slurp(os.path.join(self.root, "INDEX.md"))


class FailedComparisons(World):
    def test_the_first_failure_is_an_error_and_its_time_is_kept(self):
        before = self.meta()
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed: HTTPError", run["errors"][0]["error"])
        self.assertIs(run["gcs_compared"], False)
        self.assertEqual(self.meta(), dict(before, **{SINCE: run["started"]}))  # fails: as before

    def test_a_later_failure_keeps_that_time(self):
        self.assertEqual(self.fetch_in(), 1)
        first = self.meta().get(SINCE)
        self.assertIsNotNone(first)  # fails: None
        self.assertEqual(self.fetch_in(), 1)
        self.assertEqual(self.meta().get(SINCE), first)

    def test_one_that_succeeds_drops_it(self):
        self.assertEqual(self.fetch_in(), 1)
        self.assertIn(SINCE, self.meta())  # fails: not in it
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch_in(), 0)
        self.assertNotIn(SINCE, self.meta())

    def test_a_disk_error_recording_the_first_failure_is_said_too(self):
        real = frl.temp_path

        def full(path):
            if path == os.path.join(self.bdir(), "meta.json"):
                raise OSError(28, "No space left on device")
            return real(path)
        with mock.patch.object(frl, "temp_path", full):
            self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed: HTTPError: HTTP Error 403", run["errors"][0]["error"])
        self.assertIn("(and its meta.json could not record that: LocalIOError: ", run["errors"][0]["error"])
        # fails: not in it
        self.assertEqual((run["gcs_compared"], self.meta().get(SINCE)), (False, None))
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_a_time_that_is_not_one_starts_the_failures_again(self):
        self.edit(**{SINCE: "yesterday"})
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed: HTTPError", run["errors"][0]["error"])
        self.assertEqual(self.meta()[SINCE], run["started"])  # fails: 'yesterday'
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_a_disk_error_recording_the_give_up_leaves_it_to_the_next_run(self):
        since = self.failing_since(15)
        real = frl.temp_path

        def full(path):
            if path == os.path.join(self.bdir(), "meta.json"):
                raise OSError(28, "No space left on device")
            return real(path)
        with mock.patch.object(frl, "temp_path", full):
            self.assertEqual(self.fetch_in(), 1)
        run, m = self.runs()[-1], self.meta()
        self.assertIn("(and its meta.json could not record that: LocalIOError: ", run["errors"][0]["error"])
        # fails: not in it
        self.assertEqual((run["outcomes"], run["gcs_compared"], m.get(GIVEN_UP), m[SINCE]), ({}, False, None, since))
        self.assertEqual(self.fetch_in(), 1)
        self.assertEqual(self.runs()[-1]["outcomes"], {"comparison-given-up": [self.key]})  # fails: {}

    def test_control_a_disk_error_comparing_one_given_up_on_is_still_an_error(self):
        self.given_up()
        before = self.meta()
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
        real = os.replace

        def full(src, dst):
            if "build-log" in dst:
                raise OSError(28, "No space left on device")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", side_effect=full):
            self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertIn("comparing the archived copy with GCS failed: LocalIOError", run["errors"][0]["error"])
        self.assertEqual((run["outcomes"], run["gcs_compared"], self.meta()), ({}, False, before))

    def test_a_note_it_had_stays_through_the_give_up(self):
        note = "GCS had neither finished.json nor build-log.txt 96h after the build started"
        self.edit(note=note)
        self.failing_since(15)
        self.assertEqual(self.fetch_in(), 1)
        m = self.meta()
        self.assertEqual((m.get("note"), m.get(GIVEN_UP)), (note, ERR403))  # fails: (the note, None)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual((m.get("note"), GIVEN_UP in m), (note, False))

    def test_the_cause_of_a_failure_of_one_given_up_on_is_kept(self):
        # (GCS shows a new log, and serves other bytes than it says it stores: the cause comes after the build's URL)
        self.given_up()
        new = b"new content"

        def tampered(h):
            if h.command == "HEAD":
                return obj(new, "2")(h)
            send(h, 200, {"x-goog-stored-content-encoding": "identity", "x-goog-stored-content-length": str(len(new)),
                          "x-goog-hash": f"crc32c=AAAAAA==,md5={md5_b64(new)}", "x-goog-generation": "2",
                          "Content-Length": str(len(new))}, new.upper())
        self.srv.route(self.base + "/build-log.txt", tampered)
        with mock.patch.object(frl, "BACKOFF", 0):
            self.assertEqual(self.fetch_in(), 0)  # fails: 1
        error = (self.runs()[-1].get("given_up_still_failing") or {}).get(self.key, "")
        self.assertTrue(error.startswith("IntegrityError: build-log.txt: got ") and "GCS stores" in error, error)
        # fails: ""
        self.assertTrue(any(error[:150] in w for w in self.runs()[-1]["warnings"]))  # fails: False

    def test_control_failures_for_an_hour_less_than_14_days_are_still_errors(self):
        since = self.failing_since(14 - 1 / 24)
        self.assertEqual(self.fetch_in(), 1)
        run, m = self.runs()[-1], self.meta()
        self.assertIn("comparing the archived copy with GCS failed: HTTPError", run["errors"][0]["error"])
        self.assertEqual((run["outcomes"], run["gcs_compared"], m.get(GIVEN_UP), m[SINCE]), ({}, False, None, since))

    def test_failures_for_14_days_give_it_up_with_one_error(self):
        since = self.failing_since(14 + 1 / 24)
        self.assertEqual(self.fetch_in(), 1)
        run, m = self.runs()[-1], self.meta()
        self.assertEqual(run["outcomes"], {"comparison-given-up": [self.key]})  # fails: {}
        self.assertEqual((m["final"], m[SINCE], m.get(GIVEN_UP)), (True, since, ERR403))  # fails: (True, since, None)
        self.assertEqual([e["error"] for e in run["errors"]],  # fails: the failed comparison's
                         [f"no comparison with GCS has succeeded since {frl.iso(since)}: after 14 days of failures, the "
                          "archived copy is still compared at least once a week, but from now on a failure on GCS's side "
                          f"is a warning: {ERR403}"])
        self.assertIs(run["gcs_compared"], True)  # fails: False

    def test_one_given_up_on_that_still_fails_is_no_error(self):
        self.given_up()
        before = self.meta()
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], run["errors"], run["gcs_compared"]),  # fails: ({}, [its error], False)
                         ({"given-up-still-failing": [self.key]}, [], True))
        self.assertEqual([w for w in run["warnings"] if self.key in w],  # fails: []
                         [f"comparisons with GCS still fail for 1 archived build the runs gave up on "
                          f"({self.key}: {ERR403}): each is compared again at least once a week"])
        self.assertEqual(run.get("given_up_still_failing"), {self.key: ERR403})  # fails: None
        self.assertEqual(run["serious_warnings"], [])
        self.assertEqual(self.meta(), before)

    def test_one_given_up_on_is_kept_compared_once_gcs_serves_it(self):
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual((self.runs()[-1]["outcomes"], m["final"]), ({"already-archived": [self.key]}, True))
        self.assertNotIn(GIVEN_UP, m)  # fails: in it
        self.assertNotIn(SINCE, m)  # fails: in it

    def test_the_note_older_code_wrote_counts_too(self):
        self.edit(note="not compared with GCS after 14 days of errors: HTTPError: HTTP Error 500: Internal Server Error")
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        self.assertEqual(self.runs()[-1]["outcomes"], {"given-up-still-failing": [self.key]})  # fails: {}

    def test_one_not_final_archived_15_days_before_failing_once_is_an_error(self):
        # (one not final is compared on every run; before, its first failure 14 days after it was archived made it
        # final, and its note said "after 14 days of errors")
        self.edit(final=False, archived_at=time.time() - 15 * DAY)
        self.assertEqual(self.fetch_in(set()), 1)  # fails: 0
        m = self.meta()
        self.assertEqual(self.runs()[-1]["outcomes"], {})  # fails: {'stopped-comparing': [...]}
        self.assertEqual((m["final"], m.get("note")), (False, None))  # fails: (True, the note)
        self.assertIn(SINCE, m)  # fails: not in it

    def test_one_without_finished_json_failing_once_after_14_days_is_final_once_compared(self):
        # (compared on every run until 14 days after it was archived; before, its first failure past that made it
        # final, and its note said "after 14 days of errors", though every comparison before had succeeded)
        self.srv.route(self.base + "/finished.json", (404, {}, b""))
        self.edit(final=False, settled=False, finished=None, archived_at=time.time() - 15 * DAY)
        self.assertEqual(self.fetch_in(set()), 1)  # fails: 0
        self.assertIsNone(self.meta().get("note"))  # fails: not compared with GCS after 14 days of errors: ...
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch_in(set()), 0)
        m = self.meta()
        self.assertEqual((m["final"], m.get("note"), m.get(SINCE)), (True, None, None))  # fails: (True, the note, None)

    def test_control_a_disk_error_neither_starts_nor_ends_the_failures(self):
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
        real = os.replace

        def full(src, dst):
            if "build-log" in dst:
                raise OSError(28, "No space left on device")
            return real(src, dst)
        with mock.patch.object(frl.os, "replace", side_effect=full):
            self.assertEqual(self.fetch_in(), 1)
            self.assertNotIn(SINCE, self.meta())
            since = self.failing_since(15)
            self.assertEqual(self.fetch_in(), 1)
        m = self.meta()
        self.assertIn("comparing the archived copy with GCS failed: LocalIOError", self.runs()[-1]["errors"][0]["error"])
        self.assertEqual((m["final"], m.get(GIVEN_UP), m[SINCE]), (True, None, since))

    def test_control_a_bug_does_not_start_the_failures(self):
        # (a KeyError, say, is this code's doing, not GCS's: an error, as a disk error is)
        with mock.patch.object(frl, "unchanged_in_gcs", side_effect=KeyError("size")):
            self.assertEqual(self.fetch_in(), 1)
        self.assertIn("comparing the archived copy with GCS failed: KeyError: 'size'", self.runs()[-1]["errors"][0]["error"])
        self.assertNotIn(SINCE, self.meta())

    def test_control_a_bug_comparing_one_given_up_on_is_still_an_error(self):
        since = self.given_up()
        with mock.patch.object(frl, "unchanged_in_gcs", side_effect=TypeError("a bug")):
            self.assertEqual(self.fetch_in(), 1)
        run, m = self.runs()[-1], self.meta()
        self.assertIn("comparing the archived copy with GCS failed: TypeError: a bug", run["errors"][0]["error"])
        self.assertEqual((run["outcomes"], run["gcs_compared"], m[SINCE], m[GIVEN_UP]), ({}, False, since, ERR403))

    def test_a_response_http_client_cannot_parse_is_gcss_doing(self):
        since = self.failing_since(15)
        self.srv.route(self.base + "/build-log.txt", with_header(b"log\n", UNPARSABLE, "1"))
        with mock.patch.object(http.client, "parse_headers", unparsable()):
            self.assertEqual(self.fetch_in(), 1)
            run, m = self.runs()[-1], self.meta()
            self.assertEqual(run["outcomes"], {"comparison-given-up": [self.key]})  # fails: {}
            self.assertIn("build-log.txt: a response that cannot be read (TypeError: ", m.get(GIVEN_UP, ""))
            # fails: not in it
            self.assertEqual((m[SINCE], run["gcs_compared"]), (since, True))  # fails: (since, False)
            self.assertEqual(self.fetch_in(), 0)  # fails: 1
            self.assertEqual(self.runs()[-1]["outcomes"], {"given-up-still-failing": [self.key]})  # fails: {}

    def test_control_a_bug_recording_the_first_failure_is_an_error(self):
        real = frl.put_in_place

        def buggy(target, meta, copies=()):
            if SINCE in meta and not copies:
                raise TypeError("a bug")
            return real(target, meta, copies)
        with mock.patch.object(frl, "put_in_place", buggy):
            self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertTrue(run["errors"][0]["error"].startswith("comparing the archived copy with GCS failed: HTTPError"))
        self.assertEqual((run["gcs_compared"], SINCE in self.meta()), (False, False))
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_a_bug_writing_what_a_success_found_is_an_error(self):
        # (the failures go on: meta.json still says when they began)
        since = self.failing_since(3)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        real = frl.put_in_place

        def buggy(target, meta, copies=()):
            if SINCE not in meta and not copies:
                raise TypeError("a bug")
            return real(target, meta, copies)
        with mock.patch.object(frl, "put_in_place", buggy):
            self.assertEqual(self.fetch_in(), 1)  # fails: 0
        run = self.runs()[-1]
        self.assertEqual([e["error"] for e in run["errors"]],
                         ["comparing the archived copy with GCS failed: TypeError: a bug"])  # fails: []
        self.assertEqual((run["gcs_compared"], self.meta()[SINCE]), (False, since))  # fails: (True, since)
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_the_warning_is_not_serious_for_what_gcs_says(self):
        self.given_up()

        def refuse(h):
            h.send_response(403, "Forbidden: state.json may be missing")
            h.send_header("Content-Length", "0")
            h.end_headers()
        self.srv.route(self.base + "/build-log.txt", refuse)
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        run = self.runs()[-1]
        self.assertEqual(run["serious_warnings"], [])
        self.assertTrue(any("state.json may be missing" in w for w in run["warnings"]))  # fails: False

    def test_control_a_created_that_is_not_a_time_bounds_nothing(self):
        # (meta.json as no run writes it: compared, and its failures kept, as for any build)
        since = self.failing_since(5)
        self.edit(created="yesterday")
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertEqual([e["error"].startswith("comparing the archived copy with GCS failed: HTTPError") for e in run["errors"]],
                         [True])
        self.assertEqual((run["gcs_compared"], self.meta()[SINCE]), (False, since))
        self.assertNotIn(self.key, frl.read_json(os.path.join(self.root, "state.json"))["unresolved"])

    def test_a_time_less_than_a_day_before_the_build_was_made_counts(self):
        # (as settle_flags takes a finish time: the clocks behind the two can disagree)
        since = self.failing_since(15)
        self.edit(created=since + 12 * 3600)
        self.assertEqual(self.fetch_in(), 1)
        self.assertEqual(self.runs()[-1]["outcomes"], {"comparison-given-up": [self.key]})  # fails: {}

    def test_a_time_in_the_future_starts_the_failures_again(self):
        self.edit(**{SINCE: time.time() + 30 * DAY})
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], self.meta()[SINCE]), ({}, run["started"]))  # fails: ({}, the time 30 days on)

    def test_a_time_before_the_build_was_made_starts_the_failures_again(self):
        self.edit(**{SINCE: 1.0})
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertEqual((run["outcomes"], self.meta()[SINCE]), ({}, run["started"]))  # fails: ({}, 1.0)


class Verification(World):
    def test_the_run_that_gives_up_is_done_with_its_sevenths(self):
        # (so the next run compares its own seventh only: before, each run compared them all again, and failed)
        self.failing_since(15)
        self.assertEqual(self.fetch_in(), 1)
        self.verify()
        self.assertTrue(frl.sevenths_done(self.root, frl.run_dirs(self.root)[-1]))  # fails: False

    def test_cross_check_notes_what_gcs_holds_of_one_given_up_on(self):
        self.given_up()
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        code, v = self.verify()
        self.assertTrue(any(self.key in n and "cannot read GCS: HTTPError" in n for n in v["notes"]), v)
        # fails: no such note (a problem)
        self.assertEqual((code, v["scope"]["differ"], v["scope"].get("given_up")), (0, 0, 1))  # fails: (1, 1, None)
        line = next((x for x in self.out.getvalue().splitlines() if "integrity:" in x), "")
        want = "; of those compared, 1 given up on after failed comparisons with GCS (only noted)"
        self.assertIn(want, line)  # fails: not in it

    def test_cross_check_notes_a_changed_copy_of_one_given_up_on(self):
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", shown_not_served(b"log, uploaded again\n", "2"))
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        code, v = self.verify()
        self.assertTrue(any(self.key in n and "GCS changed since it was archived" in n for n in v["notes"]), v)
        # fails: no such note (a problem)
        self.assertEqual(len([n for n in v["notes"] if self.key in n]), 1)  # fails: 0
        self.assertEqual((code, v["scope"]["differ"]), (0, 0))  # fails: (1, 1)

    def test_cross_check_notes_what_gcs_holds_of_one_with_the_older_note(self):
        self.edit(note="not compared with GCS after 14 days of errors: HTTPError: HTTP Error 500: Internal Server Error")
        code, v = self.verify()
        self.assertTrue(any(self.key in n and "cannot read GCS: HTTPError" in n for n in v["notes"]), v)
        # fails: no such note (a problem)
        self.assertEqual((code, v["scope"]["differ"]), (0, 0))  # fails: (1, 1)

    def test_cross_check_counts_an_unsettled_one_given_up_on_only_as_given_up(self):
        self.given_up()
        self.edit(settled=False)
        self.verify()
        line = next(x for x in self.out.getvalue().splitlines() if "inventory:" in x)
        self.assertIn("1 given up on after failed comparisons with GCS", line)  # fails: not in it
        self.assertNotIn("never settled", line)  # fails: 1 never settled

    def test_control_cross_check_still_checks_the_copy_on_disk_of_one_given_up_on(self):
        self.given_up()
        with open(os.path.join(self.bdir(), "build-log.txt"), "ab") as f:
            f.write(b"damage")
        code, v = self.verify()
        self.assertEqual((code, v["scope"]["differ"]), (1, 1))
        self.assertTrue(any(self.key in p and "local md5" in p for p in v["problems"]), v)

    def test_control_cross_check_reads_no_gcs_for_one_given_up_on_outside_its_seventh(self):
        self.given_up()
        hits = self.srv.hits(self.base + "/build-log.txt")
        code, v = self.verify(sevenths=set())
        self.assertEqual((code, self.srv.hits(self.base + "/build-log.txt"), "given_up" in v["scope"]), (0, hits, False))

    def test_control_cross_check_reports_one_failing_for_less_than_14_days(self):
        self.failing_since(5)
        code, v = self.verify()
        self.assertEqual((code, v["scope"]["differ"]), (1, 1))
        self.assertTrue(any(self.key in p and "cannot verify" in p for p in v["problems"]), v)


class WithoutALog(World):
    """The same build archived without a log, then given up on as GCS refuses to say whether it has one now."""

    def setUp(self):
        ArchiveWorld.setUp(self)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": {}}'))
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.runs()[-1]["outcomes"], {"archived-without-log": [self.key]})
        self.srv.route(self.base + "/build-log.txt", (403, {}, b""))

    def test_cross_check_notes_that_gcs_refuses_to_say(self):
        self.given_up()
        code, v = self.verify()
        self.assertTrue(any(self.key in n and "cannot read GCS: HTTPError" in n for n in v["notes"]), v)
        # fails: no such note (a problem)
        self.assertEqual((code, v["scope"]["differ"]), (0, 0))  # fails: (1, 1)


class WithPodinfo(World):
    """The same build archived with its podinfo.json too."""

    def setUp(self):
        ArchiveWorld.setUp(self)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": {}}'))
        self.assertEqual(self.fetch(), 0)
        self.assertTrue(self.meta()["final"])
        self.srv.route(self.base + "/build-log.txt", (403, {}, b""))

    def test_a_repair_of_its_other_file_keeps_its_failures(self):
        # (its podinfo.json damaged here, and taken again from GCS: its log is kept, not compared with GCS)
        since = self.given_up()
        with open(os.path.join(self.bdir(), "podinfo.json"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual((m.get(SINCE), m.get(GIVEN_UP), m["final"]), (since, ERR403, True))
        # fails: (None, None, True)
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        self.assertEqual(self.runs()[-1]["outcomes"], {"given-up-still-failing": [self.key]})  # fails: {}

    def test_a_repair_of_its_log_keeps_its_failures(self):
        # (its log damaged here and taken again from GCS, its podinfo.json kept, not compared: GCS refuses that)
        since = self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        with open(os.path.join(self.bdir(), "build-log.txt"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual(self.runs()[-1]["outcomes"], {"repaired": [self.key]})
        self.assertEqual((m.get(SINCE), m.get(GIVEN_UP), m["final"]), (since, ERR403, True))
        # fails: (None, None, True)
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        self.assertEqual(self.runs()[-1]["outcomes"], {"given-up-still-failing": [self.key]})  # fails: {}

    def test_a_repair_keeps_the_note_older_code_wrote(self):
        note = "not compared with GCS after 14 days of errors: HTTPError: HTTP Error 500: Internal Server Error"
        self.edit(note=note)
        with open(os.path.join(self.bdir(), "podinfo.json"), "ab") as f:
            f.write(b"damage")
        self.assertEqual(self.fetch_in(), 0)
        self.assertEqual((self.runs()[-1]["outcomes"], self.meta().get("note")), ({"repaired": [self.key]}, note))
        # fails: ({'repaired': [...]}, None)
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        self.assertEqual(self.runs()[-1]["outcomes"], {"given-up-still-failing": [self.key]})  # fails: {}

    def test_a_repair_that_cannot_read_its_podinfo_leaves_the_folder_as_it_is(self):
        # (both copies damaged here: GCS serves the log, but refuses its podinfo.json, so the repair fails and both
        # copies stay, as for a damaged log GCS refuses; once GCS serves it, a repair takes both again, which ends its
        # failures)
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        for name in ("build-log.txt", "podinfo.json"):
            with open(os.path.join(self.bdir(), name), "ab") as f:
                f.write(b"damage")
        before = {n: self.read(n) for n in os.listdir(self.bdir())}
        self.assertEqual(self.fetch_in(), 1)  # fails: 0
        self.assertIn(ERR403, self.runs()[-1]["errors"][0]["error"])
        self.assertEqual({n: self.read(n) for n in os.listdir(self.bdir())}, before)
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": {}}'))
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual((self.runs()[-1]["outcomes"], m["repaired"], SINCE in m, GIVEN_UP in m),
                         ({"repaired": [self.key]}, ["build-log.txt", "podinfo.json"], False, False))
        self.assertEqual(self.read("podinfo.json"), b'{"pod": {}}')

    def test_a_log_taken_again_by_the_run_that_gives_up_is_reported(self):
        # (GCS serves a new log, then refuses podinfo.json: the new log is in place)
        self.failing_since(15)
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch_in(), 1)
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"comparison-given-up": [self.key], "refreshed": [self.key]})  # fails: {}
        self.assertEqual(run["bytes_downloaded"], len(b"log, uploaded again\n"))  # fails: 0
        self.assertEqual(self.read("build-log.txt"), b"log, uploaded again\n")

    def test_a_log_taken_again_before_a_failure_of_one_given_up_on_is_reported(self):
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log, uploaded again\n", "2"))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        run = self.runs()[-1]
        self.assertEqual(run["outcomes"], {"given-up-still-failing": [self.key], "refreshed": [self.key]})  # fails: {}
        self.assertEqual(run["bytes_downloaded"], len(b"log, uploaded again\n"))  # fails: 0
        self.assertEqual(self.read("build-log.txt"), b"log, uploaded again\n")

    def test_cross_check_notes_a_refused_podinfo_of_one_given_up_on(self):
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", (403, {}, b""))
        code, v = self.verify()
        self.assertTrue(any(self.key in n and "cannot read GCS: HTTPError" in n for n in v["notes"]), v)
        # fails: no such note (a problem)
        self.assertEqual((code, v["scope"]["differ"]), (0, 0))  # fails: (1, 1)

    def test_a_bug_taking_its_podinfo_is_no_refusal_of_gcs(self):
        # (its podinfo.json damaged here, and a bug as the repair takes it again: an error, as for any build)
        self.given_up()
        with open(os.path.join(self.bdir(), "podinfo.json"), "ab") as f:
            f.write(b"damage")
        real = frl.take_copy

        def buggy(base, target, compress, max_bytes, name="build-log.txt"):
            if name == "podinfo.json":
                raise TypeError("a bug")
            return real(base, target, compress, max_bytes, name)
        with mock.patch.object(frl, "take_copy", buggy):
            self.assertEqual(self.fetch_in(), 1)  # fails: 0
        run = self.runs()[-1]
        self.assertTrue(any("TypeError: a bug" in e["error"] for e in run["errors"]), run["errors"])  # fails: none
        self.assertNotIn("repaired", run["outcomes"])  # fails: in it

    def test_control_a_repair_that_takes_every_file_again_ends_its_failures(self):
        self.given_up()
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        for name in ("build-log.txt", "podinfo.json"):
            with open(os.path.join(self.bdir(), name), "ab") as f:
                f.write(b"damage")
        self.assertEqual(self.fetch_in(), 0)
        m = self.meta()
        self.assertEqual((self.runs()[-1]["outcomes"], m.get(SINCE), m.get(GIVEN_UP)), ({"repaired": [self.key]}, None, None))


class NewBuild(ArchiveWorld):
    def test_control_a_bug_taking_its_podinfo_keeps_its_log(self):
        # (archived, and its podinfo.json read again by the next run, as when GCS refuses it: failing the
        # archive would lose the log, which a repair keeps)
        self.finished(ago=3 * DAY)
        self.srv.route(self.base + "/build-log.txt", obj(b"log\n"))
        self.srv.route(self.base + "/podinfo.json", obj(b'{"pod": {}}'))
        real = frl.take_copy

        def buggy(base, target, compress, max_bytes, name="build-log.txt"):
            if name == "podinfo.json":
                raise TypeError("a bug")
            return real(base, target, compress, max_bytes, name)
        with mock.patch.object(frl, "take_copy", buggy):
            self.assertEqual(self.fetch(), 0)
        m = self.meta()
        self.assertEqual((self.runs()[-1]["outcomes"], m["podinfo"], m["final"]), ({"archived": [self.key]}, None, False))


class TwoBuilds(World):
    """Two builds final since their first run, whose logs GCS then refuses."""

    def setUp(self):
        ArchiveWorld.setUp(self)
        self.other = snowflake(self.start - 3600)
        self.show([self.build, self.other], {"job.Overall": ([12, 12], ["boom", "boom"])})
        for b in (self.build, self.other):
            self.srv.route(f"/bucket/logs/job/{b}/finished.json",
                           (200, {}, {"result": "FAILURE", "timestamp": int(time.time() - 3 * DAY)}))
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", obj(b"log\n"))
        self.assertEqual(self.fetch(), 0)
        for b in (self.build, self.other):
            self.srv.route(f"/bucket/logs/job/{b}/build-log.txt", (403, {}, b""))

    def test_run_json_keeps_why_each_one_given_up_on_failed_again(self):
        for b in (self.build, self.other):
            path = os.path.join(self.bdir(b), "meta.json")
            since = time.time() - 15 * DAY
            frl.write_json(path, dict(frl.read_json(path), final=True, created=since - DAY, **{SINCE: since, GIVEN_UP: ERR403}))
        self.assertEqual(self.fetch_in(), 0)  # fails: 1
        run = self.runs()[-1]
        warnings = " ".join(run["warnings"])
        self.assertIn("comparisons with GCS still fail for 2 archived builds the runs gave up on", warnings)  # fails: not in it
        self.assertTrue(all(f"job/{b}: {ERR403}" in warnings for b in (self.build, self.other)))  # fails: False
        want = {f"job/{b}": ERR403 for b in (self.build, self.other)}
        self.assertEqual(run.get("given_up_still_failing"), want)  # fails: None


class Index(World):
    def test_the_index_marks_one_given_up_on_and_counts_it(self):
        since = self.given_up()
        self.fetch_in()
        index = self.index()
        self.assertIn("| … given up on after failed comparisons with GCS (still compared at least once a week) | 1 |",
                      index)
        # fails: no such row
        self.assertIn(f"(no comparison with GCS has succeeded since {frl.iso(since)[:10]})", index)  # fails: not in it

    def test_the_index_marks_one_with_the_older_note_too(self):
        self.edit(note="not compared with GCS after 14 days of errors: HTTPError: HTTP Error 500: Internal Server Error")
        self.fetch_in()
        index = self.index()
        self.assertIn("| … given up on after failed comparisons with GCS (still compared at least once a week) | 1 |",
                      index)
        # fails: no such row
        self.assertIn("(its comparisons with GCS failed)", index)  # fails: not in it

    def test_one_given_up_on_whose_time_is_not_one_is_marked_too(self):
        self.given_up()
        self.edit(**{SINCE: "yesterday"})
        self.fetch_in()
        self.assertIn("(its comparisons with GCS failed)", self.index())  # fails: not in it

    def test_its_index_entry_says_since_when_and_whether_it_was_given_up_on(self):
        m, now = self.meta(), time.time()
        e = frl.index_entry("job", self.build, dict(m, **{SINCE: now - 3 * DAY}), now)
        self.assertEqual((e.get("comparison_failing_since"), "comparison_given_up" in e), (now - 3 * DAY, False))
        # fails: (None, False)
        e = frl.index_entry("job", self.build, dict(m, final=True, created=now - 16 * DAY,
                                                    **{SINCE: now - 15 * DAY, GIVEN_UP: ERR403}), now)
        got = (e.get("comparison_failing_since"), e.get("comparison_given_up"))
        self.assertEqual(got, (now - 15 * DAY, True))  # fails: (None, None)

    def test_one_given_up_on_whose_time_is_before_the_build_was_made_is_marked_without_it(self):
        self.given_up()
        self.edit(**{SINCE: 1.0})
        self.fetch_in(set())
        self.assertIn("(its comparisons with GCS failed)", self.index())  # fails: not in it
        self.assertNotIn("since 1970", self.index())

    def test_an_unsettled_one_given_up_on_is_not_said_never_to_have_settled(self):
        self.given_up()
        self.edit(settled=False)
        self.fetch_in()
        self.assertNotIn("never settled", self.index())  # fails: (never settled: finished.json did not appear)


if __name__ == "__main__":
    unittest.main()
