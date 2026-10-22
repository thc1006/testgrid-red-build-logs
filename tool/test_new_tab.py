"""A tab the archive keeps no scan of, such as one new to it or one it forgot after the tab was unlisted: GCS is
searched for the builds it no longer shows from the archive's start (less the tab's longest job time), but no further
back than GIVE_UP_DAYS, and the run's warnings say that the archive keeps no scan of the tab, and since when a run looks
for them."""
import contextlib
import io
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_red_logs as frl  # noqa: E402
import test_mutation_killers_r3 as r3  # noqa: E402  (a module import: its tests are not collected again here)
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402
from test_mutation_killers_r4 import run_folder  # noqa: E402


class NewTab(r3.World):
    def show_recent(self):
        visible = snowflake(self.now - HOUR)
        self.show([visible], {"job.Overall": ([1], [""])}, start=int(self.now - HOUR))
        return visible

    def one_red_build_in_gcs(self, age=3 * DAY):
        """An archive that old, a tab new to it, and one red build in GCS from 2 days ago the tab no longer shows."""
        run_folder(self.root, self.now - age)
        visible = self.show_recent()
        red = snowflake(self.now - 2 * DAY)
        self.gcs_build(red, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        return red

    def test_a_new_tab_in_an_old_archive_is_searched_give_up_days_back_only(self):
        run_folder(self.root, self.now - (2 * frl.GIVE_UP_DAYS + 2) * DAY)  # (the archive is older than that)
        visible = self.show_recent()
        # (just past it, by more than the listing's slack, and a day inside)
        old = snowflake(self.now - frl.GIVE_UP_DAYS * DAY - (frl.BACKFILL_SLACK_HOURS + 3) * HOUR)
        recent = snowflake(self.now - (frl.GIVE_UP_DAYS - 1) * DAY)
        for b in (old, recent):
            self.gcs_build(b, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (old, recent, visible)])
        self.assertEqual(self.fetch(), 0)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{recent}"]})

    def gap_warnings(self):
        """The gap warning of the tab in a run with --list-only, then in a run, each with the earliest and the latest
        time the run may have started at."""
        said = "dash#t: TestGrid only shows builds since"
        err, before = io.StringIO(), time.time()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(err):
            frl.main(["--archive", self.root, "--dashboard", "dash", "--testgrid", self.srv.url, "--gcs", self.srv.url,
                      "--list-only"])
        (listed,) = [w for w in err.getvalue().splitlines() if said in w]
        self.assertEqual(self.fetch(), 0)
        (ran,) = [w for w in self.runs()[-1]["warnings"] if said in w]
        start = frl.folder_time(os.path.basename(frl.run_dirs(self.root)[-1]))  # (the run's folder: its start)
        return [(listed, before, time.time()), (ran, start, start)]

    def test_its_warnings_say_the_archive_keeps_no_scan_of_it(self):
        self.one_red_build_in_gcs()
        start = frl.folder_time(os.path.basename(frl.run_dirs(self.root)[0]))  # (the archive's first run)
        warnings = self.gap_warnings()
        since = frl.iso(start - self.tab()["max_job_hours"] * HOUR)  # (less the tab's longest job time)
        said = f"and the archive keeps no scan of the tab: a run looks for them from {since};"
        for w, _, _ in warnings:
            self.assertIn(said, w)  # fails: the last scan (

    def test_in_an_archive_older_than_give_up_days_they_name_give_up_days_before_the_run(self):
        self.one_red_build_in_gcs((2 * frl.GIVE_UP_DAYS + 2) * DAY)
        back = frl.GIVE_UP_DAYS * DAY
        for w, earliest, latest in self.gap_warnings():
            since = re.search(r"a run looks for them from (\S+);", w)
            self.assertTrue(since and frl.iso(earliest - back) <= since[1] <= frl.iso(latest - back), w)  # fails: the last scan (

    def test_a_tab_the_archive_keeps_a_scan_of_is_named_by_its_last_scan(self):
        # (as before: the gap is measured from the tab's last scan)
        scan = self.now - 40 * HOUR
        self.put_state(tabs={"dash#t": {"scan": scan, "oldest": self.now - 60 * HOUR}})
        visible = self.show_recent()
        red = snowflake(self.now - 30 * HOUR)
        self.gcs_build(red, finished=self.failed())
        self.srv.gcs_listing("bucket", [f"logs/job/{b}/finished.json" for b in (red, visible)])
        for w, _, _ in self.gap_warnings():
            self.assertIn(f"but the last scan ({frl.iso(scan)}) needed them from", w)
        self.assertEqual(self.outcomes(), {"backfilled": [f"job/{red}"]})
