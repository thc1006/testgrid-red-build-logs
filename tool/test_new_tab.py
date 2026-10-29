"""A tab the archive keeps no scan of, such as one new to it or one it forgot after the tab was unlisted: GCS is
searched for the builds it no longer shows from the archive's start (less the tab's longest job time), but no further
back than GIVE_UP_DAYS."""
import os
import sys

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
