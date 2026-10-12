"""Review wave W5b: TestGrid's config over time (testgrid_config.plan/merge/decode_uploads,
fetch_red_logs.ConfigHistory and run(), testgrid_config.sh). Each test fails on 05793d9 for
the reason its docstring gives.

Run: cd tool && python -m unittest test_review_round13 -v
"""
import os
import shutil
import stat
import sys
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_red_logs as frl  # noqa: E402
import test_config_history as tch  # noqa: E402  (module import: its tests are not collected again here)
import test_review_round10 as r10  # noqa: E402
import test_testgrid_config as ttc  # noqa: E402
import testgrid_config as tgc  # noqa: E402
from test_fetch_red_logs import DAY  # noqa: E402
from test_integrity import HOUR, snowflake  # noqa: E402

NOW = tch.NOW
run, configs, loaded = tch.run, tch.configs, tch.loaded
E1, E2, E3 = tch.E1, tch.E2, tch.E3


# ------------------------------------------------------------------ a run that never finished

class NeverFinished(unittest.TestCase):
    def test_a_run_without_finished_json_does_not_keep_the_config_before_it_in_force_forever(self):
        """ConfigHistory.eras (fetch_red_logs.py:2222-2223) ends a run's era at the NEXT run's
        finish + PICKUP, and never when that next run has no finish. A run of the upload job
        that never gets a finished.json (its pod deleted, crier or sinker racing: the port
        itself handles such builds for every other job) is listed as running for as long as
        GCS keeps it (90 days), so the config before it stays "in force" for every later
        build, though every later run that finished has replaced it (whatever b uploaded or
        not, c's upload came after a's: a's config was gone by c's end + PICKUP)."""
        h = frl.ConfigHistory()
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b", finished=False), run(NOW + 200, "c")],
                       configs(a={"d#t": E1}, b={"d#t": E2}, c={"d#t": E3})))
        a_era = next(e for e in h.eras("d#t") if e[2] == E1)
        self.assertLessEqual(a_era[1], NOW + 205 + frl.PICKUP)  # fails: inf

    def test_its_event_does_not_last_for_ever(self):
        """events() (fetch_red_logs.py:2256) ends a run's event window at `now` + PICKUP + ...
        when the run has no finish, on every archive run: a "changed" group of such a run keeps
        every build of the tab's job read again (open, so undecided) until 7 days after it
        started instead of hours; a "new" one (or a config not known, which counts as "new")
        seeds a re-read of every build at any time up to now, so none is ever decided while the
        run is listed (90 days)."""
        h = frl.ConfigHistory()
        changed = dict(E1, group_digest="1" * 64)
        h.merge(loaded([run(NOW, "a"), run(NOW + 100, "b", finished=False), run(NOW + 200, "c")],
                       configs(a={"d#t": E1}, b={"d#t": changed}, c={"d#t": changed})))
        later = NOW + 30 * DAY
        windows = [hi for _, hi, _ in h.events("d#t", later)]
        # (b's change was taken by c's end + PICKUP at the latest: c uploaded it again)
        self.assertTrue(all(hi <= NOW + 205 + frl.PICKUP + frl.READ_GAP for hi in windows), windows)  # fails


class NeverFinishedWorld(ttc.ConfigWorld):
    """The fetch with a history whose middle run never finished."""

    def judged(self, middle_finished):
        """Tab t showed every row until 41 h ago (run 1); then runs 2 (41 h ago; finished only
        when `middle_finished`) and 3 (40 h ago) made it hide kubetest.Test and job.Overall."""
        runs_ = [{"build": str(snowflake(self.now - 30 * DAY)), "commit": "1" * 40, "started": int(self.now - 30 * DAY) - 5,
                  "finished": int(self.now - 30 * DAY)},
                 {"build": str(snowflake(self.now - 41 * HOUR)), "commit": "2" * 40, "started": int(self.now - 41 * HOUR),
                  "finished": int(self.now - 41 * HOUR) + 5 if middle_finished else None},
                 {"build": str(snowflake(self.now - 40 * HOUR)), "commit": "3" * 40, "started": int(self.now - 40 * HOUR) - 5,
                  "finished": int(self.now - 40 * HOUR)}]
        show_all, hiding = {"dash#t": ttc.entry("")}, {"dash#t": ttc.entry(self.HIDE)}
        h = frl.ConfigHistory()
        h.merge(loaded(runs_, {"1" * 40: (show_all, {"test_infra": "1" * 40}), "2" * 40: (hiding, {"test_infra": "2" * 40}),
                               "3" * 40: (hiding, {"test_infra": "3" * 40})}))
        uploads, entries = h.encode()
        state = self.state()
        state.update(config_uploads=uploads, config_entries=entries)
        frl.write_json(os.path.join(self.root, "state.json"), state)
        last = runs_[-1]
        path = ttc.write_config(os.path.join(self.dir, "now.json"), [last], {
            last["commit"]: {"committed": last["started"] - 60, "image": "img",
                             "dashboards": {"dash": {"t": ttc.entry(self.HIDE)}}}})
        self.fetch_with(path)
        return self.runs()[-1]

    def test_control_with_the_middle_run_finished_it_is_not_red(self):
        # (passes)
        run_ = self.judged(middle_finished=True)
        self.assertEqual((run_["outcomes"], run_["serious_warnings"]), ({}, []))

    def test_a_build_long_after_a_never_finished_run_is_not_judged_by_the_config_before_it(self):
        """What it costs: build b (30 h ago) is red only on kubetest.Test. TestGrid never showed
        it red (every config in force while it was shown hid that row), but run 1's era never
        ends, so the port judges b's reads by run 1's config too and archives it: a false red
        (for every later build red only on rows run 1 showed, for 90 days)."""
        run_ = self.judged(middle_finished=False)
        self.assertNotIn("backfilled", run_["outcomes"])  # fails: backfilled
        self.assertIsNone(self.bdir(self.b))


# ------------------------------------------------------------------ relisting

class Relist(unittest.TestCase):
    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def use_fake_gcs(self):
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)

    def test_a_relist_from_the_first_run_kept_is_done_once(self):
        """decode_uploads gives relist 0 ("from the first run kept") for a dropped run with no
        build ID (or one before every run kept); ConfigHistory keeps 0 in state.json's
        config_relist_after, and merge() clears it only when loaded["after"] <= 0
        (fetch_red_logs.py:2174), which plan() never gives (it lists from the first run's
        time - 1). So the marker stays for good, and every plan() lists (and reads three files
        of) every run of the last CONFIG_KEEP_DAYS days again, on every archive run."""
        self.use_fake_gcs()
        t = int(time.time()) - 3 * DAY
        a, b = self.upload(t, commit="a" * 40), self.upload(t + 60, commit="b" * 40)
        listed = [{k: r[k] for k in ("build", "commit", "started", "finished")} for r in tgc.upload_runs(t - 1, gcs=self.srv.url)]
        h = frl.ConfigHistory()
        h.merge(loaded(listed, configs(a={"d#t": E1}, b={"d#t": E2})))
        uploads, entries = h.encode()
        warnings = []
        h = frl.ConfigHistory(uploads + ["damaged"], entries, warnings)  # (read from state.json)
        self.assertEqual(h.relist_after, 0)
        state = {"config_uploads": h.encode()[0], "config_entries": h.encode()[1], "config_relist_after": h.relist_after}
        out = tgc.plan(state, time.time())  # (testgrid_config.sh)
        self.assertEqual([r["build"] for r in out["runs"]], [a, b])  # (listed again, from the first run kept)
        h.merge(loaded(listed, {}, after=out["after"]))  # (the fetch)
        self.assertIsNone(h.relist_after)  # fails: 0, written to state.json again, for good


# ------------------------------------------------------------------ a run from the future in state.json

class Future(unittest.TestCase):
    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def test_a_run_from_the_future_in_state_json_does_not_stop_the_listing(self):
        """decode_uploads accepts any build ID of up to 20 digits and any time below 2**40
        (valid_run, is_time); plan() lists only runs made after the last run of the known
        prefix. One run in state.json with a build ID from the future (a hand edit, a bad
        merge: the fetch checks its other state.json times with plausible_time, not these)
        makes upload_runs list from that time: no new upload is ever listed again, and the
        fetch keeps that run's config as TestGrid's config now, silently."""
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        t = int(time.time()) - DAY
        a = self.upload(t, commit="a" * 40)
        new = self.upload(t + 3600, commit="c" * 40)
        future = str(snowflake(time.time() + 400 * DAY))
        state = {"config_uploads": [
            {"build": a, "commit": "a" * 40, "started": t, "finished": t + 5, "tabs": {}, "base": True},
            {"build": future, "commit": "f" * 40, "started": t + 60, "finished": t + 65, "tabs": {}}]}
        out = tgc.plan(state, time.time())
        self.assertIn(new, [r["build"] for r in out["runs"]])  # fails: [] (listed after the future run)


# ------------------------------------------------------------------ a config that never builds

class NeverBuilds(unittest.TestCase):
    setUp = tch.Uploads.setUp
    upload = tch.Uploads.upload

    def test_the_run_after_a_config_that_cannot_be_built_is_not_rebuilt_on_every_archive_run(self):
        """plan() (testgrid_config.py:695-705) rebuilds a run whose config is known when it cannot
        tell how the run changed its tabs' test groups, with the run before for comparison. When
        the run before's config cannot be built (an image gone from the registry, a commit the
        configurator rejects with the run's arguments), nothing records that: every archive run
        rebuilds the known config again for a comparison that cannot happen, for as long as the
        history keeps the run (77 days), before older configs that are not known yet."""
        orig = tgc.GCS
        tgc.GCS = self.srv.url
        self.addCleanup(setattr, tgc, "GCS", orig)
        t = int(time.time()) - DAY
        for i, c in enumerate("ace"):  # (c's config cannot be built; e's is known)
            self.upload(t + 60 * i, commit=c * 40)
        tabs = {"d#t": E1, "d#u": dict(E1, test_group="other")}
        h = frl.ConfigHistory()
        h.merge(loaded([{k: r[k] for k in ("build", "commit", "started", "finished")}
                        for r in tgc.upload_runs(t - 1, gcs=self.srv.url)],
                       configs(a={"d#t": E1}, e=tabs), failed={ttc.recipe_of("c" * 40): "the configurator failed"}))
        uploads, entries = h.encode()
        state = {"config_uploads": uploads, "config_entries": entries}
        for _ in range(2):  # (the next archive runs: c fails again each time)
            planned = tgc.plan(state, time.time())
            self.assertIn("c" * 40, [b["commit"] for b in planned["build"]])
            self.assertNotIn("e" * 40, [b["commit"] for b in planned["build"]])  # fails: e rebuilt each time


# ------------------------------------------------------------------ warnings

class LatestNotKnown(ttc.ConfigWorld):
    def judged(self, latest_known):
        """Tab t, which TestGrid shows now, was added by the latest upload (2 h ago), whose config
        is known only when `latest_known` (else it could not be built this time)."""
        older = {"build": str(snowflake(self.now - 2 * DAY)), "commit": "1" * 40, "started": int(self.now - 2 * DAY) - 5,
                 "finished": int(self.now - 2 * DAY)}
        latest = {"build": str(snowflake(self.now - 2 * HOUR)), "commit": "2" * 40, "started": int(self.now - 2 * HOUR) - 5,
                  "finished": int(self.now - 2 * HOUR)}
        built = {"1" * 40: {"committed": older["started"] - 60, "image": "img", "dashboards": {"dash": {"other": ttc.entry("")}}}}
        if latest_known:
            built["2" * 40] = {"committed": latest["started"] - 60, "image": "img",
                               "dashboards": {"dash": {"t": ttc.entry(self.HIDE)}}}
        path = ttc.write_config(os.path.join(self.dir, "cfg.json"), [older, latest], built,
                                failed={} if latest_known else {"2" * 40: "git fetch: timed out"})
        self.fetch_with(path)
        return self.runs()[-1]

    def test_control_with_the_latest_config_known_nothing_is_serious(self):
        # (passes)
        self.assertEqual(self.judged(latest_known=True)["serious_warnings"], [])

    def test_a_tab_added_by_a_run_whose_config_is_not_built_yet_is_not_a_serious_warning(self):
        """run() (fetch_red_logs.py:3592-3608) calls a tab TestGrid shows that the latest KNOWN
        config lacks "lacks tabs TestGrid shows (is it built for their dashboard?)", a serious
        warning, unless that known run ended within PICKUP + 1 h, ignoring the later runs
        whose config is not known; and config_warnings adds "TestGrid's config was never read
        for 1 tabs ...: rows their filters hide count as shown" (serious, and not what happens:
        the tab's builds are held, its eras UNKNOWN). So a tab added by the latest upload, whose
        config could not be built this time (a git or docker hiccup, or the 8-minute budget),
        fails the workflow at once with two misleading causes, though a run whose config is not
        known is only serious after CONFIG_STALE_DAYS (its own warning says so)."""
        run_ = self.judged(latest_known=False)
        self.assertIn("runs of its upload job", " ".join(run_["warnings"]))  # (the run not known: its own warning)
        self.assertEqual(run_["outcomes"], {"held": [f"job/{self.b}"]})  # (held until the config is known)
        self.assertEqual(run_["serious_warnings"], [])  # fails: "lacks tabs TestGrid shows", "never read"


class FirstListing(ttc.ConfigWorld):
    def test_runs_the_archive_has_just_listed_are_not_still_not_known_3_days_later(self):
        """run() (fetch_red_logs.py:3615-3621) calls the config of runs "still not known 3 days
        later" (serious) by the age of the oldest such run, not by how long the archive has
        known about it. plan() lists HISTORY_DAYS (16) days of runs for an archive without
        config_uploads (a new archive, or the first run of this code: about 100 runs, real GCS
        2026-10-12), newest first, 8 minutes per archive run: every archive run fails with
        this warning until the oldest of them is built, days later."""
        runs_ = [{"build": str(snowflake(self.now - d * DAY)), "commit": f"{d:040x}", "started": int(self.now - d * DAY) - 5,
                  "finished": int(self.now - d * DAY)} for d in (15, 10, 1)]
        newest = runs_[-1]
        path = ttc.write_config(os.path.join(self.dir, "cfg.json"), runs_,
                                {newest["commit"]: {"committed": newest["started"] - 60, "image": "img",
                                                    "dashboards": {"dash": {"t": ttc.entry("")}}}})
        self.fetch_with(path)
        run_ = self.runs()[-1]
        self.assertIn("runs of its upload job", " ".join(run_["warnings"]))
        self.assertNotIn("still not known", " ".join(run_["serious_warnings"]))  # fails: the first run fails


class SilentFreeze(ttc.ConfigWorld):
    history = tch.ConfigOverTime.history

    def test_no_upload_run_for_ten_days_is_said(self):
        """The config TestGrid uses is only ever learned from the upload job's runs as
        testgrid_config.uploads lists them: if the job stops appearing there (renamed, its logs
        moved to another bucket or path, or started.json without "repo-commit", which read_run
        takes for a run that never started), the listing is empty, the latest run's config is
        "read now" on every run (config_read refreshed, no stale warning), and the archive judges
        by a config that may be weeks old, without a word. In GCS (2026-07-13 to 2026-10-10) the
        job ran 5.2 times a day, never more than 59 h apart."""
        path = self.history((self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                            (self.now - 10 * DAY, {"dash": {"t": ttc.entry("")}}))
        last = self.state()["config_uploads"][-1]
        # (testgrid_config.sh now: listed after the last run, none found)
        path = ttc.write_config(os.path.join(self.dir, "none.json"), [], {}, after=frl.created(last["build"]))
        self.fetch_with(path)
        run_ = self.runs()[-1]
        self.assertTrue(any("upload" in w for w in run_["warnings"]), run_["warnings"])  # fails: []


# ------------------------------------------------------------------ a tab that moved to another test group

OTHER = {"name": "other", "gcs_prefix": "bucket/logs/other", "days_of_results": 15, "use_kubernetes_client": True}


class MovedTab(ttc.ConfigWorld):
    history = tch.ConfigOverTime.history
    tab = tch.ConfigOverTime.tab

    def test_a_build_is_not_judged_by_a_config_whose_test_group_reads_another_job(self):
        """tab_verdict (fetch_red_logs.py:2038-2058) judges a build by every config its tab had
        while TestGrid may have shown it, whatever job that config's test group read: until
        20 h ago tab t showed test group "other" (gs://bucket/logs/other, every row), then
        test group "job" (gs://bucket/logs/job, kubetest.Test hidden). Build b (job, 30 h
        ago, red only on kubetest.Test) was never on t while t showed "other", and t hid its
        red row since: TestGrid never showed it red there. The port judges b's reads before
        the switch by the "other" config and archives it (false red)."""
        before = {"test_group": "other", "base_options": "", "group": OTHER, "group_digest": tgc.group_digest(OTHER)}
        path = self.history((self.now - 30 * DAY, self.tab(before)), (self.now - 20 * HOUR, self.tab(ttc.entry(self.HIDE))))
        self.fetch_with(path)
        run_ = self.runs()[-1]
        self.assertNotIn("backfilled", run_["outcomes"])  # fails: backfilled
        self.assertIsNone(self.bdir(self.b))


class GroupEvents(ttc.ConfigWorld):
    """events() follows a tab's key; TestGrid's re-reads follow its test group."""

    history = tch.ConfigOverTime.history
    judged = tch.ConfigOverTime.judged
    late_junit = tch.ConfigOverTime.late_junit

    def test_control_a_change_of_the_tabs_group_reads_it_again(self):
        # (passes: t's group changed 20 h ago; b's junit, written 21 h ago, was read then: red)
        self.late_junit(21 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, {"dash": {"t": ttc.entry("")}}),
                           (self.now - 20 * HOUR, {"dash": {"t": ttc.entry("", days_of_results=16)}}),
                           (self.now - 10 * HOUR, {"dash": {"t": ttc.entry("", days_of_results=16)}}))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})

    def test_a_change_of_the_group_before_the_tab_was_renamed_reads_it_again_too(self):
        """ConfigHistory.events(key) (fetch_red_logs.py:2239-2258) keeps the changes of the test
        group of the tab of that name only, run by run. The same group changed 20 h ago, while
        the tab was named u; TestGrid's update after that read b again (reprocessColumn is per
        test group: it re-reads the group's grid, whatever tab shows it), with the failing
        junit file written 21 h ago: b was red in the group's grid, on u, then on t once u was
        renamed t (10 h ago), until now. events("dash#t") has nothing before t existed, so the
        port never models that read: b is judged not red (a missed red). The same holds for a
        new tab of a group TestGrid already had."""
        self.late_junit(21 * HOUR)
        run_ = self.judged((self.now - 30 * DAY, {"dash": {"u": ttc.entry("")}}),
                           (self.now - 20 * HOUR, {"dash": {"u": ttc.entry("", days_of_results=16)}}),
                           (self.now - 10 * HOUR, {"dash": {"t": ttc.entry("", days_of_results=16)}}))
        self.assertEqual(run_["outcomes"], {"backfilled": [f"job/{self.b}"]})  # fails: {} (not red)


class RaisedDays(ttc.ConfigWorld):
    """A change to a test group whose grid holds no column older than 7 days."""

    history = tch.ConfigOverTime.history

    def judged(self, days_before):
        """Build c (10 days ago, passed) whose failing junit file was written 5 days ago, long
        after its reads; tab t's group read `days_before` days of results until 2 days ago,
        15 since."""
        c = snowflake(self.now - 10 * DAY)
        base = f"/bucket/logs/job/{c}"
        self.srv.route(f"{base}/started.json", (200, {}, {"timestamp": int(self.now - 10 * DAY)}))
        self.srv.route(f"{base}/finished.json", (200, {}, {"result": "SUCCESS", "passed": True,
                                                           "timestamp": int(self.now - 10 * DAY + HOUR)}))
        self.srv.route(f"{base}/podinfo.json", (200, {}, {"pod": {"status": {"phase": "Succeeded"}}}))
        self.srv.route(f"{base}/build-log.txt", (200, {}, b"log\n"))
        name = f"logs/job/{c}/artifacts/junit_01.xml"
        self.srv.route(f"/bucket/{name}", (200, {}, tch.KUBETEST_FAILED))
        self.srv.gcs_listing("bucket", [f"logs/job/{x}/finished.json" for x in (self.visible, self.b, c)],
                             written={name: self.now - 5 * DAY})
        self.put_state(tabs={"dash#t": {"scan": self.now - 12 * DAY, "oldest": self.now - 13 * DAY}})
        path = self.history((self.now - 30 * DAY, {"dash": {"t": ttc.entry("", days_of_results=days_before)}}),
                            (self.now - 2 * DAY, {"dash": {"t": ttc.entry("", days_of_results=15)}}))
        self.fetch_with(path)
        return c, self.runs()[-1]["outcomes"]

    def test_control_a_group_with_older_columns_reads_7_days_again(self):
        # (passes: the old grid (15 days) kept columns older than 7 days; c was not read again)
        c, outcomes = self.judged(days_before=15)
        self.assertNotIn(f"job/{c}", outcomes.get("backfilled", []))

    def test_raising_days_of_results_from_one_day_reads_every_build_back_to_the_new_days(self):
        """events "changed" (gcs_column's seeds: only builds started less than 7 days before the
        update) follows reprocessColumn's floor, but not what TestGrid then lists: the floor
        truncates every column newer than 7 days (truncateRunning, updater.go:500-519), and when
        the grid held none older (a group of under 7 days of results, such as
        ci-kubernetes-unit's 1 day), hintStarted is "" and listBuilds lists every build, read back
        to the NEW days_of_results (read.go:41-63; updater.go:602, 622-628). Raised from 1 to 15 days 2
        days ago, TestGrid read c (10 days old) then, with the failing junit file written 5 days
        ago, and t showed it red since; the port never models that read (missed red)."""
        c, outcomes = self.judged(days_before=1)
        self.assertIn(f"job/{c}", outcomes.get("backfilled", []))  # fails: c not red


class CrossCheckLacking(ttc.ConfigWorld):
    def test_cross_check_does_not_call_a_held_build_of_a_tab_the_config_lacks_not_red(self):
        """run() makes the eras of a tab TestGrid lists that the latest config lacks UNKNOWN after
        its last entry (W4b F-H), so the fetch holds its builds; cross_check.py:568-587 builds its
        rules from history.eras() without that, so the same build is "not red" there (eras None:
        no such tab): the verifier counts as checked-not-red a build the fetch has not decided,
        and would not flag it if the fetch then dropped it. (The run's serious warning is what
        makes cross_check fail here; within PICKUP + 1 h of the upload it is not serious.)"""
        self.earlier_run(self.now - 3 * DAY)  # (the archive began before the build: cross_check judges it)
        self.fetch_with(self.config_of({"dash": {"other": ttc.entry("")}}))
        self.assertEqual(self.runs()[-1]["outcomes"], {"held": [f"job/{self.b}"]})
        _, out = self.check()
        self.assertNotIn("not red=1", out)  # fails: judged by its rule: not red=1
        self.assertIn("not decided yet", out)


# ------------------------------------------------------------------ testgrid_config.sh under failure

class ScriptSteps(unittest.TestCase):
    """testgrid_config.sh offline, as test_review_round10.Script runs it, with a `timeout` that
    records each step it bounds (and bounds it as asked, scaled by SCALE)."""

    setUp_ = r10.Script.setUp
    git, commit, listing, built, run_script = r10.Script.git, r10.Script.commit, r10.Script.listing, \
        r10.Script.built, r10.Script.run_script

    def setUp(self):
        self.setUp_()
        bindir = os.path.join(self.tmp, "bin")
        real_timeout, real_git = shutil.which("timeout"), shutil.which("git")
        self.log = os.path.join(self.tmp, "steps.log")
        self.scripts = {
            # timeout -k 10 SECONDS CMD...: log SECONDS and CMD's first two words, then bound it (scaled)
            "timeout": f"""#!/usr/bin/env bash
echo "$3 $4 $5" >> {self.log}
scaled=$(python3 -c 'import sys; print(max(1, int(sys.argv[1]) // int(sys.argv[2])))' "$3" "${{W5B_SCALE:-1}}")
exec {real_timeout} -k 1 "$scaled" "${{@:4}}"
""",
            # git, with a checkout that takes W5B_CHECKOUT seconds (blobs fetched from a slow remote)
            "git": f"""#!/usr/bin/env bash
for a in "$@"; do if [ "$a" = checkout ] && [ -n "${{W5B_CHECKOUT:-}}" ]; then sleep "$W5B_CHECKOUT"; fi; done
exec {real_git} "$@"
"""}
        for name, text in self.scripts.items():
            path = os.path.join(bindir, name)
            with open(path, "w") as f:
                f.write(text)
            os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)
        # docker as r10's, with a pull that takes W5B_PULL seconds
        docker = os.path.join(bindir, "docker")
        with open(docker, "w") as f:
            f.write(r10.FAKE_DOCKER.replace("  pull) exit 0 ;;", '  pull) sleep "${W5B_PULL:-0}"; exit 0 ;;'))

    def steps(self):
        with open(self.log) as f:
            return [line.split() for line in f]

    def test_control_each_step_is_bounded(self):
        # (passes: git fetch, docker pull and docker run each get a timeout)
        self.listing([self.commit("width=1")])
        done = self.run_script()
        self.assertEqual(done.returncode, 0, done.stderr)
        timed = [[os.path.basename(s[1]), s[2]] for s in self.steps()]
        for step in (["git", "-C"], ["docker", "pull"], ["docker", "run"]):
            self.assertIn(step, timed)

    def test_each_step_is_bounded_by_what_is_left_of_the_budget(self):
        """testgrid_config.sh:41 computes `left` (budget - SECONDS + 60) once per config and gives
        each of its three bounded steps all of it: a config started with 540 s left may take
        3 x 540 s. The workflow's step allows 12 minutes (720 s) for an 8-minute budget, then
        kills the script, and nothing is written: every run listed and every config built in
        that archive run is lost (the fetch warns that the config cannot be read). Here docker
        pull takes 3 s, yet docker run is still given the time left before it."""
        self.listing([self.commit("width=1")])
        done = self.run_script(W5B_PULL="3")
        self.assertEqual(done.returncode, 0, done.stderr)
        given = {os.path.basename(s[1]) + " " + s[2]: int(s[0]) for s in self.steps()}
        self.assertLessEqual(given["docker run"], given["git -C"] - 3)  # fails: 540 and 540

    def test_the_checkout_is_bounded_too(self):
        """testgrid_config.sh:44: `git checkout` of a blobless partial clone fetches every blob of
        config/jobs and config/testgrids from GitHub (thousands of files), with no timeout: a
        stalled transfer holds the script past its budget until the workflow step's 12 minutes
        kill it, and nothing is written. Here (time scaled 18 to 1: a step bounded at 90 s is
        killed after 5 s) the checkout stalls for 30 s with 30 s of budget."""
        self.listing([self.commit("width=1")])
        start = time.monotonic()
        done = self.run_script(CONFIG_SECONDS="30", W5B_SCALE="18", W5B_CHECKOUT="30")
        took = time.monotonic() - start
        self.assertEqual(len(tgc.load(self.out)["runs"]), 1, done.stderr)
        self.assertNotIn("left for the next run", done.stderr)  # (its config was tried)
        self.assertNotIn("git fetch:", done.stderr)  # (the fetch itself had time enough)
        self.assertLess(took, 20, done.stderr)  # fails: 30 s and more


if __name__ == "__main__":
    unittest.main()
