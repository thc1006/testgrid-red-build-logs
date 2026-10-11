# Red TestGrid build logs

About twice a day, a GitHub Actions job saves the raw Prow `build-log.txt` behind
every red cell on
[sig-release-master-blocking](https://testgrid.k8s.io/sig-release-master-blocking)
and [sig-release-master-informing](https://testgrid.k8s.io/sig-release-master-informing),
one copy per build, and commits it here.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="archive/charts/trend-dark.svg">
  <img alt="Red builds per day on the blocking and informing dashboards" src="archive/charts/trend-light.svg">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="archive/charts/tabs-dark.svg">
  <img alt="Red builds per tab over the last 14 days" src="archive/charts/tabs-light.svg">
</picture>

[archive/INDEX.md](archive/INDEX.md) lists the last 14 days of runs and builds and
links a page per week for everything older; each run's downloads are in its own
folder under [archive/runs/](archive/runs/).

## What counts as red

A cell is red exactly when TestGrid paints it red: `FAIL`, `TIMED_OUT`,
`CATEGORIZED_FAIL` or `TOOL_FAIL`. Purple (flaky), black (build failed) and
grey (unknown, aborted) cells are not. A build Prow aborted is usually red all
the same: TestGrid paints its Overall cell `FAIL` because it did not pass.
Tables are read the way the dashboard page loads them, so the red cells are
the ones you see when you open a tab. Each column is one Prow build, so a
build with several red cells is saved once.

A build TestGrid no longer shows (it scrolled off between two runs), or showed
running long past its job's time limit, is judged from GCS by TestGrid's own rule:
[`testgrid_rule.py`](tool/testgrid_rule.py) ports the code TestGrid's updater
paints a column with, at the commit TestGrid production runs, and is checked
against that code (see [below](#how-the-testgrid-rule-is-checked)). Every cell
of the column counts: the job's Overall and Pod rows and each junit test, on
the rows the tab shows. Which rows those are, and the options the tab's builds
are read with, come from TestGrid's config, built from test-infra the way
testgrid.k8s.io's is (see [How a run works](#how-a-run-works)): on
gce-master-scale-correctness, for example, a build whose only red rows are
`kubetest.Test` and the job's Overall row is not red, as the tab hides both. The
column is the one TestGrid read last, from what GCS held then: a build without
`finished.json` 24 hours after it started is FAIL at TestGrid's next read and is
not read again, so it stays red even if it finishes later, without the junit
files or `podinfo.json` written after that read. A column that is not red stays
undecided while TestGrid would still read it again (while any cell of it is
running, such as the Pod row waiting up to an hour for `podinfo.json`), and is
judged again on later runs.

## Layout

Every run gets its own folder, named by its start time (UTC). A build is kept
in the folder of the run that first archived it.

| Path | Contents |
|---|---|
| `archive/runs/<YYYY-MM>/<run>/README.md` | what that run did, and the builds it archived, with links |
| `archive/runs/<YYYY-MM>/<run>/run.json` | the run's full report: tabs read, outcomes, errors, warnings |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/build-log.txt.gz` | the raw log, gzipped; `meta.json` has the raw log's md5, checked against GCS |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/build-log.txt.gz.000`, `.001`, ... | the same, in parts, for a log over 95 MiB after gzip (GitHub rejects files over 100 MB) |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/podinfo.json` | only for builds whose pod never ran, so there is no log |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/meta.json` | tabs, red tests, TestGrid messages, Prow result, links, and what GCS held |
| `archive/INDEX.md` | the charts, red builds not archived yet, the runs and builds of the last 14 days, and what the archive holds |
| `archive/weeks/<year>-W<week>.md`, `.json` | every archived build, one page per ISO week (UTC), with up to 100 of its red tests and their count (`meta.json` has them all); `archive/index.json` lists them |
| `archive/charts/*.svg` | the charts above, in light and dark versions |
| `archive/state.json` | each tab's last scan and TestGrid config, builds to retry, and builds given up on after 14 days |

To read a log: `gzip -dc archive/runs/<YYYY-MM>/<run>/<job>/<build>/build-log.txt.gz | less`,
or for one in parts: `cat build-log.txt.gz.* | gzip -dc | less` (the parts are
numbered so that `*` lists them in order; `meta.json` lists them with their sizes).

## How a run works

1. Build TestGrid's config for the tabs
   ([`testgrid_config.sh`](tool/testgrid_config.sh)): testgrid.k8s.io's config is
   built from test-infra (the TestGrid YAML in `config/testgrids` and the
   `testgrid-*` annotations of the Prow jobs) by its configurator, which
   test-infra's job `post-test-infra-upload-testgrid-config` runs after every
   change. The run takes the image and the arguments that job uses, runs it on
   the tip of test-infra (without network), and keeps each tab's base options
   (the rows it shows) and its test group (where its builds are, its days of
   results, and the options its cells are painted by).
2. Read every tab of both dashboards and find the red cells. TestGrid's data
   is checked strictly: a table that does not decode exactly fails that tab
   instead of being guessed at. Each table is also checked against the tab's
   config: the same GCS folder, and no row shown that its base options hide.
3. Download the log of each red build not archived yet, checking its length
   and md5 against GCS (or its crc32c, for an object GCS keeps no md5 for).
   Builds that cannot be fetched yet are retried on later runs straight from
   GCS, even after TestGrid drops them.
4. Compare each archived build with GCS again on every run until two days
   after it finished (14 days for a build without a log, in case one appears),
   and fetch it again if GCS now holds something else: Prow can upload a log
   twice. A copy taken before Prow was done is marked *provisional* until it
   settles.
5. If a tab's history no longer reaches back to the last run (a run was missed,
   or a tab keeps only a day of results), list that stretch in GCS, judge each
   build there by TestGrid's rule on the rows that tab shows, and archive the red ones,
   labelled as taken from GCS. A build TestGrid would still read again (not
   finished yet) is watched until it can be judged; so is one TestGrid shows
   running long past its job's time limit (it may never finish: TestGrid paints
   it red 24 hours after it started, perhaps after it has scrolled off).
6. Rewrite the index and the charts, then
   [`cross_check.py`](tool/cross_check.py) checks the archive on its own:
   every stored file, however old, against its `meta.json`; the files of the
   last 48 hours' builds, and of a seventh of the older ones (another seventh
   each day), against GCS; TestGrid's red cells now; and every build in GCS
   from the last 48 hours, judging those TestGrid no longer shows by its rule,
   with the config the run used.
   It fails the run if a red build is missing or a stored file no longer
   matches.

The run keeps as many requests in flight as the servers allow, adjusting as it
goes; a full backfill of about 620 builds takes under a minute.

## When it runs

The schedule fires every 3 hours, but a scheduled run archives only once the
archive is 11 hours old (the last commit under `archive/runs/`), so it archives
about twice a day even though GitHub starts scheduled runs hours late, or drops
one, when it is busy. A run started by hand always archives, and so does any
re-run (Re-run jobs) of a run.

If the archive was more than 26 hours old when a scheduled run started, the run
still archives (the gap is searched in GCS) and then fails, so GitHub's failure
email says archiving fell behind. GitHub sends nothing when it does not start
the workflow at all; for that, an optional heartbeat pings an outside dead
man's switch (such as healthchecks.io) from every scheduled run, about every 3
hours: set the repository variable `HEARTBEAT` to `on`, and the check's ping URL
as the secret `HEARTBEAT_URL` of an environment named `heartbeat` (used without
creating deployments), with the check expecting a ping every 12 hours or so, as
GitHub starts scheduled runs up to hours late.

GitHub disables the scheduled workflows of a public repository after 60 days
without activity in it. Every archiving run commits to `main`, but GitHub does
not document whether commits made by `github-actions[bot]` count, nor whether it
tells anyone when it disables a schedule: the heartbeat is what notices. Enable
it again from the Actions tab, or with `gh workflow enable red-build-logs.yml`.
The workflow does not re-enable itself on a timer: GitHub took down
gautamkrishnar/keepalive-workflow, a widely used action that did so, as a
terms-of-service violation for circumventing that policy.

## When something fails

Nothing is lost silently: a failure makes the workflow run fail, and GitHub
notifies the person who set up the schedule (by email, with the default
notification settings).

| What happens | What the archive does |
|---|---|
| A log cannot be downloaded (network, server error, wrong md5) | retried with backoff in the run, then on every later run for 14 days, even after TestGrid drops the build; listed under "Not archived yet" in INDEX.md |
| A tab cannot be read, or its data is malformed | that tab's red builds wait for the next run; the tab is not marked as scanned |
| A run is missed or starts late | the next run notices the gap in TestGrid's history and takes the builds TestGrid painted red in that time from GCS, by TestGrid's own rule; only a gap it cannot search fails the run (or an archive more than 26 hours old, below) |
| No archive for more than 26 hours (GitHub starts no scheduled run, or the runs fail before they commit) | the next scheduled run fills the gap from GCS, then fails to say so; the heartbeat, if set up, stops when GitHub starts no run |
| TestGrid shows a build running long past its job's time limit | it is watched, and judged from GCS by TestGrid's rule once TestGrid no longer shows it |
| A finished build to judge from GCS has junit files GCS cannot list or serve now | it is red all the same if its job's own rows make it red (its failing tests are then not listed); otherwise it cannot be judged, which fails every run until 14 days after the build was made |
| GCS replaces a log after it was archived | the next run fetches the new copy into the same folder and records it in `meta.json` |
| A step times out | what finished is committed with the run's report; the rest is retried next run |
| A download is cut off part-way (a timeout, an interrupt, a full disk) | what it left is removed by the run (or, if the run was killed outright, by the next one), and the build is fetched again |
| A log is too large for one file in git (over 95 MiB after gzip) | it is stored in numbered parts, so no file over 95 MiB is committed: a stored log or a generated page that large keeps its last committed version, and any other file that large stops the commit |
| TestGrid's config cannot be built from test-infra in a run (network, a changed upload job) | the run uses the config an earlier run built (kept in `state.json` per tab), and says so; a tab's config more than 3 days old, or none at all, fails every run until a new one is built |
| A tab's table disagrees with its config (another GCS folder, or a row shown that its filters hide: test-infra changed and TestGrid has not followed yet, or the other way round) | every run fails while it does; builds the tab no longer shows that were made before it agreed again are red if they are red by either the config's filters or the rows the table showed |
| A tab's filters use syntax the port does not follow, or take more than 30 seconds to apply | every run fails; builds the tab no longer shows are judged by the rows its table shows |
| A tab's test group sets an option the port does not follow | every run fails; builds the tab no longer shows are held, not judged, until the port follows it (60 days at most, then dropped with a failing warning) |
| A tab holds more than 500,000 red cells (a mass failure) | every red build is archived; each lists its first red cells, and counts the rest |
| A test's output puts control characters into TestGrid's messages | they are stored as spaces; the tab is read all the same |
| A run is cancelled, or hits the job's 110-minute limit | nothing is pushed; the next run fetches everything again |
| The commit cannot be pushed (a conflicting change on the branch) | the run fails and attaches its commit as a git bundle artifact; the next run fetches everything again |
| TestGrid paints a build red but GCS has nothing for it | the build is recorded without a log, and later runs look for a late one for 14 days |

To retry at once instead of waiting for the next scheduled run, start the
workflow by hand from the Actions tab (Run workflow).

## Limits

- A gap in TestGrid's history is searched in GCS at most 14 days back. If no
  run succeeds for longer than that, the next run says which stretch may be
  missing, and fails.
- TestGrid's config is built from the tip of test-infra. TestGrid production
  gets a change once test-infra's upload job has run after the merge, so for
  those minutes the archive follows a newer config than TestGrid; a tab whose
  table then disagrees with it has its builds judged by both (above). A filter
  that changed without the table showing it (a row it now hides that no column
  in view has) cannot be noticed.
- The port follows TestGrid's default cell options (rows of the same name
  merged, the Pod row, no custom evaluator rules, test names as they are), which
  every tab of both dashboards has. Builds of a tab whose test group sets another
  (`disable_prowjob_analysis`, `test_name_config`, ...) are held, rather than
  judged by a rule TestGrid does not use.
- A tab's filters are applied as TestGrid's tabulator applies them, with Go's
  regexp semantics, matched without backtracking. Syntax the archive does not
  follow (flags, named groups, Unicode classes, patterns over 400 characters;
  none of testgrid.k8s.io's filters use them) is reported, and the tab's builds
  are judged by the rows its table shows.
- When TestGrid last read a build, and what GCS held then, is inferred from when
  GCS says each object was written (Last-Modified, and the listing's creation
  times): after `finished.json` and `podinfo.json`, or the first read after the
  24-hour deadline. TestGrid's updater updates a job's builds at least every 2
  hours, an update taking up to 20 minutes, so an object written that long after
  such a moment counts both ways (red if either read is), and a build counts as too
  old only by the days of results before the update began: on a tab of one day of
  results, a build not finished after 24 hours is red, as TestGrid paints it FAIL
  when an update that began before the deadline reads it after (it mostly drops
  the build as too old instead). TestGrid also reads a column again while an
  older one still runs, which the archive does not follow: a FAIL it then turns
  green counts as red.
- Without a config (a local run without `--testgrid-config` on a new archive),
  the rows a tab hides are learned from its table, as far as it shows: its
  job's Overall and Pod rows, but not filters on test names; the run says so.
- TestGrid read the build when it did, and an error it hit reading GCS then
  cannot be seen now. TestGrid reads a build's files at once and the first
  error decides; when the outcome depends on which comes first, the build
  counts as red if any outcome is.
- A pod quantity with an exponent near ±2³¹ sends TestGrid's code into an
  endless computation (TestGrid then times out); the port answers at once.
  Prow's `podinfo.json` never holds one.

## How the TestGrid rule is checked

[`tool/oracle/`](tool/oracle/README.md) builds TestGrid's own Go code at the
commit TestGrid production runs (pinned in `tool/oracle/pinned.json`) and
compares the port with it: about 250 edge cases (one Go behaviour each),
thousands of random builds, and real builds of the archive's jobs (read from GCS
both independently, for TestGrid's code, and by the fetch's own reading code,
whose listing pages, sizes and encodings are checked that way too); it also checks that the port decodes `started.json`, `finished.json` and
`podinfo.json` into the same Go types. The unit tests replay a set of its
answers without Go. It also builds the filter TestGrid's tabulator applies to
a tab's rows, and checks [`testgrid_config.py`](tool/testgrid_config.py) against
it: the base options of testgrid.k8s.io's tabs, edge cases, and 20,000 random
ones per run. The workflow *Check the TestGrid rule* runs all of this daily and
on any change to the port. Every day it also checks that TestGrid production's
updater and tabulator still run the pinned commit (every archive run checks
too, and warns). When they moved, it checks the port against the new commit
and lists what changed in the code the port follows: if nothing did and every
check passes, it proposes the new commit on a branch; otherwise it fails, for a
person to compare the changes with the port.

## Running it locally

```sh
cd tool
uv sync --locked
PYTHON=.venv/bin/python ./testgrid_config.sh /tmp/testgrid-config.json   # needs git, and docker or podman
uv run fetch_red_logs.py --archive ../archive --gzip --testgrid-config /tmp/testgrid-config.json
uv run cross_check.py --archive ../archive
uv run --locked python -m unittest
uv run --locked ruff check .
uv run --locked actionlint ../.github/workflows/*.yml
uv run --locked zizmor --offline ../.github/workflows/
```

The tools honour the proxy environment variables (`http_proxy`, `https_proxy`,
`no_proxy`) as urllib does, a proxy's user and password included (both are needed). Checking the TestGrid rule needs Go; see
[`tool/oracle/README.md`](tool/oracle/README.md).

## License

[Apache License 2.0](LICENSE), the same license as Kubernetes. The archived
logs are copies of Kubernetes CI output that is already public in the
`kubernetes-ci-logs` bucket.
