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
testgrid.k8s.io's was (see [How a run works](#how-a-run-works)), as it was while
TestGrid may have shown the build (by each config the tab had then; red if red
by one): on gce-master-scale-correctness, for example, a build whose only red
rows are `kubetest.Test` and the job's Overall row is not red, as the tab hides
both. A tab's filters are applied as TestGrid's tabulator applies them: Go's
`url.ParseQuery` and Go's regexp ([`goregexp.py`](tool/goregexp.py): Go's parser
ported line for line, with its limits and its Unicode tables, matched without
backtracking), other base options ignored as it ignores them (the run names
them). A config TestGrid itself cannot apply (it rejects the filters) makes its
tabulator fail the tab, which then shows no new column: none counts as red there.

TestGrid reads a build's column again at every update while any cell of it runs
(the build, or its Pod row waiting up to an hour for `podinfo.json`), at updates
that begin less than 20 minutes after the build started, while its grid holds a
running column of the job older than the build's, and, after a change to the
job's test group's config (whatever tab showed the group then), if it started
within the 7 days before (every build, for a group new to TestGrid, or one that
kept 7 days of results or fewer: its grid holds no older column to list the
builds after). (TestGrid's code reads those again only if the grid it stored
holds the config it compares, and production stores its grids without it; the
archive reads them again all the same, so as not to depend on that, at worst
archiving a build TestGrid did not show red.) A build without `started.json` is read as a running column, put at
the start of the highest column below it in the same update that is good then
(its files are JSON, and it is not too old), so it holds the columns newer than
that until the next update; with none, it goes to the epoch, and the next update
drops it. A build that starts after (or with) one of a higher ID drops out of the
grid while that one's column is settled there (done, readable, not too old, and
no older column running), as TestGrid then lists only the builds after it; out of
the grid, a settled column of any higher ID keeps it out, whatever its start, and
the first update after none does (one too old, or cut at an older running column)
lists and reads it again, if it is not too old itself. But an update appends the
columns after its first only until its grace, 10 of its 20 minutes: when its
reads come slowly, that column may never be appended, and the build never drops
out, so it is read as if it did not too. Each read paints the
column from what GCS holds then, compared with the build's deadlines when the
column is made, up to 20
minutes later, and the column stays on the tab until the next read, shown by the
tab's config of the time (TestGrid's tabulator may show a tab's new filters on the
columns read before up to an hour after TestGrid took the config; a config whose
test group reads another job's builds keeps this job's columns made before it,
and reads none of them). So the build counts as red if the column
of any of those reads is red on the tab by a config the tab had while it may have
shown that read: a build without `finished.json` 24 hours after it started is FAIL
at TestGrid's next read, and stays red here even if it finished soon after. A
column that is not red is undecided while TestGrid may still read it, and is
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

1. Build TestGrid's config for the tabs, over time
   ([`testgrid_config.sh`](tool/testgrid_config.sh)): testgrid.k8s.io's config is
   built from test-infra (the TestGrid YAML in `config/testgrids` and the
   `testgrid-*` annotations of the Prow jobs) by its configurator, which
   test-infra's job `post-test-infra-upload-testgrid-config` runs after every
   change, uploading what it builds; TestGrid's config merger takes it within 25
   minutes (it merges every 15 minutes, a merge taking up to 10), and TestGrid's
   components within a minute more. The run lists that job's runs in GCS, and for
   each one whose config the archive does not know yet (the first time, those of
   the last 18 days) builds that config as the run did: at its commit, with the
   image and the arguments the run itself ran (its `prowjob.json`), without
   network, newest first, for up to 30 minutes (the rest are left to the next run;
   one that cannot be built is left out, and the run says why). It keeps each
   tab's base options (the rows it shows) and its test group (where its builds
   are, its days of results, the options its cells are painted by, and a digest of
   all of it), and how each run changed the tabs' test groups (by the whole config
   of the run before: a renamed tab, or a tab of a group TestGrid already had, is
   no change); the archive keeps when each config was in force (`state.json`, for
   77 days).
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
   it red 24 hours after it started, perhaps after it has scrolled off). If the
   tab's config changed since the last run's scan in a way that can show those
   builds otherwise (its filters, which TestGrid applies to the columns it has,
   or its test group, whose last 7 days of columns it may read again), the
   builds that scan saw that the tab no longer shows are judged the same way, by
   what the tab may have shown since; where a config was not known yet, once it
   is.
6. Rewrite the index and the charts, then
   [`cross_check.py`](tool/cross_check.py) checks the archive on its own:
   every stored file, however old, against its `meta.json`; the files of the
   last 48 hours' builds, and of a seventh of the older ones (another seventh
   each day), against GCS; TestGrid's red cells now; and every build in GCS
   from the last 48 hours, judging those TestGrid no longer shows by its rule,
   with the configs the archive keeps.
   It fails the run if a red build is missing or a stored file no longer
   matches.

The run keeps as many requests in flight as the servers allow, adjusting as it
goes; a full backfill of about 620 builds takes under a minute.

## When it runs

The schedule fires every 3 hours, but GitHub starts scheduled runs hours late
and drops most of them when it is busy (in October 2026 it started about two a
day, some 12 hours apart). A scheduled run archives unless the archive (the last
commit under `archive/runs/`) already changed in that half of the UTC day (00:00
to 12:00, 12:00 to 24:00) less than 11 hours before, so it archives about twice
a day, and an archive made by hand in between does not put the next scheduled
one off by a day. A run started by hand always archives, and so does any re-run
(Re-run jobs) of a run.

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
| TestGrid's config cannot be built from test-infra in a run (network, a changed upload job, one run's config that does not build, or one with an entry not as the archive writes them) | the run uses the configs earlier runs built (kept in `state.json`), and says so (naming each config that could not be built, and why); a build TestGrid may have shown under a config not built yet stays undecided until a run builds it; a run whose config is still not known 3 days after the archive first listed it (and was in force while a build the archive may still judge from GCS could have been shown: one in a gap a run searches, one watched, one waiting for a config), or a tab's config more than 3 days old, or none at all, fails every run until it is built. A run whose config cannot be built is tried again on every run, but the runs around it are not rebuilt to compare with it until it is built |
| The upload job lists no new run for 3 days (it runs several times a day: renamed, moved, or its runs no longer name their commit) | every run fails, saying since when; the archive keeps judging by the last config it knows |
| TestGrid shows a tab the latest config lacks (TestGrid has not taken that config yet, a later run's config is not built yet, or it is not built for the tab's dashboard) | builds TestGrid may have shown there since the tab's last config stay undecided; once TestGrid should have taken that config (an hour after its upload) and no later run's config may have the tab (one not built yet, listed less than 3 days ago), every run fails until a config with the tab is read |
| A tab's table disagrees with its config (another GCS folder, or a row shown that its filters hide: test-infra changed and TestGrid has not followed yet, or the other way round) | every run fails while it does; builds the tab no longer shows that were made before it agreed again are red if they are red by either the config's filters or the rows the table showed |
| A tab's filters take more than 30 seconds to apply here, or compile to an automaton too large to run here (more than a million states; Go's own limit is 3.3 million) | every run fails; builds the tab no longer shows are judged by the rows its table shows |
| Go rejects a tab's filters (TestGrid's tabulator fails the tab, which keeps the state it had) | the run warns; builds made while that config is in force are not shown on the tab, so not red there |
| A tab's test group has no source of results TestGrid production reads (neither `use_kubernetes_client` nor a GCS `result_source`, or only ResultStore) | the run warns; TestGrid's updater never updates the group, so builds made while that config is in force are not shown on the tab, so not red there |
| A tab's test group sets an option the port does not follow | every run fails; builds the tab no longer shows are held, not judged, until the port follows it (60 days at most, then dropped with a failing warning) |
| A tab holds more than 500,000 red cells (a mass failure) | every red build is archived; each lists its first red cells, and counts the rest |
| A test's output puts control characters into TestGrid's messages | they are stored as spaces; the tab is read all the same |
| A run is cancelled, or hits the job's 130-minute limit | nothing is pushed; the next run fetches everything again |
| The commit cannot be pushed (a conflicting change on the branch) | the run fails and attaches its commit as a git bundle artifact; the next run fetches everything again |
| TestGrid paints a build red but GCS has nothing for it | the build is recorded without a log, and later runs look for a late one for 14 days |

To retry at once instead of waiting for the next scheduled run, start the
workflow by hand from the Actions tab (Run workflow).

To go back to an earlier version of the code, revert its commits on the branch;
the archive stays as it is. The code deployed before the config history (as of
commit a006907) reads `state.json` as this code writes it without failing, but
drops what it does not know: the config history (this code builds it again over
its next runs once it is back), the parts of history gaps still to search, and
the builds a scan saw that wait for a config not known yet (neither it nor this
code judges those again); it judges the builds held for such a config by the
one config it reads.

## Limits

- A gap in TestGrid's history is searched in GCS 14 days of it a run, the most
  recent first: after an outage longer than that, the runs that follow search the
  rest, 14 days each (`state.json` keeps what is left), as far back as GCS keeps
  the builds.
- The port follows a test group's cell options: rows of one name merged or
  split, the Pod row or not, custom evaluator rules (as test-infra's configurator
  can write them: a rule without comparisons sets every test's status, one with
  them never matches), and `test_name_config` (Go's `fmt.Sprintf`, with the
  test's properties, its junit file's name and `finished.json`'s metadata), each
  config's for the reads TestGrid made under it. Two options that make one column
  of several builds or of several jobs (build overrides, several GCS prefixes)
  are not followed: none of testgrid.k8s.io's 8,275 test groups sets either (its
  config of 2026-10-12), and builds of a tab whose group does are held, with a
  failing run, rather than judged by a rule TestGrid does not use.
- When TestGrid read a build, and what GCS held then, is inferred from its
  updater's code and from when GCS says each object was written (Last-Modified,
  and the listing's creation times; an object without a time counts as there
  from the start): it hands each job's group out every 2 hours at the latest
  (and sooner when GCS notifies it of new results, often minutes apart), and
  drops a hand-out while an update of the group still runs, an update taking up
  to 20 minutes, and one whose reads come after its grace (10 of its 20
  minutes) leaving all but its first column to an update right after it, so a
  column it reads again is read again within 3 hours, at any time in that span;
  the job's other builds are read from GCS
  for when an older one runs, or one without a start holds it, back as far as
  their chains reach (21 days at most, or a tab's days of results and a week).
  That is how TestGrid goes when its
  updates run on time and succeed: an update that fails (one that runs out of
  its 20 minutes, as when its reads wait on other groups' in a busy updater) is
  tried again 30 to 60 minutes later, an update right after one cut by its grace
  may be cut too (each leaving the rest of the columns to the next), and an
  updater behind schedule starts updates late; TestGrid's summary of a tab says
  only when its grid last changed,
  as of when it is read (twice a day here), and its own records are not public.
  A build counts as too old by the days of results before the update that read
  it began: on a tab of one day of results, a build not finished after 24 hours
  is red, as TestGrid paints it FAIL when an update that began before the
  deadline reads it after (it mostly drops the build as too old instead).
- TestGrid keeps a test group's grid under 2 MB by dropping its oldest columns
  (and paints every cell of the oldest one it keeps UNKNOWN): its largest groups
  show far less than their days of results (on 2026-10-12, gce-cos-master-default
  2.5 of its 15 days, gce-ubuntu-master-containerd 3.7, kind-master 7.2). The
  grid's size is not public, so the archive takes a build to be on the tab for its
  days of results: one TestGrid reads again after a config change (whose column
  it then drops at once, the grid being too large) counts as shown, and is
  archived if that read is red, though TestGrid may not have shown it. A build the
  last scan saw that TestGrid dropped is searched in GCS if the tab's config
  changed since (see [How a run works](#how-a-run-works)); else the scan saw its
  column as TestGrid last painted it.
- A column red only between two scans, on a tab that still shows the build, is
  not seen: one TestGrid painted red and then, read again, not (a build without
  `finished.json` at its 24-hour deadline that finished later, read again while
  an older column ran). In GCS, none of the 20,284 builds of the dashboards' 46
  jobs over 22 days (to 2026-10-12) wrote `finished.json` more than 24 hours
  after `started.json`, and none rewrote its `started.json`, `finished.json` or
  `podinfo.json`.
- TestGrid takes a config within 27 minutes of its upload when its config merger
  succeeds (and its tabulator shows a tab's new filters on the columns it has
  within an hour more); a merge that fails (one of its sources cannot be read)
  keeps the config before until one succeeds, which, too, cannot be seen from here.
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
whose listing pages, sizes and encodings are checked that way too), each also
with random test groups' cell options, and Go's `fmt.Sprintf` for test names;
it also checks that the port decodes `started.json`, `finished.json` and
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
