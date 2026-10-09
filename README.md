# Red TestGrid build logs

Twice a day, a GitHub Actions job saves the raw Prow `build-log.txt` behind
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

## Layout

Every run gets its own folder, named by its start time (UTC). A build is kept
in the folder of the run that first archived it.

| Path | Contents |
|---|---|
| `archive/runs/<YYYY-MM>/<run>/README.md` | what that run did, and the builds it archived, with links |
| `archive/runs/<YYYY-MM>/<run>/run.json` | the run's full report: tabs read, outcomes, errors, warnings |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/build-log.txt.gz` | the raw log, gzipped; `meta.json` has the raw log's md5, checked against GCS |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/podinfo.json` | only for builds whose pod never ran, so there is no log |
| `archive/runs/<YYYY-MM>/<run>/<job>/<build>/meta.json` | tabs, red tests, TestGrid messages, Prow result, links, and what GCS held |
| `archive/INDEX.md` | the charts, red builds not archived yet, the runs and builds of the last 14 days |
| `archive/weeks/<year>-W<week>.md`, `.json` | every archived build, one page per ISO week (UTC); `archive/index.json` lists them |
| `archive/charts/*.svg` | the charts above, in light and dark versions |
| `archive/state.json` | each tab's last scan, builds to retry, and builds given up on after 14 days |

To read a log: `gzip -dc archive/runs/<YYYY-MM>/<run>/<job>/<build>/build-log.txt.gz | less`.

A file still over 95 MB after gzip would be rejected by GitHub, so such a file
is verified but not stored: its `meta.json` keeps the md5 and size, and the
index links the copy in GCS instead.

## How a run works

1. Read every tab of both dashboards and find the red cells. TestGrid's data
   is checked strictly: a table that does not decode exactly fails that tab
   instead of being guessed at.
2. Download the log of each red build not archived yet, checking its length
   and md5 against GCS (or its crc32c, for an object GCS keeps no md5 for).
   Builds that cannot be fetched yet are retried on later runs straight from
   GCS, even after TestGrid drops them.
3. Compare each archived build with GCS again on every run until two days
   after it finished (14 days for a build without a log, in case one appears),
   and fetch it again if GCS now holds something else: Prow can upload a log
   twice. A copy taken before Prow was done is marked *provisional* until it
   settles.
4. If a tab's history no longer reaches back to the last run (a run was missed,
   or a tab keeps only a day of results), list that stretch in GCS and archive
   the builds TestGrid painted red by its own rule (started, and did not pass or
   did not finish within 24 hours), labelled as taken from GCS. Builds still
   running there are watched until that can be decided.
5. Rewrite the index and the charts, then
   [`cross_check.py`](tool/cross_check.py) re-reads TestGrid, compares the
   last 48 hours of archived files (logs and podinfo.json) with GCS, and lists
   every build in GCS for that time; it fails the run if a red build is missing
   or a stored file no longer matches.

The run keeps as many requests in flight as the servers allow, adjusting as it
goes; a full backfill of about 620 builds takes under a minute.

## When something fails

Nothing is lost silently: a failure makes the workflow run fail, and GitHub
notifies the person who set up the schedule (by email, with the default
notification settings).

| What happens | What the archive does |
|---|---|
| A log cannot be downloaded (network, server error, wrong md5) | retried with backoff in the run, then on every later run for 14 days, even after TestGrid drops the build; listed under "Not archived yet" in INDEX.md |
| A tab cannot be read, or its data is malformed | that tab's red builds wait for the next run; the tab is not marked as scanned |
| A run is missed or starts late | the next run notices the gap in TestGrid's history and takes the builds TestGrid painted red in that time from GCS, by TestGrid's own rule (started, and did not pass or did not finish within 24 hours); only a gap it cannot search fails the run |
| GCS replaces a log after it was archived | the next run fetches the new copy into the same folder and records it in `meta.json` |
| A step times out | what finished is committed with the run's report; the rest is retried next run |
| A run is cancelled, or hits the job's 95-minute limit | nothing is pushed; the next run fetches everything again |
| The commit cannot be pushed (a conflicting change on the branch) | the run fails and attaches its commit as a git bundle artifact; the next run fetches everything again |
| TestGrid paints a build red but GCS has nothing for it | the build is recorded without a log, and later runs look for a late one for 14 days |

To retry at once instead of waiting for the next scheduled run, start the
workflow by hand from the Actions tab (Run workflow).

## Limits

- A gap in TestGrid's history is searched in GCS at most 14 days back. If no
  run succeeds for longer than that, the next run says which stretch may be
  missing, and fails.
- For a build taken from GCS, red is decided the way TestGrid's updater
  decides it, from `finished.json`, and from `started.json` unless the build
  passed. TestGrid also paints a build red when one of its files is malformed;
  the files not read (a passed build's `started.json`, and `podinfo.json` and
  junit files) are not checked for that, as Prow does not write malformed
  ones. Where a start or finish time is missing or bogus, the build counts as
  red rather than risk missing one.

## Running it locally

```sh
cd tool
uv run fetch_red_logs.py --archive ../archive --gzip
uv run cross_check.py --archive ../archive
uv run --locked python -m unittest
uv run --locked ruff check .
```

## License

[Apache License 2.0](LICENSE), the same license as Kubernetes. The archived
logs are copies of Kubernetes CI output that is already public in the
`kubernetes-ci-logs` bucket.
