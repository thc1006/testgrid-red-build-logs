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

[archive/INDEX.md](archive/INDEX.md) lists the last 14 days and links a page per week
for everything older.

## What counts as red

A cell is red exactly when TestGrid paints it red: `FAIL`, `TIMED_OUT`,
`CATEGORIZED_FAIL` or `TOOL_FAIL`. Purple (flaky), black (build failed) and
grey (unknown, aborted) cells are not. Tables are read the way the dashboard
page loads them, so the red cells are the ones you see when you open a tab.
Each column is one Prow build, so a build with several red cells is saved once.

## Layout

| Path | Contents |
|---|---|
| `archive/logs/<job>/<build>/build-log.txt.gz` | the raw log, gzipped; `meta.json` has the raw log's md5, checked against GCS |
| `archive/logs/<job>/<build>/podinfo.json` | only for builds whose pod never ran, so there is no log |
| `archive/logs/<job>/<build>/meta.json` | tabs, red tests, TestGrid messages, Prow result, links |
| `archive/INDEX.md` | the charts, red builds not archived yet, and the last 14 days of builds |
| `archive/weeks/<year>-W<week>.md`, `.json` | every archived build, one page per ISO week (UTC); `archive/index.json` lists them |
| `archive/charts/*.svg` | the charts above, in light and dark versions |
| `archive/runs/*.json` | what each run read, archived, retried and failed |
| `archive/state.json` | each tab's last scan, builds to retry, and builds given up on after 14 days |

To read a log: `gzip -dc archive/logs/<job>/<build>/build-log.txt.gz | less`.

A file still over 95 MB after gzip would be rejected by GitHub, so such a file
is verified but not stored: its `meta.json` keeps the md5 and size, and the
index links the copy in GCS instead.

## How a run works

1. Read every tab of both dashboards and find the red cells.
2. Download the log of each red build not archived yet, checking its length
   and md5 against GCS. Builds that cannot be fetched yet are retried on later
   runs straight from GCS, even after TestGrid drops them.
3. Rewrite the index and the charts, then
   [`cross_check.py`](tool/cross_check.py) re-reads TestGrid and lists every
   build in GCS for the last 48 hours, and fails the run if a red build is
   missing or a stored log no longer matches.

The run keeps as many requests in flight as the servers allow, adjusting as it
goes; a full backfill of about 620 builds takes under a minute.

## Running it locally

```sh
cd tool
uv run fetch_red_logs.py --archive ../archive --gzip
uv run cross_check.py --archive ../archive
uv run python -m unittest
uv run ruff check .
```
