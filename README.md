# Red TestGrid build logs

This repository keeps the raw Prow `build-log.txt` of Kubernetes CI builds that
TestGrid shows red on
[sig-release-master-blocking](https://testgrid.k8s.io/sig-release-master-blocking)
and [sig-release-master-informing](https://testgrid.k8s.io/sig-release-master-informing).
To find a log, start at [archive/INDEX.md](archive/INDEX.md).

A GitHub Actions workflow reads both dashboards about twice a day and saves one
copy per build. A build that scrolled off a tab between two runs is found in GCS
instead and judged by TestGrid's own rule. [Limits](#limits) says what the
archive can miss, and when a build taken from GCS may never have shown red.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="archive/charts/trend-dark.svg">
  <img alt="Red builds per day on the blocking and informing dashboards" src="archive/charts/trend-light.svg">
</picture>

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="archive/charts/tabs-dark.svg">
  <img alt="Red builds per tab over the last 14 days" src="archive/charts/tabs-light.svg">
</picture>

## Finding a log

[archive/INDEX.md](archive/INDEX.md) lists the runs and red builds of the last
14 days, and links a page for each week. Each run that archives has a folder
under [archive/runs/](archive/runs/), named by its start time in UTC. A build
stays in the folder of the run that first saved it:

```
archive/runs/<YYYY-MM>/<run>/<job>/<build>/
```

- `build-log.txt.gz`: the log. Read it with `gzip -dc build-log.txt.gz | less`.
- `build-log.txt.gz.000`, `.001` and so on: a log over 95 MiB after gzip, split
  because GitHub blocks files over 100 MiB. Read it with
  `cat build-log.txt.gz.* | gzip -dc | less`.
- `podinfo.json`: kept when GCS had no log, as for a build whose pod never ran.
- `meta.json`: the tabs, the red tests, TestGrid's messages and the log's md5.
  A build GCS had no file for has only this.

## What counts as red

A cell is red when TestGrid paints it red: `FAIL`, `TIMED_OUT`,
`CATEGORIZED_FAIL` or `TOOL_FAIL`. Flaky, build-failed, unknown and aborted
cells are not. A build Prow aborted is usually red anyway, because TestGrid
marks its Overall cell `FAIL`.

The red cells come from the same tables the TestGrid page loads. A build taken
from GCS is judged by [`testgrid_rule.py`](tool/testgrid_rule.py), a port of
TestGrid's rule, with the config its tab had at the time. It is marked as taken
from GCS.

## Limits

### Red builds the archive can miss

- A cell that was red only between two runs, on a build the tab still shows, is
  not seen.
- TestGrid does not publish which of its updates failed, or which left columns
  for more than the next update to read. The workflow takes none to have done
  so. It misses a build judged from GCS that only such a later read showed red,
  such as the retry of a failed update.
- If a build's tab may have shown it under a config the archive does not know
  yet, the build waits until it can be judged. So does a build its tab may have
  shown under a config whose test group sets an option `testgrid_rule.py` does
  not support. One still waiting 60 days after it was made is not archived, and
  the run fails with a warning that TestGrid may have shown it red. If a tab's
  test group sets such an option now, the run fails rather than guess.
- TestGrid can show a tab missing from the latest config the archive knows. A
  build TestGrid may have shown there after the tab's last config was replaced
  waits as above. If TestGrid stops listing the tab first, the build is judged
  as if the tab had been gone since then. So it is not red, though TestGrid may
  have shown it red meanwhile.
- A build can scroll off after a run read its tab and saw it, not red. If the
  tab's config changed after the last such run, the build is judged from GCS.
  If a config the archive does not know yet was in force since, the build waits
  for it, at most 14 days after that run. If it is still not known then, the
  build is dropped without a warning of its own.
- Red builds in a long gap in TestGrid's history can be missing for several
  runs. GCS is searched newest first: up to 28 days back in the run that finds
  the gap, then 14 days more in each later run.

### Builds taken from GCS that may never have shown red

- TestGrid does not publish when it read a build, and some of its reads can end
  in more than one way. The workflow works these out from TestGrid's code and
  from GCS timestamps. When that leaves a doubt, the build counts as red.
- The rows a tab's filters hide count as shown before the archive has read any
  TestGrid config. They do too on a tab it knows only from a build it still
  watches.
- On a tab with a base option the archive does not know, TestGrid's tabulator
  ignores the option, and so does the archive. The tab's page may still hide
  rows by it.

## How a run works

1. Build the TestGrid config of each new run of test-infra's upload job, so a
   build judged from GCS is judged by the config its tab had then.
2. Read every tab of both dashboards and collect the red builds.
3. Search GCS for the builds in any gap in TestGrid's history, and judge them by
   TestGrid's rule.
4. Download each new log, including those found in step 3, and check its size
   and checksum against GCS. In the same pass, compare archived builds with GCS
   again, since Prow can upload a log twice.

   A build is compared on every run until 48 hours after it finished. One
   without a log or without `finished.json` is compared until 14 days after it
   was archived. After that, it is compared at least once a week. The archive's
   builds are split into seven by a hash of their job and build. Each run
   compares the seventh for its UTC day, and those of any days since the run
   before, as `cross_check.py` does. A run cut off before it got to a build
   leaves it to the next run that compares its seventh.
5. Rewrite the index and the charts, then run
   [`cross_check.py`](tool/cross_check.py). It checks the archive again on its
   own, and fails the run if a red build is missing or a stored file changed.
   What it checked, and the outcome, are kept in the run's folder as
   `verification.json`.

The docstrings in [`tool/`](tool/) explain each step.
[`tool/oracle/`](tool/oracle/README.md) checks the port of TestGrid's rule
against TestGrid's own Go code.

## Schedule and alerts

The workflow is scheduled every 3 hours, but GitHub often starts scheduled runs
late or skips them. A scheduled run archives unless the archive already changed
in the same half of the UTC day (00:00 to 12:00, 12:00 to 24:00) less than 11
hours before. A run started by hand, and any re-run, always archives.

When a run fails, GitHub emails the person who last changed the schedule, or who
started the run. A scheduled run that finds the archive more than 26 hours old
still archives, then fails.

GitHub sends nothing when it starts no run at all. For that case, an optional
heartbeat pings an outside check that alerts when the pings stop, such as
healthchecks.io. To turn it on, set the repository variable `HEARTBEAT` to
`on`, and the ping URL as the secret `HEARTBEAT_URL` of an environment named
`heartbeat`. Only scheduled runs ping, so set the check to expect a ping about
every 12 hours.

GitHub disables the schedule of a public repository after 60 days without
activity. If that happens, enable the workflow again from the Actions tab, or
with `gh workflow enable red-build-logs.yml`.

## Running it locally

```sh
cd tool
uv sync --locked
PYTHON=.venv/bin/python ./testgrid_config.sh /tmp/testgrid-config.json   # needs git, and docker or podman
uv run fetch_red_logs.py --archive ../archive --gzip --testgrid-config /tmp/testgrid-config.json
uv run cross_check.py --archive ../archive
uv run --locked python -m unittest
```

## License

[Apache License 2.0](LICENSE). The archived logs are copies of Kubernetes CI
output that is already public in the `kubernetes-ci-logs` bucket. Parts of
`tool/` are ports of Go, TestGrid and Kubernetes code under their own licenses;
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) lists them.
