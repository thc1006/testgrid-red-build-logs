# TestGrid's own code, to check the ports against

`../testgrid_rule.py` ports the rule TestGrid paints a build's cells by. The fetch
needs it for builds TestGrid no longer shows (a gap between runs) or showed running
long past their time, and `cross_check.py` uses it to check that none of those was
missed. This folder
builds TestGrid's own code, at the commit TestGrid's production manifests deploy, to
check the port against, and the fetch's reading of Prow's index links
(`fetch_red_logs.link_folder`).

- `pinned.json`: that commit (TestGrid production's updater image tag in
  `cluster/prod/updater.yaml` ends in `-g<commit>`) and the Go version it is
  built with.
- `go/pkg/updater/zz_oracle.go`: added to TestGrid's `pkg/updater`, so it calls the
  real `readSuites` and `convertResult`. It reads a build like `readResult`, with
  the four reads one after another and every error kept, because `readResult`
  returns whichever error arrives first.
- `go/pkg/tabulator/zz_oracle.go`: added to TestGrid's `pkg/tabulator`, so it calls
  the real `filterGrid`: the rows a tab's base options keep.
- `go/cmd/tgoracle`: judges builds held in memory, one JSON object per line.
- `go/cmd/schemadump`: the Go types podinfo.json, started.json and finished.json
  decode into. `gotypes.py` turns its output into `../testgrid_types.json`.
- `go/cmd/unidump`: Go's unicode tables, which `../goregexp.py` matches by.
  `gounicode.py` checks `../go_unicode.json.gz` against its output, or writes it.
- `go/cmd/gocheck`: what Go's strconv, time.Parse, resource.ParseQuantity and
  fmt.Sprintf make of strings, the rows `filterGrid` keeps, and the builds
  `util/gcs`'s `ListBuilds` takes from an entry of Prow's index of a pull request
  job.
- `fuzz/`:
  - `edge.py`: about 290 cases, one Go behaviour each.
  - `gen.py` and `run.py`: random builds.
  - `realdiff.py`: real builds of the archive's jobs, read from GCS independently and
    by the fetch's own `gcs_column` (its listing, pages, sizes and encodings), judged
    by TestGrid's code and by the port.
  - `golden.py`: writes `../testdata/testgrid_rule_golden.jsonl.gz`, which
    `../test_testgrid_rule.py` replays without Go.
  - `filterdiff.py`: `../testgrid_config.py`'s row filter (Go's url.ParseQuery and
    regexp, matched without backtracking) against `filterGrid`: the base options
    of testgrid.k8s.io's tabs, about 250 edge cases, and random ones. Where
    TestGrid rejects base options the port must not apply them; where it keeps
    rows the port must keep the same ones; patterns of the syntax the port
    follows must be applied.
  - `linkdiff.py`: the fetch's `link_folder` against `ListBuilds`, for the link an
    entry of Prow's index holds: about 100 edge cases, random links, and links a
    few characters off ones Prow writes. Where TestGrid's listing fails the port
    must fail it; where TestGrid takes a build the port must take the same one,
    and read the files of exactly the builds the archive's own rule reads, where
    TestGrid reads started.json.

The workflow `.github/workflows/oracle.yml` runs daily, and on a change to the
port. It does all of this:

- builds TestGrid's code;
- checks that the types match `testgrid_types.json`, and that Go's unicode tables
  match `go_unicode.json.gz`;
- runs the edge cases, 5000 random builds (a seed per run; the cases that differ are
  attached to a failed run) and 2 real builds of each job;
- runs the filter's edge cases and 20,000 random ones;
- runs Go's `fmt.Sprintf` against `gofmt.py` (a test group's `test_name_config`
  names its rows with it) on 20,000 random formats (`gocheck`'s "sprintf");
- runs the index links' edge cases and 20,000 random ones (`gocheck`'s "link");
- fails if TestGrid's production manifests (`cluster/prod/updater.yaml` and
  `tabulator.yaml`) no longer deploy the pinned commit of its updater or tabulator
  (`drift.sh`, checked even when another check failed; every archive run checks it
  too, and warns);
- then, if the manifests moved, `follow.sh` builds TestGrid's code at the commit
  they deploy, runs every check above against it, and lists what changed between the
  two commits in the code the port follows (`relevant_diff.py`). When nothing did
  and every check passes, it moves `pinned.json` (and regenerates
  `testgrid_types.json` and `go_unicode.json.gz`) on the branch
  `follow-testgrid-<commit>` and proposes it
  as a pull request; GitHub Actions may not open pull requests in this repository
  (Settings, Actions, General), so the run then links to open one from the branch.
  Otherwise it fails with what changed, for a person to compare.

## Run it

```sh
tool/oracle/build.sh /tmp/oracle          # Go (pinned.json's version) and the network
export TGORACLE=/tmp/oracle/tgoracle GOCHECK=/tmp/oracle/gocheck
cd tool
python3 -I oracle/fuzz/edge.py
python3 -I oracle/fuzz/filterdiff.py 1 20000
python3 -I oracle/fuzz/fmtdiff.py 1 20000
uv run --locked --no-dev python -I oracle/fuzz/linkdiff.py 1 20000   # it imports the fetch
python3 -I oracle/fuzz/run.py 1 5000     # seed, cases; mismatches go to bad-<seed>.jsonl
uv run --locked --no-dev python -I oracle/fuzz/realdiff.py ../archive 3   # the fetch's code and environment
/tmp/oracle/schemadump | python3 -I oracle/gotypes.py | cmp - testgrid_types.json
/tmp/oracle/unidump | python3 -I oracle/gounicode.py check go_unicode.json.gz   # write instead of check, after a Go change
python3 -I oracle/fuzz/golden.py testdata/testgrid_rule_golden.jsonl.gz   # only when the rule changed
```

## When TestGrid's production manifests move to another commit

The daily workflow notices, and proposes the new commit itself when nothing the
port follows changed. When something did:

1. Compare the new commit with the pinned one. The code that matters is in
   `pkg/updater` (`read.go`, `gcs.go`, `inflate.go`, `updater.go`), `util/gcs`
   (`read.go`, and `gcs.go`'s `Path.SetURL`, which `fetch_red_logs.link_folder`
   follows), `metadata` and `metadata/junit`, `pkg/tabulator` (`filter.go`,
   and where `tabulate` calls it), plus `go.mod` (k8s.io/api is the Pod type, and
   the Go version decides encoding/json, encoding/xml, net/url, fmt and regexp).
   A TestGroup field the updater newly reads belongs in `testgrid_config.py`'s
   `GROUP_FIELDS`, and `cell_options` or `group_problems`.
2. Port any change into the file that follows that code (`THIRD_PARTY_NOTICES.md`
   names which): `testgrid_rule.py`, `testgrid_config.py`, `gofmt.py`,
   `goregexp.py` or `fetch_red_logs.py`.
3. Update `pinned.json`.
4. Rebuild, regenerate `testgrid_types.json` and `go_unicode.json.gz`, and run
   the checks above.
5. Regenerate the golden vectors, and make sure no other test changed.
6. Update `THIRD_PARTY_NOTICES.md` at the root, and the headers of the ported
   files, to the new versions and files.

## The config the port judges by

- **Test-group options and the rows a tab shows.** TestGrid production reads its
  config from GCS buckets that are not public, but it is built from public
  sources: test-infra's `config/testgrids` and its Prow jobs' `testgrid-*`
  annotations, by the configurator image test-infra's
  `post-test-infra-upload-testgrid-config` job runs. `../testgrid_config.sh`
  builds the config of each of that job's runs as the run did (at its commit, with
  the image and arguments of its `prowjob.json`), and `../testgrid_config.py` keeps
  each tab's base options and the TestGroup fields `pkg/updater` reads them by.
  The port follows a group's cell options (`testgrid_config.cell_options`; the
  oracle's random builds come with random ones); a tab whose group sets build
  overrides or several GCS prefixes is not judged from GCS. The fetch checks each
  tab's table against its config on every run.

## What the port cannot know

- **Timing.** TestGrid read a build when it did, not now: the fetch works out when
  it may have from TestGrid's code and GCS's object times (see
  `fetch_red_logs.gcs_column`), and judges every column it may have read then.
  An error TestGrid itself hit reading GCS is invisible from here.
- **Racing reads.** When `readResult`'s reads race, every outcome is possible,
  and the build counts as red if any is. This is mostly what TestGrid ends with:
  it keeps re-reading a column while any cell of it is RUNNING (the build, or its
  Pod row waiting up to an hour for podinfo.json), and the fetch does not decide a
  column that is not red until then.
- **Pathological quantities.** A pod quantity with an exponent near ±2^31 sends
  TestGrid's Go code into an effectively endless computation; TestGrid then
  times out (TOOL_FAIL). The port answers at once. Prow's podinfo.json never holds
  one, as the API server normalizes quantities. The fuzzers keep such cases out,
  and `harness.py` sets aside any case the oracle cannot finish.
