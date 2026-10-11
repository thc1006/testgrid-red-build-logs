# TestGrid's own code, to check `testgrid_rule.py` against

`../testgrid_rule.py` ports the rule TestGrid paints a build's cells by. The fetch
needs it for builds TestGrid no longer shows (a gap between runs) or showed running
long past their time, and `cross_check.py` uses it to check that none of those was
missed. This folder
builds TestGrid's own code, at the commit TestGrid production runs, to check the
port against.

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
- `go/cmd/gocheck`: what Go's strconv, time.Parse and resource.ParseQuantity make
  of strings, and the rows `filterGrid` keeps.
- `fuzz/`:
  - `edge.py`: about 250 cases, one Go behaviour each.
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

The workflow `.github/workflows/oracle.yml` runs daily, and on a change to the
port. It does all of this:

- builds TestGrid's code;
- checks that the types match `testgrid_types.json`;
- runs the edge cases, 5000 random builds (a seed per run; the cases that differ are
  attached to a failed run) and 2 real builds of each job;
- runs the filter's edge cases and 20,000 random ones;
- fails if TestGrid production's updater or tabulator no longer runs the pinned
  commit (`drift.sh`, checked even when another check failed; every archive run
  checks it too, and warns);
- then, if production moved, `follow.sh` builds TestGrid's code at the commit it
  runs, runs every check above against it, and lists what changed between the
  two commits in the code the port follows (`relevant_diff.py`). When nothing did
  and every check passes, it moves `pinned.json` (and regenerates
  `testgrid_types.json`) on the branch `follow-testgrid-<commit>` and proposes it
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
python3 -I oracle/fuzz/run.py 1 5000     # seed, cases; mismatches go to bad-<seed>.jsonl
uv run --locked --no-dev python -I oracle/fuzz/realdiff.py ../archive 3   # the fetch's code and environment
/tmp/oracle/schemadump | python3 -I oracle/gotypes.py | cmp - testgrid_types.json
python3 -I oracle/fuzz/golden.py testdata/testgrid_rule_golden.jsonl.gz   # only when the rule changed
```

## When TestGrid production moves to another commit

The daily workflow notices, and proposes the new commit itself when nothing the
port follows changed. When something did:

1. Compare the new commit with the pinned one. The code that matters is in
   `pkg/updater` (`read.go`, `gcs.go`, `inflate.go`, `updater.go`), `util/gcs`
   (`read.go`), `metadata` and `metadata/junit`, `pkg/tabulator` (`filter.go`,
   and where `tabulate` calls it), plus `go.mod` (k8s.io/api is the Pod type, and
   the Go version decides encoding/json, encoding/xml, net/url and regexp). A
   TestGroup field the updater newly reads belongs in `testgrid_config.py`'s
   `GROUP_FIELDS` and `group_problems`.
2. Port any change into `testgrid_rule.py`.
3. Update `pinned.json`.
4. Rebuild, regenerate `testgrid_types.json`, and run the checks above.
5. Regenerate the golden vectors, and make sure no other test changed.

## The config the port judges by

- **Test-group options and the rows a tab shows.** TestGrid production reads its
  config from GCS buckets that are not public, but it is built from public
  sources: test-infra's `config/testgrids` and its Prow jobs' `testgrid-*`
  annotations, by the configurator image test-infra's
  `post-test-infra-upload-testgrid-config` job runs. `../testgrid_config.sh` runs
  that image with that job's arguments on test-infra's tip, and
  `../testgrid_config.py` keeps each tab's base options and the TestGroup fields
  `pkg/updater` paints cells by. The port follows TestGrid's default cell options
  (rows of the same name merged, the Pod row, no custom evaluator rules, test
  names as they are); a tab whose group sets another is not judged from GCS. The
  fetch checks each tab's table against its config on every run.

## What the port cannot know

- **Timing.** TestGrid's last read of a build happened when it read it, not now.
  An error TestGrid itself hit reading GCS is also invisible from here.
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
