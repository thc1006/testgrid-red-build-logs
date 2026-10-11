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
- `go/cmd/tgoracle`: judges builds held in memory, one JSON object per line.
- `go/cmd/schemadump`: the Go types podinfo.json, started.json and finished.json
  decode into. `gotypes.py` turns its output into `../testgrid_types.json`.
- `go/cmd/gocheck`: what Go's strconv, time.Parse and resource.ParseQuantity make
  of strings.
- `fuzz/`:
  - `edge.py`: about 250 cases, one Go behaviour each.
  - `gen.py` and `run.py`: random builds.
  - `realdiff.py`: real builds of the archive's jobs, read from GCS independently and
    by the fetch's own `gcs_column` (its listing, pages, sizes and encodings), judged
    by TestGrid's code and by the port.
  - `golden.py`: writes `../testdata/testgrid_rule_golden.jsonl.gz`, which
    `../test_testgrid_rule.py` replays without Go.

The workflow `.github/workflows/oracle.yml` runs weekly, and on a change to the
port. It does all of this:

- builds TestGrid's code;
- checks that the types match `testgrid_types.json`;
- runs the edge cases, 5000 random builds (a seed per run; the cases that differ are
  attached to a failed run) and 2 real builds of each job;
- fails if TestGrid production no longer runs the pinned commit (checked even when
  another check failed).

## Run it

```sh
tool/oracle/build.sh /tmp/oracle          # Go (pinned.json's version) and the network
export TGORACLE=/tmp/oracle/tgoracle
cd tool
python3 -I oracle/fuzz/edge.py
python3 -I oracle/fuzz/run.py 1 5000     # seed, cases; mismatches go to bad-<seed>.jsonl
uv run --locked --no-dev python -I oracle/fuzz/realdiff.py ../archive 3   # the fetch's code and environment
/tmp/oracle/schemadump | python3 -I oracle/gotypes.py | cmp - testgrid_types.json
python3 -I oracle/fuzz/golden.py testdata/testgrid_rule_golden.jsonl.gz   # only when the rule changed
```

## When TestGrid production moves to another commit

1. Compare the new commit with the pinned one. The code that matters is in
   `pkg/updater` (`read.go`, `gcs.go`, `inflate.go`, `updater.go`), `util/gcs`
   (`read.go`), `metadata` and `metadata/junit`, plus `go.mod` (k8s.io/api is the
   Pod type, and the Go version decides encoding/json and encoding/xml).
2. Port any change into `testgrid_rule.py`.
3. Update `pinned.json`.
4. Rebuild, regenerate `testgrid_types.json`, and run the checks above.
5. Regenerate the golden vectors, and make sure no other test changed.

## What the port cannot know

- **Test-group options.** TestGrid's config is not public (`gs://k8s-testgrid/config`
  is 403). The port takes TestGrid's defaults from test-infra's
  `config/testgrids/default.yaml`: rows of the same name merged, the Pod row
  shown, no custom evaluator rules, test names as they are. A tab's days of
  results is taken as the longer of 15 (the default) and the history its table shows.
- **Rows a tab hides.** The fetch learns these from the tab's table: every column
  has its job's Overall and Pod rows, so the one missing while the other is
  there is hidden, and both are when the table has results without either. A
  tab not read in a run keeps the rows it showed when last read. A filter on test
  names cannot be learned this way, so test rows count as shown; so does the
  bare `Overall` row of a column TestGrid could not read (TOOL_FAIL), as no table
  shows whether a tab's filter would hide it.
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
