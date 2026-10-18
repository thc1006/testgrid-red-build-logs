# Third-party notices

Parts of `tool/` are Python ports of code from Go, TestGrid and Kubernetes, and
some of its data comes from them. The tool judges builds the way TestGrid does,
so those parts follow the upstream code closely. This file names the upstream
files each part comes from, their licenses, and what changed.

The ports were written from Go 1.26.5, TestGrid `f15bfa4808a4`, and
k8s.io/apimachinery and k8s.io/api v0.27.4, the versions TestGrid's `go.mod`
requires. [`tool/oracle/pinned.json`](tool/oracle/pinned.json) names the
versions the tool is checked against now. It moves on its own only when nothing
the ports follow has changed; otherwise a person ports the change, updates this
file, and moves it.

## Go

Go 1.26.5, under the BSD 3-Clause license in
[`third_party/go/LICENSE`](third_party/go/LICENSE). The license reads Copyright
2009 The Go Authors; the source files below carry years from 2009 to 2025.
Paths are under `src/`.

- `tool/goregexp.py`: the parser in `regexp/syntax/parse.go`,
  `regexp/syntax/regexp.go` and `regexp/syntax/perl_groups.go`, with
  `IsWordChar` from `regexp/syntax/prog.go`, translated to Python function by
  function. The matcher is new code, checked against Go's results.
- `tool/gofmt.py`: `fmt/print.go`, `fmt/format.go`, `strconv/quote.go` and
  `IsPrint` in `unicode/graphic.go`, translated for string operands only.
- `tool/testgrid_rule.py`: what TestGrid's reads rely on, rewritten in Python to
  behave the same: `encoding/json` (`scanner.go`, `decode.go`, `encode.go`,
  `fold.go`, `stream.go`), `encoding/xml` (`xml.go`, `read.go`), `time`
  (`format.go`, `format_rfc3339.go`, `time.go`), `internal/strconv` (`atoi.go`,
  `atof.go`), `net/url/url.go`, `strings/strings.go`, `unicode/graphic.go` and
  `unicode/utf8/utf8.go`.
- `tool/testgrid_config.py`: `ParseQuery` and `QueryUnescape` in
  `net/url/url.go`, translated to Python.
- `tool/fetch_red_logs.py`: what TestGrid's reading of Prow's index links relies
  on, rewritten in Python to behave the same: `TrimSpace` in
  `strings/strings.go`; how `url.Parse` takes a link apart and unescapes it
  (`getScheme`, `parse`, `parseAuthority`, `parseHost` and `unescape` in
  `net/url/url.go`, with the host characters of `shouldEscape`'s table in
  `net/url/encoding_table.go`); `Base` in `path/path.go`; and how `unquoteBytes`
  in `encoding/json/decode.go` reads a lone surrogate escape (as U+FFFD).
- Data: `tool/go_unicode.json.gz` holds the tables of package `unicode`, with
  the results of `unicode.SimpleFold`, dumped by `tool/oracle/go/cmd/unidump`.
  `tool/testgrid_types.json` holds the slice capacities `reflect.Value.Grow`
  gives and the ASCII case folds of `unicode.SimpleFold`, dumped by
  `tool/oracle/go/cmd/schemadump`.

## Unicode

`tool/go_unicode.json.gz` holds Unicode 15.0.0 character data as Go's `unicode`
tables carry it, and `tool/testgrid_rule.py` and `tool/fetch_red_logs.py` list
the White_Space characters `unicode.IsSpace` matches. Unicode 15.0.0 was
released under the Unicode, Inc. License Agreement for Data Files and Software,
in [`third_party/unicode/LICENSE`](third_party/unicode/LICENSE).

## TestGrid

github.com/GoogleCloudPlatform/testgrid at `f15bfa4808a4`, under the Apache
License 2.0, the license of this repository ([`LICENSE`](LICENSE)). Each file
below carries "Copyright <year> The TestGrid Authors" (TG) or "Copyright <year>
The Kubernetes Authors" (K8s), with the year shown, but
`pb/test_status/test_status.proto`, which carries none.

- `tool/testgrid_rule.py`: parts of `pkg/updater/read.go` (TG 2020),
  `pkg/updater/gcs.go` (TG 2020), `pkg/updater/eval.go` (TG 2022),
  `internal/result/results.go` (K8s 2019), `util/gcs/read.go` (K8s 2019),
  `util/gcs/gcs.go` (K8s 2018), `metadata/job.go` (K8s 2019) and
  `metadata/junit/junit.go` (K8s 2019), rewritten in Python.
- `tool/testgrid_config.py`: `filterGrid` in `pkg/tabulator/filter.go` (TG 2022),
  translated to Python. It also follows how `cmd/updater/main.go` and
  `pkg/updater/updater.go` (K8s 2018) find a test group's results, and how
  `makeOptions` in `pkg/updater/read.go` (TG 2020) and `evalProperties` in
  `pkg/updater/eval.go` (TG 2022) treat its options, as far as test-infra's
  configurator can set them.
- `tool/fetch_red_logs.py`: models when TestGrid's updater reads a build,
  following `pkg/updater/updater.go` (K8s 2018), `pkg/updater/read.go` and
  `pkg/updater/inflate.go` (TG 2020). It reads the links of Prow's index as
  `readLink`, `ListBuilds` and `Build` in `util/gcs/read.go` (K8s 2019) and
  `Path.SetURL` and `Path.Object` in `util/gcs/gcs.go` (K8s 2018) do, rewritten
  in Python.
- `tool/oracle/go/pkg/updater/zz_oracle.go`: adapted from `readResult` and
  `readColumns` in `pkg/updater/read.go` (TG 2020). The four reads run one after
  another and every error is kept, so the oracle can compare each one with the
  port.
- Data: `tool/testgrid_types.json` describes the TestGrid types that
  `started.json`, `finished.json` and `podinfo.json` are decoded into, and the
  tools use the names and values of TestGrid's `TestStatus`
  (`pb/test_status/test_status.proto`).

## Kubernetes

k8s.io/apimachinery and k8s.io/api v0.27.4, under the Apache License 2.0
([`LICENSE`](LICENSE)), Copyright The Kubernetes Authors. The apimachinery files
below carry 2014 or 2016.

- `tool/testgrid_rule.py`: `pkg/api/resource` (`quantity.go`, `suffix.go`,
  `math.go`), `pkg/apis/meta/v1` (`time.go`, `helpers.go`) and
  `pkg/util/intstr/intstr.go`, rewritten in Python.
- Data: `tool/testgrid_types.json` describes the k8s.io/api `core/v1` types and
  the apimachinery types that `podinfo.json` holds.

## Google Cloud Storage client

cloud.google.com/go/storage v1.31.0, under the Apache License 2.0
([`LICENSE`](LICENSE)), Copyright 2014 Google LLC.

- `tool/testgrid_rule.py` uses its error text for a missing object, and
  `tool/fetch_red_logs.py` follows how it reads a gzip-encoded object.
