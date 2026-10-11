#!/usr/bin/env bash
# Build the oracle: TestGrid's own code at the pinned commit (pinned.json), with
# go/pkg/updater/zz_oracle.go added to its updater package, and the commands in
# go/cmd: tgoracle (judges builds as TestGrid does), schemadump (the Go types,
# for ../testgrid_types.json) and gocheck (strconv, time and quantity parsing, and
# the tabulator's row filter, with go/pkg/tabulator/zz_oracle.go added to it).
# Usage: build.sh OUTDIR [VERSION]: TestGrid's module at VERSION instead of the
# pinned one (follow.sh does that). Needs Go and the network.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1"
out=$(cd "$1" && pwd)
version=${2:-$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["testgrid_module_version"])' "$here/pinned.json")}
work=$(mktemp -d)
trap 'chmod -R u+w "$work"; rm -rf "$work"' EXIT
# go mod download needs a main module to run in; on failure its JSON says why
if ! (cd "$work" && go mod init oracle-download >/dev/null 2>&1 &&
      go mod download -json "github.com/GoogleCloudPlatform/testgrid@$version") > "$work/download.json"; then
  echo "downloading TestGrid $version failed:" >&2
  cat "$work/download.json" >&2
  exit 1
fi
src=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["Dir"])' "$work/download.json")
cp -r "$src" "$work/testgrid"
chmod -R u+w "$work/testgrid"
cp "$here/go/pkg/updater/zz_oracle.go" "$work/testgrid/pkg/updater/"
cp "$here/go/pkg/tabulator/zz_oracle.go" "$work/testgrid/pkg/tabulator/"
for cmd in tgoracle schemadump gocheck; do
  mkdir -p "$work/testgrid/cmd/$cmd"
  cp "$here/go/cmd/$cmd/main.go" "$work/testgrid/cmd/$cmd/"
done
(cd "$work/testgrid" && GOFLAGS=-mod=mod go build -o "$out/" ./cmd/tgoracle ./cmd/schemadump ./cmd/gocheck)
echo "built $out/tgoracle, $out/schemadump and $out/gocheck from TestGrid $version"
