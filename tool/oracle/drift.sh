#!/usr/bin/env bash
# Exits 1 when TestGrid production's updater or tabulator runs another commit than
# the one testgrid_rule.py and testgrid_config.py follow (pinned.json), or when that
# cannot be read. TestGrid's image tags end in -g<commit>: a newer updater may paint
# cells by another rule, a newer tabulator filter a tab's rows another way.
# In GitHub Actions it also sets the outputs updater, tabulator (the commits
# production runs) and moved (true when either is not the pinned one).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
pinned=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["testgrid_commit"])' "$here/pinned.json")
status=0
moved=false
for component in updater tabulator; do
  # (the first image line; no head in the pipe, whose early exit would fail it under pipefail)
  tags=$(curl -fsS --retry 3 "https://raw.githubusercontent.com/GoogleCloudPlatform/testgrid/main/cluster/prod/$component.yaml" |
    sed -n "s|.*image: gcr.io/k8s-testgrid/$component:\(.*\)\$|\1|p") || tags=""
  tag=${tags%%$'\n'*}
  if [ -z "$tag" ]; then
    echo "::error::Could not read the $component image tag from TestGrid's cluster/prod/$component.yaml."
    status=1
    continue
  fi
  running=${tag##*-g}
  echo "production $component: $tag (commit $running), pinned: $pinned"
  echo "$component=$running" >> "${GITHUB_OUTPUT:-/dev/null}"
  if [ -z "$running" ] || { [ "${pinned#"$running"}" = "$pinned" ] && [ "${running#"$pinned"}" = "$running" ]; }; then
    echo "::error::TestGrid production's $component now runs $tag, not the pinned commit $pinned: see tool/oracle/README.md (When TestGrid production moves to another commit)."
    moved=true
    status=1
  fi
done
echo "moved=$moved" >> "${GITHUB_OUTPUT:-/dev/null}"
exit "$status"
