#!/usr/bin/env bash
# Exits 1 when TestGrid's production manifests (cluster/prod/updater.yaml and
# tabulator.yaml on its main branch) deploy another commit of the updater or tabulator
# than the one testgrid_rule.py and testgrid_config.py follow (pinned.json), or when that
# cannot be read. They say what production is to run: whether a rollout of them ended
# cannot be seen from here. TestGrid's image tags end in -g<commit>, or, for a release
# itself, in -v<version>: a newer updater may paint cells by another rule, a newer
# tabulator filter a tab's rows another way.
# In GitHub Actions it also sets the outputs updater, tabulator (the commits the
# manifests deploy) and moved (true when either is not the pinned one).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
pinned=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["testgrid_commit"])' "$here/pinned.json")
pinned_version=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["testgrid_module_version"])' \
  "$here/pinned.json")
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
  if [[ $tag =~ -g([0-9a-f]+)$ ]]; then
    running=${BASH_REMATCH[1]}  # (vDATE-vX.Y.Z-N-gCOMMIT: a commit after a release)
    same() { [ "${pinned#"$running"}" != "$pinned" ] || [ "${running#"$pinned"}" != "$running" ]; }
  elif [[ $tag =~ -(v[0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    running=${BASH_REMATCH[1]}  # (vDATE-vX.Y.Z: the release itself)
    same() { [ "$running" = "$pinned" ] || [ "$running" = "$pinned_version" ]; }
  else
    running=""
  fi
  echo "production manifest of the $component: $tag (commit $running), pinned: $pinned"
  echo "$component=$running" >> "${GITHUB_OUTPUT:-/dev/null}"
  if [ -z "$running" ] || ! same; then
    echo "::error::TestGrid's production manifest now deploys the $component $tag, not the pinned commit $pinned: see tool/oracle/README.md (When TestGrid production moves to another commit)."
    moved=true
    status=1
  fi
done
echo "moved=$moved" >> "${GITHUB_OUTPUT:-/dev/null}"
exit "$status"
