#!/usr/bin/env bash
# Writes TestGrid's config for the archive's tabs to $1, as JSON for
# fetch_red_logs.py --testgrid-config. It is built the way testgrid.k8s.io's is:
# by the configurator image that test-infra's post-test-infra-upload-testgrid-config
# job runs, with that job's arguments (printing the config instead of uploading
# it), on the tip of test-infra's config/testgrids and config/jobs; then
# testgrid_config.py extract keeps what the archive needs.
# Needs git, docker or podman, and $PYTHON (default: python3) with PyYAML, such as
# the tool's environment (uv sync --project tool).
set -euo pipefail
out=$1
here=$(cd "$(dirname "$0")" && pwd)
python=${PYTHON:-python3}
engine=$(command -v docker || command -v podman) || { echo "testgrid_config.sh: needs docker or podman" >&2; exit 1; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

git clone -q --depth 1 --filter=blob:none --sparse https://github.com/kubernetes/test-infra "$work/test-infra"
git -C "$work/test-infra" sparse-checkout set --no-cone /config/jobs/ /config/testgrids/ /config/prow/config.yaml
commit=$(git -C "$work/test-infra" rev-parse HEAD)
committed=$(git -C "$work/test-infra" log -1 --format=%ct)

# the image, its command, then the arguments
"$python" -I "$here/testgrid_config.py" job "$work/test-infra" > "$work/job"
mapfile -d '' -t job < "$work/job"
"$engine" pull -q "${job[0]}" > /dev/null
digest=$("$engine" image inspect --format '{{index .RepoDigests 0}}' "${job[0]}" 2> /dev/null || true)
# No network and a read-only checkout: the configurator only reads the config and prints it.
# (It logs a line per job name it shortens; only a failure's log is shown.)
if ! "$engine" run --rm --network none -v "$work/test-infra:/test-infra:ro" -w /test-infra \
    --entrypoint "${job[1]}" "${job[0]}" "${job[@]:2}" > "$work/config.yaml" 2> "$work/configurator.log"; then
  tail -n 20 "$work/configurator.log" >&2
  exit 1
fi
"$python" -I "$here/testgrid_config.py" extract "$work/config.yaml" --commit "$commit" --committed "$committed" \
  --image "${job[0]}" --digest "${digest#*@}" > "$work/config.json"
mv "$work/config.json" "$out"
echo "TestGrid's config from test-infra $commit by ${job[0]} (${digest#*@})"
