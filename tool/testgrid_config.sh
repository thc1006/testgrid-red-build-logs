#!/usr/bin/env bash
# Writes TestGrid's config for the archive's tabs to $1, as JSON for
# fetch_red_logs.py --testgrid-config: the config of each run of test-infra's
# post-test-infra-upload-testgrid-config job (TestGrid production takes the config
# that job uploads) since the last one up to which the archive's state ($2, if
# given) knows every run's config, with the configs needed to tell how each run
# changed its tabs' test groups (see testgrid_config.py plan), newest first, for as
# long as $CONFIG_SECONDS (default 480) allows; the rest are left to the next run.
# Each is built the way testgrid.k8s.io's was: by the configurator image the run
# ran, with its own arguments (printing the config instead of uploading it), on that
# commit's config/testgrids and config/jobs; then testgrid_config.py extract keeps
# what the archive needs. A config that cannot be built is left out (the fetch says
# why, and keeps the builds TestGrid may have shown under it undecided); the runs
# are written all the same.
# Needs git, docker or podman, and $PYTHON (default: python3) with PyYAML, such as
# the tool's environment (uv sync --project tool).
set -euo pipefail
out=$1
state=${2:-}
here=$(cd "$(dirname "$0")" && pwd)
python=${PYTHON:-python3}
budget=${CONFIG_SECONDS:-480}
engine=$(command -v docker || command -v podman) || { echo "testgrid_config.sh: needs docker or podman" >&2; exit 1; }
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# left: the seconds a step may still take (what is left of the budget, and a minute more
# for a step still running past it), at least one.
left() {
  local n=$(( budget - SECONDS + 60 ))
  echo $(( n > 1 ? n : 1 ))
}

# (listing the upload job's runs reads GCS: bounded too, so the script ends well within its step)
timeout -k 10 "$(left)" "$python" -I "$here/testgrid_config.py" uploads ${state:+--state "$state"} > "$work/uploads.json"
# (the configs to build go through a file: set -e does not see a command in a process
# substitution fail, and a list cut short would pass for the whole)
"$python" -I "$here/testgrid_config.py" recipes "$work/uploads.json" > "$work/recipes"
mapfile -t recipes < "$work/recipes"

git init -q "$work/test-infra"
git -C "$work/test-infra" remote add origin https://github.com/kubernetes/test-infra
git -C "$work/test-infra" config extensions.partialClone origin
git -C "$work/test-infra" sparse-checkout set --no-cone /config/jobs/ /config/testgrids/ /config/prow/config.yaml

# build RECIPE COMMIT: the config of one run (its commit, and the container it ran) in
# $work/extract-RECIPE.json; on failure, prints why and returns 1. (Each step is checked
# here: set -e does not apply inside a function called as a condition.) Each step that may
# wait on the network is bounded by what is left then.
build() {
  local recipe=$1 commit=$2 committed digest
  local -a job
  timeout -k 10 "$(left)" git -C "$work/test-infra" fetch -q --depth 1 --filter=blob:none origin "$commit" \
    2> "$work/err" || { echo "git fetch: $(tail -n 3 "$work/err")"; return 1; }
  # (a partial clone: the checkout fetches the blobs of the files it writes)
  timeout -k 10 "$(left)" git -C "$work/test-infra" checkout -q --detach "$commit" 2> "$work/err" \
    || { echo "git checkout: $(tail -n 3 "$work/err")"; return 1; }
  committed=$(git -C "$work/test-infra" log -1 --format=%ct) || { echo "git log failed"; return 1; }
  # the image, its command, then the arguments (as the run ran them)
  "$python" -I "$here/testgrid_config.py" job "$work/uploads.json" "$recipe" "$work/test-infra" > "$work/job" \
    2> "$work/err" || { echo "the upload job's container: $(tail -n 3 "$work/err")"; return 1; }
  mapfile -d '' -t job < "$work/job"
  timeout -k 10 "$(left)" "$engine" pull -q "${job[0]}" > /dev/null 2> "$work/err" \
    || { echo "pulling ${job[0]}: $(tail -n 3 "$work/err")"; return 1; }
  digest=$("$engine" image inspect --format '{{index .RepoDigests 0}}' "${job[0]}" 2> /dev/null || true)
  # No network and a read-only checkout: the configurator only reads the config and prints it.
  # (It logs a line per job name it shortens; only a failure's log is shown.)
  timeout -k 10 "$(left)" "$engine" run --rm --network none -v "$work/test-infra:/test-infra:ro" -w /test-infra \
      --entrypoint "${job[1]}" "${job[0]}" "${job[@]:2}" > "$work/config.yaml" 2> "$work/configurator.log" \
    || { echo "the configurator failed: $(tail -n 5 "$work/configurator.log")"; return 1; }
  "$python" -I "$here/testgrid_config.py" extract "$work/config.yaml" --recipe "$recipe" --commit "$commit" \
      --committed "$committed" --image "${job[0]}" --digest "${digest#*@}" > "$work/extract.json" 2> "$work/err" \
    || { echo "extract: $(tail -n 3 "$work/err")"; return 1; }
  mv "$work/extract.json" "$work/extract-$recipe.json"
}

built=0 failed=0
: > "$work/failed.tsv"
for line in ${recipes[@]+"${recipes[@]}"}; do
  if (( SECONDS >= budget )); then
    echo "testgrid_config.sh: $(( ${#recipes[@]} - built - failed )) of ${#recipes[@]} configs left for the next run" >&2
    break
  fi
  recipe=${line%% *} commit=${line#* }
  if why=$(build "$recipe" "$commit"); then
    built=$(( built + 1 ))
  else
    failed=$(( failed + 1 ))
    why=${why//$'\n'/ }
    why=${why//$'\t'/ }
    printf '%s\t%s\n' "$recipe" "$why" >> "$work/failed.tsv"
    echo "testgrid_config.sh: the config of test-infra ${commit:0:12} ($recipe) cannot be built: $why" >&2
  fi
done
shopt -s nullglob
extracts=("$work"/extract-*.json)
"$python" -I "$here/testgrid_config.py" merge --failed "$work/failed.tsv" "$work/uploads.json" \
  ${extracts[@]+"${extracts[@]}"} > "$work/config.json"
mv "$work/config.json" "$out"
echo "TestGrid's config: built $built of ${#recipes[@]} configs of the upload job's runs ($failed could not be built)"
