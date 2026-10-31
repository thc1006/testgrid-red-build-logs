#!/usr/bin/env bash
# When TestGrid's production manifests deploy another commit than the pinned one (drift.sh):
# build TestGrid's code at that commit, check the port against it as against the
# pinned one, and list what changed in the code the port follows
# (relevant_diff.py). When nothing it follows changed and every check passes,
# pinned.json moves to that commit on the branch follow-testgrid-<commit>, which
# is pushed and proposed as a pull request (or, where GitHub Actions may not open
# one, linked to open it from). Otherwise it exits 1 with what changed, for a
# person to compare with the port (README.md).
# Usage: follow.sh COMMIT SEED, from anywhere. Needs Go, uv, git, gh, and GH_TOKEN
# to push and propose.
set -euo pipefail
commit=$1
seed=$2
here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
work=$(mktemp -d)
trap 'chmod -R u+w "$work" 2> /dev/null; rm -rf "$work"' EXIT
summary=${GITHUB_STEP_SUMMARY:-/dev/stdout}
module=github.com/GoogleCloudPlatform/testgrid

pinned=$(python3 -c 'import json; print(json.load(open("oracle/pinned.json"))["testgrid_module_version"])')
(cd "$work" && go mod init follow-testgrid > /dev/null 2>&1)
# (GOTOOLCHAIN=auto: a module that needs a newer Go than setup-go's is looked up with that Go)
lookup() {
  local key=$1
  shift
  (cd "$work" && GOTOOLCHAIN=auto go "$@") | python3 -I -c 'import json, sys
try:
    found = json.load(sys.stdin)
except ValueError:
    found = {}
value = found
for name in sys.argv[1].split("."):
    value = value.get(name) if isinstance(value, dict) else None
if found.get("Error") or not value:
    sys.exit(found.get("Error") or "go gave no " + sys.argv[1])
print(value)' "$key"
}
version=$(lookup Version list -m -json "$module@$commit")
dir_of() { lookup Dir mod download -json "$module@$1"; }
old_dir=$(dir_of "$pinned")
new_dir=$(dir_of "$version")
changed=$(python3 -I oracle/relevant_diff.py "$old_dir" "$new_dir")
# (written now, before the build and the checks: a build that fails, or a run cut off, still leaves it)
{
  echo "## TestGrid's production manifests deploy $version ($commit)"
  echo
  echo "Pinned: $pinned."
  if [ -n "$changed" ]; then
    echo "Changed in the code the port follows:"
    echo
    while IFS= read -r line; do echo "- \`$line\`"; done <<< "$changed"
  else
    echo "Nothing the port follows changed."
  fi
  echo
} >> "$summary"
# The Go TestGrid's production images are built with (its Bazel WORKSPACE; else go.mod's): the
# oracle is built with it too (setup-go's GOTOOLCHAIN=local would refuse a newer one).
go_version=$(python3 -I -c 'import sys
sys.path.insert(0, "oracle")
import relevant_diff
print(relevant_diff.bazel(sys.argv[1])["go"] or relevant_diff.go_mod(sys.argv[1]).get("go") or "")' "$new_dir")
export GOTOOLCHAIN=${go_version:+go$go_version}
export GOTOOLCHAIN=${GOTOOLCHAIN:-auto}
if [ -n "$go_version" ]; then
  echo "The checks below use an oracle built with Go $go_version." >> "$summary"
fi

oracle/build.sh "$work/oracle" "$version"
export TGORACLE="$work/oracle/tgoracle" GOCHECK="$work/oracle/gocheck" SCHEMADUMP="$work/oracle/schemadump" \
  UNIDUMP="$work/oracle/unidump"
failed=()
# (check() runs its command as an if's condition, where set -e does not apply: a check of several
# commands joins them with &&, else a failure before the last command would go unseen)
check() {
  local name=$1
  shift
  echo "::group::$name"
  if "$@"; then echo "::endgroup::"; else echo "::endgroup::"; failed+=("$name"); fi
}
# (testgrid_types.json names the pinned commit in "about"; the types themselves must not change)
types() {
  "$SCHEMADUMP" | python3 -I oracle/gotypes.py > "$work/types.json" &&
    python3 -I -c 'import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:])
sys.exit(a.pop("about") is None or b.pop("about") is None or a != b)' "$work/types.json" testgrid_types.json
}
check "Go types" types
# (the tables of the Go the new commit is built with; a change there changes Go's regexp: a person compares)
unicode() { "$UNIDUMP" | python3 -I oracle/gounicode.py check go_unicode.json.gz --any-go; }
check "Go's unicode tables" unicode
check "edge cases" python3 -I oracle/fuzz/edge.py
check "random builds" python3 -I oracle/fuzz/run.py "$seed" 5000
check "tab filters" python3 -I oracle/fuzz/filterdiff.py "$seed" 20000
check "test names" python3 -I oracle/fuzz/fmtdiff.py "$seed" 20000
check "index links" uv run --locked --no-dev python -I oracle/fuzz/linkdiff.py "$seed" 20000
check "real builds" uv run --locked --no-dev python -I oracle/fuzz/realdiff.py ../archive 2

{
  if [ ${#failed[@]} -gt 0 ]; then
    echo "Checks that failed against the new commit: ${failed[*]}."
  else
    echo "Every check passed against the new commit (Go types, Go's unicode tables, edge cases, random builds, tab filters, test names, index links, real builds)."
  fi
} >> "$summary"
if [ -n "$changed" ] || [ ${#failed[@]} -gt 0 ]; then
  echo "::error::TestGrid's production manifests moved to $version: compare what changed with the port (tool/oracle/README.md), then move pinned.json."
  exit 1
fi

# Nothing the port follows changed, and the checks pass: propose the new pinned.json. (A
# pseudo-version ends in its commit; a release's commit is go's Origin.Hash. drift.sh
# compares an image tag's -g<commit> with it.)
short=${version##*-}
if ! [[ $short =~ ^[0-9a-f]{12}$ ]]; then
  short=$(lookup Origin.Hash list -m -json "$module@$version")
  short=${short:0:12}
fi
branch="follow-testgrid-$short"
python3 - "$short" "$version" "$go_version" <<'PY'
import json, sys
path = "oracle/pinned.json"
with open(path) as f:
    pinned = json.load(f)
pinned.update(testgrid_commit=sys.argv[1], testgrid_module_version=sys.argv[2], go=sys.argv[3] or pinned["go"])
with open(path, "w") as f:
    f.write(json.dumps(pinned, indent=2) + "\n")
PY
"$SCHEMADUMP" | python3 -I oracle/gotypes.py > "$work/types.json"  # (its "about" names the new commit)
mv "$work/types.json" testgrid_types.json
"$UNIDUMP" | python3 -I oracle/gounicode.py write go_unicode.json.gz  # (it names the Go it came from)
auth=$(printf 'x-access-token:%s' "$GH_TOKEN" | base64 -w0)
echo "::add-mask::$auth"
if git -c "http.https://github.com/.extraheader=AUTHORIZATION: basic $auth" ls-remote --exit-code origin "refs/heads/$branch" > /dev/null; then
  echo "The branch $branch is already there."
else
  git config user.name "github-actions[bot]"
  git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  git switch -q -c "$branch"
  git add oracle/pinned.json testgrid_types.json go_unicode.json.gz
  git commit -q -m "oracle: follow TestGrid's production manifests to $short" \
    -m "Nothing in the code the port follows changed since the pinned commit, and every check of the oracle workflow passed against $version."
  git -c "http.https://github.com/.extraheader=AUTHORIZATION: basic $auth" push -q origin "HEAD:refs/heads/$branch"
fi
if url=$(gh pr create --head "$branch" --base main --title "oracle: follow TestGrid's production manifests to $short" \
    --body "TestGrid's production manifests deploy $version. Nothing in the code the port follows changed since $pinned, and every check passed against it: see $GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID." 2>&1); then
  echo "Proposed: $url" | tee -a "$summary"
else
  echo "Could not open a pull request ($url). Open one from $GITHUB_SERVER_URL/$GITHUB_REPOSITORY/compare/main...$branch" | tee -a "$summary"
  echo "::warning::Open a pull request from $GITHUB_SERVER_URL/$GITHUB_REPOSITORY/compare/main...$branch to move pinned.json to $short."
fi
