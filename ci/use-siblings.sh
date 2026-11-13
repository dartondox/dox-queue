#!/usr/bin/env bash
# CI tests this repo against the newest code of the Dox packages it uses,
# not the versions on pub.dev. So a change that needs a change in another
# Dox repo can be tested, and merged, before anything is released.
#
# For each repo in the list below, it clones github.com/dartondox/<repo>
# next to this repo (../<repo>). It takes the branch with the same name as
# this PR (or push) if that repo has one, else main. A folder that is
# already there is kept (local dev). Then it writes the root
# pubspec_overrides.yaml (gitignored, never committed or published) with
# path overrides. Run `dart pub get` after it.
#
# The list also has the Dox repos that those packages use (like
# dox-validation for dox_core). Overrides count for the whole dependency
# tree, so all Dox packages must come from the newest code, or pub may not
# resolve. Only the list is cloned, never the lists of the clones.
#
# The native packages (dox_http_native, dox_postgres_native,
# dox_mysql_native) are cloned too. `ci/use-siblings.sh build` builds their
# Rust library with cargo, only for this machine (linux x64 on CI), into
# native/target/release. Their build hook takes the library from there.
#
# On a tag (a release) it does nothing. A release tests on pub.dev only.
set -euo pipefail

# Each line: the repo, then package:folder for each package in it.
siblings=(
  "dox dox_annotation:packages/dox-annotation dox_builder:packages/dox-builder dox_cache:packages/dox-cache dox_core:packages/dox-core"
  "dox-validation dox_validation:."
  "dox-openapi dox_openapi:."
  "ioredis ioredis:."
  "dox-http-native dox_http_native:."
  "dox-orm dox_orm:packages/dox-orm dox_migration:packages/dox-migration"
  "dox-postgres-native dox_postgres_native:."
  "dox-mysql-native dox_mysql_native:."
)

if [ "${GITHUB_REF_TYPE:-}" = tag ]; then
  echo "tag $GITHUB_REF_NAME: the Dox packages come from pub.dev"
  exit 0
fi

root=$(cd "$(dirname "$0")/.." && pwd)
parent=$(cd "$root/.." && pwd)
branch=${GITHUB_HEAD_REF:-${GITHUB_REF_NAME:-}}

if [ "${1:-}" = build ]; then
  for entry in "${siblings[@]}"; do
    read -r repo _ <<< "$entry"
    manifest="$parent/$repo/native/Cargo.toml"
    [ -f "$manifest" ] || continue
    start=$SECONDS
    cargo build --release --manifest-path "$manifest"
    echo "$repo: built in $((SECONDS - start))s"
  done
  exit 0
fi

out="# written by ci/use-siblings.sh
dependency_overrides:"

for entry in "${siblings[@]}"; do
  read -r repo packages <<< "$entry"
  dir="$parent/$repo"
  if [ -d "$dir" ]; then
    echo "$repo: $dir is there"
  else
    url="https://github.com/dartondox/$repo.git"
    ref=main
    if [ -n "$branch" ] && git ls-remote --exit-code "$url" "refs/heads/$branch" >/dev/null; then
      ref=$branch
    fi
    echo "$repo: branch $ref"
    git clone -q --depth 1 --branch "$ref" "$url" "$dir"
  fi
  for p in $packages; do
    path="$dir/${p#*:}"
    out+="
  ${p%%:*}:
    path: ${path%/.}"
  done
done

echo "$out" > "$root/pubspec_overrides.yaml"
cat "$root/pubspec_overrides.yaml"
