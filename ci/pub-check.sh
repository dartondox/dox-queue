#!/usr/bin/env bash
# Checks each package to release (PACKAGES, from ci/publish-order.sh)
# against pub.dev only. In the pub workspace, sibling packages come from
# local folders, so a missing release would not show. So each package is
# copied to a temp folder outside the workspace. There, `resolution:
# workspace` is removed and every dependency comes from pub.dev. Then it
# runs `dart pub get` (with retries, see ci/pub-get-retry.sh) and
# `dart analyze --no-fatal-warnings`.
# One tag releases all packages in this repo, so a package that needs a
# sibling of this repo gets a path override to the copy of that sibling.
# Those siblings are not on pub.dev yet when the check runs. Every other
# dependency comes from pub.dev.
set -euo pipefail
cd "$(dirname "$0")/.."
root=$PWD
tmp=${RUNNER_TEMP:-$(mktemp -d)}
entries=${PACKAGES:?PACKAGES is empty, run ci/publish-order.sh first}

for entry in $entries; do
  dir=${entry%%:*}
  work="$tmp/pub-check/$dir"
  rm -rf "$work"
  mkdir -p "$work"
  cp -R "packages/$dir/." "$work/"
  rm -f "$work/pubspec_overrides.yaml" "$work/pubspec.lock"
  sed -i.bak '/^resolution:[[:space:]]*workspace/d' "$work/pubspec.yaml"
  rm -f "$work/pubspec.yaml.bak"
  # Path overrides, only for siblings released in this same run.
  overrides=""
  for other in $entries; do
    [ "$other" = "$entry" ] && continue
    if grep -qE "^[[:space:]]+${other#*:}:" "$work/pubspec.yaml"; then
      overrides+="  ${other#*:}:"$'\n'"    path: $tmp/pub-check/${other%%:*}"$'\n'
    fi
  done
  if [ -n "$overrides" ]; then
    printf '\ndependency_overrides:\n%s' "$overrides" >> "$work/pubspec.yaml"
  fi
  echo "== $entry (from pub.dev${overrides:+, siblings of this tag by path})"
done

for entry in $entries; do
  dir=${entry%%:*}
  echo "== check $entry"
  (cd "$tmp/pub-check/$dir" && "$root/ci/pub-get-retry.sh" && dart analyze --no-fatal-warnings)
done
