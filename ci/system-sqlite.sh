#!/usr/bin/env bash
# CI only. Makes the sqlite3 build hook use the SQLite library of the
# runner (libsqlite3.so), so it does not download one from GitHub
# releases. GitHub downloads fail now and then (503/504), and the hook
# does not check the HTTP status.
#
# It adds the `hooks` block to each pubspec.yaml given as an argument
# (the root pubspec of a workspace or an app). Never commit that block to
# a package that is published to pub.dev: users would get it too.
# Run it before `dart pub get`.
set -euo pipefail

# libsqlite3.so (no version in the name) comes from the -dev package.
# Read the list first: `grep -q` stops at the first match, and under
# pipefail the SIGPIPE of `ldconfig` would look like "not found".
if [ "$(uname)" = Linux ]; then
  libs="$(ldconfig -p)"
  if ! grep -q 'libsqlite3\.so ' <<<"$libs"; then
    sudo apt-get install -y libsqlite3-dev
  fi
fi

for file in "$@"; do
  if grep -q '^hooks:' "$file"; then
    # a GitHub warning, so it shows on the run page
    echo "::warning file=$file::has a hooks block already, not changed." \
      "sqlite3 may download its library. Add 'user_defines: sqlite3:" \
      "source: system' to it by hand."
    continue
  fi
  printf '\nhooks:\n  user_defines:\n    sqlite3:\n      source: system\n' >> "$file"
  echo "$file: sqlite3 uses the system library"
done
