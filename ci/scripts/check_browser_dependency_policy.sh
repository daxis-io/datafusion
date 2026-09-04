#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
cd "$repo_root"

cargo_bin=${CARGO:-cargo}
target=wasm32-unknown-unknown
features=browser,sql

graph=$(
  "$cargo_bin" tree --package datafusion --target "$target" --locked \
    --no-default-features --features "$features" --prefix none \
    --edges normal,build
)
feature_graph=$(
  "$cargo_bin" tree --package datafusion --target "$target" --locked \
    --no-default-features --features "$features" --prefix none \
    --edges normal,build,features
)
denied_packages=(
  aws-lc-sys
  bindgen
  cc
  cmake
  hyper
  hyper-util
  liblzma-sys
  mio
  native-tls
  openssl-sys
  pkg-config
  ring
  socket2
  tempfile
  tokio
  tokio-util
  zstd-sys
)

violations=0

if rg -q '^source = "(git\+|path\+)' Cargo.lock; then
  printf 'non-registry source in root Cargo.lock\n' >&2
  violations=$((violations + 1))
fi

for package in "${denied_packages[@]}"; do
  if rg -q "^${package} v" <<<"$graph"; then
    printf 'denied package in browser graph: %s\n' "$package" >&2
    violations=$((violations + 1))
  fi
done

denied_features=(
  'object_store feature "aws"'
  'object_store feature "azure"'
  'object_store feature "fs"'
  'object_store feature "gcp"'
  'object_store feature "tokio"'
)

for feature in "${denied_features[@]}"; do
  if rg -Fq "$feature" <<<"$feature_graph"; then
    printf 'denied feature in browser graph: %s\n' "$feature" >&2
    violations=$((violations + 1))
  fi
done

for package in arrow arrow-array arrow-buffer arrow-data arrow-ipc arrow-ord \
  arrow-schema arrow-select parquet object_store; do
  universe_count=$(
    awk -v prefix="${package} v" 'index($0, prefix) == 1 { print }' <<<"$graph" \
      | sed 's/ (\*)$//' \
      | LC_ALL=C sort -u \
      | wc -l \
      | tr -d ' '
  )
  if (( universe_count > 1 )); then
    printf 'duplicate browser dependency universe: %s\n' "$package" >&2
    violations=$((violations + 1))
  fi
done

expected_packages=(
  'arrow v59.3.0'
  'arrow-array v59.3.0'
  'arrow-buffer v59.3.0'
  'arrow-data v59.3.0'
  'arrow-ipc v59.3.0'
  'arrow-ord v59.3.0'
  'arrow-schema v59.3.0'
  'arrow-select v59.3.0'
  'object_store v0.14.1'
)
for package in "${expected_packages[@]}"; do
  if ! rg -q "^${package}( |$)" <<<"$graph"; then
    printf 'missing required browser package: %s\n' "$package" >&2
    violations=$((violations + 1))
  fi
done

if rg -q ' \(https?://| \(git\+' <<<"$graph"; then
  printf 'Git dependency in browser graph\n' >&2
  violations=$((violations + 1))
fi

if (( violations != 0 )); then
  printf 'browser dependency policy violations: %d\n' "$violations" >&2
  exit 1
fi

printf 'browser dependency policy violations: 0\n'
