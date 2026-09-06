#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datetime
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile

# Run each advertised profile alone: workspace-wide feature unification would
# mask missing dependency activation. No overlay or direct Parquet dependency.
root = Path(__file__).resolve().parents[2]
cargo = shlex.split(os.environ.get("CARGO", "cargo"))
candidate = subprocess.check_output(
    ["git", "-C", str(root), "rev-parse", "HEAD", "HEAD^{tree}"], text=True
).splitlines()


def run(args, cwd=root, expected_error=None):
    command = cargo + args
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(json.dumps({
        "command": command,
        "cwd": str(cwd),
        "candidate_sha_tree": candidate,
        "started_utc": started,
        "environment": {
            key: value for key, value in os.environ.items()
            if key.startswith(("CARGO", "RUST")) or key in ("PATH", "TMPDIR")
        },
    }), flush=True)
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    print(json.dumps({
        "command": command,
        "ended_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "exit_code": result.returncode,
        "expected_error": expected_error,
    }), flush=True)
    if expected_error is None:
        result.check_returncode()
    else:
        assert result.returncode == 101, "expected a compile failure"
        assert expected_error in result.stderr, "failure did not prove API absence"
    return result


for features in ["parquet-read,runtime-tokio", "parquet-read,proto,runtime-tokio"]:
    profile = [
        "--locked", "-p", "datafusion-datasource-parquet",
        "--no-default-features", "--features", features,
    ]
    run(["check"] + profile)
    result = run(
        ["test", "--profile", "ci"] + profile + ["--test", "feature_contract"]
    )
    assert "1 passed; 0 failed" in result.stdout, "missing-factory test did not execute"

# The reader-dependent integration test must also compile when reading is off.
run([
    "test", "--profile", "ci", "--locked", "-p", "datafusion-datasource-parquet",
    "--no-default-features", "--features", "runtime-tokio",
    "--test", "feature_contract",
])

# Keep the generated downstream consumer and diagnostics for local evidence.
probe = Path(tempfile.mkdtemp(prefix="parquet-feature-contract-"))
print(f"downstream probe: {probe}", flush=True)
(probe / "src").mkdir()
dependency = json.dumps(str(root / "datafusion/datasource-parquet"))
(probe / "Cargo.toml").write_text(f"""[package]
name = "parquet-feature-contract-probe"
version = "0.0.0"
edition = "2024"
[workspace]
[dependencies]
datafusion-datasource-parquet = {{ path = {dependency}, default-features = false, features = ["runtime-tokio"] }}
[features]
read = ["datafusion-datasource-parquet/parquet-read"]
default-reader = ["read", "datafusion-datasource-parquet/object-store-reader"]
""")
shutil.copyfile(root / "Cargo.lock", probe / "Cargo.lock")
source = probe / "src/main.rs"
source.write_text("fn main() {}\n")
# Prune the seed workspace lock to this standalone consumer, retaining versions.
run(["check", "--offline"], cwd=probe)
for item, feature in [
    ("source::ParquetSource", "read"),
    ("ParquetFileReaderFactory", "read"),
    ("DefaultParquetFileReaderFactory", "default-reader"),
]:
    source.write_text(
        f"#[allow(unused_imports)]\nuse datafusion_datasource_parquet::{item};\n"
        "fn main() {}\n"
    )
    run(["check", "--locked", "--offline", "--features", feature], cwd=probe)
    disabled = [] if feature == "read" else ["--features", "read"]
    missing = item.split("::")[0] if feature == "read" else item
    run(
        ["check", "--locked", "--offline"] + disabled,
        cwd=probe,
        expected_error=f"unresolved import `datafusion_datasource_parquet::{missing}`",
    )
print("native Parquet feature contracts passed", flush=True)
