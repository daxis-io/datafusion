<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache DataFusion Parquet DataSource

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that defines an [Apache Parquet] based file source.

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[apache parquet]: https://parquet.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion

## Reader features

With defaults disabled, native callers can enable `parquet-read,runtime-tokio`
to inject a `ParquetFileReaderFactory`. Adding `proto` also supports protobuf
plan decoding with a custom reader resolver. Neither profile requires callers
to enable Parquet dependency features themselves. The native target dependency
activates registry Parquet's `async` APIs.

`parquet-read` exposes reader and source APIs. `object-store-reader` separately
exposes the default reader; without it a missing injected factory returns the
typed `ParquetFileReaderFactoryRequired` error. Writer APIs require
`parquet-write`.

The committed `datafusion` browser profile (`browser,sql`) uses registry-valid
manifests. Browser Parquet requires the frozen local Arrow/Parquet overlay,
which provides the unpublished `async-core,zstd` features. Standalone registry
browser Parquet is deferred from Phase B qualification. Cargo resolver 2 keeps
the native target's `async` activation out of the browser dependency graph.

Run `python3 ci/scripts/check_parquet_feature_contract.py` from the repository
root to check the standalone native profiles, typed missing-factory behavior,
and downstream reader/default-reader API visibility. Each Cargo invocation
selects only its advertised profile, without overlay feature unification.
