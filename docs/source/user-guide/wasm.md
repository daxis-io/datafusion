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

# WebAssembly browser profile

DataFusion has a deliberately narrow browser profile for
`wasm32-unknown-unknown`. It covers in-memory execution and Parquet reads from
an asynchronously registered object store. The tested session uses one target
partition and a disabled disk manager.

The browser profile currently verifies projection, filtering, ordering, and
aggregation over Snappy Parquet in Chrome and Firefox. A browser HTTP store must
be registered explicitly, and its server must expose the HTTP range and
validator headers required by the object-store client.

Filesystem-backed object stores, temporary files, spilling, Tokio's
multi-threaded runtime, and generalized multi-partition execution are outside
this profile. Configuring a filesystem disk manager or requesting a spill
returns an operation-time error before filesystem access.

Gzip and bzip2 file streams remain available. Xz and zstd file-stream formats
are recognized, but compression or decompression returns an error that names
the operation, codec, and `wasm32-unknown-unknown` target. Parquet and Arrow IPC
have their own codec availability rules; a file's metadata may be readable even
when a compressed data page is not.
