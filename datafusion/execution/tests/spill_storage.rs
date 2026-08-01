// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]

use std::sync::Arc;

use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::DiskManager;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::spill_storage::{
    DiskManagerSpillStorage, SpillStorage, SpillStorageConfig, SpillStorageErrorReason,
    UnavailableSpillStorage, configured_spill_storage,
};

#[test]
fn session_config_accepts_a_storage_neutral_spill_backend() {
    futures::executor::block_on(async {
        let runtime = RuntimeEnvBuilder::new().build().unwrap();
        let storage: Arc<dyn SpillStorage> = Arc::new(UnavailableSpillStorage);
        let config = SessionConfig::new()
            .with_extension(Arc::new(SpillStorageConfig::new(storage)));

        let storage =
            configured_spill_storage(&config, Arc::clone(&runtime.disk_manager));
        let error = storage.create_scope().await.unwrap_err();
        assert_eq!(error.reason(), SpillStorageErrorReason::Unavailable);
    });
}

#[test]
fn native_spill_storage_is_opaque_sequential_accounted_and_scope_owned() {
    futures::executor::block_on(async {
        let disk_manager = Arc::new(DiskManager::builder().build().unwrap());
        let storage = DiskManagerSpillStorage::new(disk_manager);
        let scope = storage.create_scope().await.unwrap();

        let (file_ref, mut writer) = scope.create_file().await.unwrap();
        writer.append(b"first ").await.unwrap();
        writer.append(b"second").await.unwrap();
        writer.finalize().await.unwrap();

        let metrics = scope.metrics();
        assert_eq!(metrics.current_bytes, 12);
        assert_eq!(metrics.peak_bytes, 12);
        assert_eq!(metrics.files_created, 1);
        assert_eq!(metrics.active_files, 1);
        assert!(!format!("{file_ref:?}").contains('/'));

        let mut reader = scope.open_reader(file_ref).await.unwrap();
        let mut actual = Vec::new();
        while let Some(chunk) = reader.read_next(3).await.unwrap() {
            actual.extend_from_slice(&chunk);
        }
        assert_eq!(actual, b"first second");

        scope.delete_file(file_ref).await.unwrap();
        assert_eq!(scope.metrics().current_bytes, 0);
        let error = scope.open_reader(file_ref).await.unwrap_err();
        assert_eq!(error.reason(), SpillStorageErrorReason::Unavailable);

        scope.delete_scope().await.unwrap();
        assert_eq!(scope.metrics().active_files, 0);
    });
}
