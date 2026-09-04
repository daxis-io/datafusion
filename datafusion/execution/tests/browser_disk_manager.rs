// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion_common::error::{
    RuntimeCapability, RuntimeProfile, UnsupportedRuntimeCapability,
};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion_execution::{SpillFile, TempFileFactory};
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[derive(Debug)]
struct CountingFactory(Arc<AtomicUsize>);

impl TempFileFactory for CountingFactory {
    fn create_temp_file(&self, _description: &str) -> Result<Arc<dyn SpillFile>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        internal_err!("browser host callback must not run")
    }
}

fn assert_capability(error: DataFusionError, capability: RuntimeCapability) {
    let DataFusionError::External(source) = error else {
        panic!("expected external capability error, got {error}");
    };
    let unsupported = source
        .downcast_ref::<UnsupportedRuntimeCapability>()
        .expect("typed unsupported runtime capability source");
    assert_eq!(unsupported.profile, RuntimeProfile::Browser);
    assert_eq!(unsupported.capability, capability);
}

#[wasm_bindgen_test]
fn browser_disk_defaults_disabled_and_rejects_host_modes_before_callbacks() {
    let disabled = DiskManager::builder().build().expect("disabled by default");
    assert!(!disabled.tmp_files_enabled());
    assert!(disabled.temp_dir_paths().is_empty());

    let error = DiskManager::builder()
        .with_mode(DiskManagerMode::OsTmpDirectory)
        .build()
        .expect_err("OS temp paths are unavailable");
    assert_capability(error, RuntimeCapability::Disk);

    let calls = Arc::new(AtomicUsize::new(0));
    let error = DiskManager::builder()
        .with_temp_file_factory(Arc::new(CountingFactory(Arc::clone(&calls))))
        .build()
        .expect_err("custom host temp storage is unavailable");
    assert_capability(error, RuntimeCapability::Disk);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[wasm_bindgen_test]
fn browser_spill_request_is_structured_and_never_touches_a_path() {
    let manager = Arc::new(DiskManager::builder().build().unwrap());
    let error = match manager.create_tmp_file("forced spill") {
        Err(error) => error,
        Ok(_) => panic!("browser spill is unavailable"),
    };
    assert_capability(error, RuntimeCapability::Spill);
    assert!(manager.temp_dir_paths().is_empty());
    assert_eq!(manager.used_disk_space(), 0);
}
