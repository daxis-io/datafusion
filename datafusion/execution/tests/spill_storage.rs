// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.

#![cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]

use std::fmt;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_execution::disk_manager::{DiskManager, DiskManagerMode};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::spill_storage::{
    NativeSpillStorage, SpillFileRef, SpillReader, SpillScopeId, SpillStorage,
    SpillStorageAccounting, SpillWriter,
};
use futures::FutureExt;

#[test]
fn native_adapter_uses_the_runtime_disk_manager_and_round_trips_bytes() {
    let disk_manager = Arc::new(
        DiskManager::builder()
            .build()
            .expect("disk manager should construct"),
    );
    let storage = NativeSpillStorage::new(Arc::clone(&disk_manager));

    let scope = futures::executor::block_on(storage.create_scope())
        .expect("scope should construct");
    let mut writer = futures::executor::block_on(storage.create_writer(&scope))
        .expect("writer should construct");
    writer.write_all(b"schema").expect("schema should append");
    writer.write_all(b"-batch").expect("batch should append");
    let file = writer.finish().expect("writer should finalize");

    assert_eq!(disk_manager.spilling_progress().active_files_count, 1);
    assert_eq!(
        disk_manager.spilling_progress().current_bytes,
        b"schema-batch".len() as u64
    );

    let mut reader = futures::executor::block_on(storage.open_reader(&file))
        .expect("reader should open");
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).expect("reader should read");
    assert_eq!(bytes, b"schema-batch");

    let accounting = storage.accounting();
    assert_eq!(accounting.files_created, 1);
    assert_eq!(accounting.bytes_written, bytes.len() as u64);
    assert_eq!(accounting.bytes_read, bytes.len() as u64);

    futures::executor::block_on(storage.delete_scope(&scope))
        .expect("scope cleanup should succeed");
    assert_eq!(disk_manager.spilling_progress().active_files_count, 0);
    assert_eq!(storage.accounting().active_files, 0);
}

struct DelayedSpillStorage {
    scope_gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
}

impl fmt::Debug for DelayedSpillStorage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DelayedSpillStorage")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpillStorage for DelayedSpillStorage {
    async fn create_scope(&self) -> Result<SpillScopeId> {
        let receiver = self
            .scope_gate
            .lock()
            .expect("scope gate should not be poisoned")
            .take()
            .expect("scope gate should exist");
        receiver.await.expect("scope gate should be released");
        Ok(SpillScopeId::new("private-scope"))
    }

    async fn create_writer(&self, _scope: &SpillScopeId) -> Result<Box<dyn SpillWriter>> {
        unimplemented!("not needed for the pending acquisition test")
    }

    async fn open_reader(&self, _file: &SpillFileRef) -> Result<Box<dyn SpillReader>> {
        unimplemented!("not needed for the pending acquisition test")
    }

    async fn delete_file(&self, _file: &SpillFileRef) -> Result<()> {
        Ok(())
    }

    async fn delete_scope(&self, _scope: &SpillScopeId) -> Result<()> {
        Ok(())
    }

    fn accounting(&self) -> SpillStorageAccounting {
        SpillStorageAccounting::default()
    }
}

#[test]
fn storage_acquisition_can_remain_pending() {
    let (release, receiver) = futures::channel::oneshot::channel();
    let storage = DelayedSpillStorage {
        scope_gate: Mutex::new(Some(receiver)),
    };
    let mut acquisition = Box::pin(storage.create_scope());

    assert!(
        acquisition.as_mut().now_or_never().is_none(),
        "operators must be able to observe asynchronous storage acquisition"
    );
    release
        .send(())
        .expect("scope acquisition should be waiting");
    let scope = futures::executor::block_on(acquisition)
        .expect("released acquisition should complete");
    assert_eq!(scope.opaque_id(), "private-scope");
}

#[test]
fn runtime_rebuild_preserves_the_exact_injected_backend() {
    let disk_manager = Arc::new(
        DiskManager::builder()
            .build()
            .expect("disk manager should construct"),
    );
    let storage: Arc<dyn SpillStorage> = Arc::new(NativeSpillStorage::new(disk_manager));
    let runtime = RuntimeEnvBuilder::new()
        .with_spill_storage(Arc::clone(&storage))
        .build()
        .expect("runtime should accept a path-free spill backend");

    assert!(Arc::ptr_eq(
        runtime
            .spill_storage()
            .expect("injected storage should be retained"),
        &storage,
    ));

    let rebuilt = RuntimeEnvBuilder::from_runtime_env(&runtime)
        .build()
        .expect("runtime rebuild should succeed");
    assert!(Arc::ptr_eq(
        rebuilt
            .spill_storage()
            .expect("rebuilt runtime should retain storage"),
        &storage,
    ));
}

#[test]
fn runtime_selects_the_native_adapter_only_when_disk_spilling_is_enabled() {
    let enabled = RuntimeEnvBuilder::new()
        .build()
        .expect("default runtime should construct");
    assert!(
        enabled.spill_storage().is_some(),
        "native runtime should expose its existing disk manager through the path-free adapter"
    );

    let disabled = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(
            DiskManager::builder().with_mode(DiskManagerMode::Disabled),
        )
        .build()
        .expect("disabled runtime should construct");
    assert!(
        disabled.spill_storage().is_none(),
        "disabled disk manager must not silently create native spill authority"
    );
}

#[test]
fn opaque_ids_are_redacted_from_debug_output() {
    let scope = SpillScopeId::new("secret-scope-name");
    let file = SpillFileRef::new(scope.clone(), "secret-file-name");

    assert_eq!(format!("{scope:?}"), "SpillScopeId(REDACTED)");
    assert_eq!(format!("{file:?}"), "SpillFileRef(REDACTED)");
}
