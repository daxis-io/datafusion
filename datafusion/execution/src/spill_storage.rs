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

//! Path-free, sequential storage contracts for operator spill data.
//!
//! Operators use opaque scope and file references and cannot obtain filesystem
//! paths. A storage implementation may therefore use native temporary files,
//! browser OPFS handles, or an in-memory test backend without changing physical
//! operators.

use std::fmt::Debug;
use std::sync::Arc;

use crate::config::SessionConfig;
use crate::disk_manager::DiskManager;
use async_trait::async_trait;

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use {
    crate::disk_manager::RefCountedTempFile,
    datafusion_common::DataFusionError,
    parking_lot::Mutex,
    std::collections::HashMap,
    std::fs::{File, OpenOptions},
    std::io::{Read, Write},
    std::sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Opaque identity for one query-owned spill namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpillScopeId(u64);

impl SpillScopeId {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Opaque identity for one file inside a query-owned spill namespace.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpillFileRef {
    scope_id: SpillScopeId,
    file_id: u64,
}

impl SpillFileRef {
    pub fn new(scope_id: SpillScopeId, file_id: u64) -> Self {
        Self { scope_id, file_id }
    }

    pub fn scope_id(self) -> SpillScopeId {
        self.scope_id
    }

    pub fn file_id(self) -> u64 {
        self.file_id
    }
}

/// Stable storage failure categories that callers can translate without
/// inspecting backend-specific error strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpillStorageErrorReason {
    Unavailable,
    QuotaExceeded,
    IoFailure,
}

/// A backend-neutral spill storage failure.
#[derive(Debug)]
pub struct SpillStorageError {
    reason: SpillStorageErrorReason,
    message: &'static str,
}

impl SpillStorageError {
    pub fn new(reason: SpillStorageErrorReason, message: &'static str) -> Self {
        Self { reason, message }
    }

    pub fn reason(&self) -> SpillStorageErrorReason {
        self.reason
    }
}

impl std::fmt::Display for SpillStorageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for SpillStorageError {}

pub type SpillStorageResult<T> = Result<T, SpillStorageError>;

/// Query-scope spill accounting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpillStorageMetrics {
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub files_created: u64,
    pub active_files: u64,
    pub merge_passes: u64,
}

/// Storage entry point. Creating a scope may be asynchronous because browser
/// storage capability and quota are established by the worker host.
#[async_trait]
pub trait SpillStorage: Debug + Send + Sync {
    /// Whether this runtime has installed a backend capable of attempting
    /// external-memory execution. Capability and quota checks may still fail
    /// asynchronously when a spill scope is actually created.
    fn supports_spill(&self) -> bool {
        true
    }

    /// Whether operators must use the asynchronous path-free bridge.
    ///
    /// Native temporary files return `false` to preserve their established
    /// direct-file implementation and performance characteristics.
    fn uses_external_bridge(&self) -> bool {
        true
    }

    async fn create_scope(&self) -> SpillStorageResult<Arc<dyn SpillScope>>;
}

/// Session-scoped external spill backend configuration.
///
/// Keeping this capability in [`SessionConfig`] preserves the source-compatible
/// public shape of [`crate::runtime_env::RuntimeEnv`] while allowing operators
/// to select a path-free backend for one execution session.
#[derive(Clone)]
pub struct SpillStorageConfig {
    storage: Arc<dyn SpillStorage>,
}

impl SpillStorageConfig {
    pub fn new(storage: Arc<dyn SpillStorage>) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> Arc<dyn SpillStorage> {
        Arc::clone(&self.storage)
    }
}

impl Debug for SpillStorageConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SpillStorageConfig")
    }
}

/// Resolves the session override or the platform's native default backend.
pub fn configured_spill_storage(
    config: &SessionConfig,
    disk_manager: Arc<DiskManager>,
) -> Arc<dyn SpillStorage> {
    config
        .get_extension::<SpillStorageConfig>()
        .map(|config| config.storage())
        .unwrap_or_else(|| default_spill_storage(disk_manager))
}

/// One execution-owned namespace. Dropping or deleting the scope releases all
/// files created through it.
#[async_trait]
pub trait SpillScope: Debug + Send + Sync {
    fn id(&self) -> SpillScopeId;

    async fn create_file(
        &self,
    ) -> SpillStorageResult<(SpillFileRef, Box<dyn SpillAppendWriter>)>;

    async fn open_reader(
        &self,
        file: SpillFileRef,
    ) -> SpillStorageResult<Box<dyn SpillSequentialReader>>;

    async fn delete_file(&self, file: SpillFileRef) -> SpillStorageResult<()>;

    async fn delete_scope(&self) -> SpillStorageResult<()>;

    /// Records one external merge pass without exposing operator data to the
    /// storage backend.
    fn record_merge_pass(&self) {}

    fn metrics(&self) -> SpillStorageMetrics;
}

/// Append-only byte sink used by Arrow IPC stream writers.
#[async_trait]
pub trait SpillAppendWriter: Debug + Send {
    async fn append(&mut self, bytes: &[u8]) -> SpillStorageResult<()>;

    async fn finalize(self: Box<Self>) -> SpillStorageResult<()>;
}

/// Sequential byte source used by Arrow IPC stream decoders.
#[async_trait]
pub trait SpillSequentialReader: Debug + Send {
    async fn read_next(
        &mut self,
        max_bytes: usize,
    ) -> SpillStorageResult<Option<Vec<u8>>>;
}

/// Explicitly unavailable backend used by runtimes that have not installed a
/// spill storage capability.
#[derive(Debug, Default)]
pub struct UnavailableSpillStorage;

#[async_trait]
impl SpillStorage for UnavailableSpillStorage {
    fn supports_spill(&self) -> bool {
        false
    }

    fn uses_external_bridge(&self) -> bool {
        false
    }

    async fn create_scope(&self) -> SpillStorageResult<Arc<dyn SpillScope>> {
        Err(SpillStorageError::new(
            SpillStorageErrorReason::Unavailable,
            "spill storage is unavailable",
        ))
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
pub struct DiskManagerSpillStorage {
    disk_manager: Arc<DiskManager>,
    next_scope_id: AtomicU64,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl DiskManagerSpillStorage {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        Self {
            disk_manager,
            next_scope_id: AtomicU64::new(1),
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
impl SpillStorage for DiskManagerSpillStorage {
    fn supports_spill(&self) -> bool {
        self.disk_manager.tmp_files_enabled()
    }

    fn uses_external_bridge(&self) -> bool {
        false
    }

    async fn create_scope(&self) -> SpillStorageResult<Arc<dyn SpillScope>> {
        let id = SpillScopeId(self.next_scope_id.fetch_add(1, Ordering::Relaxed));
        Ok(Arc::new(DiskManagerSpillScope {
            id,
            disk_manager: Arc::clone(&self.disk_manager),
            state: Arc::new(DiskManagerSpillScopeState {
                files: Mutex::new(HashMap::new()),
                next_file_id: AtomicU64::new(1),
                peak_bytes: AtomicU64::new(0),
                files_created: AtomicU64::new(0),
                deleted: AtomicBool::new(false),
            }),
        }))
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
struct DiskManagerSpillScope {
    id: SpillScopeId,
    disk_manager: Arc<DiskManager>,
    state: Arc<DiskManagerSpillScopeState>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
struct DiskManagerSpillScopeState {
    files: Mutex<HashMap<SpillFileRef, DiskManagerSpillFile>>,
    next_file_id: AtomicU64,
    peak_bytes: AtomicU64,
    files_created: AtomicU64,
    deleted: AtomicBool,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
struct DiskManagerSpillFile {
    temp_file: RefCountedTempFile,
    finalized: bool,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl DiskManagerSpillScope {
    fn ensure_active(&self) -> SpillStorageResult<()> {
        if self.state.deleted.load(Ordering::Acquire) {
            Err(SpillStorageError::new(
                SpillStorageErrorReason::Unavailable,
                "spill scope is unavailable",
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
impl SpillScope for DiskManagerSpillScope {
    fn id(&self) -> SpillScopeId {
        self.id
    }

    async fn create_file(
        &self,
    ) -> SpillStorageResult<(SpillFileRef, Box<dyn SpillAppendWriter>)> {
        self.ensure_active()?;
        let file_id = self.state.next_file_id.fetch_add(1, Ordering::Relaxed);
        let file_ref = SpillFileRef {
            scope_id: self.id,
            file_id,
        };
        let temp_file = self
            .disk_manager
            .create_tmp_file("operator spill")
            .map_err(map_create_error)?;
        let writer = OpenOptions::new()
            .append(true)
            .open(temp_file.path())
            .map_err(|_| io_failure())?;
        self.state.files.lock().insert(
            file_ref,
            DiskManagerSpillFile {
                temp_file,
                finalized: false,
            },
        );
        self.state.files_created.fetch_add(1, Ordering::Relaxed);
        Ok((
            file_ref,
            Box::new(DiskManagerSpillWriter {
                scope: Arc::clone(&self.state),
                file_ref,
                writer: Some(writer),
                finalized: false,
            }),
        ))
    }

    async fn open_reader(
        &self,
        file: SpillFileRef,
    ) -> SpillStorageResult<Box<dyn SpillSequentialReader>> {
        self.ensure_active()?;
        if file.scope_id != self.id {
            return Err(unavailable());
        }
        let reader = {
            let files = self.state.files.lock();
            let spill_file = files.get(&file).ok_or_else(unavailable)?;
            if !spill_file.finalized {
                return Err(unavailable());
            }
            File::open(spill_file.temp_file.path()).map_err(|_| io_failure())?
        };
        Ok(Box::new(DiskManagerSpillReader { reader }))
    }

    async fn delete_file(&self, file: SpillFileRef) -> SpillStorageResult<()> {
        self.ensure_active()?;
        if file.scope_id != self.id {
            return Err(unavailable());
        }
        self.state
            .files
            .lock()
            .remove(&file)
            .ok_or_else(unavailable)?;
        Ok(())
    }

    async fn delete_scope(&self) -> SpillStorageResult<()> {
        self.state.deleted.store(true, Ordering::Release);
        self.state.files.lock().clear();
        Ok(())
    }

    fn metrics(&self) -> SpillStorageMetrics {
        let files = self.state.files.lock();
        let current_bytes = files
            .values()
            .map(|file| file.temp_file.current_disk_usage())
            .sum();
        SpillStorageMetrics {
            current_bytes,
            peak_bytes: self.state.peak_bytes.load(Ordering::Relaxed),
            files_created: self.state.files_created.load(Ordering::Relaxed),
            active_files: u64::try_from(files.len()).unwrap_or(u64::MAX),
            merge_passes: 0,
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
struct DiskManagerSpillWriter {
    scope: Arc<DiskManagerSpillScopeState>,
    file_ref: SpillFileRef,
    writer: Option<File>,
    finalized: bool,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl DiskManagerSpillWriter {
    fn update_accounting(&self) -> SpillStorageResult<()> {
        let mut files = self.scope.files.lock();
        let spill_file = files.get_mut(&self.file_ref).ok_or_else(unavailable)?;
        spill_file
            .temp_file
            .update_disk_usage()
            .map_err(map_accounting_error)?;
        let current_bytes = files
            .values()
            .map(|file| file.temp_file.current_disk_usage())
            .sum();
        self.scope
            .peak_bytes
            .fetch_max(current_bytes, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
impl SpillAppendWriter for DiskManagerSpillWriter {
    async fn append(&mut self, bytes: &[u8]) -> SpillStorageResult<()> {
        self.writer
            .as_mut()
            .ok_or_else(unavailable)?
            .write_all(bytes)
            .map_err(|_| io_failure())?;
        self.update_accounting()
    }

    async fn finalize(mut self: Box<Self>) -> SpillStorageResult<()> {
        let mut writer = self.writer.take().ok_or_else(unavailable)?;
        writer.flush().map_err(|_| io_failure())?;
        self.update_accounting()?;
        let mut files = self.scope.files.lock();
        files
            .get_mut(&self.file_ref)
            .ok_or_else(unavailable)?
            .finalized = true;
        self.finalized = true;
        Ok(())
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl Drop for DiskManagerSpillWriter {
    fn drop(&mut self) {
        if !self.finalized {
            self.scope.files.lock().remove(&self.file_ref);
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Debug)]
struct DiskManagerSpillReader {
    reader: File,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[async_trait]
impl SpillSequentialReader for DiskManagerSpillReader {
    async fn read_next(
        &mut self,
        max_bytes: usize,
    ) -> SpillStorageResult<Option<Vec<u8>>> {
        if max_bytes == 0 {
            return Err(SpillStorageError::new(
                SpillStorageErrorReason::IoFailure,
                "spill read size must be positive",
            ));
        }
        let mut bytes = vec![0; max_bytes];
        let read = self.reader.read(&mut bytes).map_err(|_| io_failure())?;
        if read == 0 {
            return Ok(None);
        }
        bytes.truncate(read);
        Ok(Some(bytes))
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn map_create_error(error: DataFusionError) -> SpillStorageError {
    match error.find_root() {
        DataFusionError::ResourcesExhausted(_) => SpillStorageError::new(
            SpillStorageErrorReason::QuotaExceeded,
            "spill storage quota exceeded",
        ),
        DataFusionError::NotImplemented(_) => unavailable(),
        _ => io_failure(),
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn map_accounting_error(error: DataFusionError) -> SpillStorageError {
    match error.find_root() {
        DataFusionError::ResourcesExhausted(_) => SpillStorageError::new(
            SpillStorageErrorReason::QuotaExceeded,
            "spill storage quota exceeded",
        ),
        _ => io_failure(),
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn unavailable() -> SpillStorageError {
    SpillStorageError::new(
        SpillStorageErrorReason::Unavailable,
        "spill storage is unavailable",
    )
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn io_failure() -> SpillStorageError {
    SpillStorageError::new(
        SpillStorageErrorReason::IoFailure,
        "spill storage I/O failed",
    )
}

pub(crate) fn default_spill_storage(
    disk_manager: Arc<DiskManager>,
) -> Arc<dyn SpillStorage> {
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    {
        Arc::new(DiskManagerSpillStorage::new(disk_manager))
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let _ = disk_manager;
        Arc::new(UnavailableSpillStorage)
    }
}
