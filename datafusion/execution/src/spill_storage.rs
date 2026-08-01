// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.

//! Storage-neutral, sequential spill I/O.
//!
//! Physical operators need query-scoped sequential Arrow IPC streams, not
//! filesystem paths. Browser runtimes can therefore keep browser objects in a
//! worker-local registry while exposing only opaque identities to DataFusion.

use std::fmt;
use std::io::{Read, Write};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::Result;

/// Opaque identity for one query-scoped spill namespace.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct SpillScopeId(Arc<str>);

impl SpillScopeId {
    /// Creates an identity owned by a spill-storage implementation.
    pub fn new(opaque_id: impl Into<Arc<str>>) -> Self {
        Self(opaque_id.into())
    }

    /// Returns the backend-private identity.
    pub fn opaque_id(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SpillScopeId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SpillScopeId(REDACTED)")
    }
}

/// Opaque reference to one finalized spill stream.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct SpillFileRef {
    scope: SpillScopeId,
    opaque_id: Arc<str>,
}

impl SpillFileRef {
    /// Creates a reference owned by a spill-storage implementation.
    pub fn new(scope: SpillScopeId, opaque_id: impl Into<Arc<str>>) -> Self {
        Self {
            scope,
            opaque_id: opaque_id.into(),
        }
    }

    /// Returns the query scope containing this file.
    pub fn scope(&self) -> &SpillScopeId {
        &self.scope
    }

    /// Returns the backend-private identity.
    pub fn opaque_id(&self) -> &str {
        &self.opaque_id
    }
}

impl fmt::Debug for SpillFileRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SpillFileRef(REDACTED)")
    }
}

/// Point-in-time accounting without storage identifiers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SpillStorageAccounting {
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub files_created: u64,
    pub active_files: u64,
    pub active_bytes: u64,
    pub peak_active_bytes: u64,
    pub merge_passes: u64,
}

/// Append-only sequential writer for one Arrow IPC stream.
pub trait SpillWriter: Write + Send {
    /// Flushes and finalizes the stream.
    fn finish(self: Box<Self>) -> Result<SpillFileRef>;
}

/// Where synchronous reads from an opened spill stream should execute.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpillReadMode {
    /// Run reads on the current executor thread. Browser OPFS uses this mode
    /// because the query already runs in a dedicated worker.
    Inline,
    /// Offload reads to the native blocking executor.
    Blocking,
}

/// Sequential reader for one finalized Arrow IPC stream.
pub trait SpillReader: Read + Send {
    fn read_mode(&self) -> SpillReadMode;
}

/// Path-free spill storage used by physical operators.
#[async_trait]
pub trait SpillStorage: fmt::Debug + Send + Sync {
    async fn create_scope(&self) -> Result<SpillScopeId>;
    async fn create_writer(&self, scope: &SpillScopeId) -> Result<Box<dyn SpillWriter>>;
    async fn open_reader(&self, file: &SpillFileRef) -> Result<Box<dyn SpillReader>>;
    async fn delete_file(&self, file: &SpillFileRef) -> Result<()>;
    async fn delete_scope(&self, scope: &SpillScopeId) -> Result<()>;
    fn accounting(&self) -> SpillStorageAccounting;
    /// Maximum number of sorted inputs merged in one pass, when the backend
    /// requires bounded fan-in. Native storage keeps the established default.
    fn max_merge_fan_in(&self) -> Option<usize> {
        None
    }
    /// Records one streaming merge over two or more sorted runs.
    fn record_merge_pass(&self) {}
    /// Releases a file when its sequential consumer is dropped. Native
    /// backends use this to preserve temporary-file RAII; browser backends can
    /// defer deletion to deterministic query-scope cleanup.
    fn release_file(&self, _file: &SpillFileRef) {}
    /// Releases an operator scope when its last manager is dropped.
    fn release_scope(&self, _scope: &SpillScopeId) {}
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod native {
    use std::collections::{HashMap, HashSet};
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::sync::atomic::{AtomicU64, Ordering};

    use datafusion_common::DataFusionError;
    use parking_lot::Mutex;

    use super::*;
    use crate::disk_manager::{DiskManager, RefCountedTempFile};

    #[derive(Debug, Default)]
    struct NativeState {
        scopes: HashSet<SpillScopeId>,
        files: HashMap<SpillFileRef, RefCountedTempFile>,
        accounting: SpillStorageAccounting,
    }

    #[derive(Debug)]
    struct NativeInner {
        disk_manager: Arc<DiskManager>,
        next_scope: AtomicU64,
        next_file: AtomicU64,
        state: Mutex<NativeState>,
    }

    /// Native adapter that delegates file creation, quota enforcement, and
    /// lifetime accounting to DataFusion's existing [`DiskManager`].
    #[derive(Clone, Debug)]
    pub struct NativeSpillStorage {
        inner: Arc<NativeInner>,
    }

    impl NativeSpillStorage {
        pub fn new(disk_manager: Arc<DiskManager>) -> Self {
            Self {
                inner: Arc::new(NativeInner {
                    disk_manager,
                    next_scope: AtomicU64::new(1),
                    next_file: AtomicU64::new(1),
                    state: Mutex::new(NativeState::default()),
                }),
            }
        }
    }

    #[async_trait]
    impl SpillStorage for NativeSpillStorage {
        async fn create_scope(&self) -> Result<SpillScopeId> {
            let id = self.inner.next_scope.fetch_add(1, Ordering::Relaxed);
            let scope = SpillScopeId::new(format!("scope-{id:016x}"));
            self.inner.state.lock().scopes.insert(scope.clone());
            Ok(scope)
        }

        async fn create_writer(
            &self,
            scope: &SpillScopeId,
        ) -> Result<Box<dyn SpillWriter>> {
            if !self.inner.state.lock().scopes.contains(scope) {
                return Err(DataFusionError::Execution(
                    "spill scope is unavailable".to_owned(),
                ));
            }

            let id = self.inner.next_file.fetch_add(1, Ordering::Relaxed);
            let file_ref = SpillFileRef::new(scope.clone(), format!("spill-{id:016x}"));
            let temp_file = self
                .inner
                .disk_manager
                .create_tmp_file("creating a path-free spill stream")?;
            let writer = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(temp_file.path())
                .map_err(DataFusionError::IoError)?;

            let mut state = self.inner.state.lock();
            state.files.insert(file_ref.clone(), temp_file.clone());
            state.accounting.files_created =
                state.accounting.files_created.saturating_add(1);
            drop(state);

            Ok(Box::new(NativeSpillWriter {
                inner: Arc::clone(&self.inner),
                writer: Some(writer),
                temp_file,
                file_ref,
            }))
        }

        async fn open_reader(&self, file: &SpillFileRef) -> Result<Box<dyn SpillReader>> {
            let temp_file = self
                .inner
                .state
                .lock()
                .files
                .get(file)
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution("spill file is unavailable".to_owned())
                })?;
            let reader =
                File::open(temp_file.path()).map_err(DataFusionError::IoError)?;
            Ok(Box::new(NativeSpillReader {
                inner: Arc::clone(&self.inner),
                reader,
            }))
        }

        async fn delete_file(&self, file: &SpillFileRef) -> Result<()> {
            self.inner.state.lock().files.remove(file);
            Ok(())
        }

        async fn delete_scope(&self, scope: &SpillScopeId) -> Result<()> {
            let mut state = self.inner.state.lock();
            state.files.retain(|file, _| file.scope() != scope);
            state.scopes.remove(scope);
            Ok(())
        }

        fn accounting(&self) -> SpillStorageAccounting {
            let mut accounting = self.inner.state.lock().accounting;
            let progress = self.inner.disk_manager.spilling_progress();
            accounting.active_files =
                u64::try_from(progress.active_files_count).unwrap_or(u64::MAX);
            accounting.active_bytes = progress.current_bytes;
            accounting.peak_active_bytes =
                accounting.peak_active_bytes.max(accounting.active_bytes);
            accounting
        }

        fn record_merge_pass(&self) {
            let mut state = self.inner.state.lock();
            state.accounting.merge_passes =
                state.accounting.merge_passes.saturating_add(1);
        }

        fn release_file(&self, file: &SpillFileRef) {
            let mut state = self.inner.state.lock();
            state.files.remove(file);
            if !state
                .files
                .keys()
                .any(|candidate| candidate.scope() == file.scope())
            {
                state.scopes.remove(file.scope());
            }
        }

        fn release_scope(&self, scope: &SpillScopeId) {
            let mut state = self.inner.state.lock();
            state.files.retain(|file, _| file.scope() != scope);
            state.scopes.remove(scope);
        }
    }

    struct NativeSpillWriter {
        inner: Arc<NativeInner>,
        writer: Option<File>,
        temp_file: RefCountedTempFile,
        file_ref: SpillFileRef,
    }

    impl Write for NativeSpillWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            let written = self
                .writer
                .as_mut()
                .ok_or_else(|| io::Error::other("spill writer already finalized"))?
                .write(buffer)?;
            self.temp_file
                .update_disk_usage()
                .map_err(datafusion_error_to_io)?;

            let mut state = self.inner.state.lock();
            state.accounting.bytes_written = state
                .accounting
                .bytes_written
                .saturating_add(u64::try_from(written).unwrap_or(u64::MAX));
            state.accounting.peak_active_bytes = state
                .accounting
                .peak_active_bytes
                .max(self.inner.disk_manager.spilling_progress().current_bytes);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.writer
                .as_mut()
                .ok_or_else(|| io::Error::other("spill writer already finalized"))?
                .flush()?;
            self.temp_file
                .update_disk_usage()
                .map_err(datafusion_error_to_io)
        }
    }

    impl SpillWriter for NativeSpillWriter {
        fn finish(mut self: Box<Self>) -> Result<SpillFileRef> {
            self.flush().map_err(DataFusionError::IoError)?;
            self.writer.take();
            Ok(self.file_ref.clone())
        }
    }

    struct NativeSpillReader {
        inner: Arc<NativeInner>,
        reader: File,
    }

    impl SpillReader for NativeSpillReader {
        fn read_mode(&self) -> SpillReadMode {
            SpillReadMode::Blocking
        }
    }

    impl Read for NativeSpillReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let read = self.reader.read(buffer)?;
            let mut state = self.inner.state.lock();
            state.accounting.bytes_read = state
                .accounting
                .bytes_read
                .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
            Ok(read)
        }
    }

    fn datafusion_error_to_io(error: DataFusionError) -> io::Error {
        match error {
            DataFusionError::ResourcesExhausted(message) => {
                io::Error::new(io::ErrorKind::StorageFull, message)
            }
            error => io::Error::other(error),
        }
    }
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use native::NativeSpillStorage;
