// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use async_trait::async_trait;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::spill_storage::{
    SpillFileRef, SpillReader, SpillScopeId, SpillStorage, SpillStorageAccounting,
    SpillWriter,
};
use futures::future::poll_fn;

/// Test adapter that holds writer creation pending until the returned sender is
/// released. It also records synchronous scope release on stream cancellation.
#[derive(Debug)]
pub(crate) struct GatedWriterSpillStorage {
    inner: Arc<dyn SpillStorage>,
    writer_gate: Mutex<Option<futures::channel::oneshot::Receiver<()>>>,
    writer_acquisition_started: AtomicBool,
    released_scope_count: AtomicU64,
}

impl GatedWriterSpillStorage {
    pub(crate) fn new(
        inner: Arc<dyn SpillStorage>,
    ) -> (Self, futures::channel::oneshot::Sender<()>) {
        let (sender, receiver) = futures::channel::oneshot::channel();
        (
            Self {
                inner,
                writer_gate: Mutex::new(Some(receiver)),
                writer_acquisition_started: AtomicBool::new(false),
                released_scope_count: AtomicU64::new(0),
            },
            sender,
        )
    }

    pub(crate) fn writer_acquisition_started(&self) -> bool {
        self.writer_acquisition_started.load(Ordering::Relaxed)
    }

    pub(crate) fn released_scope_count(&self) -> u64 {
        self.released_scope_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SpillStorage for GatedWriterSpillStorage {
    async fn create_scope(&self) -> Result<SpillScopeId> {
        self.inner.create_scope().await
    }

    async fn create_writer(&self, scope: &SpillScopeId) -> Result<Box<dyn SpillWriter>> {
        self.writer_acquisition_started
            .store(true, Ordering::Relaxed);
        let receiver = self
            .writer_gate
            .lock()
            .expect("writer gate should not be poisoned")
            .take()
            .ok_or_else(|| {
                DataFusionError::Execution("writer gate was already used".to_owned())
            })?;
        receiver.await.map_err(|_| {
            DataFusionError::Execution("writer gate was cancelled".to_owned())
        })?;
        self.inner.create_writer(scope).await
    }

    async fn open_reader(&self, file: &SpillFileRef) -> Result<Box<dyn SpillReader>> {
        self.inner.open_reader(file).await
    }

    async fn delete_file(&self, file: &SpillFileRef) -> Result<()> {
        self.inner.delete_file(file).await
    }

    async fn delete_scope(&self, scope: &SpillScopeId) -> Result<()> {
        self.inner.delete_scope(scope).await
    }

    fn accounting(&self) -> SpillStorageAccounting {
        self.inner.accounting()
    }

    fn max_merge_fan_in(&self) -> Option<usize> {
        self.inner.max_merge_fan_in()
    }

    fn record_merge_pass(&self) {
        self.inner.record_merge_pass();
    }

    fn release_file(&self, file: &SpillFileRef) {
        self.inner.release_file(file);
    }

    fn release_scope(&self, scope: &SpillScopeId) {
        self.released_scope_count.fetch_add(1, Ordering::Relaxed);
        self.inner.release_scope(scope);
    }
}

/// Test adapter that forces every asynchronous storage call to return
/// `Pending` once before delegating to the real backend.
#[derive(Debug)]
pub(crate) struct YieldingSpillStorage {
    inner: Arc<dyn SpillStorage>,
    pending_poll_count: AtomicU64,
}

impl YieldingSpillStorage {
    pub(crate) fn new(inner: Arc<dyn SpillStorage>) -> Self {
        Self {
            inner,
            pending_poll_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn pending_poll_count(&self) -> u64 {
        self.pending_poll_count.load(Ordering::Relaxed)
    }

    async fn yield_once(&self) {
        let mut yielded = false;
        poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                self.pending_poll_count.fetch_add(1, Ordering::Relaxed);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}

#[async_trait]
impl SpillStorage for YieldingSpillStorage {
    async fn create_scope(&self) -> Result<SpillScopeId> {
        self.yield_once().await;
        self.inner.create_scope().await
    }

    async fn create_writer(&self, scope: &SpillScopeId) -> Result<Box<dyn SpillWriter>> {
        self.yield_once().await;
        self.inner.create_writer(scope).await
    }

    async fn open_reader(&self, file: &SpillFileRef) -> Result<Box<dyn SpillReader>> {
        self.yield_once().await;
        self.inner.open_reader(file).await
    }

    async fn delete_file(&self, file: &SpillFileRef) -> Result<()> {
        self.yield_once().await;
        self.inner.delete_file(file).await
    }

    async fn delete_scope(&self, scope: &SpillScopeId) -> Result<()> {
        self.yield_once().await;
        self.inner.delete_scope(scope).await
    }

    fn accounting(&self) -> SpillStorageAccounting {
        self.inner.accounting()
    }

    fn max_merge_fan_in(&self) -> Option<usize> {
        self.inner.max_merge_fan_in()
    }

    fn record_merge_pass(&self) {
        self.inner.record_merge_pass();
    }

    fn release_file(&self, file: &SpillFileRef) {
        self.inner.release_file(file);
    }

    fn release_scope(&self, scope: &SpillScopeId) {
        self.inner.release_scope(scope);
    }
}
