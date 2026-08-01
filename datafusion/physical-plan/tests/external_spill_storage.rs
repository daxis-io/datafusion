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

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion_execution::spill_storage::{
    SpillAppendWriter, SpillFileRef, SpillScope, SpillScopeId, SpillSequentialReader,
    SpillStorage, SpillStorageError, SpillStorageErrorReason, SpillStorageMetrics,
    SpillStorageResult,
};
use datafusion_physical_plan::spill::ExternalSpillManager;
use futures::{StreamExt, TryStreamExt, future::poll_fn, task::noop_waker};
use parking_lot::Mutex;

#[derive(Debug, Default)]
struct DelayedMemoryStorage {
    scope_polled: Mutex<bool>,
    files: Arc<Mutex<HashMap<SpillFileRef, Vec<u8>>>>,
    append_control: Option<Arc<AppendControl>>,
    read_control: Option<Arc<AppendControl>>,
    empty_read_once: bool,
}

#[derive(Debug, Default)]
struct AppendControl {
    pending: AtomicBool,
    release: AtomicBool,
}

#[async_trait]
impl SpillStorage for DelayedMemoryStorage {
    async fn create_scope(&self) -> SpillStorageResult<Arc<dyn SpillScope>> {
        poll_fn(|context| {
            let mut polled = self.scope_polled.lock();
            if !*polled {
                *polled = true;
                context.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(())
        })
        .await;
        Ok(Arc::new(DelayedMemoryScope {
            files: Arc::clone(&self.files),
            next_file_id: Mutex::new(1),
            append_control: self.append_control.clone(),
            read_control: self.read_control.clone(),
            empty_read_once: self.empty_read_once,
        }))
    }
}

#[derive(Debug)]
struct DelayedMemoryScope {
    files: Arc<Mutex<HashMap<SpillFileRef, Vec<u8>>>>,
    next_file_id: Mutex<u64>,
    append_control: Option<Arc<AppendControl>>,
    read_control: Option<Arc<AppendControl>>,
    empty_read_once: bool,
}

#[async_trait]
impl SpillScope for DelayedMemoryScope {
    fn id(&self) -> SpillScopeId {
        SpillScopeId::new(1)
    }

    async fn create_file(
        &self,
    ) -> SpillStorageResult<(SpillFileRef, Box<dyn SpillAppendWriter>)> {
        let file_id = {
            let mut next = self.next_file_id.lock();
            let current = *next;
            *next += 1;
            current
        };
        let file = SpillFileRef::new(self.id(), file_id);
        self.files.lock().insert(file, Vec::new());
        Ok((
            file,
            Box::new(MemoryWriter {
                file,
                files: Arc::clone(&self.files),
                append_control: self.append_control.clone(),
            }),
        ))
    }

    async fn open_reader(
        &self,
        file: SpillFileRef,
    ) -> SpillStorageResult<Box<dyn SpillSequentialReader>> {
        let bytes = self
            .files
            .lock()
            .get(&file)
            .cloned()
            .ok_or_else(unavailable)?;
        Ok(Box::new(MemoryReader {
            bytes,
            offset: 0,
            read_control: self.read_control.clone(),
            empty_read_once: self.empty_read_once,
        }))
    }

    async fn delete_file(&self, file: SpillFileRef) -> SpillStorageResult<()> {
        self.files.lock().remove(&file).ok_or_else(unavailable)?;
        Ok(())
    }

    async fn delete_scope(&self) -> SpillStorageResult<()> {
        self.files.lock().clear();
        Ok(())
    }

    fn metrics(&self) -> SpillStorageMetrics {
        let files = self.files.lock();
        SpillStorageMetrics {
            current_bytes: files.values().map(|bytes| bytes.len() as u64).sum(),
            active_files: files.len() as u64,
            ..SpillStorageMetrics::default()
        }
    }
}

#[derive(Debug)]
struct MemoryWriter {
    file: SpillFileRef,
    files: Arc<Mutex<HashMap<SpillFileRef, Vec<u8>>>>,
    append_control: Option<Arc<AppendControl>>,
}

#[async_trait]
impl SpillAppendWriter for MemoryWriter {
    async fn append(&mut self, bytes: &[u8]) -> SpillStorageResult<()> {
        self.files
            .lock()
            .get_mut(&self.file)
            .ok_or_else(unavailable)?
            .extend_from_slice(bytes);
        if let Some(control) = &self.append_control {
            control.pending.store(true, Ordering::Release);
            poll_fn(|context| {
                if control.release.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        poll_fn(|context| {
            context.waker().wake_by_ref();
            Poll::Ready(())
        })
        .await;
        Ok(())
    }

    async fn finalize(self: Box<Self>) -> SpillStorageResult<()> {
        Ok(())
    }
}

fn sample_batch() -> (Arc<Schema>, RecordBatch) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    (schema, batch)
}

#[derive(Debug)]
struct MemoryReader {
    bytes: Vec<u8>,
    offset: usize,
    read_control: Option<Arc<AppendControl>>,
    empty_read_once: bool,
}

#[async_trait]
impl SpillSequentialReader for MemoryReader {
    async fn read_next(
        &mut self,
        max_bytes: usize,
    ) -> SpillStorageResult<Option<Vec<u8>>> {
        if let Some(control) = &self.read_control {
            control.pending.store(true, Ordering::Release);
            poll_fn(|context| {
                if control.release.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    context.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        if self.empty_read_once {
            self.empty_read_once = false;
            return Ok(Some(Vec::new()));
        }
        if self.offset == self.bytes.len() {
            return Ok(None);
        }
        let end = (self.offset + max_bytes).min(self.bytes.len());
        let bytes = self.bytes[self.offset..end].to_vec();
        self.offset = end;
        Ok(Some(bytes))
    }
}

fn unavailable() -> SpillStorageError {
    SpillStorageError::new(
        SpillStorageErrorReason::Unavailable,
        "test spill file is unavailable",
    )
}

#[test]
fn external_spill_manager_handles_pending_storage_without_paths() {
    futures::executor::block_on(async {
        let (schema, batch) = sample_batch();
        let storage = Arc::new(DelayedMemoryStorage::default());
        let manager = ExternalSpillManager::try_new(storage, Arc::clone(&schema))
            .await
            .unwrap();

        let file = manager
            .spill_record_batches(&[batch.clone()])
            .await
            .unwrap();
        let actual = manager
            .read_spill_as_stream(file, 5)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(actual, vec![batch]);
        manager.delete_scope().await.unwrap();
    });
}

#[test]
fn external_spill_keeps_bridge_bytes_reserved_until_append_completes() {
    let (schema, batch) = sample_batch();
    let control = Arc::new(AppendControl::default());
    let storage = Arc::new(DelayedMemoryStorage {
        append_control: Some(Arc::clone(&control)),
        ..DelayedMemoryStorage::default()
    });
    let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
    let pool_dyn: Arc<dyn MemoryPool> = pool.clone();
    let reservation = MemoryConsumer::new("external spill bridge").register(&pool_dyn);
    let manager = futures::executor::block_on(
        ExternalSpillManager::try_new_with_reservation(storage, schema, reservation),
    )
    .unwrap();
    let batches = vec![batch];
    let mut spill = Box::pin(manager.spill_record_batches(&batches));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);

    for _ in 0..8 {
        assert!(spill.as_mut().poll(&mut context).is_pending());
        if control.pending.load(Ordering::Acquire) {
            break;
        }
    }
    assert!(control.pending.load(Ordering::Acquire));
    assert!(
        pool.reserved() > 0,
        "live bridge bytes must remain reserved while append is pending"
    );

    control.release.store(true, Ordering::Release);
    futures::executor::block_on(spill).unwrap();
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn external_spill_writer_oom_remains_resources_exhausted() {
    futures::executor::block_on(async {
        let (schema, batch) = sample_batch();
        let storage = Arc::new(DelayedMemoryStorage::default());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
        let reservation = MemoryConsumer::new("external spill bridge").register(&pool);
        let manager =
            ExternalSpillManager::try_new_with_reservation(storage, schema, reservation)
                .await
                .unwrap();

        let error = manager.spill_record_batches(&[batch]).await.unwrap_err();
        assert!(
            matches!(
                error.find_root(),
                datafusion_common::DataFusionError::ResourcesExhausted(_)
            ),
            "expected ResourcesExhausted, got {error:?}"
        );
    });
}

#[test]
fn external_spill_reserves_read_capacity_before_backend_allocation() {
    futures::executor::block_on(async {
        let (schema, batch) = sample_batch();
        let read_control = Arc::new(AppendControl::default());
        let storage = Arc::new(DelayedMemoryStorage {
            read_control: Some(Arc::clone(&read_control)),
            ..DelayedMemoryStorage::default()
        });
        let pool = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let pool_dyn: Arc<dyn MemoryPool> = pool.clone();
        let reservation =
            MemoryConsumer::new("external spill bridge").register(&pool_dyn);
        let manager = ExternalSpillManager::try_new_with_reservation(
            storage,
            Arc::clone(&schema),
            reservation,
        )
        .await
        .unwrap();
        let file = manager.spill_record_batches(&[batch]).await.unwrap();
        let mut stream = manager.read_spill_as_stream(file, 64).await.unwrap();
        let mut next = Box::pin(stream.next());
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);

        assert!(next.as_mut().poll(&mut context).is_pending());
        assert!(read_control.pending.load(Ordering::Acquire));
        assert_eq!(
            pool.reserved(),
            64,
            "the requested backend read must be reserved before it allocates"
        );

        read_control.release.store(true, Ordering::Release);
        next.await.unwrap().unwrap();
    });
}

#[test]
fn external_spill_adapts_large_reads_to_the_available_pool() {
    futures::executor::block_on(async {
        let (schema, batch) = sample_batch();
        let storage = Arc::new(DelayedMemoryStorage::default());
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(8 * 1024));
        let reservation = MemoryConsumer::new("external spill bridge").register(&pool);
        let manager = ExternalSpillManager::try_new_with_reservation(
            storage,
            Arc::clone(&schema),
            reservation,
        )
        .await
        .unwrap();
        let file = manager
            .spill_record_batches(&[batch.clone()])
            .await
            .unwrap();

        let actual = manager
            .read_spill_as_stream(file, 64 * 1024)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(actual, vec![batch]);
    });
}

#[test]
fn external_spill_rejects_zero_progress_reads() {
    futures::executor::block_on(async {
        let (schema, batch) = sample_batch();
        let storage = Arc::new(DelayedMemoryStorage {
            empty_read_once: true,
            ..DelayedMemoryStorage::default()
        });
        let manager = ExternalSpillManager::try_new(storage, Arc::clone(&schema))
            .await
            .unwrap();
        let file = manager.spill_record_batches(&[batch]).await.unwrap();
        let mut stream = manager.read_spill_as_stream(file, 64).await.unwrap();

        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("made no progress"));
    });
}
