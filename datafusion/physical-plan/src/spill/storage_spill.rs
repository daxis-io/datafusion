// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.

//! Arrow IPC streams backed by path-free [`SpillStorage`].

use std::io::BufReader;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::MetadataVersion;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::SpillCompression;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::RecordBatchStream;
use datafusion_execution::spill_storage::{
    SpillFileRef, SpillReadMode, SpillReader, SpillStorage, SpillWriter,
};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream};
use log::debug;

use crate::metrics::SpillMetrics;

use super::{get_max_alignment_for_schema, spill_manager::SpillScopeLease};

const SPILL_BATCH_MEMORY_MARGIN: usize = 4096;

/// Incremental Arrow IPC writer whose underlying stream is supplied by a
/// path-free spill backend.
pub(crate) struct StorageInProgressSpillFile {
    writer: Option<StreamWriter<Box<dyn SpillWriter>>>,
    storage: Arc<dyn SpillStorage>,
    metrics: SpillMetrics,
}

impl StorageInProgressSpillFile {
    pub(crate) fn try_new(
        writer: Box<dyn SpillWriter>,
        storage: Arc<dyn SpillStorage>,
        metrics: SpillMetrics,
        schema: &Schema,
        compression: SpillCompression,
    ) -> Result<Self> {
        let before = storage.accounting().bytes_written;
        let alignment = get_max_alignment_for_schema(schema);
        let options = IpcWriteOptions::try_new(alignment, false, MetadataVersion::V5)?
            .try_with_compression(compression.into())?;
        let writer = StreamWriter::try_new_with_options(writer, schema, options)?;
        let after = storage.accounting().bytes_written;

        metrics.spill_file_count.add(1);
        metrics
            .spilled_bytes
            .add(after.saturating_sub(before) as usize);
        Ok(Self {
            writer: Some(writer),
            storage,
            metrics,
        })
    }

    /// Appends exactly one batch. The aggregate state machine calls this once
    /// per poll so large runs yield cooperatively.
    pub(crate) fn append_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let before = self.storage.accounting().bytes_written;
        let writer = self.writer.as_mut().ok_or_else(|| {
            exec_datafusion_err!("path-free spill writer is already finalized")
        })?;
        writer.write(batch)?;
        let after = self.storage.accounting().bytes_written;
        self.metrics.spilled_rows.add(batch.num_rows());
        self.metrics
            .spilled_bytes
            .add(after.saturating_sub(before) as usize);
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<SpillFileRef> {
        let before = self.storage.accounting().bytes_written;
        let writer = self.writer.take().ok_or_else(|| {
            exec_datafusion_err!("path-free spill writer is already finalized")
        })?;
        let writer = writer.into_inner()?;
        let file = writer.finish()?;
        let after = self.storage.accounting().bytes_written;
        self.metrics
            .spilled_bytes
            .add(after.saturating_sub(before) as usize);
        Ok(file)
    }
}

type StorageIpcReader = StreamReader<BufReader<Box<dyn SpillReader>>>;
type NextRecordBatchResult = Result<(StorageIpcReader, Option<RecordBatch>)>;

enum StorageReaderState {
    Opening(BoxFuture<'static, Result<Box<dyn SpillReader>>>),
    Inline(StorageIpcReader),
    BlockingWaiting(StorageIpcReader),
    BlockingRead(SpawnedTask<NextRecordBatchResult>),
    Done,
}

/// Opens the backend asynchronously and then reads one IPC batch per poll.
pub(crate) struct StorageSpillReaderStream {
    schema: SchemaRef,
    storage: Arc<dyn SpillStorage>,
    file: SpillFileRef,
    state: StorageReaderState,
    max_record_batch_memory: Option<usize>,
    _scope_lease: Arc<SpillScopeLease>,
}

impl StorageSpillReaderStream {
    pub(crate) fn new(
        schema: SchemaRef,
        storage: Arc<dyn SpillStorage>,
        file: SpillFileRef,
        max_record_batch_memory: Option<usize>,
        scope_lease: Arc<SpillScopeLease>,
    ) -> Self {
        let opening_storage = Arc::clone(&storage);
        let opening_file = file.clone();
        let opening =
            async move { opening_storage.open_reader(&opening_file).await }.boxed();
        Self {
            schema,
            storage,
            file,
            state: StorageReaderState::Opening(opening),
            max_record_batch_memory,
            _scope_lease: scope_lease,
        }
    }

    fn validate_batch(&self, batch: &RecordBatch) {
        let Some(max_record_batch_memory) = self.max_record_batch_memory else {
            return;
        };
        let actual_size = get_record_batch_memory_size(batch);
        if actual_size > max_record_batch_memory + SPILL_BATCH_MEMORY_MARGIN {
            debug!(
                "Record batch memory usage ({actual_size} bytes) exceeds the expected \
                 limit ({max_record_batch_memory} bytes) by more than \
                 {SPILL_BATCH_MEMORY_MARGIN} bytes"
            );
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StorageReaderState::Opening(opening) => {
                    let reader = futures::ready!(opening.poll_unpin(cx));
                    let reader = match reader {
                        Ok(reader) => reader,
                        Err(error) => {
                            self.state = StorageReaderState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    let read_mode = reader.read_mode();
                    let reader = unsafe {
                        StreamReader::try_new(BufReader::new(reader), None)?
                            .with_skip_validation(true)
                    };
                    self.state = match read_mode {
                        SpillReadMode::Inline => StorageReaderState::Inline(reader),
                        SpillReadMode::Blocking => {
                            StorageReaderState::BlockingWaiting(reader)
                        }
                    };
                }
                StorageReaderState::Inline(reader) => {
                    let batch = reader.next().transpose();
                    match batch {
                        Ok(Some(batch)) => {
                            self.validate_batch(&batch);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Ok(None) => {
                            self.state = StorageReaderState::Done;
                            return Poll::Ready(None);
                        }
                        Err(error) => {
                            self.state = StorageReaderState::Done;
                            return Poll::Ready(Some(Err(error.into())));
                        }
                    }
                }
                StorageReaderState::BlockingWaiting(_) => {
                    let StorageReaderState::BlockingWaiting(mut reader) =
                        std::mem::replace(&mut self.state, StorageReaderState::Done)
                    else {
                        unreachable!()
                    };
                    self.state = StorageReaderState::BlockingRead(
                        SpawnedTask::spawn_blocking(move || {
                            let next = reader.next().transpose()?;
                            Ok((reader, next))
                        }),
                    );
                }
                StorageReaderState::BlockingRead(task) => {
                    let result =
                        futures::ready!(task.poll_unpin(cx)).unwrap_or_else(|error| {
                            Err(DataFusionError::External(Box::new(error)))
                        });
                    match result {
                        Ok((reader, Some(batch))) => {
                            self.validate_batch(&batch);
                            self.state = StorageReaderState::BlockingWaiting(reader);
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Ok((_reader, None)) => {
                            self.state = StorageReaderState::Done;
                            return Poll::Ready(None);
                        }
                        Err(error) => {
                            self.state = StorageReaderState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                }
                StorageReaderState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for StorageSpillReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_inner(cx)
    }
}

impl RecordBatchStream for StorageSpillReaderStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for StorageSpillReaderStream {
    fn drop(&mut self) {
        self.storage.release_file(&self.file);
    }
}

pub(crate) fn into_stream(
    schema: SchemaRef,
    storage: Arc<dyn SpillStorage>,
    file: SpillFileRef,
    max_record_batch_memory: Option<usize>,
    scope_lease: Arc<SpillScopeLease>,
) -> StorageSpillReaderStream {
    StorageSpillReaderStream::new(
        schema,
        storage,
        file,
        max_record_batch_memory,
        scope_lease,
    )
}
