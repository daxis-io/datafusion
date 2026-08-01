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

use std::io::{Error as IoError, ErrorKind, Write};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion_execution::spill_storage::{
    SpillAppendWriter, SpillFileRef, SpillScope, SpillSequentialReader, SpillStorage,
    SpillStorageError, SpillStorageMetrics, SpillStorageResult,
};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt};
use parking_lot::Mutex;

/// Path-free Arrow IPC spill bridge used by physical operators.
///
/// File creation and reads are asynchronous so a browser backend can cross an
/// OPFS host boundary without blocking the execution stream. IPC bytes are
/// appended after each record batch and decoded from bounded chunks.
#[derive(Clone, Debug)]
pub struct ExternalSpillManager {
    scope: Arc<dyn SpillScope>,
    schema: SchemaRef,
    reservation: Arc<MemoryReservation>,
}

impl ExternalSpillManager {
    /// Creates a spill manager with an unbounded bridge-buffer reservation.
    ///
    /// Production operators should use [`Self::try_new_with_reservation`] to
    /// charge codec and storage bridge buffers to their execution memory pool.
    pub async fn try_new(
        storage: Arc<dyn SpillStorage>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("ExternalSpillManager").register(&pool);
        Self::try_new_with_reservation(storage, schema, reservation).await
    }

    /// Creates a spill manager that charges bridge buffers to `reservation`.
    pub async fn try_new_with_reservation(
        storage: Arc<dyn SpillStorage>,
        schema: SchemaRef,
        reservation: MemoryReservation,
    ) -> Result<Self> {
        let scope = storage.create_scope().await.map_err(storage_error)?;
        Ok(Self {
            scope,
            schema,
            reservation: Arc::new(reservation),
        })
    }

    /// Writes an uncompressed Arrow IPC stream, yielding to storage after each
    /// record batch.
    pub async fn spill_record_batches(
        &self,
        batches: &[RecordBatch],
    ) -> Result<SpillFileRef> {
        let (file, mut sink) = self.scope.create_file().await.map_err(storage_error)?;
        let buffer = SharedWriteBuffer::new(Arc::new(self.reservation.new_empty()));

        let result = async {
            let mut writer = StreamWriter::try_new(buffer.clone(), self.schema.as_ref())
                .map_err(spill_ipc_error)?;
            append_buffer(&buffer, sink.as_mut()).await?;

            for batch in batches {
                writer.write(batch).map_err(spill_ipc_error)?;
                append_buffer(&buffer, sink.as_mut()).await?;
            }

            writer.finish().map_err(spill_ipc_error)?;
            append_buffer(&buffer, sink.as_mut()).await?;
            sink.finalize().await.map_err(storage_error)
        }
        .await;

        if let Err(error) = result {
            let _ = self.scope.delete_file(file).await;
            return Err(error);
        }

        Ok(file)
    }

    /// Writes a stream as uncompressed Arrow IPC while yielding between
    /// record batches. Returns `None` for an empty stream.
    pub async fn spill_record_batch_stream(
        &self,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<Option<(SpillFileRef, usize)>> {
        let (file, mut sink) = self.scope.create_file().await.map_err(storage_error)?;
        let buffer = SharedWriteBuffer::new(Arc::new(self.reservation.new_empty()));
        let mut max_record_batch_memory = 0;
        let mut wrote_batch = false;
        let result = async {
            let mut writer = StreamWriter::try_new(buffer.clone(), self.schema.as_ref())
                .map_err(spill_ipc_error)?;
            append_buffer(&buffer, sink.as_mut()).await?;

            while let Some(batch) = stream.next().await {
                let batch = batch?;
                wrote_batch = true;
                max_record_batch_memory = max_record_batch_memory
                    .max(super::get_record_batch_memory_size(&batch));
                writer.write(&batch).map_err(spill_ipc_error)?;
                append_buffer(&buffer, sink.as_mut()).await?;
            }

            writer.finish().map_err(spill_ipc_error)?;
            append_buffer(&buffer, sink.as_mut()).await?;
            sink.finalize().await.map_err(storage_error)
        }
        .await;

        if let Err(error) = result {
            let _ = self.scope.delete_file(file).await;
            return Err(error);
        }
        if !wrote_batch {
            self.scope.delete_file(file).await.map_err(storage_error)?;
            return Ok(None);
        }

        Ok(Some((file, max_record_batch_memory)))
    }

    /// Opens a sequential Arrow IPC stream using reads no larger than
    /// `read_chunk_bytes`.
    pub async fn read_spill_as_stream(
        &self,
        file: SpillFileRef,
        read_chunk_bytes: usize,
    ) -> Result<SendableRecordBatchStream> {
        if read_chunk_bytes == 0 {
            return Err(DataFusionError::Execution(
                "spill read chunk size must be greater than zero".to_string(),
            ));
        }
        let reader = self.scope.open_reader(file).await.map_err(storage_error)?;
        let stream = ExternalSpillReaderStream {
            schema: Arc::clone(&self.schema),
            decoder: StreamDecoder::new(),
            pending_buffer: None,
            consumed_since_batch: 0,
            read_chunk_bytes,
            pending_read_reservation: 0,
            reservation: self.reservation.new_empty(),
            scope: Arc::clone(&self.scope),
            file,
            state: ReaderState::Ready(reader),
        };
        Ok(Box::pin(stream))
    }

    /// Deletes every file owned by this query scope.
    pub async fn delete_scope(&self) -> Result<()> {
        self.scope.delete_scope().await.map_err(storage_error)
    }

    /// Records a merge pass in backend telemetry.
    pub fn record_merge_pass(&self) {
        self.scope.record_merge_pass();
    }

    /// Returns backend-accounted bytes and file counts for this query scope.
    pub fn metrics(&self) -> SpillStorageMetrics {
        self.scope.metrics()
    }
}

async fn append_buffer(
    buffer: &SharedWriteBuffer,
    sink: &mut dyn SpillAppendWriter,
) -> Result<()> {
    let bytes = buffer.take();
    if !bytes.is_empty() {
        sink.append(bytes.as_ref()).await.map_err(storage_error)?;
    }
    Ok(())
}

fn storage_error(error: SpillStorageError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn spill_ipc_error(error: ArrowError) -> DataFusionError {
    if matches!(
        &error,
        ArrowError::IoError(_, source) if source.kind() == ErrorKind::OutOfMemory
    ) {
        DataFusionError::ResourcesExhausted(error.to_string())
    } else {
        error.into()
    }
}

#[derive(Clone, Debug)]
struct SharedWriteBuffer {
    bytes: Arc<Mutex<Vec<u8>>>,
    reservation: Arc<MemoryReservation>,
}

impl SharedWriteBuffer {
    fn new(reservation: Arc<MemoryReservation>) -> Self {
        Self {
            bytes: Arc::new(Mutex::new(Vec::new())),
            reservation,
        }
    }

    fn take(&self) -> ReservedWriteBuffer {
        let bytes = std::mem::take(&mut *self.bytes.lock());
        let reserved = bytes.capacity();
        ReservedWriteBuffer {
            bytes,
            reservation: Arc::clone(&self.reservation),
            reserved,
        }
    }
}

impl Write for SharedWriteBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut output = self.bytes.lock();
        let required = output.len().checked_add(bytes.len()).ok_or_else(|| {
            IoError::new(ErrorKind::OutOfMemory, "spill bridge buffer size overflow")
        })?;
        if required > output.capacity() {
            let old_capacity = output.capacity();
            let requested = required - old_capacity;
            self.reservation.try_grow(requested).map_err(|error| {
                IoError::new(ErrorKind::OutOfMemory, error.to_string())
            })?;
            if let Err(error) = output.try_reserve_exact(requested) {
                self.reservation.shrink(requested);
                return Err(IoError::new(ErrorKind::OutOfMemory, error.to_string()));
            }
            let actual_growth = output.capacity() - old_capacity;
            if actual_growth > requested {
                if let Err(error) = self.reservation.try_grow(actual_growth - requested) {
                    self.reservation.shrink(old_capacity + requested);
                    *output = Vec::new();
                    return Err(IoError::new(ErrorKind::OutOfMemory, error.to_string()));
                }
            } else if actual_growth < requested {
                self.reservation.shrink(requested - actual_growth);
            }
        }
        output.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct ReservedWriteBuffer {
    bytes: Vec<u8>,
    reservation: Arc<MemoryReservation>,
    reserved: usize,
}

impl AsRef<[u8]> for ReservedWriteBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl ReservedWriteBuffer {
    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl Drop for ReservedWriteBuffer {
    fn drop(&mut self) {
        self.reservation.shrink(self.reserved);
    }
}

type ReadResult = SpillStorageResult<(Box<dyn SpillSequentialReader>, Option<Vec<u8>>)>;

enum ReaderState {
    Ready(Box<dyn SpillSequentialReader>),
    Reading(BoxFuture<'static, ReadResult>),
    Deleting(BoxFuture<'static, SpillStorageResult<()>>),
    Done,
}

struct ExternalSpillReaderStream {
    schema: SchemaRef,
    decoder: StreamDecoder,
    pending_buffer: Option<Buffer>,
    consumed_since_batch: usize,
    read_chunk_bytes: usize,
    pending_read_reservation: usize,
    reservation: MemoryReservation,
    scope: Arc<dyn SpillScope>,
    file: SpillFileRef,
    state: ReaderState,
}

impl ExternalSpillReaderStream {
    fn reserve_read_chunk(&mut self) -> Result<usize> {
        let mut bytes = self.read_chunk_bytes;
        loop {
            match self.reservation.try_grow(bytes) {
                Ok(()) => return Ok(bytes),
                Err(_) if bytes > 1 => bytes /= 2,
                Err(error) => return Err(error),
            }
        }
    }

    fn fail(&mut self, error: DataFusionError) -> Poll<Option<Result<RecordBatch>>> {
        self.state = ReaderState::Done;
        self.pending_read_reservation = 0;
        self.reservation.free();
        Poll::Ready(Some(Err(error)))
    }

    fn poll_next_inner(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if let Some(buffer) = self.pending_buffer.as_mut() {
                let before = buffer.len();
                let decoded = self.decoder.decode(buffer);
                let consumed = before - buffer.len();
                self.consumed_since_batch += consumed;

                if buffer.is_empty() {
                    self.pending_buffer = None;
                }

                match decoded {
                    Ok(Some(batch)) => {
                        self.reservation.shrink(self.consumed_since_batch);
                        self.consumed_since_batch = 0;
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Ok(None) => {}
                    Err(error) => return self.fail(error.into()),
                }
            }

            match &mut self.state {
                ReaderState::Ready(_) => {
                    let read_chunk_bytes = match self.reserve_read_chunk() {
                        Ok(bytes) => bytes,
                        Err(error) => return self.fail(error),
                    };
                    self.pending_read_reservation = read_chunk_bytes;
                    let ReaderState::Ready(mut reader) =
                        std::mem::replace(&mut self.state, ReaderState::Done)
                    else {
                        unreachable!()
                    };
                    self.state = ReaderState::Reading(
                        async move {
                            let bytes = reader.read_next(read_chunk_bytes).await?;
                            Ok((reader, bytes))
                        }
                        .boxed(),
                    );
                }
                ReaderState::Reading(future) => {
                    let (reader, bytes) =
                        match futures::ready!(future.as_mut().poll(context)) {
                            Ok(result) => result,
                            Err(error) => return self.fail(storage_error(error)),
                        };
                    self.state = ReaderState::Ready(reader);
                    let read_reservation =
                        std::mem::take(&mut self.pending_read_reservation);

                    match bytes {
                        Some(bytes) if !bytes.is_empty() => {
                            if bytes.len() > read_reservation {
                                return self.fail(storage_error(SpillStorageError::new(
                                    datafusion_execution::spill_storage::SpillStorageErrorReason::IoFailure,
                                    "spill storage read exceeded its requested size",
                                )));
                            }
                            self.reservation.shrink(read_reservation - bytes.len());
                            self.pending_buffer = Some(Buffer::from(bytes));
                        }
                        Some(_) => {
                            self.reservation.shrink(read_reservation);
                            return self.fail(storage_error(SpillStorageError::new(
                                datafusion_execution::spill_storage::SpillStorageErrorReason::IoFailure,
                                "spill storage read made no progress",
                            )));
                        }
                        None => {
                            self.reservation.shrink(read_reservation);
                            if let Err(error) = self.decoder.finish() {
                                return self.fail(error.into());
                            }
                            let scope = Arc::clone(&self.scope);
                            let file = self.file;
                            self.state = ReaderState::Deleting(
                                async move { scope.delete_file(file).await }.boxed(),
                            );
                        }
                    }
                }
                ReaderState::Deleting(future) => {
                    match futures::ready!(future.as_mut().poll(context)) {
                        Ok(()) => {
                            self.state = ReaderState::Done;
                            self.reservation.free();
                            return Poll::Ready(None);
                        }
                        Err(error) => return self.fail(storage_error(error)),
                    }
                }
                ReaderState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Stream for ExternalSpillReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_inner(context)
    }
}

impl RecordBatchStream for ExternalSpillReaderStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
