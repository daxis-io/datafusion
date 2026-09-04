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

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::memory::InMemory;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common_runtime::{SpawnedTask, yield_now};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{
    ParquetFileReaderFactory, ParquetFileReaderFactoryRequired,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::basic::Compression;
use parquet::compression::{CodecOptions, create_codec};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use sha2::{Digest, Sha256};

use crate::axon_fixture::{fixture_batches, normalize_batches};
use crate::qualification_log;

const UNCOMPRESSED: &[u8] = include_bytes!("../fixtures/orders-uncompressed.parquet");
const ZSTD: &[u8] = include_bytes!("../fixtures/orders-zstd.parquet");
const UNCOMPRESSED_SHA256: &str =
    "d1cacf639ebbd275ca08c448fcd9e97cec3a1098b45086cd33635a6a0113c462";
const ZSTD_SHA256: &str =
    "aa933393c3ad8d2c866941e05bb77b264047622a1de24013baf30eb37c24db7f";
const CHECKSUMMED_ZSTD_FRAME: &[u8] = &[
    0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x2d, 0x01, 0x00, 0xf0, 0x62, 0x72, 0x6f, 0x77,
    0x73, 0x65, 0x72, 0x20, 0x7a, 0x73, 0x74, 0x64, 0x20, 0x70, 0x61, 0x79, 0x6c, 0x6f,
    0x61, 0x64, 0x20, 0x72, 0x65, 0x70, 0x65, 0x61, 0x74, 0x65, 0x64, 0x20, 0x01, 0x00,
    0x06, 0x5a, 0x39, 0x01, 0x22, 0xab, 0x4c, 0x02,
];

#[derive(Debug, Default)]
struct ReadTracker {
    factory_calls: AtomicUsize,
    ranges: Mutex<Vec<(Range<u64>, String)>>,
    late_publication: AtomicBool,
}

#[derive(Debug)]
struct FixtureReadError {
    kind: &'static str,
    detail: String,
}

impl Display for FixtureReadError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "fixture {} error: {}", self.kind, self.detail)
    }
}

impl std::error::Error for FixtureReadError {}

#[derive(Debug)]
struct TrackingReaderFactory {
    bytes: Bytes,
    expected_sha256: String,
    tracker: Arc<ReadTracker>,
    delay_turns: usize,
}

impl ParquetFileReaderFactory for TrackingReaderFactory {
    fn create_reader(
        &self,
        _partition_index: usize,
        _partitioned_file: PartitionedFile,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let actual = sha256_hex(&self.bytes);
        if actual != self.expected_sha256 {
            return Err(DataFusionError::External(Box::new(FixtureReadError {
                kind: "checksum",
                detail: format!("expected {}, observed {actual}", self.expected_sha256),
            })));
        }
        self.tracker.factory_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(TrackingAsyncReader {
            bytes: self.bytes.clone(),
            tracker: Arc::clone(&self.tracker),
            delay_turns: self.delay_turns,
        }))
    }
}

#[derive(Clone, Debug)]
struct TrackingAsyncReader {
    bytes: Bytes,
    tracker: Arc<ReadTracker>,
    delay_turns: usize,
}

impl AsyncFileReader for TrackingAsyncReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes = self.bytes.clone();
        let tracker = Arc::clone(&self.tracker);
        let delay_turns = self.delay_turns;
        async move {
            for _ in 0..delay_turns {
                yield_now().await;
            }
            let start = usize::try_from(range.start).map_err(parquet_external)?;
            let end = usize::try_from(range.end).map_err(parquet_external)?;
            if start > end || end > bytes.len() {
                return Err(ParquetError::External(Box::new(FixtureReadError {
                    kind: "range",
                    detail: format!(
                        "requested {start}..{end} from object of length {}",
                        bytes.len()
                    ),
                })));
            }
            let value = bytes.slice(start..end);
            tracker
                .ranges
                .lock()
                .expect("range tracker mutex poisoned")
                .push((range, sha256_hex(&value)));
            Ok(value)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let file_size = self.bytes.len() as u64;
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, file_size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

pub async fn qualify() -> Result<String> {
    qualification_log("exact-stack:uncompressed:start");
    let uncompressed = verify_fixture(
        "orders-uncompressed.parquet",
        UNCOMPRESSED,
        UNCOMPRESSED_SHA256,
        Compression::UNCOMPRESSED,
    )
    .await?;
    qualification_log("exact-stack:uncompressed:complete");
    qualification_log("exact-stack:zstd:start");
    let zstd = verify_fixture(
        "orders-zstd.parquet",
        ZSTD,
        ZSTD_SHA256,
        Compression::ZSTD(Default::default()),
    )
    .await?;
    qualification_log("exact-stack:zstd:complete");
    qualification_log("exact-stack:checksum:start");
    prove_outer_checksum_mismatch().await?;
    qualification_log("exact-stack:checksum:complete");
    qualification_log("exact-stack:range:start");
    prove_invalid_range().await?;
    qualification_log("exact-stack:range:complete");
    qualification_log("exact-stack:footer:start");
    prove_truncated_footer().await?;
    qualification_log("exact-stack:footer:complete");
    qualification_log("exact-stack:zstd-corrupt:start");
    prove_corrupt_zstd_payload().await?;
    qualification_log("exact-stack:zstd-corrupt:complete");
    qualification_log("exact-stack:missing-factory:start");
    prove_factory_is_required().await?;
    qualification_log("exact-stack:missing-factory:complete");
    qualification_log("exact-stack:cancellation:start");
    prove_delayed_read_cancellation().await?;
    qualification_log("exact-stack:cancellation:complete");
    Ok(format!(
        "uncompressed_ranges={uncompressed};zstd_ranges={zstd};checksum,range,footer,zstd,cancellation=ok"
    ))
}

async fn verify_fixture(
    name: &str,
    fixture: &'static [u8],
    expected_sha256: &str,
    expected_compression: Compression,
) -> Result<usize> {
    if sha256_hex(fixture) != expected_sha256 {
        return Err(DataFusionError::Execution(format!(
            "committed fixture {name} does not match its frozen checksum"
        )));
    }
    let tracker = Arc::new(ReadTracker::default());
    let mut metadata_reader = TrackingAsyncReader {
        bytes: Bytes::from_static(fixture),
        tracker: Arc::clone(&tracker),
        delay_turns: 0,
    };
    let metadata = metadata_reader
        .get_metadata(None)
        .await
        .map_err(parquet_to_df)?;
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            if column.compression() != expected_compression {
                return Err(DataFusionError::Execution(format!(
                    "{name} uses {}, expected {expected_compression}",
                    column.compression()
                )));
            }
        }
    }

    let batches = scan_fixture(
        name,
        Bytes::from_static(fixture),
        expected_sha256,
        Arc::clone(&tracker),
    )
    .await?;
    let (orders, _, _) = fixture_batches()?;
    if normalize_batches(&batches)? != normalize_batches(&[orders])? {
        return Err(DataFusionError::Execution(format!(
            "{name} decoded rows differ from the frozen Axon orders"
        )));
    }
    if tracker.factory_calls.load(Ordering::SeqCst) == 0 {
        return Err(DataFusionError::Execution(format!(
            "{name} did not invoke the custom Parquet reader factory"
        )));
    }
    let ranges = tracker.ranges.lock().expect("range tracker mutex poisoned");
    if ranges.is_empty()
        || ranges.iter().any(|(range, hash)| {
            range.start > range.end
                || range.end > fixture.len() as u64
                || hash.len() != 64
        })
    {
        return Err(DataFusionError::Execution(format!(
            "{name} produced invalid or unchecksummed range observations"
        )));
    }
    Ok(ranges.len())
}

async fn scan_fixture(
    name: &str,
    bytes: Bytes,
    expected_sha256: &str,
    tracker: Arc<ReadTracker>,
) -> Result<Vec<datafusion::arrow::array::RecordBatch>> {
    let (orders, _, _) = fixture_batches()?;
    let source = Arc::new(
        ParquetSource::new(orders.schema()).with_parquet_file_reader_factory(Arc::new(
            TrackingReaderFactory {
                bytes: bytes.clone(),
                expected_sha256: expected_sha256.to_owned(),
                tracker,
                delay_turns: 0,
            },
        )),
    );
    let object_store_url = ObjectStoreUrl::parse("memory://browser-qualification")?;
    let config = FileScanConfigBuilder::new(object_store_url.clone(), source)
        .with_file(PartitionedFile::new(name, bytes.len() as u64))
        .build();
    let plan = DataSourceExec::from_data_source(config);
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.runtime_env()
        .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
    datafusion::physical_plan::collect(plan, ctx.task_ctx()).await
}

async fn prove_outer_checksum_mismatch() -> Result<()> {
    let tracker = Arc::new(ReadTracker::default());
    let error = scan_fixture(
        "checksum-mismatch.parquet",
        Bytes::from_static(UNCOMPRESSED),
        &"0".repeat(64),
        Arc::clone(&tracker),
    )
    .await
    .expect_err("wrong outer checksum must fail before decoding");
    if !error_chain_contains(&error, "fixture checksum error")
        || !tracker
            .ranges
            .lock()
            .expect("range tracker mutex poisoned")
            .is_empty()
    {
        return Err(DataFusionError::Execution(format!(
            "outer checksum failure was not preserved before reads: {error}"
        )));
    }
    Ok(())
}

async fn prove_invalid_range() -> Result<()> {
    let mut reader = TrackingAsyncReader {
        bytes: Bytes::from_static(UNCOMPRESSED),
        tracker: Arc::new(ReadTracker::default()),
        delay_turns: 0,
    };
    let error = reader
        .get_bytes(0..(UNCOMPRESSED.len() as u64 + 1))
        .await
        .expect_err("out-of-bounds range must fail");
    if !error_chain_contains(&error, "fixture range error") {
        return Err(DataFusionError::Execution(format!(
            "range error lost its typed cause: {error}"
        )));
    }
    Ok(())
}

async fn prove_truncated_footer() -> Result<()> {
    let truncated = Bytes::copy_from_slice(&UNCOMPRESSED[..UNCOMPRESSED.len() - 3]);
    let checksum = sha256_hex(&truncated);
    let error = scan_fixture(
        "truncated-footer.parquet",
        truncated,
        &checksum,
        Arc::new(ReadTracker::default()),
    )
    .await
    .expect_err("truncated footer must fail");
    if !error_chain_contains(&error, "footer") && !error_chain_contains(&error, "parquet")
    {
        return Err(DataFusionError::Execution(format!(
            "truncated footer returned an unclassified error: {error}"
        )));
    }
    Ok(())
}

async fn prove_corrupt_zstd_payload() -> Result<()> {
    let mut corrupted = CHECKSUMMED_ZSTD_FRAME.to_vec();
    *corrupted
        .last_mut()
        .expect("checksummed ZSTD fixture is non-empty") ^= 0x01;
    let corrupted = Bytes::from(corrupted);
    let checksum = sha256_hex(&corrupted);
    let tracker = Arc::new(ReadTracker::default());
    let factory = TrackingReaderFactory {
        bytes: corrupted.clone(),
        expected_sha256: checksum,
        tracker: Arc::clone(&tracker),
        delay_turns: 0,
    };
    let mut reader = factory.create_reader(
        0,
        PartitionedFile::new("corrupt-zstd-frame", corrupted.len() as u64),
        None,
        &ExecutionPlanMetricsSet::new(),
    )?;
    let frame = reader.get_bytes(0..corrupted.len() as u64).await?;
    let mut codec = create_codec(
        Compression::ZSTD(Default::default()),
        &CodecOptions::default(),
    )?
    .ok_or_else(|| DataFusionError::Execution("browser ZSTD codec is absent".into()))?;
    let mut output = b"prefix".to_vec();
    let error = codec.decompress(&frame, &mut output, Some(50)).expect_err(
        "corrupt ZSTD frame checksum must fail after outer checksum validation",
    );
    if error.to_string() != "Parquet error: ZSTD frame checksum mismatch"
        || output != b"prefix"
        || tracker.factory_calls.load(Ordering::SeqCst) != 1
        || tracker
            .ranges
            .lock()
            .expect("range tracker mutex poisoned")
            .len()
            != 1
    {
        return Err(DataFusionError::Execution(format!(
            "corrupt ZSTD frame checksum contract was not preserved: {error}"
        )));
    }
    Ok(())
}

async fn prove_factory_is_required() -> Result<()> {
    let (orders, _, _) = fixture_batches()?;
    let source = Arc::new(ParquetSource::new(orders.schema()));
    let object_store_url = ObjectStoreUrl::parse("memory://browser-qualification")?;
    let config = FileScanConfigBuilder::new(object_store_url.clone(), source)
        .with_file(PartitionedFile::new(
            "missing-factory.parquet",
            UNCOMPRESSED.len() as u64,
        ))
        .build();
    let plan = DataSourceExec::from_data_source(config);
    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.runtime_env()
        .register_object_store(object_store_url.as_ref(), Arc::new(InMemory::new()));
    let error = datafusion::physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect_err("custom-reader-only source must require an injected factory");
    if !error_chain_contains(&error, "ParquetFileReaderFactory must be injected")
        || !error_chain_contains(&error, "object-store-reader")
    {
        return Err(DataFusionError::Execution(format!(
            "missing factory did not return its structured capability error: {error}"
        )));
    }
    let _type_identity = std::any::TypeId::of::<ParquetFileReaderFactoryRequired>();
    Ok(())
}

async fn prove_delayed_read_cancellation() -> Result<()> {
    let tracker = Arc::new(ReadTracker::default());
    let mut reader = TrackingAsyncReader {
        bytes: Bytes::from_static(UNCOMPRESSED),
        tracker: Arc::clone(&tracker),
        delay_turns: 64,
    };
    let tracker_in_task = Arc::clone(&tracker);
    let task = SpawnedTask::spawn_local(async move {
        let result = reader.get_bytes(0..8).await;
        tracker_in_task
            .late_publication
            .store(true, Ordering::SeqCst);
        result
    });
    yield_now().await;
    drop(task);
    for _ in 0..4 {
        yield_now().await;
    }
    if tracker.late_publication.load(Ordering::SeqCst) {
        return Err(DataFusionError::Execution(
            "cancelled delayed Parquet read published a late result".into(),
        ));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parquet_external(
    error: impl std::error::Error + Send + Sync + 'static,
) -> ParquetError {
    ParquetError::External(Box::new(error))
}

fn parquet_to_df(error: ParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn error_chain_contains(error: &(dyn std::error::Error + 'static), needle: &str) -> bool {
    let needle = needle.to_ascii_lowercase();
    let mut current = Some(error);
    while let Some(error) = current {
        if error.to_string().to_ascii_lowercase().contains(&needle) {
            return true;
        }
        current = error.source();
    }
    false
}
