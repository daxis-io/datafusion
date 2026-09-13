//! Shared real Delta qualification, called unchanged by native and Chromium.
use super::*;
use datafusion::execution::{
    config::SessionConfig,
    memory_pool::{GreedyMemoryPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion::object_store::{self, ObjectStoreExt};
use datafusion_datasource_parquet::ParquetFileReaderFactoryResolver;
use datafusion_executor::qualification_fixture::{FixtureStore, LogVariant};
use delta_kernel::tasks::{Resource, TaskLimits};
use serde_json::{Value, json};

#[derive(Debug)]
struct DeltaReader {
    store: Arc<FixtureStore>,
    tracker: Arc<ReadTracker>,
    files: Arc<Mutex<Vec<String>>>,
}
impl ParquetFileReaderFactory for DeltaReader {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        let name = file.object_meta.location.as_ref();
        if !matches!(name, "table/part-a.parquet" | "table/part-b.parquet") {
            return Err(DataFusionError::Execution(format!(
                "unexpected active Parquet path {name}"
            )));
        }
        self.files.lock().unwrap().push(name.to_owned());
        let _ = (partition, hint, metrics);
        self.tracker.factory_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(StoreReader {
            store: self.store.clone(),
            tracker: self.tracker.clone(),
            location: file.object_meta.location,
            size: file.object_meta.size,
        }))
    }
}
#[derive(Debug)]
struct StoreReader {
    store: Arc<FixtureStore>,
    tracker: Arc<ReadTracker>,
    location: object_store::path::Path,
    size: u64,
}
impl AsyncFileReader for StoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            self.tracker.read_started.store(true, Ordering::SeqCst);
            let _active = ActiveRead {
                tracker: self.tracker.clone(),
            };
            // The registered store forces Pending and records the real path
            // and range. Changed stored objects are the bytes decoded here.
            let bytes = self
                .store
                .get_range(&self.location, range.clone())
                .await
                .map_err(parquet_external)?;
            self.tracker
                .ranges
                .lock()
                .unwrap()
                .push((range, sha256_hex(&bytes)));
            Ok(bytes)
        }
        .boxed()
    }
    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        let size = self.size;
        async move {
            // Preserve Kernel's existing FooterLimits projection (local_io.rs)
            // at this caller-owned factory, before fetching/decoding metadata.
            let bounds = limits().footer_limits();
            let finite = parquet::file::metadata::ParquetMetaDataLimits::builder()
                .with_footer_bytes(bounds.footer_bytes)
                .with_schema_elements(bounds.schema_nodes)
                .with_schema_depth(bounds.schema_depth)
                .with_row_groups(bounds.row_groups)
                .with_column_chunks_per_row_group(bounds.column_chunks)
                .with_column_chunks(bounds.column_chunks)
                .with_collection_elements(bounds.metadata_allocated_bytes)
                .with_variable_width_bytes(bounds.metadata_allocated_bytes)
                .with_total_variable_width_bytes(bounds.metadata_allocated_bytes)
                .with_decoded_bytes(bounds.metadata_allocated_bytes)
                .with_page_index_bytes(bounds.page_index_bytes)
                .with_page_index_entries(bounds.page_index_entries)
                .build()?;
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .with_metadata_options(Some(
                    parquet::file::metadata::ParquetMetaDataOptions::new()
                        .with_limits(finite),
                ))
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

struct Caller {
    session: SessionContext,
    store: Arc<FixtureStore>,
    files: Arc<Mutex<Vec<String>>>,
    tracker: Arc<ReadTracker>,
    pool: Arc<GreedyMemoryPool>,
}
async fn caller(variant: LogVariant, parquet: Bytes, inject: bool) -> Result<Caller> {
    let store = Arc::new(FixtureStore::new(variant));
    // Both ordinary data objects remain physically present after the log's
    // remove action. No JSON objects are registered in the ordinary store.
    for path in ["table/part-a.parquet", "table/part-b.parquet"] {
        store
            .put(
                &object_store::path::Path::from(path),
                parquet.clone().into(),
            )
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }
    let pool = Arc::new(GreedyMemoryPool::new(64 << 20));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let tracker = Arc::new(ReadTracker::default());
    let files = Arc::new(Mutex::new(Vec::new()));
    let mut config = datafusion_executor::qualification_fixture::register(
        store.clone(),
        SessionConfig::new().with_target_partitions(1),
        runtime.as_ref(),
    );
    if inject {
        config = config.with_extension(Arc::new(ParquetFileReaderFactoryResolver::new(
            Arc::new(DeltaReader {
                store: store.clone(),
                tracker: tracker.clone(),
                files: files.clone(),
            }),
        )));
    }
    Ok(Caller {
        session: SessionContext::new_with_config_rt(config, runtime),
        store,
        files,
        tracker,
        pool,
    })
}
fn limits() -> TaskLimits {
    TaskLimits::qualification().with_limit(Resource::ListingEntries, 1)
}
fn root() -> url::Url {
    url::Url::parse("memory:///table/").unwrap()
}
async fn query(caller: &Caller, version: Option<u64>) -> Result<Value> {
    let table = Box::pin(deltalake_browser::open(
        &root(),
        version,
        &caller.session,
        limits(),
    ))
    .await?;
    let selected = table.version();
    let expected_version = version.unwrap_or(1);
    assert_eq!(selected, expected_version, "requested snapshot version");
    caller.session.register_table("orders", table)?;
    let dataframe = caller.session.sql("SELECT order_id, discount_cents, is_priority FROM orders WHERE is_priority = true OR discount_cents IS NULL").await?;
    let batches = dataframe.collect().await?;
    let schema = json!(batches[0].schema().fields().iter().map(|field| json!({
        "name": field.name(), "data_type": format!("{:?}", field.data_type()), "nullable": field.is_nullable(),
    })).collect::<Vec<_>>());
    let mut rows = normalize_batches(&batches)?
        .into_iter()
        .map(|row| json!(row))
        .collect::<Vec<_>>();
    rows.sort_by_key(|row| row[0].as_i64());
    let oracle: Value =
        serde_json::from_str(include_str!("../fixtures/narrow-query-oracle.json"))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    assert_eq!(schema, oracle["schema"]);
    let mut expected = oracle["rows"].as_array().unwrap().clone();
    if expected_version == 0 {
        expected.extend(oracle["rows"].as_array().unwrap().iter().cloned());
    }
    expected.sort_by_key(|row| row[0].as_i64());
    assert_eq!(rows, expected);
    let mut paths = caller.files.lock().unwrap().clone();
    paths.sort();
    assert!(paths.iter().any(|p| p == "table/part-a.parquet"));
    if expected_version == 1 {
        assert!(paths.iter().all(|p| p != "table/part-b.parquet"));
    } else {
        assert!(paths.iter().any(|p| p == "table/part-b.parquet"));
    }
    assert_eq!(caller.store.ordinary_log_gets.load(Ordering::SeqCst), 0);
    assert!(caller.store.ordinary_gets.load(Ordering::SeqCst) > 0);
    assert_eq!(caller.store.reads[0].load(Ordering::SeqCst), 2);
    assert_eq!(
        caller.store.reads[1].load(Ordering::SeqCst),
        if expected_version == 0 { 0 } else { 2 }
    );
    assert_eq!(
        caller.store.lists.load(Ordering::SeqCst),
        match version {
            None => 3,
            Some(0) => 1,
            Some(1) => 2,
            _ => unreachable!(),
        }
    );
    assert!(caller.tracker.factory_calls.load(Ordering::SeqCst) > 0);
    let ranges = caller
        .tracker
        .ranges
        .lock()
        .unwrap()
        .iter()
        .map(|(range, hash)| json!({"start":range.start,"end":range.end,"sha256":hash}))
        .collect::<Vec<_>>();
    assert!(!ranges.is_empty());
    let store_reads = caller
        .store
        .ordinary_reads
        .lock()
        .unwrap()
        .iter()
        .map(|(path, range)| {
            assert!(paths.contains(path));
            if expected_version == 1 {
                assert_ne!(path, "table/part-b.parquet");
            }
            let range = range
                .as_ref()
                .unwrap()
                .as_range(UNCOMPRESSED.len() as u64)
                .unwrap();
            assert!(range.start < range.end);
            json!({"path":path,"start":range.start,"end":range.end})
        })
        .collect::<Vec<_>>();
    assert_eq!(store_reads.len(), ranges.len());
    assert_eq!(caller.store.pending.load(Ordering::SeqCst), 0);
    caller.session.deregister_table("orders")?;
    drop(batches);
    assert_eq!(caller.pool.reserved(), 0);
    Ok(
        json!({"version":selected,"schema":schema,"rows":rows,"parquet_files":paths,"ranges":ranges,"store_reads":store_reads,
        "log_reads":[caller.store.reads[0].load(Ordering::SeqCst),caller.store.reads[1].load(Ordering::SeqCst)],
        "listing_pages":caller.store.lists.load(Ordering::SeqCst)}),
    )
}
fn find_error<'a, T: std::error::Error + 'static>(
    mut error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a T> {
    loop {
        if let Some(value) = error.downcast_ref::<T>() {
            return Some(value);
        }
        error = error.source()?;
    }
}
fn kernel_error_in_chain(
    mut error: &(dyn std::error::Error + 'static),
    predicate: impl Fn(&delta_kernel::Error) -> bool,
) -> bool {
    loop {
        if error
            .downcast_ref::<delta_kernel::Error>()
            .is_some_and(&predicate)
        {
            return true;
        }
        let Some(next) = error.source() else {
            return false;
        };
        error = next;
    }
}
pub(super) async fn qualify() -> Result<Value> {
    assert_eq!(sha256_hex(UNCOMPRESSED), UNCOMPRESSED_SHA256);
    let latest = Box::pin(query(
        &caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?,
        None,
    ))
    .await?;
    let version1 = Box::pin(query(
        &caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?,
        Some(1),
    ))
    .await?;
    let version0 = Box::pin(query(
        &caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?,
        Some(0),
    ))
    .await?;
    let malformed = caller(
        LogVariant::MalformedLatest,
        Bytes::from_static(UNCOMPRESSED),
        true,
    )
    .await?;
    let malformed_error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &malformed.session,
        limits(),
    ))
    .await
    .unwrap_err();
    let failure = find_error::<delta_kernel::tasks::OperationFailure>(&malformed_error)
        .expect("typed malformed log failure");
    assert_eq!(
        failure.kind(),
        delta_kernel::tasks::FailureKind::MalformedResponse
    );
    let malformed_message = malformed_error.to_string();
    drop(malformed_error);
    assert_eq!(malformed.pool.reserved(), 0);
    assert!(malformed.files.lock().unwrap().is_empty());
    let malformed_v0 = Box::pin(query(
        &caller(
            LogVariant::MalformedLatest,
            Bytes::from_static(UNCOMPRESSED),
            true,
        )
        .await?,
        Some(0),
    ))
    .await?;
    let unsupported = caller(
        LogVariant::UnsupportedFeatures,
        Bytes::from_static(UNCOMPRESSED),
        true,
    )
    .await?;
    let unsupported_error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &unsupported.session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert!(kernel_error_in_chain(&unsupported_error, |e| matches!(
        e,
        delta_kernel::Error::Unsupported(_)
    )));
    let unsupported_message = unsupported_error.to_string();
    drop(unsupported_error);
    assert_eq!(unsupported.pool.reserved(), 0);
    assert!(unsupported.files.lock().unwrap().is_empty());
    let missing =
        caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), false).await?;
    let missing_error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &missing.session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert!(find_error::<ParquetFileReaderFactoryRequired>(&missing_error).is_some());
    let missing_message = missing_error.to_string();
    drop(missing_error);
    assert_eq!(missing.pool.reserved(), 0);
    assert_eq!(missing.store.lists.load(Ordering::SeqCst), 0);
    // Exercise resource refusal through the public adapter, with the same
    // caller pool and registrations as successful native/browser queries.
    for resource in [
        Resource::TaskStateBytes,
        Resource::MetadataAllocatedBytes,
        Resource::WorkUnits,
    ] {
        let bounded =
            caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
        let error = Box::pin(deltalake_browser::open(
            &root(),
            None,
            &bounded.session,
            limits().with_limit(resource, 0),
        ))
        .await
        .unwrap_err();
        let exhausted = find_error::<delta_kernel::tasks::ResourceExhausted>(&error)
            .copied()
            .or_else(|| {
                find_error::<delta_kernel::tasks::OperationFailure>(&error).and_then(
                    |failure| match failure.kind() {
                        delta_kernel::tasks::FailureKind::ResourceExhausted(value) => {
                            Some(value)
                        }
                        _ => None,
                    },
                )
            })
            .expect("typed task resource exhaustion");
        assert_eq!(exhausted.resource, resource);
        drop(error);
        assert_eq!(bounded.store.lists.load(Ordering::SeqCst), 0);
        assert_eq!(bounded.store.ordinary_gets.load(Ordering::SeqCst), 0);
        assert_eq!(bounded.store.pending.load(Ordering::SeqCst), 0);
        assert_eq!(bounded.pool.reserved(), 0);
    }
    // Drop an actual open while its first capability listing is Pending.
    // The caller stays alive, so zero pool/pending owners prove cancellation
    // released the opening's reservations rather than dropping the runtime.
    let cancelled =
        caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
    let table_root = root();
    let mut opening = Box::pin(deltalake_browser::open(
        &table_root,
        None,
        &cancelled.session,
        limits(),
    ));
    assert!(futures::poll!(opening.as_mut()).is_pending());
    assert_eq!(cancelled.store.pending.load(Ordering::SeqCst), 1);
    drop(opening);
    assert_eq!(cancelled.store.pending.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.store.cancellations.load(Ordering::SeqCst), 1);
    assert_eq!(cancelled.store.allocations.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.store.ordinary_gets.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled.pool.reserved(), 0);
    let no_identity =
        caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
    no_identity
        .store
        .missing_identity
        .store(true, Ordering::SeqCst);
    let error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &no_identity.session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert!(kernel_error_in_chain(&error, |e| matches!(
        e,
        delta_kernel::Error::Unsupported(_)
    )));
    drop(error);
    assert_eq!(no_identity.store.lists.load(Ordering::SeqCst), 0);
    assert_eq!(no_identity.pool.reserved(), 0);
    let gap = caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
    gap.store.omit_version_zero.store(true, Ordering::SeqCst);
    let error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &gap.session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert!(kernel_error_in_chain(&error, |e| matches!(
        e,
        delta_kernel::Error::MissingVersion
    )));
    drop(error);
    assert_eq!(
        gap.store.reads[0].load(Ordering::SeqCst)
            + gap.store.reads[1].load(Ordering::SeqCst),
        0
    );
    assert_eq!(gap.pool.reserved(), 0);
    let mismatch =
        caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
    mismatch
        .session
        .runtime_env()
        .register_object_store(&root(), Arc::new(InMemory::new()));
    let error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &mismatch.session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert_eq!(
        find_error::<datafusion_executor::log_storage::LogStorageAdmissionError>(&error),
        Some(&datafusion_executor::log_storage::LogStorageAdmissionError::StoreMismatch)
    );
    drop(error);
    assert_eq!(mismatch.store.lists.load(Ordering::SeqCst), 0);
    assert_eq!(mismatch.pool.reserved(), 0);
    let no_capability =
        caller(LogVariant::Valid, Bytes::from_static(UNCOMPRESSED), true).await?;
    let bare_session = SessionContext::new_with_config_rt(
        SessionConfig::new(),
        no_capability.session.runtime_env().clone(),
    );
    let error = Box::pin(deltalake_browser::open(
        &root(),
        None,
        &bare_session,
        limits(),
    ))
    .await
    .unwrap_err();
    assert_eq!(find_error::<datafusion_executor::log_storage::LogStorageAdmissionError>(&error),
        Some(&datafusion_executor::log_storage::LogStorageAdmissionError::MissingCapability));
    drop(error);
    assert_eq!(no_capability.store.lists.load(Ordering::SeqCst), 0);
    assert_eq!(no_capability.pool.reserved(), 0);
    for bytes in [
        Bytes::copy_from_slice(&UNCOMPRESSED[..UNCOMPRESSED.len() - 3]),
        {
            let mut corrupted = UNCOMPRESSED.to_vec();
            let len = corrupted.len();
            corrupted[len - 4..].fill(0);
            Bytes::from(corrupted)
        },
    ] {
        let bad = caller(LogVariant::Valid, bytes, true).await?;
        let table = Box::pin(deltalake_browser::open(
            &root(),
            None,
            &bad.session,
            limits(),
        ))
        .await?;
        bad.session.register_table("bad_orders", table)?;
        let error = bad
            .session
            .sql("SELECT * FROM bad_orders")
            .await?
            .collect()
            .await
            .unwrap_err();
        assert!(
            find_error::<ParquetError>(&error).is_some(),
            "expected Parquet decoding failure: {error}"
        );
        drop(error);
        assert!(
            bad.files
                .lock()
                .unwrap()
                .iter()
                .all(|p| p != "table/part-b.parquet")
        );
        assert_eq!(bad.store.ordinary_log_gets.load(Ordering::SeqCst), 0);
        assert!(bad.store.ordinary_gets.load(Ordering::SeqCst) > 0);
        assert!(
            bad.store
                .ordinary_reads
                .lock()
                .unwrap()
                .iter()
                .all(|(path, _)| path != "table/part-b.parquet")
        );
        bad.session.deregister_table("bad_orders")?;
        assert_eq!(bad.pool.reserved(), 0);
    }
    Ok(
        json!({"fixture_sha256":UNCOMPRESSED_SHA256,"latest":latest,"version0":version0,"version1":version1,
        "malformed_latest":"rejected","malformed_latest_version0":malformed_v0,
        "unsupported_features":"rejected","missing_reader":"rejected_before_io",
        "errors":{"malformed_latest":malformed_message,"unsupported_features":unsupported_message,"missing_reader":missing_message},
        "truncated_active_parquet":"rejected","corrupt_active_parquet":"rejected","missing_identity":"unsupported_before_io","missing_version":"rejected_before_log_reads","mismatched_store":"rejected_before_io","missing_capability":"rejected_before_io","resource_refusal":"typed_before_io_and_released","cancelled_open":"released_while_pending"}),
    )
}
