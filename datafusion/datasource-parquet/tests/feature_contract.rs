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

#[cfg(not(feature = "object-store-reader"))]
#[test]
fn parquet_read_requires_an_injected_factory_without_default_reader() {
    use arrow::datatypes::Schema;
    use datafusion_common::DataFusionError;
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource_parquet::ParquetFileReaderFactoryRequired;
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    let source = ParquetSource::new(Arc::new(Schema::empty())).with_batch_size(1024);
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("memory://").expect("valid memory URL"),
        Arc::clone(&source),
    )
    .build();

    let error = source
        .create_morselizer(Arc::new(InMemory::new()), &config, 0)
        .expect_err("a reader factory must be injected when the default is disabled");

    let DataFusionError::External(source) = error else {
        panic!("expected an external capability error, got {error}");
    };
    let error = source
        .downcast_ref::<ParquetFileReaderFactoryRequired>()
        .expect("typed reader-factory requirement must be preserved");
    assert_eq!(error.profile(), "custom-reader-only");
    assert_eq!(error.capability(), "object-store-reader");
}
