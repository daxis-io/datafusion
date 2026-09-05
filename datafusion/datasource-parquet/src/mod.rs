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

//! DataFusion Parquet Reader: [`ParquetSource`]
//!
//! [`ParquetSource`]: source::ParquetSource

// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

#[cfg(feature = "parquet-read")]
pub mod access_plan;
#[cfg(feature = "parquet-read")]
mod bloom_filter;
#[cfg(feature = "parquet-read")]
mod decoder_projection;
#[cfg(feature = "object-store-reader")]
pub mod file_format;
#[cfg(feature = "object-store-reader")]
pub mod metadata;
#[cfg(feature = "parquet-read")]
mod metrics;
#[cfg(feature = "parquet-read")]
mod nested_schema_pruning;
#[cfg(feature = "parquet-read")]
mod opener;
#[cfg(feature = "parquet-read")]
mod page_filter;
#[cfg(feature = "parquet-read")]
mod projection_read_plan;
#[cfg(feature = "parquet-read")]
mod push_decoder;
#[cfg(feature = "parquet-read")]
mod reader;
#[cfg(feature = "parquet-read")]
mod row_filter;
#[cfg(feature = "parquet-read")]
mod row_group_filter;
#[cfg(feature = "parquet-read")]
mod schema_coercion;
#[cfg(feature = "parquet-write")]
mod sink;
#[cfg(feature = "parquet-read")]
mod sort;
#[cfg(feature = "parquet-read")]
pub mod source;
#[cfg(feature = "parquet-read")]
mod supported_predicates;
#[cfg(all(test, feature = "parquet-read"))]
mod test_util;
#[cfg(feature = "parquet-read")]
mod virtual_column;
#[cfg(feature = "parquet-write")]
mod writer;

#[cfg(feature = "parquet-read")]
pub use access_plan::{ParquetAccessPlan, ParquetRowSelection, RowGroupAccess};
#[cfg(feature = "parquet-read")]
pub use bloom_filter::BloomFilterStatistics;
#[cfg(feature = "object-store-reader")]
pub use file_format::*;
#[cfg(feature = "parquet-read")]
pub use metrics::ParquetFileMetrics;
#[cfg(feature = "parquet-read")]
pub use page_filter::PagePruningAccessPlanFilter;
#[cfg(feature = "parquet-read")]
pub use reader::*; // Expose so downstream crates can use it
#[cfg(feature = "parquet-read")]
pub use row_filter::build_row_filter;
#[cfg(feature = "parquet-read")]
pub use row_filter::can_expr_be_pushed_down_with_schemas;
#[cfg(feature = "parquet-read")]
pub use row_group_filter::RowGroupAccessPlanFilter;
#[cfg(feature = "parquet-read")]
#[expect(deprecated)]
pub use schema_coercion::coerce_int96_to_resolution;
#[cfg(feature = "parquet-read")]
pub use schema_coercion::{
    Int96Coercer, apply_file_schema_type_coercions, transform_binary_to_string,
    transform_schema_to_view,
};
#[cfg(feature = "parquet-write")]
pub use sink::ParquetSink;
#[cfg(feature = "parquet-read")]
pub use virtual_column::ParquetVirtualColumn;
#[cfg(feature = "parquet-write")]
pub use writer::plan_to_parquet;
