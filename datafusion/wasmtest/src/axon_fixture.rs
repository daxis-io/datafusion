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

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

const CORPUS_JSON: &str = include_str!("../fixtures/axon-engine-query-uat-corpus.json");
const ROWS_JSON: &str = include_str!("../fixtures/fixture-rows.json");
const PROVENANCE_JSON: &str = include_str!("../fixtures/provenance.json");

const AXON_COMMIT: &str = "b88fbd44b329584b19682bcac5dc0a1a909a3e1e";
const CORPUS_PATH: &str = "tests/conformance/axon-engine-query-uat-corpus.json";
const CORPUS_BLOB: &str = "0bc869e5e22e0c5a3f8e86f9ad5f51b77f1909f9";
const CORPUS_SHA256: &str =
    "4f121b173f2e8dbddc34573bf048cf293d034b92aa15142d4e3b89363991d7de";
const FIXTURE_SOURCE_PATH: &str = "crates/wasm-datafusion-poc/tests/support/uat.rs";
const FIXTURE_SOURCE_BLOB: &str = "eb3e5a5addcefc77c17615053086167cd8d37957";
const FIXTURE_SOURCE_SHA256: &str =
    "cf0e62bcb57ed5dac2fb64c6ffcb1ce17b921daa64308cd874013b79f9b65700";
const NORMALIZED_FIXTURE_ROWS_SHA256: &str =
    "7a392b6b45b22f553a5992be46fd8297f54e2570a90eb5f20a2b4c97774a3cd4";
const SOURCE_DATAFUSION_VERSION: &str = "53.1.0";
const QUALIFICATION_ROLE: &str = "input provenance only; not Phase B evidence";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Provenance {
    axon_commit: String,
    corpus: SourceProvenance,
    fixture_source: SourceProvenance,
    normalized_fixture_rows_sha256: String,
    source_datafusion_version: String,
    qualification_role: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceProvenance {
    path: String,
    git_blob: String,
    sha256: String,
}

pub const REQUIRED_SQL_CLASSES: [&str; 26] = [
    "projection",
    "filtering",
    "boolean_logic",
    "null_semantics",
    "arithmetic",
    "cast",
    "case_expression",
    "string_function",
    "aggregate_count",
    "aggregate_sum",
    "aggregate_min_max",
    "aggregate_avg",
    "grouped_aggregate",
    "having",
    "distinct",
    "ordering",
    "limit",
    "offset",
    "cte",
    "derived_table",
    "subquery",
    "inner_join",
    "left_join",
    "union_all",
    "window_function",
    "descriptor_backed_scan",
];

#[derive(Debug)]
pub struct CorpusCase {
    pub name: String,
    pub sql: String,
    pub covered_sql_classes: BTreeSet<String>,
    pub expected_columns: Vec<String>,
    pub expected_rows: Vec<Vec<Scalar>>,
}

#[derive(Debug, PartialEq)]
pub enum Scalar {
    Int(i64),
    Float(String),
    Utf8(String),
    Bool(bool),
    Null,
}

pub fn load_corpus() -> Result<Vec<CorpusCase>> {
    let corpus: Value = serde_json::from_str(CORPUS_JSON).map_err(external)?;
    let cases = corpus
        .as_array()
        .ok_or_else(|| DataFusionError::Plan("Axon corpus must be an array".into()))?
        .iter()
        .map(parse_case)
        .collect::<Result<Vec<_>>>()?;
    if cases.len() != 18 {
        return Err(DataFusionError::Plan(format!(
            "Axon corpus contains {} cases, expected exactly 18",
            cases.len()
        )));
    }
    let covered = cases
        .iter()
        .flat_map(|case| case.covered_sql_classes.iter().cloned())
        .collect::<BTreeSet<_>>();
    for required in REQUIRED_SQL_CLASSES {
        if !covered.contains(required) {
            return Err(DataFusionError::Plan(format!(
                "Axon corpus does not cover required SQL class {required}"
            )));
        }
    }
    Ok(cases)
}

pub fn fixture_batches() -> Result<(RecordBatch, RecordBatch, RecordBatch)> {
    // Parsing both committed inputs ensures the browser suite cannot silently
    // stop consuming either the normalized rows or their provenance record.
    let rows: Value = serde_json::from_str(ROWS_JSON).map_err(external)?;
    validate_provenance(
        PROVENANCE_JSON,
        CORPUS_JSON.as_bytes(),
        ROWS_JSON.as_bytes(),
    )?;

    let orders = table_rows(&rows, "orders")?;
    let customers = table_rows(&rows, "customers")?;
    let shipments = table_rows(&rows, "shipments")?;

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer_id", DataType::Int64, false),
        Field::new("order_date", DataType::Utf8, false),
        Field::new("customer_tier", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("amount_cents", DataType::Int64, false),
        Field::new("discount_cents", DataType::Int64, true),
        Field::new("is_priority", DataType::Boolean, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let orders = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(int_column(orders, 0)?)),
            Arc::new(Int64Array::from(int_column(orders, 1)?)),
            Arc::new(StringArray::from(string_column(orders, 2)?)),
            Arc::new(StringArray::from(string_column(orders, 3)?)),
            Arc::new(StringArray::from(string_column(orders, 4)?)),
            Arc::new(Int64Array::from(int_column(orders, 5)?)),
            Arc::new(Int64Array::from(optional_int_column(orders, 6)?)),
            Arc::new(BooleanArray::from(optional_bool_column(orders, 7)?)),
            Arc::new(StringArray::from(optional_string_column(orders, 8)?)),
        ],
    )?;

    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("segment", DataType::Utf8, false),
        Field::new("target_cents", DataType::Int64, false),
    ]));
    let customers = RecordBatch::try_new(
        customers_schema,
        vec![
            Arc::new(Int64Array::from(int_column(customers, 0)?)),
            Arc::new(StringArray::from(string_column(customers, 1)?)),
            Arc::new(StringArray::from(string_column(customers, 2)?)),
            Arc::new(Int64Array::from(int_column(customers, 3)?)),
        ],
    )?;

    let shipments_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("shipped_at", DataType::Utf8, true),
        Field::new("carrier", DataType::Utf8, true),
    ]));
    let shipments = RecordBatch::try_new(
        shipments_schema,
        vec![
            Arc::new(Int64Array::from(int_column(shipments, 0)?)),
            Arc::new(StringArray::from(optional_string_column(shipments, 1)?)),
            Arc::new(StringArray::from(optional_string_column(shipments, 2)?)),
        ],
    )?;

    Ok((orders, customers, shipments))
}

fn parse_case(value: &Value) -> Result<CorpusCase> {
    Ok(CorpusCase {
        name: string_field(value, "name")?,
        sql: string_field(value, "sql")?,
        covered_sql_classes: string_array_field(value, "covered_sql_classes")?
            .into_iter()
            .collect(),
        expected_columns: string_array_field(value, "expected_columns")?,
        expected_rows: value
            .get("expected_rows")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                DataFusionError::Plan("expected_rows must be an array".into())
            })?
            .iter()
            .map(|row| {
                row.as_array()
                    .ok_or_else(|| {
                        DataFusionError::Plan("expected row must be an array".into())
                    })?
                    .iter()
                    .map(parse_scalar)
                    .collect()
            })
            .collect::<Result<Vec<_>>>()?,
    })
}

fn string_field(value: &Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| DataFusionError::Plan(format!("{field} must be a string")))
}

fn string_array_field(value: &Value, field: &str) -> Result<Vec<String>> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| DataFusionError::Plan(format!("{field} must be an array")))?
        .iter()
        .map(|item| {
            item.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                DataFusionError::Plan(format!("{field} must contain strings"))
            })
        })
        .collect()
}

fn parse_scalar(value: &Value) -> Result<Scalar> {
    if value.is_null() {
        Ok(Scalar::Null)
    } else if let Some(value) = value.as_i64() {
        Ok(Scalar::Int(value))
    } else if let Some(value) = value.as_str() {
        Ok(Scalar::Utf8(value.to_owned()))
    } else if let Some(value) = value.as_bool() {
        Ok(Scalar::Bool(value))
    } else if let Some(value) = value.as_f64() {
        Ok(Scalar::Float(format_float(value)))
    } else {
        Err(DataFusionError::Plan(format!(
            "unsupported expected scalar {value}"
        )))
    }
}

pub fn normalize_batches(batches: &[RecordBatch]) -> Result<Vec<Vec<Scalar>>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| normalize_scalar(column.as_ref(), row))
                    .collect()
            })
        })
        .collect()
}

fn normalize_scalar(column: &dyn Array, row: usize) -> Result<Scalar> {
    if column.is_null(row) {
        return Ok(Scalar::Null);
    }
    match column.data_type() {
        DataType::Int64 => Ok(Scalar::Int(
            column
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        DataType::Int32 => Ok(Scalar::Int(i64::from(
            column
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        ))),
        DataType::UInt64 => Ok(Scalar::Int(
            column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row)
                .try_into()
                .map_err(external)?,
        )),
        DataType::Float64 => Ok(Scalar::Float(format_float(
            column
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        ))),
        DataType::Utf8 => Ok(Scalar::Utf8(
            column
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        )),
        DataType::Boolean => Ok(Scalar::Bool(
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        )),
        other => Err(DataFusionError::Plan(format!(
            "unsupported actual Arrow type {other:?}"
        ))),
    }
}

fn table_rows<'a>(root: &'a Value, table: &str) -> Result<&'a [Value]> {
    root.get(table)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| {
            DataFusionError::Plan(format!("fixture table {table} must be an array"))
        })
}

fn cell(rows: &[Value], row: usize, column: usize) -> Result<&Value> {
    rows.get(row)
        .and_then(Value::as_array)
        .and_then(|row| row.get(column))
        .ok_or_else(|| {
            DataFusionError::Plan(format!("missing fixture cell {row}:{column}"))
        })
}

fn int_column(rows: &[Value], column: usize) -> Result<Vec<i64>> {
    (0..rows.len())
        .map(|row| {
            cell(rows, row, column)?.as_i64().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "fixture cell {row}:{column} must be an integer"
                ))
            })
        })
        .collect()
}

fn optional_int_column(rows: &[Value], column: usize) -> Result<Vec<Option<i64>>> {
    (0..rows.len())
        .map(|row| {
            let value = cell(rows, row, column)?;
            if value.is_null() {
                Ok(None)
            } else {
                value.as_i64().map(Some).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "fixture cell {row}:{column} must be an integer or null"
                    ))
                })
            }
        })
        .collect()
}

fn string_column(rows: &[Value], column: usize) -> Result<Vec<&str>> {
    (0..rows.len())
        .map(|row| {
            cell(rows, row, column)?.as_str().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "fixture cell {row}:{column} must be a string"
                ))
            })
        })
        .collect()
}

fn optional_string_column(rows: &[Value], column: usize) -> Result<Vec<Option<&str>>> {
    (0..rows.len())
        .map(|row| {
            let value = cell(rows, row, column)?;
            if value.is_null() {
                Ok(None)
            } else {
                value.as_str().map(Some).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "fixture cell {row}:{column} must be a string or null"
                    ))
                })
            }
        })
        .collect()
}

fn optional_bool_column(rows: &[Value], column: usize) -> Result<Vec<Option<bool>>> {
    (0..rows.len())
        .map(|row| {
            let value = cell(rows, row, column)?;
            if value.is_null() {
                Ok(None)
            } else {
                value.as_bool().map(Some).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "fixture cell {row}:{column} must be a boolean or null"
                    ))
                })
            }
        })
        .collect()
}

fn format_float(value: f64) -> String {
    format!("{value:.6}")
}

fn validate_provenance(
    provenance_json: &str,
    corpus_bytes: &[u8],
    rows_bytes: &[u8],
) -> Result<()> {
    let provenance: Provenance =
        serde_json::from_str(provenance_json).map_err(external)?;
    let corpus_sha256 = sha256_hex(corpus_bytes);
    let rows_sha256 = sha256_hex(rows_bytes);
    let valid = provenance.axon_commit == AXON_COMMIT
        && provenance.corpus.path == CORPUS_PATH
        && provenance.corpus.git_blob == CORPUS_BLOB
        && provenance.corpus.sha256 == CORPUS_SHA256
        && corpus_sha256 == CORPUS_SHA256
        && provenance.fixture_source.path == FIXTURE_SOURCE_PATH
        && provenance.fixture_source.git_blob == FIXTURE_SOURCE_BLOB
        && provenance.fixture_source.sha256 == FIXTURE_SOURCE_SHA256
        && provenance.normalized_fixture_rows_sha256 == NORMALIZED_FIXTURE_ROWS_SHA256
        && rows_sha256 == NORMALIZED_FIXTURE_ROWS_SHA256
        && provenance.source_datafusion_version == SOURCE_DATAFUSION_VERSION
        && provenance.qualification_role == QUALIFICATION_ROLE;
    if !valid {
        return Err(DataFusionError::Plan(
            "Axon fixture provenance does not match the frozen sources and bytes".into(),
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

fn external(error: impl std::error::Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_fixture_columns_reject_wrong_non_null_types() {
        let integer_rows = vec![serde_json::json!(["not-an-integer"])];
        let integer_error = optional_int_column(&integer_rows, 0)
            .expect_err("a non-null string must not become a null integer");
        assert!(matches!(integer_error, DataFusionError::Plan(_)));

        let string_rows = vec![serde_json::json!([42])];
        let string_error = optional_string_column(&string_rows, 0)
            .expect_err("a non-null integer must not become a null string");
        assert!(matches!(string_error, DataFusionError::Plan(_)));

        let boolean_rows = vec![serde_json::json!(["true"])];
        let boolean_error = optional_bool_column(&boolean_rows, 0)
            .expect_err("a non-null string must not become a null boolean");
        assert!(matches!(boolean_error, DataFusionError::Plan(_)));
    }

    #[test]
    fn provenance_rejects_drifted_corpus_and_normalized_rows() {
        let corpus_error =
            validate_provenance(PROVENANCE_JSON, b"drifted corpus", ROWS_JSON.as_bytes())
                .expect_err("corpus drift must invalidate provenance");
        assert!(matches!(corpus_error, DataFusionError::Plan(_)));

        let mut self_consistent_provenance: Value =
            serde_json::from_str(PROVENANCE_JSON).expect("valid fixture provenance");
        self_consistent_provenance["corpus"]["sha256"] =
            Value::String(sha256_hex(b"drifted corpus"));
        let self_consistent_provenance =
            serde_json::to_string(&self_consistent_provenance).unwrap();
        let corpus_error = validate_provenance(
            &self_consistent_provenance,
            b"drifted corpus",
            ROWS_JSON.as_bytes(),
        )
        .expect_err("a rewritten provenance file must not authorize corpus drift");
        assert!(matches!(corpus_error, DataFusionError::Plan(_)));

        let rows_error =
            validate_provenance(PROVENANCE_JSON, CORPUS_JSON.as_bytes(), b"drifted rows")
                .expect_err("normalized-row drift must invalidate provenance");
        assert!(matches!(rows_error, DataFusionError::Plan(_)));
    }

    #[test]
    fn provenance_rejects_source_identity_drift() {
        let mut provenance: Value =
            serde_json::from_str(PROVENANCE_JSON).expect("valid fixture provenance");
        provenance["fixture_source"]["git_blob"] = Value::String("0".repeat(40));
        let provenance = serde_json::to_string(&provenance).unwrap();
        let error = validate_provenance(
            &provenance,
            CORPUS_JSON.as_bytes(),
            ROWS_JSON.as_bytes(),
        )
        .expect_err("source blob drift must invalidate provenance");
        assert!(matches!(error, DataFusionError::Plan(_)));
    }
}
