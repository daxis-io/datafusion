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

//! Browser qualification consumer for DataFusion's no-default browser profile.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod axon_fixture;
mod axon_provider;
mod qualification;

#[cfg(feature = "exact-stack-parquet")]
mod exact_stack_parquet;

use wasm_bindgen::prelude::*;

#[cfg(feature = "exact-stack-parquet")]
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(message: &str);
}

#[cfg(feature = "exact-stack-parquet")]
pub(crate) fn qualification_log(message: &str) {
    console_log(message);
}

fn install_panic_hook() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

/// Parse the committed fixture files without constructing DataFusion state.
#[wasm_bindgen]
pub fn qualify_fixture_parse() -> Result<String, JsValue> {
    qualification::qualify_fixture_parse().map_err(js_error)
}

/// Construct browser session state without registering a table.
#[wasm_bindgen]
pub fn qualify_context_construction() -> Result<String, JsValue> {
    qualification::qualify_context_construction().map_err(js_error)
}

/// Parse the committed fixtures and register the three Axon-shaped tables.
#[wasm_bindgen]
pub async fn qualify_fixture_registration() -> Result<String, JsValue> {
    install_panic_hook();
    qualification::qualify_fixture_registration()
        .await
        .map_err(js_error)
}

/// Parse SQL and produce its optimized logical plan.
#[wasm_bindgen]
pub async fn qualify_logical_planning() -> Result<String, JsValue> {
    install_panic_hook();
    qualification::qualify_logical_planning()
        .await
        .map_err(js_error)
}

/// Parse and physically plan an Axon-shaped query without executing it.
#[wasm_bindgen]
pub async fn qualify_planning() -> Result<String, JsValue> {
    install_panic_hook();
    qualification::qualify_planning().await.map_err(js_error)
}

/// Execute the complete Axon corpus and browser runtime contracts.
#[wasm_bindgen]
pub async fn qualify_execution() -> Result<String, JsValue> {
    install_panic_hook();
    qualification::qualify_execution().await.map_err(js_error)
}

/// Force more than one cooperative stream budget and report the item count.
#[wasm_bindgen]
pub async fn qualify_cooperation() -> Result<u32, JsValue> {
    install_panic_hook();
    qualification::qualify_cooperation().await.map_err(js_error)
}

/// Exercise the exact local Arrow/Parquet/object_store composition.
#[cfg(feature = "exact-stack-parquet")]
#[wasm_bindgen]
pub async fn qualify_exact_stack_parquet() -> Result<String, JsValue> {
    install_panic_hook();
    exact_stack_parquet::qualify().await.map_err(js_error)
}
