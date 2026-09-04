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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]

#[cfg(all(
    target_arch = "wasm32",
    target_os = "unknown",
    feature = "runtime-tokio"
))]
compile_error!(
    "runtime-tokio is not supported on wasm32-unknown-unknown; select runtime-browser"
);
#[cfg(all(
    target_arch = "wasm32",
    target_os = "unknown",
    not(feature = "runtime-browser")
))]
compile_error!(
    "a browser runtime is required on wasm32-unknown-unknown; enable runtime-browser"
);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser_join_set;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser_task;
pub mod channel;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub mod common;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod join_set;
pub mod sync;
mod trace_utils;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser_join_set::{AbortHandle, JoinSet};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser_task::{BrowserJoinError, SpawnedTask};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use common::SpawnedTask;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use join_set::JoinSet;
pub use trace_utils::{
    JoinSetTracer, JoinSetTracerError, set_join_set_tracer, trace_block, trace_future,
};

/// Yield execution to the selected runtime's task queue.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub async fn yield_now() {
    tokio::task::yield_now().await;
}

/// Yield execution to the browser event loop.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub async fn yield_now() {
    let (sender, receiver) = futures::channel::oneshot::channel();
    gloo_timers::callback::Timeout::new(0, move || {
        let _ = sender.send(());
    })
    .forget();
    let _ = receiver.await;
}

/// Wake a task after yielding through the browser task queue.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub fn wake_after_yield(waker: std::task::Waker) {
    wasm_bindgen_futures::spawn_local(async move {
        yield_now().await;
        waker.wake();
    });
}
