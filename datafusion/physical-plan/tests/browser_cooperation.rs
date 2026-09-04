// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::cell::Cell;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_common_runtime::SpawnedTask;
use datafusion_physical_plan::RecordBatchStream;
use datafusion_physical_plan::coop::cooperative;
use futures::Stream;
use futures::StreamExt;
use futures::task::noop_waker;
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

struct ReadyBatches {
    schema: SchemaRef,
    remaining: usize,
}

impl ReadyBatches {
    fn new(remaining: usize) -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
            remaining,
        }
    }
}

impl Stream for ReadyBatches {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        self.remaining -= 1;
        Poll::Ready(Some(Ok(RecordBatch::new_empty(Arc::clone(&self.schema)))))
    }
}

impl RecordBatchStream for ReadyBatches {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[wasm_bindgen_test]
async fn browser_stream_budget_is_exactly_128_ready_items() {
    let mut stream = cooperative(ReadyBatches::new(129));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);

    for _ in 0..128 {
        assert!(Pin::new(&mut stream).poll_next(&mut context).is_ready());
    }
    assert!(Pin::new(&mut stream).poll_next(&mut context).is_pending());

    datafusion_common_runtime::yield_now().await;
    assert!(Pin::new(&mut stream).poll_next(&mut context).is_ready());
}

#[wasm_bindgen_test]
async fn browser_stream_budget_allows_an_event_loop_heartbeat() {
    let heartbeat = Rc::new(Cell::new(false));
    let heartbeat_in_task = Rc::clone(&heartbeat);
    let heartbeat_task = SpawnedTask::spawn_local(async move {
        heartbeat_in_task.set(true);
    });

    let mut stream = cooperative(ReadyBatches::new(129));
    for _ in 0..129 {
        stream.next().await.unwrap().unwrap();
    }

    assert!(
        heartbeat.get(),
        "browser task queue must advance after 128 ready items"
    );
    heartbeat_task.await.unwrap();
}
