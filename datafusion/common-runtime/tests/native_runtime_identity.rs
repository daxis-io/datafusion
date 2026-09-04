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

#![cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]

use datafusion_common_runtime::channel::{mpsc, oneshot};
use datafusion_common_runtime::sync::{Mutex, Notify, RwLock, Semaphore, watch};
use datafusion_common_runtime::{JoinSet, SpawnedTask};

#[test]
#[allow(
    unused_qualifications,
    reason = "qualified Tokio paths are the identity asserted by this compatibility test"
)]
fn native_channels_and_sync_types_are_tokio_types() {
    let (sender, receiver): (tokio::sync::mpsc::Sender<u8>, _) = mpsc::channel(1);
    let _: tokio::sync::mpsc::Receiver<u8> = receiver;
    let _: mpsc::Sender<u8> = sender;

    let (sender, receiver): (tokio::sync::oneshot::Sender<u8>, _) = oneshot::channel();
    let _: tokio::sync::oneshot::Receiver<u8> = receiver;
    let _: oneshot::Sender<u8> = sender;

    fn accepts_tokio_mutex(_: tokio::sync::Mutex<u8>) {}
    fn accepts_tokio_rwlock(_: tokio::sync::RwLock<u8>) {}
    fn accepts_tokio_semaphore(_: tokio::sync::Semaphore) {}
    fn accepts_tokio_notify(_: tokio::sync::Notify) {}

    accepts_tokio_mutex(Mutex::new(1));
    accepts_tokio_rwlock(RwLock::new(1));
    accepts_tokio_semaphore(Semaphore::new(1));
    accepts_tokio_notify(Notify::new());
    let (sender, receiver): (watch::Sender<u8>, watch::Receiver<u8>) = watch::channel(1);
    let _: tokio::sync::watch::Sender<u8> = sender;
    let _: tokio::sync::watch::Receiver<u8> = receiver;
}

#[tokio::test]
async fn native_task_and_join_set_keep_tokio_result_and_handle_identity() {
    let task = SpawnedTask::spawn(async { 1_u8 });
    let result: Result<u8, tokio::task::JoinError> = task.await;
    assert_eq!(result.unwrap(), 1);

    let mut set = JoinSet::new();
    let abort: tokio::task::AbortHandle = set.spawn(async { 2_u8 });
    assert!(!abort.is_finished());
    let result: Option<Result<u8, tokio::task::JoinError>> = set.join_next().await;
    assert_eq!(result.unwrap().unwrap(), 2);

    let runtime = tokio::runtime::Handle::current();
    let mut set = JoinSet::new();
    let _: tokio::task::AbortHandle = set.spawn_on(async { 3_u8 }, &runtime);
    assert_eq!(set.join_next().await.unwrap().unwrap(), 3);
}
