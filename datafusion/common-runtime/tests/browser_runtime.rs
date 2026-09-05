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

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::cell::Cell;
use std::future::pending;
use std::rc::Rc;
use std::sync::Arc;

use datafusion_common_runtime::channel::{mpsc, oneshot};
use datafusion_common_runtime::sync::{Mutex, Notify, RwLock, Semaphore, watch};
use datafusion_common_runtime::{JoinSet, SpawnedTask, yield_now};
use wasm_bindgen_test::wasm_bindgen_test;

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn spawned_task_returns_success_and_inner_error() {
    let task = SpawnedTask::spawn(async { 42 });
    assert_eq!(task.await.expect("task should complete"), 42);

    let task = SpawnedTask::spawn(async { Result::<(), &'static str>::Err("inner") });
    assert_eq!(task.await.expect("task should complete"), Err("inner"));
}

#[wasm_bindgen_test]
async fn spawned_task_aborts_before_poll_and_when_dropped_in_flight() {
    let polled = Rc::new(Cell::new(false));
    let polled_in_task = Rc::clone(&polled);
    let task = SpawnedTask::spawn_local(async move {
        polled_in_task.set(true);
    });
    drop(task);
    yield_now().await;
    assert!(
        !polled.get(),
        "drop must abort a task before its first poll"
    );

    struct Dropped(Rc<Cell<bool>>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.set(true);
        }
    }

    let dropped = Rc::new(Cell::new(false));
    let dropped_in_task = Rc::clone(&dropped);
    let (started_tx, started_rx) = oneshot::channel();
    let task = SpawnedTask::spawn_local(async move {
        let _guard = Dropped(dropped_in_task);
        started_tx.send(()).expect("receiver remains alive");
        pending::<()>().await;
    });
    started_rx.await.expect("task should start");
    drop(task);
    yield_now().await;
    assert!(dropped.get(), "aborting an in-flight task must drop it");

    let (started_tx, started_rx) = oneshot::channel();
    let task = SpawnedTask::spawn_local(async move {
        started_tx.send(()).expect("receiver remains alive");
        pending::<()>().await;
    });
    started_rx.await.expect("task should start");
    task.abort();
    let error = task.await.expect_err("explicit abort must cancel the task");
    assert!(error.is_cancelled());
}

#[wasm_bindgen_test]
async fn join_set_reports_completion_order_and_aborts_remaining_tasks() {
    let (release_tx, release_rx) = oneshot::channel();
    let mut set = JoinSet::new();
    set.spawn_local(async move {
        release_rx.await.expect("release sender remains alive");
        1
    });
    set.spawn_local(async { 2 });

    assert_eq!(set.join_next().await.unwrap().unwrap(), 2);
    release_tx.send(()).expect("release receiver remains alive");
    assert_eq!(set.join_next().await.unwrap().unwrap(), 1);
    assert!(set.is_empty());

    let dropped = Rc::new(Cell::new(false));
    let dropped_in_task = Rc::clone(&dropped);
    let mut set = JoinSet::new();
    set.spawn_local(async move {
        struct MarkDropped(Rc<Cell<bool>>);
        impl Drop for MarkDropped {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let _guard = MarkDropped(dropped_in_task);
        pending::<()>().await;
    });
    yield_now().await;
    set.abort_all();
    while set.join_next().await.is_some() {}
    assert!(dropped.get());
}

#[wasm_bindgen_test]
async fn join_set_abort_handle_reports_completion_not_abort_request() {
    let mut set = JoinSet::new();
    let completed = set.spawn_local(async { 7_u8 });
    assert!(
        !completed.is_finished(),
        "a newly spawned task must not be finished"
    );
    assert_eq!(set.join_next().await.unwrap().unwrap(), 7);
    assert!(
        completed.is_finished(),
        "normal task completion must update every retained abort handle"
    );

    let mut set = JoinSet::<()>::new();
    let aborted = set.spawn_local(pending());
    yield_now().await;
    aborted.abort();
    assert!(
        !aborted.is_finished(),
        "requesting cancellation is not task completion"
    );
    let error = set
        .join_next()
        .await
        .expect("the cancelled task must publish a completion")
        .expect_err("the task was cancelled");
    assert!(error.is_cancelled());
    assert!(
        aborted.is_finished(),
        "observed cancellation completion must update every retained abort handle"
    );
}

#[wasm_bindgen_test]
async fn bounded_channel_enforces_one_global_capacity_across_clones() {
    let (sender, mut receiver) = mpsc::channel(2);
    let sender_clone = sender.clone();

    sender.try_send(1).expect("first credit");
    sender_clone.try_send(2).expect("second credit");
    assert!(sender.try_send(3).is_err(), "capacity must be global");

    assert_eq!(receiver.recv().await, Some(1));
    sender_clone
        .try_send(3)
        .expect("receiving a message must return its credit");
    assert_eq!(receiver.recv().await, Some(2));
    assert_eq!(receiver.recv().await, Some(3));

    drop(sender);
    drop(sender_clone);
    assert_eq!(receiver.recv().await, None);
}

#[wasm_bindgen_test]
async fn bounded_channel_preserves_waiter_order_and_reports_receiver_closure() {
    let (sender, mut receiver) = mpsc::channel(1);
    sender.try_send(0).expect("initial credit");

    let first_sender = sender.clone();
    let first = SpawnedTask::spawn_local(async move {
        first_sender.send(1).await.expect("receiver remains alive");
    });
    yield_now().await;
    let second_sender = sender.clone();
    let second = SpawnedTask::spawn_local(async move {
        second_sender.send(2).await.expect("receiver remains alive");
    });
    yield_now().await;

    assert_eq!(receiver.recv().await, Some(0));
    first.await.unwrap();
    assert_eq!(receiver.recv().await, Some(1));
    second.await.unwrap();
    assert_eq!(receiver.recv().await, Some(2));

    drop(receiver);
    sender.closed().await;
    let error = sender.send(3).await.expect_err("receiver was dropped");
    assert_eq!(error.into_inner(), 3);
}

#[wasm_bindgen_test]
async fn bounded_channel_close_wakes_blocked_senders_before_buffer_drain() {
    let (sender, mut receiver) = mpsc::channel(1);
    sender.try_send(0).expect("initial credit");

    let first_sender = sender.clone();
    let first = SpawnedTask::spawn_local(async move { first_sender.send(1).await });
    let second_sender = sender.clone();
    let second = SpawnedTask::spawn_local(async move { second_sender.send(2).await });
    yield_now().await;

    receiver.close();

    let first_error = first
        .await
        .expect("blocked sender task must complete when the receiver closes")
        .expect_err("closed receiver must reject the first blocked send");
    let second_error = second
        .await
        .expect("blocked sender task must complete when the receiver closes")
        .expect_err("closed receiver must reject the second blocked send");
    assert_eq!(first_error.into_inner(), 1);
    assert_eq!(second_error.into_inner(), 2);

    assert_eq!(
        receiver.recv().await,
        Some(0),
        "closure must wake blocked sends without consuming the buffered value"
    );
    assert_eq!(receiver.recv().await, None);
}

#[wasm_bindgen_test]
async fn receivers_report_close_before_eof_and_drain_buffered_values() {
    let (sender, mut receiver) = mpsc::channel(2);
    sender.try_send(1).expect("receiver is open");
    let sender_clone = sender.clone();
    drop(sender);
    assert!(
        !receiver.is_closed(),
        "one remaining sender keeps the receiver open"
    );
    drop(sender_clone);
    assert!(
        receiver.is_closed(),
        "dropping the last sender must close the receiver before an EOF poll"
    );
    assert_eq!(receiver.recv().await, Some(1));
    assert_eq!(receiver.recv().await, None);

    let (sender, mut receiver) = mpsc::channel(2);
    sender.try_send(2).expect("receiver is open");
    receiver.close();
    assert!(
        receiver.is_closed(),
        "Receiver::close must be visible before an EOF poll"
    );
    assert!(sender.is_closed());
    assert_eq!(receiver.recv().await, Some(2));
    assert_eq!(receiver.recv().await, None);

    let (sender, mut receiver) = mpsc::unbounded_channel();
    sender.send(3).expect("receiver is open");
    let sender_clone = sender.clone();
    drop(sender);
    assert!(!receiver.is_closed());
    drop(sender_clone);
    assert!(
        receiver.is_closed(),
        "dropping the last unbounded sender must close before an EOF poll"
    );
    assert_eq!(receiver.recv().await, Some(3));
    assert_eq!(receiver.recv().await, None);

    let (sender, mut receiver) = mpsc::unbounded_channel();
    sender.send(4).expect("receiver is open");
    receiver.close();
    assert!(receiver.is_closed());
    assert!(sender.is_closed());
    assert_eq!(receiver.recv().await, Some(4));
    assert_eq!(receiver.recv().await, None);
}

#[wasm_bindgen_test]
async fn browser_lock_and_semaphore_facades_release_on_drop() {
    let mutex = Arc::new(Mutex::new(1));
    *mutex.lock().await += 1;
    assert_eq!(*mutex.lock().await, 2);

    let lock = RwLock::new(4);
    assert_eq!(*lock.read().await, 4);
    *lock.write().await = 5;
    assert_eq!(*lock.read().await, 5);

    let semaphore = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
    assert!(Arc::clone(&semaphore).try_acquire_owned().is_err());
    drop(permit);
    assert!(Arc::clone(&semaphore).try_acquire_owned().is_ok());
}

#[wasm_bindgen_test]
async fn lock_contention_and_semaphore_cancellation_release_waiters_and_credits() {
    let mutex = Arc::new(Mutex::new(false));
    let guard = mutex.lock().await;
    let task_mutex = Arc::clone(&mutex);
    let acquired = Rc::new(Cell::new(false));
    let acquired_in_task = Rc::clone(&acquired);
    let waiter = SpawnedTask::spawn_local(async move {
        *task_mutex.lock().await = true;
        acquired_in_task.set(true);
    });
    yield_now().await;
    assert!(!acquired.get(), "mutex waiter must remain blocked");
    drop(guard);
    waiter.await.unwrap();
    assert!(acquired.get());

    let semaphore = Arc::new(Semaphore::new(1));
    let held = Arc::clone(&semaphore).acquire_owned().await.unwrap();
    let waiting_semaphore = Arc::clone(&semaphore);
    let waiter = SpawnedTask::spawn_local(async move {
        waiting_semaphore.acquire_owned().await.unwrap()
    });
    yield_now().await;
    waiter.abort();
    assert!(
        waiter
            .await
            .expect_err("waiter was cancelled")
            .is_cancelled()
    );
    drop(held);
    assert!(Arc::clone(&semaphore).try_acquire_owned().is_ok());
}

#[wasm_bindgen_test]
async fn notify_retains_one_permit_and_watch_serves_late_subscribers() {
    let notify = Notify::new();
    let waiting = notify.notified();
    notify.notify_one();
    waiting.await;

    let (sender, mut first) = watch::channel(1_u8);
    sender.send(2).expect("receiver remains alive");
    let mut late = sender.subscribe();
    assert_eq!(*late.borrow_and_update(), 2);

    sender.send(3).expect("receivers remain alive");
    first.changed().await.expect("sender remains alive");
    late.changed().await.expect("sender remains alive");
    assert_eq!(*first.borrow_and_update(), 3);
    assert_eq!(*late.borrow_and_update(), 3);
    drop(sender);
    assert!(first.changed().await.is_err());
}
