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
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use futures::future::{AbortHandle, Abortable};

/// An error produced while joining a browser-local task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrowserJoinError {
    cancelled: bool,
}

impl BrowserJoinError {
    pub(crate) fn cancelled() -> Self {
        Self { cancelled: true }
    }

    /// Returns true when the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Browser-local task panics trap through WebAssembly rather than becoming join errors.
    pub fn is_panic(&self) -> bool {
        false
    }

    /// Browser-local task panics cannot be recovered as Rust panic payloads.
    pub fn into_panic(self) -> Box<dyn std::any::Any + Send + 'static> {
        panic!("browser-local task cancellation is not a panic")
    }
}

impl Display for BrowserJoinError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("browser-local task was cancelled")
    }
}

impl std::error::Error for BrowserJoinError {}

/// A browser-local spawned task that is cancelled when dropped.
pub struct SpawnedTask<R> {
    receiver: oneshot::Receiver<Result<R, BrowserJoinError>>,
    abort: AbortHandle,
}

impl<R> std::fmt::Debug for SpawnedTask<R> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SpawnedTask")
            .field("aborted", &self.abort.is_aborted())
            .finish_non_exhaustive()
    }
}

impl<R: 'static> SpawnedTask<R> {
    /// Spawn a future on the browser's local event loop.
    pub fn spawn<F>(task: F) -> Self
    where
        F: Future<Output = R> + 'static,
    {
        Self::spawn_inner(task)
    }

    /// Spawn a future that may hold browser-local, non-`Send` values.
    pub fn spawn_local<F>(task: F) -> Self
    where
        F: Future<Output = R> + 'static,
    {
        Self::spawn_inner(task)
    }

    fn spawn_inner<F>(task: F) -> Self
    where
        F: Future<Output = R> + 'static,
    {
        let (abort, registration) = AbortHandle::new_pair();
        let (sender, receiver) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = Abortable::new(task, registration)
                .await
                .map_err(|_| BrowserJoinError::cancelled());
            let _ = sender.send(result);
        });
        Self { receiver, abort }
    }

    /// Join the task and return its result or cancellation error.
    pub async fn join(self) -> Result<R, BrowserJoinError> {
        self.await
    }

    /// Join the task. Panics propagate as WebAssembly traps in the browser profile.
    pub async fn join_unwind(self) -> Result<R, BrowserJoinError> {
        self.await
    }

    /// Join the task through a mutable reference.
    pub async fn join_unwind_mut(&mut self) -> Result<R, BrowserJoinError> {
        self.await
    }

    /// Request cancellation without dropping the handle.
    pub fn abort(&self) {
        self.abort.abort();
    }
}

impl<R> Future for SpawnedTask<R> {
    type Output = Result<R, BrowserJoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(BrowserJoinError::cancelled())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> Drop for SpawnedTask<R> {
    fn drop(&mut self) {
        self.abort.abort();
    }
}
