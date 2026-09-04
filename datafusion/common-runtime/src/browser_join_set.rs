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

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::future::{AbortHandle as FuturesAbortHandle, Abortable};
use futures::{Stream, StreamExt};

use crate::BrowserJoinError;
/// An abort handle for a browser-local task.
#[derive(Debug, Clone)]
pub struct AbortHandle {
    inner: FuturesAbortHandle,
}

impl AbortHandle {
    /// Request cancellation of the associated task.
    pub fn abort(&self) {
        self.inner.abort();
    }

    /// Returns true when cancellation has been requested.
    pub fn is_finished(&self) -> bool {
        self.inner.is_aborted()
    }
}

/// A completion-ordered set of browser-local tasks.
#[derive(Debug)]
pub struct JoinSet<T> {
    sender: mpsc::UnboundedSender<(u64, Result<T, BrowserJoinError>)>,
    receiver: mpsc::UnboundedReceiver<(u64, Result<T, BrowserJoinError>)>,
    aborts: HashMap<u64, FuturesAbortHandle>,
    next_id: u64,
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JoinSet<T> {
    /// Create an empty browser join set.
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded();
        Self {
            sender,
            receiver,
            aborts: HashMap::new(),
            next_id: 0,
        }
    }

    /// Return the number of tasks that have not yet been joined.
    pub fn len(&self) -> usize {
        self.aborts.len()
    }

    /// Return true when no tasks remain to be joined.
    pub fn is_empty(&self) -> bool {
        self.aborts.is_empty()
    }
}

impl<T: 'static> JoinSet<T> {
    /// Spawn a future on the browser's local event loop.
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + 'static,
    {
        self.spawn_inner(task)
    }

    /// Spawn a browser-local future.
    pub fn spawn_local<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + 'static,
    {
        self.spawn_inner(task)
    }

    fn spawn_inner<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + 'static,
    {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let (abort, registration) = FuturesAbortHandle::new_pair();
        self.aborts.insert(id, abort.clone());
        let sender = self.sender.clone();
        wasm_bindgen_futures::spawn_local(async move {
            let result = Abortable::new(task, registration)
                .await
                .map_err(|_| BrowserJoinError::cancelled());
            let _ = sender.unbounded_send((id, result));
        });
        AbortHandle { inner: abort }
    }

    /// Await the next task in completion order.
    pub async fn join_next(&mut self) -> Option<Result<T, BrowserJoinError>> {
        if self.aborts.is_empty() {
            return None;
        }
        let (id, result) = self.receiver.next().await?;
        self.aborts.remove(&id);
        Some(result)
    }

    /// Return the next completed task without waiting.
    pub fn try_join_next(&mut self) -> Option<Result<T, BrowserJoinError>> {
        if self.aborts.is_empty() {
            return None;
        }
        let (id, result) = self.receiver.try_recv().ok()?;
        self.aborts.remove(&id);
        Some(result)
    }

    /// Poll the next task in completion order.
    pub fn poll_join_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<T, BrowserJoinError>>> {
        if self.aborts.is_empty() {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Ready(Some((id, result))) => {
                self.aborts.remove(&id);
                Poll::Ready(Some(result))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// Cancel all tasks, retaining them until their cancellation completions are joined.
    pub fn abort_all(&mut self) {
        for abort in self.aborts.values() {
            abort.abort();
        }
    }

    /// Detach all tasks so dropping the set does not cancel them.
    pub fn detach_all(&mut self) {
        self.aborts.clear();
    }

    /// Cancel all tasks and drain their completion notifications.
    pub async fn shutdown(&mut self) {
        self.abort_all();
        while self.join_next().await.is_some() {}
    }

    /// Join all successful tasks. Cancellation is a programming error, matching Tokio's API.
    pub async fn join_all(mut self) -> Vec<T> {
        let mut output = Vec::with_capacity(self.len());
        while let Some(result) = self.join_next().await {
            output.push(result.expect("browser JoinSet task was cancelled"));
        }
        output
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        for abort in self.aborts.values() {
            abort.abort();
        }
    }
}
