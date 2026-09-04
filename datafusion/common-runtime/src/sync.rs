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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, RwLock, Semaphore};

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub mod watch {
    pub use tokio::sync::watch::*;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser {
    use std::fmt::{Display, Formatter};
    use std::ops::Deref;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock as StdRwLock, RwLockReadGuard};

    use async_lock::SemaphoreGuardArc;
    use event_listener::Event;

    pub use async_lock::{Mutex, RwLock};

    /// Error returned when a semaphore has been closed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AcquireError;

    impl Display for AcquireError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("semaphore closed")
        }
    }

    impl std::error::Error for AcquireError {}

    /// Error returned by a nonblocking semaphore acquisition.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TryAcquireError;

    impl Display for TryAcquireError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("no semaphore permits available")
        }
    }

    impl std::error::Error for TryAcquireError {}

    /// Browser semaphore backed by `async-lock`.
    #[derive(Debug)]
    pub struct Semaphore {
        inner: Arc<async_lock::Semaphore>,
    }

    impl Semaphore {
        /// Create a semaphore with `permits` available permits.
        pub fn new(permits: usize) -> Self {
            Self {
                inner: Arc::new(async_lock::Semaphore::new(permits)),
            }
        }

        /// Acquire one owned permit.
        pub async fn acquire_owned(
            self: Arc<Self>,
        ) -> Result<OwnedSemaphorePermit, AcquireError> {
            self.acquire_many_owned(1).await
        }

        /// Acquire `permits` owned permits. Partial acquisition is cancellation-safe.
        pub async fn acquire_many_owned(
            self: Arc<Self>,
            permits: u32,
        ) -> Result<OwnedSemaphorePermit, AcquireError> {
            let mut guards = Vec::with_capacity(permits as usize);
            for _ in 0..permits {
                guards.push(Arc::clone(&self.inner).acquire_arc().await);
            }
            Ok(OwnedSemaphorePermit { guards })
        }

        /// Attempt to acquire one owned permit.
        pub fn try_acquire_owned(
            self: Arc<Self>,
        ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
            self.try_acquire_many_owned(1)
        }

        /// Attempt to acquire `permits` without waiting.
        pub fn try_acquire_many_owned(
            self: Arc<Self>,
            permits: u32,
        ) -> Result<OwnedSemaphorePermit, TryAcquireError> {
            let mut guards = Vec::with_capacity(permits as usize);
            for _ in 0..permits {
                let Some(guard) = Arc::clone(&self.inner).try_acquire_arc() else {
                    return Err(TryAcquireError);
                };
                guards.push(guard);
            }
            Ok(OwnedSemaphorePermit { guards })
        }

        /// Add permits to the semaphore.
        pub fn add_permits(&self, permits: usize) {
            self.inner.add_permits(permits);
        }
    }

    /// Owned browser semaphore credits returned when dropped.
    pub struct OwnedSemaphorePermit {
        guards: Vec<SemaphoreGuardArc>,
    }

    impl std::fmt::Debug for OwnedSemaphorePermit {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("OwnedSemaphorePermit")
                .field("permits", &self.guards.len())
                .finish()
        }
    }

    /// Browser notification primitive with Tokio-compatible one-permit behavior.
    #[derive(Debug)]
    pub struct Notify {
        permit: AtomicBool,
        event: Event,
    }

    impl Notify {
        /// Create an empty notification primitive.
        pub const fn new() -> Self {
            Self {
                permit: AtomicBool::new(false),
                event: Event::new(),
            }
        }

        /// Wake one listener, retaining one permit if no listener is registered.
        pub fn notify_one(&self) {
            if self.event.notify(1) == 0 {
                self.permit.store(true, Ordering::Release);
                self.event.notify(1);
            }
        }

        /// Wake every listener registered at the time of this call.
        pub fn notify_waiters(&self) {
            self.event.notify(usize::MAX);
        }

        /// Wait for a stored permit or a future notification.
        pub async fn notified(&self) {
            if self.permit.swap(false, Ordering::AcqRel) {
                return;
            }
            let listener = self.event.listen();
            if self.permit.swap(false, Ordering::AcqRel) {
                return;
            }
            listener.await;
        }
    }

    impl Default for Notify {
        fn default() -> Self {
            Self::new()
        }
    }

    pub mod watch {
        use super::*;

        #[derive(Debug)]
        struct State<T> {
            value: StdRwLock<T>,
            version: AtomicU64,
            senders: AtomicUsize,
            receivers: AtomicUsize,
            changed: Event,
        }

        /// Error returned when a watch channel has no receivers.
        #[derive(Debug, PartialEq, Eq)]
        pub struct SendError<T>(pub T);

        impl<T> Display for SendError<T> {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("watch channel has no receivers")
            }
        }

        impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

        /// Error returned after every watch sender has closed.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct RecvError;

        impl Display for RecvError {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("watch channel closed")
            }
        }

        impl std::error::Error for RecvError {}

        /// Borrowed watch value.
        pub struct Ref<'a, T> {
            guard: RwLockReadGuard<'a, T>,
        }

        impl<T> Deref for Ref<'_, T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.guard
            }
        }

        impl<T: std::fmt::Debug> std::fmt::Debug for Ref<'_, T> {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                self.guard.fmt(formatter)
            }
        }

        /// Sending side of a browser watch channel.
        #[derive(Debug)]
        pub struct Sender<T> {
            state: Arc<State<T>>,
        }

        impl<T> Clone for Sender<T> {
            fn clone(&self) -> Self {
                self.state.senders.fetch_add(1, Ordering::AcqRel);
                Self {
                    state: Arc::clone(&self.state),
                }
            }
        }

        impl<T> Sender<T> {
            /// Publish a new value to all current and future receivers.
            pub fn send(&self, value: T) -> Result<(), SendError<T>> {
                if self.state.receivers.load(Ordering::Acquire) == 0 {
                    return Err(SendError(value));
                }
                *self.state.value.write().expect("watch value lock poisoned") = value;
                self.state.version.fetch_add(1, Ordering::AcqRel);
                self.state.changed.notify(usize::MAX);
                Ok(())
            }

            /// Subscribe at the latest published version.
            pub fn subscribe(&self) -> Receiver<T> {
                self.state.receivers.fetch_add(1, Ordering::AcqRel);
                Receiver {
                    seen: self.state.version.load(Ordering::Acquire),
                    state: Arc::clone(&self.state),
                }
            }
        }

        impl<T> Drop for Sender<T> {
            fn drop(&mut self) {
                if self.state.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
                    self.state.changed.notify(usize::MAX);
                }
            }
        }

        /// Receiving side of a browser watch channel.
        #[derive(Debug)]
        pub struct Receiver<T> {
            state: Arc<State<T>>,
            seen: u64,
        }

        impl<T> Clone for Receiver<T> {
            fn clone(&self) -> Self {
                self.state.receivers.fetch_add(1, Ordering::AcqRel);
                Self {
                    state: Arc::clone(&self.state),
                    seen: self.seen,
                }
            }
        }

        impl<T> Receiver<T> {
            /// Borrow the latest value and mark its version as observed.
            pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
                let guard = self.state.value.read().expect("watch value lock poisoned");
                self.seen = self.state.version.load(Ordering::Acquire);
                Ref { guard }
            }

            /// Borrow the latest value without changing the observed version.
            pub fn borrow(&self) -> Ref<'_, T> {
                Ref {
                    guard: self.state.value.read().expect("watch value lock poisoned"),
                }
            }

            /// Return whether an unobserved value is available.
            pub fn has_changed(&self) -> Result<bool, RecvError> {
                let changed = self.state.version.load(Ordering::Acquire) != self.seen;
                if changed {
                    Ok(true)
                } else if self.state.senders.load(Ordering::Acquire) == 0 {
                    Err(RecvError)
                } else {
                    Ok(false)
                }
            }

            /// Wait for a new value, marking its version as observed.
            pub async fn changed(&mut self) -> Result<(), RecvError> {
                loop {
                    let version = self.state.version.load(Ordering::Acquire);
                    if version != self.seen {
                        self.seen = version;
                        return Ok(());
                    }
                    if self.state.senders.load(Ordering::Acquire) == 0 {
                        return Err(RecvError);
                    }
                    let listener = self.state.changed.listen();
                    let version = self.state.version.load(Ordering::Acquire);
                    if version != self.seen {
                        self.seen = version;
                        return Ok(());
                    }
                    if self.state.senders.load(Ordering::Acquire) == 0 {
                        return Err(RecvError);
                    }
                    listener.await;
                }
            }

            /// Wait until the predicate accepts the latest value.
            pub async fn wait_for<F>(
                &mut self,
                mut predicate: F,
            ) -> Result<Ref<'_, T>, RecvError>
            where
                F: FnMut(&T) -> bool,
            {
                loop {
                    let ready = {
                        let value = self.borrow();
                        predicate(&value)
                    };
                    if ready {
                        return Ok(self.borrow_and_update());
                    }
                    self.changed().await?;
                }
            }
        }

        impl<T> Drop for Receiver<T> {
            fn drop(&mut self) {
                self.state.receivers.fetch_sub(1, Ordering::AcqRel);
            }
        }

        /// Create a browser watch channel with one initial receiver.
        pub fn channel<T>(value: T) -> (Sender<T>, Receiver<T>) {
            let state = Arc::new(State {
                value: StdRwLock::new(value),
                version: AtomicU64::new(0),
                senders: AtomicUsize::new(1),
                receivers: AtomicUsize::new(1),
                changed: Event::new(),
            });
            (
                Sender {
                    state: Arc::clone(&state),
                },
                Receiver { state, seen: 0 },
            )
        }
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser::*;
