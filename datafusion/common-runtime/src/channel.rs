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

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub mod mpsc {
    pub use tokio::sync::mpsc::*;
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub mod oneshot {
    pub use tokio::sync::oneshot::*;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod oneshot {
    pub use futures::channel::oneshot::{Canceled, Receiver, Sender, channel};
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub mod mpsc {
    use std::fmt::{Display, Formatter};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use async_lock::{Semaphore, SemaphoreGuardArc};
    use event_listener::Event;
    use futures::Stream;
    use futures::channel::mpsc as futures_mpsc;

    #[derive(Debug)]
    struct ChannelState {
        receiver_alive: AtomicBool,
        senders: AtomicUsize,
        len: AtomicUsize,
        closed: Event,
    }

    impl ChannelState {
        fn new() -> Self {
            Self {
                receiver_alive: AtomicBool::new(true),
                senders: AtomicUsize::new(1),
                len: AtomicUsize::new(0),
                closed: Event::new(),
            }
        }

        fn close(&self) {
            if self.receiver_alive.swap(false, Ordering::AcqRel) {
                self.closed.notify(usize::MAX);
            }
        }

        async fn closed(&self) {
            loop {
                if !self.receiver_alive.load(Ordering::Acquire) {
                    return;
                }
                let listener = self.closed.listen();
                if !self.receiver_alive.load(Ordering::Acquire) {
                    return;
                }
                listener.await;
            }
        }
    }

    /// Error returned when a channel's receiver has closed.
    #[derive(Debug, PartialEq, Eq)]
    pub struct SendError<T>(T);

    impl<T> SendError<T> {
        /// Return the value that could not be sent.
        pub fn into_inner(self) -> T {
            self.0
        }
    }

    impl<T> Display for SendError<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("channel closed")
        }
    }

    impl<T: std::fmt::Debug> std::error::Error for SendError<T> {}

    /// Error returned by a nonblocking bounded send.
    #[derive(Debug, PartialEq, Eq)]
    pub struct TrySendError<T> {
        value: T,
        full: bool,
    }

    impl<T> TrySendError<T> {
        /// Return true when the configured capacity was exhausted.
        pub fn is_full(&self) -> bool {
            self.full
        }

        /// Return true when the receiver was closed.
        pub fn is_closed(&self) -> bool {
            !self.full
        }

        /// Return the value that could not be sent.
        pub fn into_inner(self) -> T {
            self.value
        }
    }

    impl<T> Display for TrySendError<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            if self.full {
                formatter.write_str("no available channel capacity")
            } else {
                formatter.write_str("channel closed")
            }
        }
    }

    impl<T: std::fmt::Debug> std::error::Error for TrySendError<T> {}

    struct Envelope<T> {
        value: T,
        _credit: SemaphoreGuardArc,
    }

    /// Sending side of a globally bounded browser channel.
    pub struct Sender<T> {
        inner: futures_mpsc::UnboundedSender<Envelope<T>>,
        credits: Arc<Semaphore>,
        state: Arc<ChannelState>,
    }

    impl<T> std::fmt::Debug for Sender<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("Sender")
                .field("closed", &self.is_closed())
                .finish_non_exhaustive()
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            self.state.senders.fetch_add(1, Ordering::Relaxed);
            Self {
                inner: self.inner.clone(),
                credits: Arc::clone(&self.credits),
                state: Arc::clone(&self.state),
            }
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            self.state.senders.fetch_sub(1, Ordering::Release);
        }
    }

    impl<T> Sender<T> {
        /// Send a value once a global channel credit becomes available.
        pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
            if self.is_closed() {
                return Err(SendError(value));
            }
            let credit = Arc::clone(&self.credits).acquire_arc().await;
            if self.is_closed() {
                return Err(SendError(value));
            }
            let envelope = Envelope {
                value,
                _credit: credit,
            };
            match self.inner.unbounded_send(envelope) {
                Ok(()) => {
                    self.state.len.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
                Err(error) => Err(SendError(error.into_inner().value)),
            }
        }

        /// Attempt to send without waiting for a channel credit.
        pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
            if self.is_closed() {
                return Err(TrySendError { value, full: false });
            }
            let Some(credit) = Arc::clone(&self.credits).try_acquire_arc() else {
                return Err(TrySendError { value, full: true });
            };
            let envelope = Envelope {
                value,
                _credit: credit,
            };
            match self.inner.unbounded_send(envelope) {
                Ok(()) => {
                    self.state.len.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
                Err(error) => Err(TrySendError {
                    value: error.into_inner().value,
                    full: false,
                }),
            }
        }

        /// Wait until the receiver closes.
        pub async fn closed(&self) {
            self.state.closed().await;
        }

        /// Return true when the receiver is closed.
        pub fn is_closed(&self) -> bool {
            !self.state.receiver_alive.load(Ordering::Acquire)
        }
    }

    /// Receiving side of a globally bounded browser channel.
    pub struct Receiver<T> {
        inner: futures_mpsc::UnboundedReceiver<Envelope<T>>,
        state: Arc<ChannelState>,
    }

    impl<T> std::fmt::Debug for Receiver<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("Receiver")
                .field("len", &self.len())
                .finish_non_exhaustive()
        }
    }

    impl<T> Receiver<T> {
        /// Receive the next value, returning its channel credit immediately.
        pub async fn recv(&mut self) -> Option<T> {
            use futures::StreamExt;
            let envelope = self.inner.next().await?;
            self.state.len.fetch_sub(1, Ordering::AcqRel);
            Some(envelope.value)
        }

        /// Poll the next value.
        pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(envelope)) => {
                    self.state.len.fetch_sub(1, Ordering::AcqRel);
                    Poll::Ready(Some(envelope.value))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }

        /// Prevent further sends while retaining already queued values.
        pub fn close(&mut self) {
            self.state.close();
            self.inner.close();
        }

        /// Return the number of queued values.
        pub fn len(&self) -> usize {
            self.state.len.load(Ordering::Acquire)
        }

        /// Return true when no values are queued.
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }

        /// Return true when the channel can no longer receive values.
        pub fn is_closed(&self) -> bool {
            !self.state.receiver_alive.load(Ordering::Acquire)
                || self.state.senders.load(Ordering::Acquire) == 0
        }
    }

    impl<T> Drop for Receiver<T> {
        fn drop(&mut self) {
            self.state.close();
        }
    }

    /// Create a globally bounded browser channel.
    pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        assert!(capacity > 0, "mpsc channel capacity must be positive");
        let (inner_sender, inner_receiver) = futures_mpsc::unbounded();
        let state = Arc::new(ChannelState::new());
        (
            Sender {
                inner: inner_sender,
                credits: Arc::new(Semaphore::new(capacity)),
                state: Arc::clone(&state),
            },
            Receiver {
                inner: inner_receiver,
                state,
            },
        )
    }

    /// Sending side of an unbounded browser channel.
    pub struct UnboundedSender<T> {
        inner: futures_mpsc::UnboundedSender<T>,
        state: Arc<ChannelState>,
    }

    impl<T> std::fmt::Debug for UnboundedSender<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("UnboundedSender")
                .field("closed", &self.is_closed())
                .finish_non_exhaustive()
        }
    }

    impl<T> Clone for UnboundedSender<T> {
        fn clone(&self) -> Self {
            self.state.senders.fetch_add(1, Ordering::Relaxed);
            Self {
                inner: self.inner.clone(),
                state: Arc::clone(&self.state),
            }
        }
    }

    impl<T> Drop for UnboundedSender<T> {
        fn drop(&mut self) {
            self.state.senders.fetch_sub(1, Ordering::Release);
        }
    }

    impl<T> UnboundedSender<T> {
        /// Send a value without waiting for capacity.
        pub fn send(&self, value: T) -> Result<(), SendError<T>> {
            match self.inner.unbounded_send(value) {
                Ok(()) => {
                    self.state.len.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
                Err(error) => Err(SendError(error.into_inner())),
            }
        }

        /// Alias for [`Self::send`].
        pub fn unbounded_send(&self, value: T) -> Result<(), SendError<T>> {
            self.send(value)
        }

        /// Wait until the receiver closes.
        pub async fn closed(&self) {
            self.state.closed().await;
        }

        /// Return true when the receiver is closed.
        pub fn is_closed(&self) -> bool {
            !self.state.receiver_alive.load(Ordering::Acquire)
        }
    }

    /// Receiving side of an unbounded browser channel.
    pub struct UnboundedReceiver<T> {
        inner: futures_mpsc::UnboundedReceiver<T>,
        state: Arc<ChannelState>,
    }

    impl<T> std::fmt::Debug for UnboundedReceiver<T> {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("UnboundedReceiver")
                .field("len", &self.len())
                .finish_non_exhaustive()
        }
    }

    impl<T> UnboundedReceiver<T> {
        /// Receive the next value.
        pub async fn recv(&mut self) -> Option<T> {
            use futures::StreamExt;
            let value = self.inner.next().await?;
            self.state.len.fetch_sub(1, Ordering::AcqRel);
            Some(value)
        }

        /// Poll the next value.
        pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(value)) => {
                    self.state.len.fetch_sub(1, Ordering::AcqRel);
                    Poll::Ready(Some(value))
                }
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Pending => Poll::Pending,
            }
        }

        /// Prevent further sends while retaining queued values.
        pub fn close(&mut self) {
            self.state.close();
            self.inner.close();
        }

        /// Return the number of queued values.
        pub fn len(&self) -> usize {
            self.state.len.load(Ordering::Acquire)
        }

        /// Return true when no values are queued.
        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }

        /// Return true when the channel can no longer receive values.
        pub fn is_closed(&self) -> bool {
            !self.state.receiver_alive.load(Ordering::Acquire)
                || self.state.senders.load(Ordering::Acquire) == 0
        }
    }

    impl<T> Drop for UnboundedReceiver<T> {
        fn drop(&mut self) {
            self.state.close();
        }
    }

    /// Create an unbounded browser channel.
    pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
        let (inner_sender, inner_receiver) = futures_mpsc::unbounded();
        let state = Arc::new(ChannelState::new());
        (
            UnboundedSender {
                inner: inner_sender,
                state: Arc::clone(&state),
            },
            UnboundedReceiver {
                inner: inner_receiver,
                state,
            },
        )
    }
}
