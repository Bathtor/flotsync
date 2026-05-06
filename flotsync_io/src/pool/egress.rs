use super::{
    ChunkPoolState,
    IoLease,
    IoPoolConfig,
    LeaseRecycler,
    LeaseSegment,
    PoolRequest,
    PooledChunk,
    chunks_for_bytes,
};
use crate::{
    api::IoPayload,
    errors::{Error, Result},
    logging::RuntimeLogger,
};
use bytes::{Buf, BufMut, Bytes, buf::UninitSlice};
use slog::{error, warn};
use std::{
    io,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
};

macro_rules! payload_writer_fixed_width_methods {
    ($(($be_name:ident, $le_name:ident, $ty:ty)),* $(,)?) => {
        $(
            async fn $be_name(&mut self, value: $ty) -> Result<()> {
                let bytes = value.to_be_bytes();
                self.write_slice(&bytes).await
            }

            async fn $le_name(&mut self, value: $ty) -> Result<()> {
                let bytes = value.to_le_bytes();
                self.write_slice(&bytes).await
            }
        )*
    };
}

/// Shared egress pool used for outbound serialisation.
#[derive(Clone, Debug)]
pub struct EgressPool {
    config: IoPoolConfig,
    inner: Arc<Mutex<EgressPoolState>>,
}

impl EgressPool {
    pub(super) fn new(config: IoPoolConfig, logger: RuntimeLogger) -> Self {
        let state = EgressPoolState {
            chunks: ChunkPoolState::new(config.clone()),
            logger,
            waiters: std::collections::VecDeque::new(),
        };
        Self {
            config,
            inner: Arc::new(Mutex::new(state)),
        }
    }

    /// Asynchronously reserves enough pooled capacity for a write of `requested_bytes`.
    ///
    /// Requests are granted strictly in FIFO order.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    pub fn reserve(&self, requested_bytes: usize) -> Result<PoolRequest<EgressReservation>> {
        if requested_bytes == 0 {
            return Err(Error::InvalidEgressReservationSize { requested_bytes });
        }

        let max_bytes = self.config.total_capacity_bytes();
        if requested_bytes > max_bytes {
            return Err(Error::EgressReservationTooLarge {
                requested_bytes,
                max_bytes,
            });
        }

        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
        let (ready, logger) = {
            let mut state = self.lock_state()?;
            state
                .waiters
                .push_back(EgressWaiter::exact_with_reply(requested_bytes, reply_tx));
            let ready = state.dispatch_waiters(&self.inner);
            let logger = state.logger.clone();
            (ready, logger)
        };
        EgressPoolState::deliver_ready(&logger, ready);

        Ok(PoolRequest::new(reply_rx))
    }

    /// Returns the configuration used by this pool.
    #[must_use]
    pub fn config(&self) -> IoPoolConfig {
        self.config.clone()
    }

    /// Creates one growable async writer over this pool.
    #[must_use]
    pub fn writer(&self, hint_bytes: Option<usize>) -> EgressAsyncWriter {
        let chunk_size = self.config.chunk_size;
        let total_capacity = self.config.total_capacity_bytes();
        let preferred_bytes = hint_bytes
            .unwrap_or(chunk_size)
            .max(chunk_size)
            .min(total_capacity);
        EgressAsyncWriter::new(self.clone(), preferred_bytes)
    }

    pub(crate) fn replace_logger(&self, logger: RuntimeLogger) -> Result<()> {
        let mut state = self.lock_state()?;
        state.logger = logger;
        Ok(())
    }

    fn reserve_up_to(&self, preferred_bytes: usize) -> Result<PoolRequest<EgressReservation>> {
        let max_bytes = self.config.total_capacity_bytes();
        let chunk_size = self.config.chunk_size;
        let preferred_bytes = preferred_bytes.max(chunk_size).min(max_bytes);

        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
        let (ready, logger) = {
            let mut state = self.lock_state()?;
            state
                .waiters
                .push_back(EgressWaiter::up_to_with_reply(preferred_bytes, reply_tx));
            let ready = state.dispatch_waiters(&self.inner);
            let logger = state.logger.clone();
            (ready, logger)
        };
        EgressPoolState::deliver_ready(&logger, ready);

        Ok(PoolRequest::new(reply_rx))
    }

    /// Retargets a uniquely owned pooled lease to this egress pool without copying bytes.
    ///
    /// Non-pooled leases are returned unchanged. Pooled leases must be uniquely owned so the
    /// recycler can be retargeted without affecting other shared readers.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    pub fn adopt_lease(&self, mut lease: IoLease) -> Result<IoLease> {
        self.adopt_lease_in_place(&mut lease)?;
        Ok(lease)
    }

    /// Retargets every uniquely owned pooled fragment inside one payload to this egress pool.
    ///
    /// This works recursively for chained payloads and for lease-backed fragments produced through
    /// payload slicing as long as each pooled fragment is uniquely owned. Shared pooled fragments
    /// fail with [`Error::SharedIoPayloadOwnership`] unless the payload is already fully owned by
    /// this egress pool, in which case adoption is a no-op.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    pub fn adopt_payload(&self, mut payload: IoPayload) -> Result<IoPayload> {
        self.adopt_payload_in_place(&mut payload)?;
        Ok(payload)
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, EgressPoolState>> {
        self.inner.lock().map_err(|_| Error::IoBufferStatePoisoned {
            pool_kind: "egress",
        })
    }

    fn import_live_chunks(&self, chunk_count: usize) -> Result<()> {
        let mut state = self.lock_state()?;
        if state.chunks.import_live_chunks(chunk_count) {
            return Ok(());
        }
        Err(Error::EgressLiveChunkAdoptionExhausted {
            chunk_count,
            max_chunk_count: state.chunks.config.max_chunk_count,
        })
    }

    fn adopt_lease_in_place(&self, lease: &mut IoLease) -> Result<()> {
        let payload = &mut lease.payload;
        if payload.recycler.is_owned_by_egress(&self.inner) {
            return Ok(());
        }
        let Some(payload) = Arc::get_mut(payload) else {
            return Err(Error::SharedIoPayloadOwnership);
        };

        let chunk_count = payload.segment_count();
        self.import_live_chunks(chunk_count)?;
        if let Err(error) = payload.recycler.release_live_chunks(chunk_count) {
            EgressPoolState::release_live_chunks_from_weak(
                &Arc::downgrade(&self.inner),
                chunk_count,
            )?;
            return Err(error);
        }
        payload.recycler = LeaseRecycler::Egress(Arc::downgrade(&self.inner));
        Ok(())
    }

    fn adopt_payload_in_place(&self, payload: &mut IoPayload) -> Result<()> {
        if self.payload_is_already_owned(payload) {
            return Ok(());
        }
        match payload {
            IoPayload::Lease(lease) => self.adopt_lease_in_place(lease),
            IoPayload::Bytes(_) => Ok(()),
            IoPayload::Chain(parts) => {
                let Some(parts) = Arc::get_mut(parts) else {
                    return Err(Error::SharedIoPayloadOwnership);
                };
                for part in parts.iter_mut() {
                    self.adopt_payload_in_place(part)?;
                }
                Ok(())
            }
        }
    }

    /// Returns whether `payload` already needs no retargeting or copying for this egress pool.
    ///
    /// This covers plain byte payloads and pooled fragments already owned by the same egress pool
    /// even when they are shared behind `Arc`.
    fn payload_is_already_owned(&self, payload: &IoPayload) -> bool {
        match payload {
            IoPayload::Bytes(_) => true,
            IoPayload::Lease(lease) => lease.payload.recycler.is_owned_by_egress(&self.inner),
            IoPayload::Chain(parts) => parts.iter().all(|part| self.payload_is_already_owned(part)),
        }
    }
}

/// Async payload-serialization surface for pool-backed egress writers.
///
/// This deliberately models "build one payload and seal it later", not a byte-stream sink. The
/// enclosing API decides when the finished payload is sent, so this trait does not expose
/// `flush`/`close`-style sink semantics. Implementations may either copy readable bytes into pooled
/// storage or adopt owned payload fragments zero-copy when `adopt_payload` is used.
#[allow(async_fn_in_trait)]
pub trait PayloadWriter {
    /// Writes the full byte slice into the pending payload.
    async fn write_slice(&mut self, bytes: &[u8]) -> Result<()>;

    /// Writes the readable bytes from one payload by copying them into this writer.
    async fn copy_payload(&mut self, payload: &IoPayload) -> Result<()> {
        let mut cursor = payload.cursor();
        while cursor.has_remaining() {
            let chunk = cursor.chunk();
            let chunk_len = chunk.len();
            self.write_slice(chunk).await?;
            cursor.advance(chunk_len);
        }
        Ok(())
    }

    /// Writes one readable sub-range from an existing payload by copying it into this writer.
    async fn copy_payload_slice(&mut self, payload: &IoPayload, range: Range<usize>) -> Result<()> {
        let payload_len = payload.len();
        let Some(slice) = payload.clone().try_slice(range.clone()) else {
            return Err(invalid_payload_slice_range(payload_len, range));
        };
        self.copy_payload(&slice).await
    }

    /// Adopts one owned payload fragment into the pending payload.
    ///
    /// This must preserve the fragment as an owned payload part rather than copying its readable
    /// bytes back through pooled storage.
    async fn adopt_payload(&mut self, payload: IoPayload) -> Result<()>;

    /// Splices one static byte slice into the pending payload without copying it.
    ///
    /// This seals any currently open pooled fragment and appends `bytes` as a separate payload
    /// fragment. It is a good fit for occasional prebuilt/static fragments and a poor fit for many
    /// tiny pieces in a loop, where [`PayloadWriter::write_slice`] keeps the payload more compact.
    async fn splice_static(&mut self, bytes: &'static [u8]) -> Result<()> {
        self.splice_bytes(Bytes::from_static(bytes)).await
    }

    /// Splices one owned byte buffer into the pending payload without copying it.
    ///
    /// This seals any currently open pooled fragment and appends `bytes` as a separate payload
    /// fragment. It is a good fit for occasional prebuilt fragments and a poor fit for many tiny
    /// pieces in a loop, where [`PayloadWriter::write_slice`] keeps the payload more compact.
    async fn splice_bytes(&mut self, bytes: Bytes) -> Result<()> {
        self.adopt_payload(IoPayload::Bytes(bytes)).await
    }

    /// Adopts one readable sub-range from an owned payload fragment.
    async fn adopt_payload_slice(&mut self, payload: IoPayload, range: Range<usize>) -> Result<()>;

    /// Writes a boolean as `0` or `1`.
    async fn write_bool(&mut self, value: bool) -> Result<()> {
        self.write_u8(u8::from(value)).await
    }

    /// Writes one unsigned byte.
    async fn write_u8(&mut self, value: u8) -> Result<()> {
        self.write_slice(&[value]).await
    }

    /// Writes one signed byte.
    async fn write_i8(&mut self, value: i8) -> Result<()> {
        let bytes = value.to_be_bytes();
        self.write_slice(&bytes).await
    }

    payload_writer_fixed_width_methods!(
        (write_u16_be, write_u16_le, u16),
        (write_u32_be, write_u32_le, u32),
        (write_u64_be, write_u64_le, u64),
        (write_u128_be, write_u128_le, u128),
        (write_i16_be, write_i16_le, i16),
        (write_i32_be, write_i32_le, i32),
        (write_i64_be, write_i64_le, i64),
        (write_i128_be, write_i128_le, i128),
        (write_f32_be, write_f32_le, f32),
        (write_f64_be, write_f64_le, f64),
    );

    /// Writes a string's UTF-8 bytes verbatim.
    async fn write_str(&mut self, value: &str) -> Result<()> {
        self.write_slice(value.as_bytes()).await
    }
}

/// Internal egress-side state guarded by [`EgressPool::inner`].
///
/// `waiters` is a FIFO queue. The dispatcher always examines the front request first and stops as
/// soon as that request cannot be satisfied, which preserves fairness for larger reservations.
#[derive(Debug)]
pub(super) struct EgressPoolState {
    pub(super) chunks: ChunkPoolState,
    logger: RuntimeLogger,
    waiters: std::collections::VecDeque<EgressWaiter>,
}

impl EgressPoolState {
    fn dispatch_waiters(
        &mut self,
        pool: &Arc<Mutex<Self>>,
    ) -> Vec<(
        futures_channel::oneshot::Sender<Result<EgressReservation>>,
        EgressReservation,
    )> {
        let mut ready = Vec::new();

        'waiters: while let Some(waiter) = self.waiters.front() {
            let chunk_count = match waiter.kind {
                EgressWaiterKind::Exact { requested_bytes } => {
                    let chunk_count =
                        chunks_for_bytes(requested_bytes, self.chunks.config.chunk_size);
                    if !self.chunks.can_reserve_chunks(chunk_count) {
                        break 'waiters;
                    }
                    chunk_count
                }
                EgressWaiterKind::UpTo { preferred_bytes } => {
                    let preferred_chunk_count =
                        chunks_for_bytes(preferred_bytes, self.chunks.config.chunk_size);
                    let chunk_count = self
                        .chunks
                        .reservable_chunk_count()
                        .min(preferred_chunk_count);
                    if chunk_count == 0 {
                        break 'waiters;
                    }
                    chunk_count
                }
            };

            let waiter = self.waiters.pop_front().expect("front waiter vanished");
            let chunks = self
                .chunks
                .reserve_chunks(chunk_count)
                .expect("chunk availability changed while dispatching waiters");
            let reserved_bytes = match waiter.kind {
                EgressWaiterKind::Exact { requested_bytes } => requested_bytes,
                EgressWaiterKind::UpTo { .. } => chunk_count * self.chunks.config.chunk_size,
            };
            ready.push((
                waiter.reply_tx,
                EgressReservation::new(reserved_bytes, chunks, Arc::downgrade(pool)),
            ));
        }

        ready
    }

    fn return_chunks(&mut self, chunks: Vec<PooledChunk>) {
        self.chunks.return_chunks(chunks);
    }

    fn release_live_chunks(
        &mut self,
        chunk_count: usize,
        pool: &Arc<Mutex<Self>>,
    ) -> Vec<(
        futures_channel::oneshot::Sender<Result<EgressReservation>>,
        EgressReservation,
    )> {
        self.chunks.release_live_chunks(chunk_count);
        self.dispatch_waiters(pool)
    }

    fn deliver_ready(
        logger: &RuntimeLogger,
        ready: Vec<(
            futures_channel::oneshot::Sender<Result<EgressReservation>>,
            EgressReservation,
        )>,
    ) {
        for (reply_tx, reservation) in ready {
            if reply_tx.send(Ok(reservation)).is_err() {
                warn!(
                    logger,
                    "dropping egress reservation because the waiter was already gone"
                );
            }
        }
    }

    pub(super) fn return_chunks_from_weak(inner: &Weak<Mutex<Self>>, chunks: Vec<PooledChunk>) {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        let (ready, logger) = match inner.lock() {
            Ok(mut state) => {
                state.return_chunks(chunks);
                let ready = state.dispatch_waiters(&inner);
                let logger = state.logger.clone();
                (ready, logger)
            }
            Err(poisoned) => {
                let logger = poisoned.into_inner().logger.clone();
                error!(
                    logger,
                    "egress pool state is poisoned; dropping returned chunks"
                );
                return;
            }
        };

        Self::deliver_ready(&logger, ready);
    }

    pub(super) fn release_live_chunks_from_weak(
        inner: &Weak<Mutex<Self>>,
        chunk_count: usize,
    ) -> Result<()> {
        let Some(inner) = inner.upgrade() else {
            return Ok(());
        };

        let (ready, logger) = match inner.lock() {
            Ok(mut state) => {
                let ready = state.release_live_chunks(chunk_count, &inner);
                let logger = state.logger.clone();
                (ready, logger)
            }
            Err(_) => {
                return Err(Error::IoBufferStatePoisoned {
                    pool_kind: "egress",
                });
            }
        };

        Self::deliver_ready(&logger, ready);
        Ok(())
    }
}

#[derive(Debug)]
struct EgressWaiter {
    kind: EgressWaiterKind,
    reply_tx: futures_channel::oneshot::Sender<Result<EgressReservation>>,
}

impl EgressWaiter {
    fn exact_with_reply(
        requested_bytes: usize,
        reply_tx: futures_channel::oneshot::Sender<Result<EgressReservation>>,
    ) -> Self {
        Self {
            kind: EgressWaiterKind::Exact { requested_bytes },
            reply_tx,
        }
    }

    fn up_to_with_reply(
        preferred_bytes: usize,
        reply_tx: futures_channel::oneshot::Sender<Result<EgressReservation>>,
    ) -> Self {
        Self {
            kind: EgressWaiterKind::UpTo { preferred_bytes },
            reply_tx,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum EgressWaiterKind {
    Exact { requested_bytes: usize },
    UpTo { preferred_bytes: usize },
}

/// Exclusive reservation of pooled egress capacity.
pub struct EgressReservation {
    reserved_bytes: usize,
    chunks: Option<Vec<PooledChunk>>,
    pool: Weak<Mutex<EgressPoolState>>,
}

impl EgressReservation {
    fn new(
        reserved_bytes: usize,
        chunks: Vec<PooledChunk>,
        pool: Weak<Mutex<EgressPoolState>>,
    ) -> Self {
        Self {
            reserved_bytes,
            chunks: Some(chunks),
            pool,
        }
    }

    /// Returns the reserved payload budget in bytes.
    #[must_use]
    pub fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    /// Convenience helper for copying a byte slice into the reserved pooled memory.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    ///
    /// # Panics
    ///
    /// Panics if the reservation's pooled chunks were already consumed before copying.
    pub fn copy_bytes(mut self, bytes: &[u8]) -> Result<IoLease> {
        if bytes.is_empty() {
            return Err(Error::EmptyIoLease);
        }
        if bytes.len() > self.reserved_bytes {
            return Err(Error::EgressReservationOverflow {
                reserved_bytes: self.reserved_bytes,
                attempted_bytes: bytes.len(),
            });
        }

        let mut chunks = self
            .chunks
            .take()
            .expect("egress reservation consumed twice");
        copy_bytes_into_chunks(&mut chunks, bytes);
        let (used_segments, unused_chunks) = split_chunks_by_written_prefix(chunks, bytes.len());
        let lease = IoLease::from_pooled(used_segments, LeaseRecycler::Egress(self.pool.clone()));
        EgressPoolState::return_chunks_from_weak(&self.pool, unused_chunks);
        Ok(lease)
    }
}

impl Drop for EgressReservation {
    fn drop(&mut self) {
        if let Some(chunks) = self.chunks.take() {
            EgressPoolState::return_chunks_from_weak(&self.pool, chunks);
        }
    }
}

/// Growable async writer over pooled egress memory and appended payload fragments.
///
/// This is the default high-level serialization path. It can grow incrementally by requesting
/// more pooled capacity from the egress pool and can also append pre-existing payload fragments
/// without copying them back into pooled byte storage.
///
/// Staged bytes are only sealed into a finished payload when [`EgressAsyncWriter::finish`] is
/// called. Dropping the writer discards any staged payload and returns pooled capacity to the
/// egress pool.
pub struct EgressAsyncWriter {
    pool: EgressPool,
    parts: Vec<IoPayload>,
    chunks: Vec<PooledChunk>,
    written_bytes: usize,
    chunk_index: usize,
    chunk_offset: usize,
    next_preferred_bytes: usize,
    pending_request: Option<PoolRequest<EgressReservation>>,
}

impl EgressAsyncWriter {
    fn new(pool: EgressPool, preferred_bytes: usize) -> Self {
        Self {
            pool,
            parts: Vec::new(),
            chunks: Vec::new(),
            written_bytes: 0,
            chunk_index: 0,
            chunk_offset: 0,
            next_preferred_bytes: preferred_bytes,
            pending_request: None,
        }
    }

    async fn write_all_bytes(&mut self, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let written = std::future::poll_fn(|cx| self.poll_write_result(cx, bytes)).await?;
            bytes = &bytes[written..];
        }
        Ok(())
    }

    /// Reserves enough staged pooled capacity for one bounded synchronous write window.
    ///
    /// The returned writer is only a temporary proxy into this writer's current pooled fragment.
    /// It may write at most `reserved_bytes`, advances this writer immediately as bytes are
    /// written, and leaves any unused reserved capacity available for later async or sync writes.
    ///
    /// This is intended for serializers that already know an exact size or a hard upper bound and
    /// want to reuse one sync `BufMut` / `Write` implementation without giving up pooled memory.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    pub async fn write_with_reserved(
        &mut self,
        reserved_bytes: usize,
    ) -> Result<EgressReservedWriter<'_>> {
        if reserved_bytes > self.pool.config.total_capacity_bytes() {
            return Err(Error::EgressReservationTooLarge {
                requested_bytes: reserved_bytes,
                max_bytes: self.pool.config.total_capacity_bytes(),
            });
        }
        std::future::poll_fn(|cx| self.poll_reserve_for_sync(cx, reserved_bytes)).await?;
        Ok(EgressReservedWriter::new(self, reserved_bytes))
    }

    fn adopt_payload_part(&mut self, payload: IoPayload) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        let payload = self.pool.adopt_payload(payload)?;
        self.flush_current_part()?;
        if !payload.is_empty() {
            self.parts.push(payload);
        }
        Ok(())
    }

    /// Seals the staged bytes into one payload and returns it.
    ///
    /// This is the only commit point for bytes staged through the writer. Dropping the writer
    /// without calling `finish` discards any unsent staged payload and returns pooled capacity.
    ///
    /// # Errors
    ///
    /// See `Error` for failure conditions.
    ///
    /// # Panics
    ///
    /// Panics if exactly one staged payload part is expected but no part is available.
    pub fn finish(mut self) -> Result<Option<IoPayload>> {
        self.flush_current_part()?;
        Ok(match self.parts.len() {
            0 => None,
            1 => Some(self.parts.pop().expect("single finished part vanished")),
            _ => Some(IoPayload::chain(std::mem::take(&mut self.parts))),
        })
    }

    fn poll_write_result(&mut self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut written = 0;
        loop {
            let available = self.current_chunk_remaining();
            if available > 0 {
                let to_copy = available.min(bytes.len() - written);
                let chunk = &mut self.chunks[self.chunk_index];
                chunk[self.chunk_offset..self.chunk_offset + to_copy]
                    .copy_from_slice(&bytes[written..written + to_copy]);
                self.written_bytes += to_copy;
                self.chunk_offset += to_copy;
                written += to_copy;
                if self.chunk_offset == chunk.len() {
                    self.chunk_index += 1;
                    self.chunk_offset = 0;
                }
                if written == bytes.len() {
                    return Poll::Ready(Ok(written));
                }
            }

            match self.poll_acquire_more(cx) {
                Poll::Ready(Ok(())) => {
                    // Continue with the newly acquired staged capacity.
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending if written > 0 => return Poll::Ready(Ok(written)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_reserve_for_sync(
        &mut self,
        cx: &mut Context<'_>,
        reserved_bytes: usize,
    ) -> Poll<Result<()>> {
        while self.remaining_staged_capacity() < reserved_bytes {
            match self.poll_acquire_more(cx) {
                Poll::Ready(Ok(())) => {
                    // Continue reserving until the requested synchronous budget is available.
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_acquire_more(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.pending_request.is_none() {
            let request = self.pool.reserve_up_to(self.next_preferred_bytes)?;
            self.pending_request = Some(request);
            self.next_preferred_bytes = self.grow_preferred_bytes();
        }

        let request = self
            .pending_request
            .as_mut()
            .expect("pending request must exist after starting one");
        match Pin::new(request).poll(cx) {
            Poll::Ready(Ok(mut reservation)) => {
                let chunks = reservation
                    .chunks
                    .take()
                    .expect("ready async reservation lost its chunks");
                self.chunks.extend(chunks);
                self.pending_request = None;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.pending_request = None;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn current_chunk_remaining(&self) -> usize {
        if self.chunk_index >= self.chunks.len() {
            return 0;
        }
        self.chunks[self.chunk_index].len() - self.chunk_offset
    }

    fn remaining_staged_capacity(&self) -> usize {
        if self.chunk_index >= self.chunks.len() {
            return 0;
        }
        let current = self.current_chunk_remaining();
        let rest = self.chunks[self.chunk_index + 1..]
            .iter()
            .map(|chunk| chunk.len())
            .sum::<usize>();
        current + rest
    }

    fn advance_to_next_writable_chunk(&mut self) {
        while self.chunk_index < self.chunks.len()
            && self.chunk_offset == self.chunks[self.chunk_index].len()
        {
            self.chunk_index += 1;
            self.chunk_offset = 0;
        }
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "The flush helper shares the Result shape of neighbouring pool operations."
    )]
    fn flush_current_part(&mut self) -> Result<()> {
        let recycler = LeaseRecycler::Egress(Arc::downgrade(&self.pool.inner));
        if self.written_bytes == 0 {
            if !self.chunks.is_empty() {
                let chunks = std::mem::take(&mut self.chunks);
                EgressPoolState::return_chunks_from_weak(&Arc::downgrade(&self.pool.inner), chunks);
            }
            self.chunk_index = 0;
            self.chunk_offset = 0;
            return Ok(());
        }

        let (used_segments, unused_chunks) =
            split_chunks_by_written_prefix(std::mem::take(&mut self.chunks), self.written_bytes);
        EgressPoolState::return_chunks_from_weak(&Arc::downgrade(&self.pool.inner), unused_chunks);

        let lease = IoLease::from_pooled(used_segments, recycler);
        self.parts.push(IoPayload::Lease(lease));
        self.written_bytes = 0;
        self.chunk_index = 0;
        self.chunk_offset = 0;
        Ok(())
    }

    fn grow_preferred_bytes(&self) -> usize {
        self.next_preferred_bytes
            .saturating_mul(2)
            .max(self.pool.config.chunk_size)
            .min(self.pool.config.total_capacity_bytes())
    }
}

impl Drop for EgressAsyncWriter {
    fn drop(&mut self) {
        if !self.chunks.is_empty() {
            let chunks = std::mem::take(&mut self.chunks);
            EgressPoolState::return_chunks_from_weak(&Arc::downgrade(&self.pool.inner), chunks);
        }
    }
}

impl PayloadWriter for EgressAsyncWriter {
    async fn write_slice(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all_bytes(bytes).await
    }

    async fn adopt_payload(&mut self, payload: IoPayload) -> Result<()> {
        self.adopt_payload_part(payload)
    }

    async fn adopt_payload_slice(&mut self, payload: IoPayload, range: Range<usize>) -> Result<()> {
        let payload_len = payload.len();
        let Some(slice) = payload.try_slice(range.clone()) else {
            return Err(invalid_payload_slice_range(payload_len, range));
        };
        self.adopt_payload_part(slice)
    }
}

/// Temporary bounded synchronous writer over one reserved window inside an [`EgressAsyncWriter`].
///
/// This type does not own memory on its own. It is only a scoped mutable view into the parent
/// writer's current pooled fragment. As bytes are written, the parent writer advances
/// immediately. Any unused reserved budget remains available to the parent once this proxy drops.
pub struct EgressReservedWriter<'a> {
    writer: &'a mut EgressAsyncWriter,
    reserved_remaining: usize,
}

impl<'a> EgressReservedWriter<'a> {
    fn new(writer: &'a mut EgressAsyncWriter, reserved_remaining: usize) -> Self {
        Self {
            writer,
            reserved_remaining,
        }
    }

    /// Returns how many bytes may still be written through this bounded proxy.
    #[must_use]
    pub fn reserved_remaining(&self) -> usize {
        self.reserved_remaining
    }

    fn ensure_writable_chunk(&mut self) {
        self.writer.advance_to_next_writable_chunk();
        assert!(
            self.reserved_remaining == 0 || self.writer.current_chunk_remaining() > 0,
            "EgressReservedWriter ran out of pooled capacity before exhausting its reserved budget"
        );
    }

    fn panic_overflow(&self, attempted_bytes: usize) -> ! {
        panic!(
            "EgressReservedWriter overflowed its reserved budget: attempted {attempted_bytes} bytes with only {} bytes remaining",
            self.reserved_remaining
        );
    }
}

// Safety:
//
// - `write_with_reserved` only constructs this proxy after the parent writer has acquired at
//   least `reserved_remaining` bytes of staged pooled capacity.
// - the proxy holds the only `&mut EgressAsyncWriter`, so `chunk_mut` cannot alias another
//   mutable view into the same chunks while the proxy is alive.
// - every successful write path reduces `reserved_remaining` and advances the parent writer's
//   `(written_bytes, chunk_index, chunk_offset)` in lock-step, so the proxy never exposes bytes
//   beyond its reserved window.
// - `advance_to_next_writable_chunk` only skips fully consumed chunks, so the returned pointer in
//   `chunk_mut` always targets the current live writable region.
unsafe impl BufMut for EgressReservedWriter<'_> {
    fn remaining_mut(&self) -> usize {
        self.reserved_remaining
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        if cnt > self.reserved_remaining {
            self.panic_overflow(cnt);
        }

        let mut remaining = cnt;
        while remaining > 0 {
            self.ensure_writable_chunk();
            let available = self.writer.current_chunk_remaining();
            let advance = available.min(remaining);
            self.writer.written_bytes += advance;
            self.writer.chunk_offset += advance;
            self.reserved_remaining -= advance;
            remaining -= advance;
            self.writer.advance_to_next_writable_chunk();
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.ensure_writable_chunk();
        let chunk_len = self
            .writer
            .current_chunk_remaining()
            .min(self.reserved_remaining);
        if chunk_len == 0 {
            // Safety: a zero-length slice may use a dangling pointer because it is never
            // dereferenced. This is only the `BufMut` empty-chunk case once the reserved window
            // is exhausted.
            return unsafe {
                UninitSlice::from_raw_parts_mut(std::ptr::NonNull::<u8>::dangling().as_ptr(), 0)
            };
        }

        let chunk = &mut self.writer.chunks[self.writer.chunk_index];
        let ptr = chunk.as_mut_ptr();
        // Safety: `ensure_writable_chunk` guarantees that `chunk_offset` points at the current
        // writable prefix inside `chunk`, and `chunk_len` is capped to both the remaining bytes in
        // this chunk and the proxy's remaining reserved budget.
        unsafe { UninitSlice::from_raw_parts_mut(ptr.add(self.writer.chunk_offset), chunk_len) }
    }

    fn put<T: Buf>(&mut self, mut src: T)
    where
        Self: Sized,
    {
        while src.has_remaining() {
            let chunk = src.chunk();
            let chunk_len = chunk.len();
            self.put_slice(chunk);
            src.advance(chunk_len);
        }
    }

    fn put_slice(&mut self, src: &[u8]) {
        if src.len() > self.reserved_remaining {
            self.panic_overflow(src.len());
        }

        let mut offset = 0usize;
        while offset < src.len() {
            self.ensure_writable_chunk();
            let available = self
                .writer
                .current_chunk_remaining()
                .min(self.reserved_remaining);
            let to_copy = available.min(src.len() - offset);
            let chunk = &mut self.writer.chunks[self.writer.chunk_index];
            chunk[self.writer.chunk_offset..self.writer.chunk_offset + to_copy]
                .copy_from_slice(&src[offset..offset + to_copy]);
            self.writer.written_bytes += to_copy;
            self.writer.chunk_offset += to_copy;
            self.reserved_remaining -= to_copy;
            offset += to_copy;
            self.writer.advance_to_next_writable_chunk();
        }
    }
}

impl io::Write for EgressReservedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.put_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn invalid_payload_slice_range(payload_len: usize, range: Range<usize>) -> Error {
    Error::InvalidIoPayloadSliceRange {
        start: range.start,
        end: range.end,
        payload_len,
    }
}

fn copy_bytes_into_chunks(chunks: &mut [PooledChunk], mut bytes: &[u8]) {
    for chunk in chunks.iter_mut() {
        if bytes.is_empty() {
            break;
        }

        let to_copy = bytes.len().min(chunk.len());
        chunk[..to_copy].copy_from_slice(&bytes[..to_copy]);
        bytes = &bytes[to_copy..];
    }

    debug_assert!(
        bytes.is_empty(),
        "copy_bytes_into_chunks was asked to copy more bytes than the provided chunks can hold"
    );
}

fn split_chunks_by_written_prefix(
    chunks: Vec<PooledChunk>,
    written_bytes: usize,
) -> (Vec<LeaseSegment>, Vec<PooledChunk>) {
    let mut remaining = written_bytes;
    let mut used_segments = Vec::new();
    let mut unused_chunks = Vec::new();
    for chunk in chunks {
        if remaining == 0 {
            unused_chunks.push(chunk);
            continue;
        }

        let written_in_chunk = remaining.min(chunk.len());
        remaining -= written_in_chunk;
        used_segments.push(LeaseSegment {
            chunk,
            written_len: written_in_chunk,
        });
    }

    (used_segments, unused_chunks)
}
