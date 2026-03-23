use super::{
    ChunkPoolState,
    IoLease,
    IoLeaseInner,
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
use bytes::{Buf, BufMut, buf::UninitSlice};
use slog::{error, warn};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

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
            state.waiters.push_back(EgressWaiter {
                requested_bytes,
                reply_tx,
            });
            let ready = state.dispatch_waiters(&self.inner);
            let logger = state.logger.clone();
            (ready, logger)
        };
        EgressPoolState::deliver_ready(&logger, ready);

        Ok(PoolRequest::new(reply_rx))
    }

    /// Returns the configuration used by this pool.
    pub fn config(&self) -> IoPoolConfig {
        self.config.clone()
    }

    pub(crate) fn replace_logger(&self, logger: RuntimeLogger) -> Result<()> {
        let mut state = self.lock_state()?;
        state.logger = logger;
        Ok(())
    }

    /// Retargets a uniquely owned pooled lease to this egress pool without copying bytes.
    ///
    /// Non-pooled leases are returned unchanged. Pooled leases must be uniquely owned so the
    /// recycler can be retargeted without affecting other shared readers.
    pub fn adopt_lease(&self, mut lease: IoLease) -> Result<IoLease> {
        self.adopt_lease_in_place(&mut lease)?;
        Ok(lease)
    }

    /// Retargets every uniquely owned pooled fragment inside one payload to this egress pool.
    ///
    /// This works recursively for chained payloads and for lease-backed fragments produced through
    /// payload slicing as long as each pooled fragment is uniquely owned. Shared pooled fragments
    /// fail with [`Error::SharedIoPayloadOwnership`].
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
        let IoLeaseInner::Pooled(payload) = &mut lease.inner else {
            return Ok(());
        };
        let Some(payload) = Arc::get_mut(payload) else {
            return Err(Error::SharedIoPayloadOwnership);
        };
        if payload.recycler.is_owned_by_egress(&self.inner) {
            return Ok(());
        }

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
            let chunk_count =
                chunks_for_bytes(waiter.requested_bytes, self.chunks.config.chunk_size);
            if !self.chunks.can_reserve_chunks(chunk_count) {
                break 'waiters;
            }

            let waiter = self.waiters.pop_front().expect("front waiter vanished");
            let chunks = self
                .chunks
                .reserve_chunks(chunk_count)
                .expect("chunk availability changed while dispatching waiters");
            ready.push((
                waiter.reply_tx,
                EgressReservation::new(waiter.requested_bytes, chunks, Arc::downgrade(pool)),
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
    requested_bytes: usize,
    reply_tx: futures_channel::oneshot::Sender<Result<EgressReservation>>,
}

/// Exclusive reservation of pooled egress capacity.
pub struct EgressReservation {
    requested_bytes: usize,
    chunks: Option<Vec<PooledChunk>>,
    pool: Weak<Mutex<EgressPoolState>>,
}

impl EgressReservation {
    fn new(
        requested_bytes: usize,
        chunks: Vec<PooledChunk>,
        pool: Weak<Mutex<EgressPoolState>>,
    ) -> Self {
        Self {
            requested_bytes,
            chunks: Some(chunks),
            pool,
        }
    }

    /// Returns the reserved payload budget in bytes.
    pub fn requested_bytes(&self) -> usize {
        self.requested_bytes
    }

    /// Serialises into the reserved pooled memory and optionally returns the resulting lease.
    ///
    /// A closure that writes zero bytes succeeds with `None` and returns the entire reservation to
    /// the pool immediately.
    pub(crate) fn write_with_optional<T>(
        mut self,
        write: impl FnOnce(&mut IoBufWriter) -> Result<T>,
    ) -> Result<(T, Option<IoLease>)> {
        let chunks = self
            .chunks
            .take()
            .expect("egress reservation consumed twice");
        let mut writer = IoBufWriter::new(chunks, self.requested_bytes);

        let write_result = write(&mut writer);
        match write_result {
            Ok(value) => {
                if writer.written_bytes() == 0 {
                    EgressPoolState::return_chunks_from_weak(&self.pool, writer.into_chunks());
                    return Ok((value, None));
                }
                let (lease, unused_chunks) =
                    writer.finish(LeaseRecycler::Egress(self.pool.clone()))?;
                EgressPoolState::return_chunks_from_weak(&self.pool, unused_chunks);
                Ok((value, Some(lease)))
            }
            Err(error) => {
                EgressPoolState::return_chunks_from_weak(&self.pool, writer.into_chunks());
                Err(error)
            }
        }
    }

    /// Serialises into the reserved pooled memory and returns the resulting lease.
    pub fn write_with<T>(
        self,
        write: impl FnOnce(&mut IoBufWriter) -> Result<T>,
    ) -> Result<(T, IoLease)> {
        let (value, lease) = self.write_with_optional(write)?;
        let lease = lease.ok_or(Error::EmptyIoLease)?;
        Ok((value, lease))
    }

    /// Convenience helper for copying a byte slice into the reserved pooled memory.
    pub fn copy_bytes(self, bytes: &[u8]) -> Result<IoLease> {
        let ((), lease) = self.write_with(|writer| {
            writer.put_slice(bytes);
            Ok(())
        })?;
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

/// Writer over an egress reservation's pooled memory.
///
/// The writer owns all reserved chunks exclusively until it is finished or dropped. `chunk_index`
/// and `chunk_offset` always point at the next writable byte, while `written_bytes` tracks the
/// total payload prefix that will become visible in the resulting [`IoLease`].
pub struct IoBufWriter {
    chunks: Vec<PooledChunk>,
    requested_bytes: usize,
    written_bytes: usize,
    chunk_index: usize,
    chunk_offset: usize,
}

impl IoBufWriter {
    fn new(chunks: Vec<PooledChunk>, requested_bytes: usize) -> Self {
        Self {
            chunks,
            requested_bytes,
            written_bytes: 0,
            chunk_index: 0,
            chunk_offset: 0,
        }
    }

    /// Returns the number of bytes written so far.
    pub fn written_bytes(&self) -> usize {
        self.written_bytes
    }

    fn finish(self, recycler: LeaseRecycler) -> Result<(IoLease, Vec<PooledChunk>)> {
        if self.written_bytes == 0 {
            return Err(Error::EmptyIoLease);
        }

        let mut remaining = self.written_bytes;
        let mut used_segments = Vec::new();
        let mut unused_chunks = Vec::new();
        for chunk in self.chunks {
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

        let lease = IoLease::from_pooled(used_segments, recycler);
        Ok((lease, unused_chunks))
    }

    fn into_chunks(self) -> Vec<PooledChunk> {
        self.chunks
    }

    /// Copies one payload into the reserved pooled memory.
    ///
    /// This writes the payload's currently readable bytes in order, independent of whether the
    /// source is lease-backed, byte-backed, or chained.
    pub fn put_payload(&mut self, payload: &IoPayload) {
        let mut cursor = payload.cursor();
        while cursor.has_remaining() {
            let chunk = cursor.chunk();
            let chunk_len = chunk.len();
            self.put_slice(chunk);
            cursor.advance(chunk_len);
        }
    }

    /// Copies one readable payload sub-range into the reserved pooled memory.
    ///
    /// The caller must supply a valid readable range for `payload`. This panics if the range is
    /// invalid because all current use-sites derive the range from previously validated parser
    /// state.
    pub fn put_payload_slice(&mut self, payload: &IoPayload, offset: usize, len: usize) {
        let slice = payload
            .clone()
            .try_slice(offset, len)
            .unwrap_or_else(|| panic!("invalid IoPayload slice range {offset}..{}", offset + len));
        self.put_payload(&slice);
    }
}

// SAFETY:
// - `IoBufWriter` owns all reserved chunks exclusively while the writer exists.
// - `chunk_mut` exposes only the unwritten tail of the current chunk, clamped to the reservation
//   budget reported by `remaining_mut`.
// - `advance_mut` maintains `chunk_index` and `chunk_offset` within chunk boundaries and never lets
//   the visible writable region extend past the reserved payload budget.
unsafe impl BufMut for IoBufWriter {
    fn remaining_mut(&self) -> usize {
        self.requested_bytes - self.written_bytes
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining_mut(),
            "advanced past reserved writer budget"
        );

        let current_chunk = &self.chunks[self.chunk_index];
        let current_remaining = current_chunk.len() - self.chunk_offset;
        assert!(
            cnt <= current_remaining,
            "advanced past current chunk boundary"
        );

        self.written_bytes += cnt;
        self.chunk_offset += cnt;
        if self.chunk_offset == current_chunk.len() && self.written_bytes < self.requested_bytes {
            self.chunk_index += 1;
            self.chunk_offset = 0;
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let budget = self.remaining_mut();
        let chunk = &mut self.chunks[self.chunk_index];
        let available_in_chunk = chunk.len() - self.chunk_offset;
        let length = available_in_chunk.min(budget);
        unsafe {
            // SAFETY: `chunk` is owned exclusively by this writer, `chunk_offset..chunk_offset +
            // length` stays within the current chunk, and `length` is additionally clamped by the
            // remaining reservation budget reported via `remaining_mut`.
            let ptr = chunk.as_mut_ptr().add(self.chunk_offset);
            UninitSlice::from_raw_parts_mut(ptr, length)
        }
    }
}
