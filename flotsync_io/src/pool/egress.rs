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
use crate::errors::{Error, Result};
use bytes::{BufMut, buf::UninitSlice};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

/// Shared egress pool used for outbound serialisation.
#[derive(Clone, Debug)]
pub struct EgressPool {
    config: IoPoolConfig,
    inner: Arc<Mutex<EgressPoolState>>,
}

impl EgressPool {
    pub(super) fn new(config: IoPoolConfig) -> Self {
        let state = EgressPoolState {
            chunks: ChunkPoolState::new(config.clone()),
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
        let ready = {
            let mut state = self.lock_state()?;
            state.waiters.push_back(EgressWaiter {
                requested_bytes,
                reply_tx,
            });
            state.dispatch_waiters(&self.inner)
        };
        EgressPoolState::deliver_ready(ready);

        Ok(PoolRequest::new(reply_rx))
    }

    /// Returns the configuration used by this pool.
    pub fn config(&self) -> IoPoolConfig {
        self.config.clone()
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, EgressPoolState>> {
        self.inner.lock().map_err(|_| Error::IoBufferStatePoisoned {
            pool_kind: "egress",
        })
    }
}

/// Internal egress-side state guarded by [`EgressPool::inner`].
///
/// `waiters` is a FIFO queue. The dispatcher always examines the front request first and stops as
/// soon as that request cannot be satisfied, which preserves fairness for larger reservations.
#[derive(Debug)]
pub(super) struct EgressPoolState {
    pub(super) chunks: ChunkPoolState,
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

    fn deliver_ready(
        ready: Vec<(
            futures_channel::oneshot::Sender<Result<EgressReservation>>,
            EgressReservation,
        )>,
    ) {
        for (reply_tx, reservation) in ready {
            if reply_tx.send(Ok(reservation)).is_err() {
                log::warn!("dropping egress reservation because the waiter was already gone");
            }
        }
    }

    pub(super) fn return_chunks_from_weak(inner: &Weak<Mutex<Self>>, chunks: Vec<PooledChunk>) {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        let ready = match inner.lock() {
            Ok(mut state) => {
                state.return_chunks(chunks);
                state.dispatch_waiters(&inner)
            }
            Err(_) => {
                log::error!("egress pool state is poisoned; dropping returned chunks");
                return;
            }
        };

        Self::deliver_ready(ready);
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

    /// Serialises into the reserved pooled memory and returns the resulting lease.
    pub fn write_with<T>(
        mut self,
        write: impl FnOnce(&mut IoBufWriter) -> Result<T>,
    ) -> Result<(T, IoLease)> {
        let chunks = self
            .chunks
            .take()
            .expect("egress reservation consumed twice");
        let mut writer = IoBufWriter::new(chunks, self.requested_bytes);

        let write_result = write(&mut writer);
        match write_result {
            Ok(value) => {
                let (lease, unused_chunks) =
                    writer.finish(LeaseRecycler::Egress(self.pool.clone()))?;
                EgressPoolState::return_chunks_from_weak(&self.pool, unused_chunks);
                Ok((value, lease))
            }
            Err(error) => {
                EgressPoolState::return_chunks_from_weak(&self.pool, writer.into_chunks());
                Err(error)
            }
        }
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
