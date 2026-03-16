use super::{
    ChunkPoolState,
    IoLease,
    IoPoolConfig,
    LeaseRecycler,
    LeaseSegment,
    PoolAvailabilityNotifier,
    PooledChunk,
};
use crate::errors::{Error, Result};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

/// Shared ingress pool used for inbound reads.
#[derive(Clone, Debug)]
pub struct IngressPool {
    config: IoPoolConfig,
    inner: Arc<Mutex<IngressPoolState>>,
}

impl IngressPool {
    pub(super) fn new(
        config: IoPoolConfig,
        availability_notifier: PoolAvailabilityNotifier,
    ) -> Self {
        let state = IngressPoolState {
            chunks: ChunkPoolState::new(config.clone()),
            availability_notifier,
        };
        Self {
            config,
            inner: Arc::new(Mutex::new(state)),
        }
    }

    /// Attempts to acquire one writable ingress chunk without blocking.
    ///
    /// Returns `Ok(None)` when the pool is currently out of capacity.
    pub fn try_acquire(&self) -> Result<Option<IngressBuffer>> {
        let chunk = {
            let mut state = self.lock_state()?;
            state.chunks.reserve_one()
        };

        Ok(chunk.map(|chunk| IngressBuffer {
            pool: Arc::downgrade(&self.inner),
            chunk: Some(chunk),
        }))
    }

    /// Returns the configuration used by this pool.
    pub fn config(&self) -> IoPoolConfig {
        self.config.clone()
    }

    pub(crate) fn has_available_capacity(&self) -> Result<bool> {
        let state = self.lock_state()?;
        Ok(state.has_available_chunk())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, IngressPoolState>> {
        self.inner.lock().map_err(|_| Error::IoBufferStatePoisoned {
            pool_kind: "ingress",
        })
    }
}

/// Internal ingress-side state guarded by [`IngressPool::inner`].
///
/// `availability_notifier` wakes the owning driver when ingress capacity transitions from
/// exhausted back to available.
#[derive(Debug)]
pub(super) struct IngressPoolState {
    pub(super) chunks: ChunkPoolState,
    availability_notifier: PoolAvailabilityNotifier,
}

impl IngressPoolState {
    /// Returns whether one chunk can currently be reserved without allocating beyond the limit.
    fn has_available_chunk(&self) -> bool {
        self.chunks.can_reserve_chunks(1)
    }

    /// Returns `true` when this return operation transitions the pool from exhausted to available.
    ///
    /// The caller must notify `availability_notifier` after dropping the mutex if this returns
    /// `true`.
    fn return_chunks(&mut self, chunks: Vec<PooledChunk>) -> bool {
        let had_available_capacity = self.has_available_chunk();
        self.chunks.return_chunks(chunks);
        !had_available_capacity && self.has_available_chunk()
    }

    pub(super) fn return_chunks_from_weak(inner: &Weak<Mutex<Self>>, chunks: Vec<PooledChunk>) {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        match inner.lock() {
            Ok(mut state) => {
                let should_notify = state.return_chunks(chunks);
                if should_notify {
                    let notifier = state.availability_notifier.clone();
                    // Drop the lock before notifying listeners to avoid immediate contention.
                    drop(state);
                    notifier.notify();
                }
            }
            Err(_) => {
                log::error!("ingress pool state is poisoned; dropping returned chunks");
            }
        };
    }
}

/// Writable ingress buffer acquired from the shared ingress pool.
pub struct IngressBuffer {
    pool: Weak<Mutex<IngressPoolState>>,
    chunk: Option<PooledChunk>,
}

impl IngressBuffer {
    /// Returns the full writable byte slice for the reserved chunk.
    pub fn writable(&mut self) -> &mut [u8] {
        self.chunk
            .as_deref_mut()
            .expect("ingress buffer used after commit")
    }

    /// Returns the chunk capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.chunk
            .as_deref()
            .expect("ingress buffer used after commit")
            .len()
    }

    /// Commits the written prefix of the buffer and returns it as an [`IoLease`].
    pub fn commit(mut self, written_bytes: usize) -> Result<IoLease> {
        let chunk = self.chunk.take().expect("ingress buffer committed twice");
        let chunk_capacity = chunk.len();
        if written_bytes == 0 || written_bytes > chunk_capacity {
            IngressPoolState::return_chunks_from_weak(&self.pool, vec![chunk]);
            return Err(Error::InvalidIngressCommitLength {
                written_bytes,
                chunk_capacity,
            });
        }

        let lease = IoLease::from_pooled(
            vec![LeaseSegment {
                chunk,
                written_len: written_bytes,
            }],
            LeaseRecycler::Ingress(self.pool.clone()),
        );
        Ok(lease)
    }
}

impl Drop for IngressBuffer {
    fn drop(&mut self) {
        if let Some(chunk) = self.chunk.take() {
            IngressPoolState::return_chunks_from_weak(&self.pool, vec![chunk]);
        }
    }
}
