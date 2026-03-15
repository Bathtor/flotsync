use super::{
    ChunkPoolState,
    IoLease,
    IoPoolConfig,
    LeaseRecycler,
    LeaseSegment,
    PoolRequest,
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
    pub(super) fn new(config: IoPoolConfig) -> Self {
        let state = IngressPoolState {
            chunks: ChunkPoolState::new(config.clone()),
            waiters: Vec::new(),
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

    /// Returns a request that resolves once ingress capacity may be available again.
    pub fn wait_for_available(&self) -> Result<PoolRequest<()>> {
        let (reply_tx, reply_rx) = futures_channel::oneshot::channel();
        let mut reply_tx = Some(reply_tx);
        let send_now = {
            let mut state = self.lock_state()?;
            if state.has_available_chunk() {
                true
            } else {
                state
                    .waiters
                    .push(reply_tx.take().expect("reply sender already taken"));
                false
            }
        };

        if send_now {
            let _ = reply_tx
                .take()
                .expect("reply sender missing for immediate wake")
                .send(Ok(()));
        }

        Ok(PoolRequest::new(reply_rx))
    }

    /// Returns the configuration used by this pool.
    pub fn config(&self) -> IoPoolConfig {
        self.config.clone()
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, IngressPoolState>> {
        self.inner.lock().map_err(|_| Error::IoBufferStatePoisoned {
            pool_kind: "ingress",
        })
    }
}

/// Internal ingress-side state guarded by [`IngressPool::inner`].
///
/// `waiters` holds tasks that want a notification when at least one chunk becomes available again.
#[derive(Debug)]
pub(super) struct IngressPoolState {
    pub(super) chunks: ChunkPoolState,
    waiters: Vec<futures_channel::oneshot::Sender<Result<()>>>,
}

impl IngressPoolState {
    fn has_available_chunk(&self) -> bool {
        self.chunks.can_reserve_chunks(1)
    }

    fn return_chunks(
        &mut self,
        chunks: Vec<PooledChunk>,
    ) -> Vec<futures_channel::oneshot::Sender<Result<()>>> {
        self.chunks.return_chunks(chunks);
        std::mem::take(&mut self.waiters)
    }

    pub(super) fn return_chunks_from_weak(inner: &Weak<Mutex<Self>>, chunks: Vec<PooledChunk>) {
        let Some(inner) = inner.upgrade() else {
            return;
        };

        let waiters = match inner.lock() {
            Ok(mut state) => state.return_chunks(chunks),
            Err(_) => {
                log::error!("ingress pool state is poisoned; dropping returned chunks");
                return;
            }
        };

        for waiter in waiters {
            let _ = waiter.send(Ok(()));
        }
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
