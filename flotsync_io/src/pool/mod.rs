//! Shared ingress and egress buffer pools for lease-backed I/O.
//!
//! The pool layer gives the driver two different acquisition styles:
//! - ingress is `try`-based because reads happen in reaction to socket readiness and may need to
//!   decline work immediately when no capacity is available;
//! - egress is async and FIFO because producers can wait until enough pooled memory is available
//!   to serialise a payload.
//!
//! Both sides hand out [`IoLease`] values. These keep the underlying chunks alive until drop and
//! return pooled capacity to the owning side precisely when the last shared payload handle and all
//! derived read cursors go away.
//!
//! Reading is modeled separately through [`IoCursor`]. An `IoLease` is immutable and cheaply
//! cloneable, while each `IoCursor` tracks one consumer's read progress independently.

use crate::{
    api::MAX_UDP_PAYLOAD_BYTES,
    errors::{Error, Result},
    logging::{RuntimeLogger, default_runtime_logger},
};
use ::kompact::prelude::{ChunkLease, ChunkRef};
use bytes::{Buf, Bytes};
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll as TaskPoll},
};

mod egress;
mod ingress;

use self::{egress::EgressPoolState, ingress::IngressPoolState};
pub use self::{
    egress::{EgressPool, EgressReservation, IoBufWriter},
    ingress::{IngressBuffer, IngressPool},
};

/// Configuration for a single shared pool.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IoPoolConfig {
    /// Size of each reusable chunk in bytes.
    pub chunk_size: usize,
    /// Number of chunks to allocate eagerly when the pool starts.
    pub initial_chunk_count: usize,
    /// Maximum number of chunks the pool may keep alive at once.
    pub max_chunk_count: usize,
    /// Minimum spare capacity that writers prefer to keep in a chunk.
    ///
    /// This mirrors Kompact's buffer tuning surface so the pool config can stay compatible even
    /// though `flotsync_io` does not currently delegate to Kompact's internal `BufferPool`.
    pub encode_buf_min_free_space: usize,
}

impl IoPoolConfig {
    /// Validates the pool configuration.
    pub fn validate(&self) -> Result<()> {
        if self.initial_chunk_count > self.max_chunk_count {
            return Err(Error::InvalidIoPoolConfig {
                details: format!(
                    "initial_chunk_count ({}) may not exceed max_chunk_count ({})",
                    self.initial_chunk_count, self.max_chunk_count
                ),
            });
        }
        if self.chunk_size <= self.encode_buf_min_free_space {
            return Err(Error::InvalidIoPoolConfig {
                details: format!(
                    "chunk_size ({}) must be greater than encode_buf_min_free_space ({})",
                    self.chunk_size, self.encode_buf_min_free_space
                ),
            });
        }
        if self.chunk_size < 128 {
            return Err(Error::InvalidIoPoolConfig {
                details: format!("chunk_size ({}) must be at least 128", self.chunk_size),
            });
        }
        if self.max_chunk_count < 2 {
            return Err(Error::InvalidIoPoolConfig {
                details: format!(
                    "max_chunk_count ({}) must be at least 2",
                    self.max_chunk_count
                ),
            });
        }
        Ok(())
    }

    /// Returns the maximum number of bytes that can be backed by this pool at once.
    pub fn total_capacity_bytes(&self) -> usize {
        self.chunk_size * self.max_chunk_count
    }
}

impl Default for IoPoolConfig {
    fn default() -> Self {
        Self {
            chunk_size: MAX_UDP_PAYLOAD_BYTES,
            initial_chunk_count: 100,
            max_chunk_count: 100_000,
            encode_buf_min_free_space: 64,
        }
    }
}

/// Configuration for the ingress and egress shared pools owned by one `IoDriver`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IoBufferConfig {
    /// Pool used for inbound reads before payloads are delivered to components.
    pub ingress: IoPoolConfig,
    /// Pool used for outbound serialisation before payloads are handed to the driver.
    pub egress: IoPoolConfig,
}

impl IoBufferConfig {
    /// Validates both ingress and egress pool configurations.
    pub fn validate(&self) -> Result<()> {
        self.ingress.validate()?;
        self.egress.validate()?;
        Ok(())
    }
}

/// Shared ingress and egress pool handles owned by one `IoDriver`.
#[derive(Clone, Debug)]
pub struct IoBufferPools {
    config: IoBufferConfig,
    ingress: IngressPool,
    egress: EgressPool,
    ingress_notifier: PoolAvailabilityNotifier,
}

impl IoBufferPools {
    /// Creates the shared ingress and egress pools.
    pub fn new(config: IoBufferConfig) -> Result<Self> {
        Self::new_with_logger(config, default_runtime_logger())
    }

    /// Creates the shared ingress and egress pools using the supplied runtime logger.
    pub(crate) fn new_with_logger(config: IoBufferConfig, logger: RuntimeLogger) -> Result<Self> {
        let ingress_notifier = PoolAvailabilityNotifier::new(|| {});
        Self::new_with_logger_and_ingress_notifier(config, logger, ingress_notifier)
    }

    /// Creates the shared ingress and egress pools using the supplied runtime logger and ingress
    /// availability notifier.
    pub(crate) fn new_with_logger_and_ingress_notifier(
        config: IoBufferConfig,
        logger: RuntimeLogger,
        ingress_notifier: PoolAvailabilityNotifier,
    ) -> Result<Self> {
        config.validate()?;

        let ingress = IngressPool::new(
            config.ingress.clone(),
            logger.clone(),
            ingress_notifier.clone(),
        );
        let egress = EgressPool::new(config.egress.clone(), logger);

        Ok(Self {
            config,
            ingress,
            egress,
            ingress_notifier,
        })
    }

    /// Returns the pool configuration used by these shared pools.
    pub fn config(&self) -> &IoBufferConfig {
        &self.config
    }

    /// Returns the shared ingress pool handle.
    pub fn ingress(&self) -> IngressPool {
        self.ingress.clone()
    }

    /// Returns the shared egress pool handle.
    pub fn egress(&self) -> EgressPool {
        self.egress.clone()
    }

    pub(crate) fn replace_ingress_notifier(&self, callback: impl Fn() + Send + Sync + 'static) {
        self.ingress_notifier.replace(callback);
    }

    pub(crate) fn replace_runtime_logger(&self, logger: RuntimeLogger) -> Result<()> {
        self.ingress.replace_logger(logger.clone())?;
        self.egress.replace_logger(logger)?;
        Ok(())
    }
}

/// Awaitable result handle for async pool operations.
#[derive(Debug)]
pub struct PoolRequest<T> {
    receiver: futures_channel::oneshot::Receiver<Result<T>>,
}

impl<T> PoolRequest<T> {
    pub(super) fn new(receiver: futures_channel::oneshot::Receiver<Result<T>>) -> Self {
        Self { receiver }
    }

    /// Attempts to retrieve the completed pool reply without blocking.
    pub fn try_receive(&mut self) -> Result<Option<T>> {
        match self.receiver.try_recv() {
            Ok(Some(reply)) => reply.map(Some),
            Ok(None) => Ok(None),
            Err(_) => Err(Error::IoBufferRequestChannelClosed),
        }
    }
}

impl<T> Future for PoolRequest<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> TaskPoll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            TaskPoll::Ready(Ok(reply)) => TaskPoll::Ready(reply),
            TaskPoll::Ready(Err(_)) => TaskPoll::Ready(Err(Error::IoBufferRequestChannelClosed)),
            TaskPoll::Pending => TaskPoll::Pending,
        }
    }
}

/// Driver-internal callback invoked when a pool transitions from exhausted to available again.
///
/// The current use-site is ingress read resumption: a dropped lease can return capacity from any
/// thread, so the driver needs an out-of-band wakeup that does not rely on further socket traffic.
#[derive(Clone)]
pub(crate) struct PoolAvailabilityNotifier {
    callback: Arc<Mutex<Arc<dyn Fn() + Send + Sync + 'static>>>,
}

impl PoolAvailabilityNotifier {
    pub(crate) fn new(callback: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            callback: Arc::new(Mutex::new(Arc::new(callback))),
        }
    }

    pub(crate) fn replace(&self, callback: impl Fn() + Send + Sync + 'static) {
        let callback = Arc::new(callback);
        match self.callback.lock() {
            Ok(mut current) => {
                *current = callback;
            }
            Err(poisoned) => {
                *poisoned.into_inner() = callback;
            }
        }
    }

    fn notify(&self) {
        let callback = match self.callback.lock() {
            Ok(current) => Arc::clone(&current),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        };
        (callback)();
    }
}

impl fmt::Debug for PoolAvailabilityNotifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PoolAvailabilityNotifier(..)")
    }
}

/// Immutable lease-backed payload memory used by `IoPayload::Lease`.
///
/// `IoLease` owns the underlying memory lifetime but does not track read progress. Call
/// [`IoLease::cursor`] to create an [`IoCursor`] for one consumer.
#[derive(Clone)]
pub struct IoLease {
    /* `IoLease` deliberately hides whether the payload comes from a `flotsync_io` pool or from an externally produced Kompact buffer. */
    inner: IoLeaseInner,
    offset: usize,
    len: usize,
}

impl IoLease {
    fn from_pooled(segments: Vec<LeaseSegment>, recycler: LeaseRecycler) -> Self {
        let payload = Arc::new(PooledPayload::new(segments, recycler));
        let len = payload.len();
        Self {
            inner: IoLeaseInner::Pooled(payload),
            offset: 0,
            len,
        }
    }

    /// Wraps an externally produced Kompact `ChunkLease` in the `IoLease` abstraction.
    ///
    /// The mutable `ChunkLease` is converted into an immutable [`ChunkRef`] immediately so the
    /// shared payload can be cloned cheaply and each consumer can create an independent cursor.
    pub fn from_chunk_lease(lease: ChunkLease) -> Self {
        let chunk_ref = lease.into_chunk_ref();
        let len = chunk_ref.remaining();
        Self {
            inner: IoLeaseInner::External(chunk_ref),
            offset: 0,
            len,
        }
    }

    /// Wraps an externally produced immutable Kompact `ChunkRef`.
    ///
    /// The wrapped payload starts at the `ChunkRef`'s current readable position and future cursors
    /// will all see that same readable region.
    pub fn from_chunk_ref(chunk_ref: ChunkRef) -> Self {
        let len = chunk_ref.remaining();
        Self {
            inner: IoLeaseInner::External(chunk_ref),
            offset: 0,
            len,
        }
    }

    /// Returns the total readable payload length in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a fresh read cursor over the full payload.
    ///
    /// Each cursor advances independently, so the same payload can be read many times before the
    /// shared memory is finally released.
    pub fn cursor(&self) -> IoCursor {
        let mut inner = match &self.inner {
            IoLeaseInner::External(chunk_ref) => IoCursorInner::External(chunk_ref.clone()),
            IoLeaseInner::Pooled(payload) => {
                IoCursorInner::Pooled(PooledCursor::new(Arc::clone(payload)))
            }
        };
        if self.offset > 0 {
            inner.advance(self.offset);
        }
        IoCursor {
            inner,
            remaining: self.len,
        }
    }

    /// Returns one sliced view over the readable payload bytes.
    pub fn try_slice(self, offset: usize, len: usize) -> Option<Self> {
        if offset > self.len || len > self.len.saturating_sub(offset) {
            return None;
        }
        Some(Self {
            inner: self.inner,
            offset: self.offset + offset,
            len,
        })
    }

    /// Creates a byte-clone of the full readable payload contents.
    pub fn create_byte_clone(&self) -> Bytes {
        if self.is_empty() {
            return Bytes::new();
        }

        let mut bytes = Vec::with_capacity(self.len());
        let mut cursor = self.cursor();
        while cursor.has_remaining() {
            let chunk = cursor.chunk();
            let chunk_len = chunk.len();
            debug_assert!(
                chunk_len > 0,
                "IoLease cursor produced an empty chunk while bytes remained"
            );
            bytes.extend_from_slice(chunk);
            cursor.advance(chunk_len);
        }
        Bytes::from(bytes)
    }
}

impl From<ChunkLease> for IoLease {
    fn from(lease: ChunkLease) -> Self {
        Self::from_chunk_lease(lease)
    }
}

impl From<ChunkRef> for IoLease {
    fn from(chunk_ref: ChunkRef) -> Self {
        Self::from_chunk_ref(chunk_ref)
    }
}

impl fmt::Debug for IoLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoLease").field("len", &self.len()).finish()
    }
}

/// Cursor over an [`IoLease`]'s readable bytes.
///
/// This type owns one consumer's read position and implements [`bytes::Buf`]. Cloning a cursor
/// duplicates the current position rather than rewinding to the start.
#[derive(Clone)]
pub struct IoCursor {
    inner: IoCursorInner,
    remaining: usize,
}

impl fmt::Debug for IoCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoCursor")
            .field("remaining", &self.remaining())
            .finish()
    }
}

impl Buf for IoCursor {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }
        let chunk = match &self.inner {
            IoCursorInner::External(chunk_ref) => chunk_ref.chunk(),
            IoCursorInner::Pooled(cursor) => cursor.chunk(),
        };
        &chunk[..self.remaining.min(chunk.len())]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining, "advanced past end of IoCursor");
        match &mut self.inner {
            IoCursorInner::External(chunk_ref) => chunk_ref.advance(cnt),
            IoCursorInner::Pooled(cursor) => cursor.advance(cnt),
        }
        self.remaining -= cnt;
    }
}

#[derive(Clone)]
enum IoLeaseInner {
    External(ChunkRef),
    Pooled(Arc<PooledPayload>),
}

#[derive(Clone)]
enum IoCursorInner {
    External(ChunkRef),
    Pooled(PooledCursor),
}

impl IoCursorInner {
    fn advance(&mut self, cnt: usize) {
        match self {
            Self::External(chunk_ref) => chunk_ref.advance(cnt),
            Self::Pooled(cursor) => cursor.advance(cnt),
        }
    }
}

/// Shared immutable pooled payload representation.
///
/// Each segment owns one chunk and records how many bytes in that chunk belong to the readable
/// payload. The final chunk is often only partially used, so we cannot rely on the chunk capacity.
struct PooledPayload {
    segments: Vec<LeaseSegment>,
    len: usize,
    recycler: LeaseRecycler,
}

impl PooledPayload {
    fn new(segments: Vec<LeaseSegment>, recycler: LeaseRecycler) -> Self {
        let len = segments.iter().map(|segment| segment.written_len).sum();
        Self {
            segments,
            len,
            recycler,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

impl Drop for PooledPayload {
    fn drop(&mut self) {
        let chunks = self
            .segments
            .drain(..)
            .map(|segment| segment.chunk)
            .collect::<Vec<_>>();
        self.recycler.recycle(chunks);
    }
}

/// Read cursor for one shared pooled payload.
///
/// `remaining` always refers to the bytes still visible through this cursor, starting from the
/// current `(segment_index, segment_offset)` position inside the shared payload segments.
#[derive(Clone)]
struct PooledCursor {
    payload: Arc<PooledPayload>,
    segment_index: usize,
    segment_offset: usize,
    remaining: usize,
}

impl PooledCursor {
    fn new(payload: Arc<PooledPayload>) -> Self {
        let remaining = payload.len();
        Self {
            payload,
            segment_index: 0,
            segment_offset: 0,
            remaining,
        }
    }

    fn chunk(&self) -> &[u8] {
        if self.remaining == 0 {
            return &[];
        }

        let segment = &self.payload.segments[self.segment_index];
        &segment.chunk[self.segment_offset..segment.written_len]
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.remaining, "advanced past end of IoCursor");

        while cnt > 0 {
            let segment = &self.payload.segments[self.segment_index];
            let available = segment.written_len - self.segment_offset;
            if cnt < available {
                self.segment_offset += cnt;
                self.remaining -= cnt;
                return;
            }

            cnt -= available;
            self.remaining -= available;
            self.segment_index += 1;
            self.segment_offset = 0;
        }
    }
}

/// One readable payload segment within a shared pooled payload.
struct LeaseSegment {
    chunk: PooledChunk,
    /// Number of payload bytes written into this chunk.
    written_len: usize,
}

/// Lease return target for pooled payload memory.
enum LeaseRecycler {
    Ingress(Weak<Mutex<IngressPoolState>>),
    Egress(Weak<Mutex<EgressPoolState>>),
}

impl LeaseRecycler {
    fn recycle(&self, chunks: Vec<PooledChunk>) {
        match self {
            Self::Ingress(inner) => IngressPoolState::return_chunks_from_weak(inner, chunks),
            Self::Egress(inner) => EgressPoolState::return_chunks_from_weak(inner, chunks),
        }
    }

    fn release_live_chunks(&self, chunk_count: usize) -> Result<()> {
        match self {
            Self::Ingress(inner) => {
                IngressPoolState::release_live_chunks_from_weak(inner, chunk_count)
            }
            Self::Egress(inner) => {
                EgressPoolState::release_live_chunks_from_weak(inner, chunk_count)
            }
        }
    }

    fn is_owned_by_egress(&self, inner: &Arc<Mutex<EgressPoolState>>) -> bool {
        match self {
            Self::Ingress(_) => false,
            Self::Egress(current) => current.ptr_eq(&Arc::downgrade(inner)),
        }
    }
}

type PooledChunk = Box<[u8]>;

/// Shared chunk allocator state used by both ingress and egress.
///
/// `available` is a FIFO queue of reusable chunks. `allocated` tracks total live chunks, including
/// chunks currently leased out to callers, so capacity decisions remain correct even while memory is
/// in flight.
#[derive(Debug)]
struct ChunkPoolState {
    config: IoPoolConfig,
    available: VecDeque<PooledChunk>,
    allocated: usize,
}

impl ChunkPoolState {
    fn new(config: IoPoolConfig) -> Self {
        let initial_chunk_count = config.initial_chunk_count;
        let mut available = VecDeque::with_capacity(config.initial_chunk_count);
        for _ in 0..config.initial_chunk_count {
            available.push_back(new_chunk(config.chunk_size));
        }
        Self {
            config,
            available,
            allocated: initial_chunk_count,
        }
    }

    fn reserve_one(&mut self) -> Option<PooledChunk> {
        if let Some(chunk) = self.available.pop_front() {
            return Some(chunk);
        }
        if self.allocated < self.config.max_chunk_count {
            self.allocated += 1;
            Some(new_chunk(self.config.chunk_size))
        } else {
            None
        }
    }

    /// Returns whether the pool can currently satisfy `chunk_count` chunk reservations.
    fn can_reserve_chunks(&self, chunk_count: usize) -> bool {
        let free_chunks = self.available.len();
        let allocatable_chunks = self.config.max_chunk_count - self.allocated;
        free_chunks + allocatable_chunks >= chunk_count
    }

    fn reserve_chunks(&mut self, chunk_count: usize) -> Option<Vec<PooledChunk>> {
        if !self.can_reserve_chunks(chunk_count) {
            return None;
        }

        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            let chunk = self.reserve_one()?;
            chunks.push(chunk);
        }
        Some(chunks)
    }

    fn return_chunks(&mut self, chunks: impl IntoIterator<Item = PooledChunk>) {
        self.available.extend(chunks);
    }

    fn can_import_live_chunks(&self, chunk_count: usize) -> bool {
        self.allocated + chunk_count <= self.config.max_chunk_count
    }

    fn import_live_chunks(&mut self, chunk_count: usize) -> bool {
        if !self.can_import_live_chunks(chunk_count) {
            return false;
        }
        self.allocated += chunk_count;
        true
    }

    fn release_live_chunks(&mut self, chunk_count: usize) {
        let live_chunks = self.allocated.saturating_sub(self.available.len());
        assert!(
            chunk_count <= live_chunks,
            "released more live chunks than the pool currently owns"
        );
        self.allocated -= chunk_count;
    }
}

fn new_chunk(size: usize) -> PooledChunk {
    vec![0u8; size].into_boxed_slice()
}

fn chunks_for_bytes(requested_bytes: usize, chunk_size: usize) -> usize {
    requested_bytes.div_ceil(chunk_size)
}

#[cfg(test)]
pub(super) fn wait_for_request<T>(mut request: PoolRequest<T>) -> Result<T> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    loop {
        if let Some(reply) = request.try_receive()? {
            return Ok(reply);
        }

        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for flotsync_io pool request reply");
        }

        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{logging::default_runtime_logger, test_support::init_test_logger};

    fn tiny_config() -> IoPoolConfig {
        IoPoolConfig {
            chunk_size: 128,
            initial_chunk_count: 1,
            max_chunk_count: 2,
            encode_buf_min_free_space: 8,
        }
    }

    #[test]
    fn ingress_notifier_fires_after_last_shared_lease_drop() {
        init_test_logger();
        let notifications = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let notifications_clone = Arc::clone(&notifications);

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = IngressPool::new(
            config,
            default_runtime_logger(),
            PoolAvailabilityNotifier::new(move || {
                notifications_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
        );
        let mut buffer = pool
            .try_acquire()
            .expect("ingress pool lock")
            .expect("acquire ingress buffer");
        buffer.writable()[..4].copy_from_slice(b"ping");
        let lease = buffer.commit(4).expect("commit ingress buffer");
        let lease_clone = lease.clone();

        assert_eq!(notifications.load(std::sync::atomic::Ordering::SeqCst), 0);

        drop(lease);
        assert_eq!(notifications.load(std::sync::atomic::Ordering::SeqCst), 0);

        drop(lease_clone);

        assert_eq!(notifications.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn ingress_try_acquire_reuses_returned_capacity() {
        init_test_logger();

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = IngressPool::new(
            config,
            default_runtime_logger(),
            PoolAvailabilityNotifier::new(|| {}),
        );

        let mut buffer = pool
            .try_acquire()
            .expect("ingress pool lock")
            .expect("first acquire");
        buffer.writable()[..3].copy_from_slice(b"abc");
        let lease = buffer.commit(3).expect("commit");

        assert!(pool.try_acquire().expect("ingress pool lock").is_none());

        drop(lease);

        assert!(pool.try_acquire().expect("ingress pool lock").is_some());
    }

    #[test]
    fn egress_waiters_are_fifo_and_woken_by_lease_drop() {
        init_test_logger();

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = EgressPool::new(config, default_runtime_logger());

        let first =
            wait_for_request(pool.reserve(64).expect("reserve first")).expect("first ready");
        let mut second = pool.reserve(64).expect("reserve second");
        assert!(second.try_receive().expect("second pending").is_none());

        let first_lease = first.copy_bytes(b"hello").expect("write first");
        let first_lease_clone = first_lease.clone();
        assert!(second.try_receive().expect("still pending").is_none());

        drop(first_lease);
        assert!(second.try_receive().expect("still pending").is_none());

        drop(first_lease_clone);

        let second = wait_for_request(second).expect("second ready after drop");
        let second_lease = second.copy_bytes(b"world").expect("write second");
        assert_eq!(
            second_lease.create_byte_clone(),
            Bytes::from_static(b"world")
        );
    }

    #[test]
    fn io_lease_cursors_are_independent() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(pool.reserve(5).expect("reserve payload")).expect("payload ready");
        let lease = reservation.copy_bytes(b"hello").expect("write payload");
        let cloned_lease = lease.clone();

        let mut first_cursor = lease.cursor();
        first_cursor.advance(2);

        let mut second_cursor = cloned_lease.cursor();
        let second_bytes = second_cursor.copy_to_bytes(second_cursor.remaining());

        assert_eq!(
            first_cursor.copy_to_bytes(first_cursor.remaining()),
            b"llo"[..]
        );
        assert_eq!(second_bytes, Bytes::from_static(b"hello"));
    }

    #[test]
    fn egress_writer_returns_unused_chunks_immediately() {
        init_test_logger();

        let config = IoPoolConfig {
            chunk_size: 128,
            initial_chunk_count: 2,
            max_chunk_count: 2,
            encode_buf_min_free_space: 8,
        };
        let pool = EgressPool::new(config, default_runtime_logger());

        let first =
            wait_for_request(pool.reserve(256).expect("reserve first")).expect("first ready");
        let lease = first.copy_bytes(b"small payload").expect("small write");

        let second =
            wait_for_request(pool.reserve(128).expect("reserve second")).expect("second ready");
        let second_lease = second.copy_bytes(b"next").expect("next write");

        assert_eq!(
            lease.create_byte_clone(),
            Bytes::from_static(b"small payload")
        );
        assert_eq!(
            second_lease.create_byte_clone(),
            Bytes::from_static(b"next")
        );
    }

    #[test]
    fn zero_byte_egress_write_returns_chunks_without_leaking_capacity() {
        init_test_logger();

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = EgressPool::new(config, default_runtime_logger());

        let first =
            wait_for_request(pool.reserve(64).expect("reserve first")).expect("first ready");
        let ((), lease) = first
            .write_with_optional(|_writer| Ok(()))
            .expect("zero-byte write succeeds");
        assert!(lease.is_none());

        let second =
            wait_for_request(pool.reserve(64).expect("reserve second")).expect("second ready");
        drop(second);
    }

    #[test]
    fn adopted_ingress_lease_releases_ingress_capacity_and_consumes_egress_capacity() {
        init_test_logger();

        let config = IoBufferConfig {
            ingress: IoPoolConfig {
                chunk_size: 128,
                initial_chunk_count: 2,
                max_chunk_count: 2,
                encode_buf_min_free_space: 8,
            },
            egress: IoPoolConfig {
                chunk_size: 128,
                initial_chunk_count: 0,
                max_chunk_count: 2,
                encode_buf_min_free_space: 8,
            },
        };
        let pools = IoBufferPools::new(config).expect("create shared pools");

        let held_ingress = pools
            .ingress()
            .try_acquire()
            .expect("lock ingress")
            .expect("acquire held ingress chunk");
        let mut ingress = pools
            .ingress()
            .try_acquire()
            .expect("lock ingress")
            .expect("acquire ingress chunk");
        ingress.writable()[..4].copy_from_slice(b"echo");
        let lease = ingress.commit(4).expect("commit ingress lease");

        assert!(
            pools
                .ingress()
                .try_acquire()
                .expect("lock ingress")
                .is_none()
        );

        let adopted = pools
            .egress()
            .adopt_lease(lease)
            .expect("adopt ingress lease");

        assert!(
            pools
                .ingress()
                .try_acquire()
                .expect("lock ingress")
                .is_some()
        );

        let mut pending = pools
            .egress()
            .reserve(256)
            .expect("reserve queued egress waiter");
        assert!(pending.try_receive().expect("poll queued waiter").is_none());

        drop(adopted);
        drop(held_ingress);

        let reservation = wait_for_request(pending).expect("ready after adopted lease drop");
        let leased = reservation
            .copy_bytes(b"next")
            .expect("write next egress payload");
        assert_eq!(leased.create_byte_clone(), Bytes::from_static(b"next"));
    }

    #[test]
    fn io_buffer_config_validation_rejects_bad_values() {
        init_test_logger();

        let mut config = IoBufferConfig::default();
        config.egress.chunk_size = 64;

        let error = IoBufferPools::new(config).expect_err("invalid config rejected");
        assert!(matches!(error, Error::InvalidIoPoolConfig { .. }));
    }
}
