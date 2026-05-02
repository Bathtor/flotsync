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
use bytes::{Buf, Bytes};
#[cfg(test)]
use futures_util::FutureExt;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll as TaskPoll},
};

mod egress;
mod ingress;

use self::{egress::EgressPoolState, ingress::IngressPoolState};
pub use self::{
    egress::{
        EgressAsyncWriter,
        EgressPool,
        EgressReservation,
        EgressReservedWriter,
        PayloadWriter,
    },
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
    #[must_use]
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
    #[must_use]
    pub fn config(&self) -> &IoBufferConfig {
        &self.config
    }

    /// Returns the shared ingress pool handle.
    #[must_use]
    pub fn ingress(&self) -> IngressPool {
        self.ingress.clone()
    }

    /// Returns the shared egress pool handle.
    #[must_use]
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
    payload: Arc<PooledPayload>,
    offset: usize,
    len: usize,
}

impl IoLease {
    fn from_pooled(segments: Vec<LeaseSegment>, recycler: LeaseRecycler) -> Self {
        let payload = Arc::new(PooledPayload::new(segments, recycler));
        let len = payload.len();
        Self {
            payload,
            offset: 0,
            len,
        }
    }

    /// Returns the total readable payload length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the payload is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Creates a fresh read cursor over the full payload.
    ///
    /// Each cursor advances independently, so the same payload can be read many times before the
    /// shared memory is finally released.
    #[must_use]
    pub fn cursor(&self) -> IoCursor {
        let mut cursor = PooledCursor::new(Arc::clone(&self.payload));
        if self.offset > 0 {
            cursor.advance(self.offset);
        }
        IoCursor {
            inner: cursor,
            remaining: self.len,
        }
    }

    /// Returns one sliced view over the readable payload bytes.
    #[must_use]
    pub fn try_slice(self, range: Range<usize>) -> Option<Self> {
        if range.start > range.end || range.end > self.len {
            return None;
        }
        Some(Self {
            payload: self.payload,
            offset: self.offset + range.start,
            len: range.len(),
        })
    }

    /// Returns the readable payload bytes when the visible lease range is already contiguous.
    ///
    /// This borrows directly from the underlying storage and therefore performs no allocation.
    /// It returns `None` when the visible range spans more than one pooled segment.
    #[must_use]
    pub fn try_as_contiguous_slice(&self) -> Option<&[u8]> {
        if self.is_empty() {
            return Some(&[]);
        }

        try_slice_one_pooled_segment(&self.payload, self.offset, self.len)
    }

    /// Creates a byte-clone of the full readable payload contents.
    #[must_use]
    pub fn create_byte_clone(&self) -> Bytes {
        Bytes::from(self.to_vec())
    }

    /// Copies the full readable payload contents into one owned `Vec<u8>`.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
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
        bytes
    }
}

fn try_slice_one_pooled_segment(
    payload: &PooledPayload,
    offset: usize,
    len: usize,
) -> Option<&[u8]> {
    if len == 0 {
        return Some(&[]);
    }

    let mut skipped = 0usize;
    for segment in &payload.segments {
        let segment_len = segment.written_len;
        if skipped + segment_len <= offset {
            skipped += segment_len;
            continue;
        }

        let segment_offset = offset.saturating_sub(skipped);
        let segment_end = segment_offset.checked_add(len)?;
        if segment_end > segment_len {
            return None;
        }
        return Some(&segment.chunk[segment_offset..segment_end]);
    }

    None
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
    inner: PooledCursor,
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
        let chunk = self.inner.chunk();
        &chunk[..self.remaining.min(chunk.len())]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.remaining, "advanced past end of IoCursor");
        self.inner.advance(cnt);
        self.remaining -= cnt;
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
        self.reservable_chunk_count() >= chunk_count
    }

    fn reservable_chunk_count(&self) -> usize {
        let free_chunks = self.available.len();
        let allocatable_chunks = self.config.max_chunk_count - self.allocated;
        free_chunks + allocatable_chunks
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

        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for flotsync_io pool request reply"
        );

        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
pub(super) fn poll_future_once<F>(future: std::pin::Pin<&mut F>) -> Option<F::Output>
where
    F: Future + ?Sized,
{
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.poll(&mut context) {
        TaskPoll::Ready(output) => Some(output),
        TaskPoll::Pending => None,
    }
}

#[cfg(test)]
pub(super) fn wait_for_future<F>(future: F) -> F::Output
where
    F: Future,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let mut future = Box::pin(future);

    loop {
        if let Some(output) = future.as_mut().now_or_never() {
            return output;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for flotsync_io future completion"
        );

        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{api::IoPayload, logging::default_runtime_logger, test_support::init_test_logger};
    use bytes::BufMut;

    fn tiny_config() -> IoPoolConfig {
        IoPoolConfig {
            chunk_size: 128,
            initial_chunk_count: 1,
            max_chunk_count: 2,
            encode_buf_min_free_space: 8,
        }
    }

    fn assert_exactly_one_chunk_of_quota_remains(pool: &EgressPool) {
        let chunk_size = pool.config().chunk_size;
        let second = wait_for_request(pool.reserve(chunk_size).expect("reserve remaining chunk"))
            .expect("exactly one chunk of quota should remain");
        let mut blocked = pool
            .reserve(chunk_size)
            .expect("request over the remaining quota should enqueue");
        assert!(
            blocked
                .try_receive()
                .expect("queued request channel must stay live")
                .is_none()
        );
        drop(second);
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
        assert_eq!(second_lease.to_vec().as_slice(), b"world");
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
    fn io_payload_as_contiguous_slice_returns_correct_bytes() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(pool.reserve(8).expect("reserve contiguous payload")).expect("ready");
        let lease = reservation
            .copy_bytes(b"hello")
            .expect("write contiguous payload");
        let mut contiguous = IoPayload::Lease(lease);
        assert_eq!(contiguous.try_as_contiguous_slice(), Some(&b"hello"[..]));
        assert_eq!(contiguous.as_contiguous_slice(), b"hello");
        assert!(matches!(contiguous, IoPayload::Lease(_)));

        let mut fragmented = IoPayload::chain([
            IoPayload::from_static(b"he"),
            IoPayload::from_static(b"llo"),
        ]);
        assert!(fragmented.try_as_contiguous_slice().is_none());
        assert_eq!(fragmented.as_contiguous_slice(), b"hello");
        assert!(matches!(&fragmented, IoPayload::Bytes(bytes) if bytes.as_ref() == b"hello"));
    }

    #[test]
    fn io_payload_as_contiguous_slice_releases_pooled_capacity_after_coalescing() {
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
            .expect("acquire ingress buffer");
        buffer.writable()[..4].copy_from_slice(b"ping");
        let lease = buffer.commit(4).expect("commit ingress buffer");

        assert!(pool.try_acquire().expect("ingress pool lock").is_none());

        let mut payload = IoPayload::chain([IoPayload::Lease(lease), IoPayload::from_static(b"!")]);
        assert_eq!(payload.as_contiguous_slice(), b"ping!");
        assert!(matches!(&payload, IoPayload::Bytes(bytes) if bytes.as_ref() == b"ping!"));

        assert!(pool.try_acquire().expect("ingress pool lock").is_some());
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

        assert_eq!(lease.to_vec().as_slice(), b"small payload");
        assert_eq!(second_lease.to_vec().as_slice(), b"next");
    }

    #[test]
    fn zero_byte_egress_write_returns_chunks_without_leaking_capacity() {
        init_test_logger();

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = EgressPool::new(config, default_runtime_logger());

        let writer = pool.writer(None);
        let payload = writer.finish().expect("finish empty async payload");
        assert!(payload.is_none());

        let second =
            wait_for_request(pool.reserve(64).expect("reserve second")).expect("second ready");
        drop(second);
    }

    #[test]
    fn async_egress_writer_serialises_multichunk_payload_without_a_hint() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let bytes = vec![b'x'; 200];
        let mut writer = pool.writer(None);

        wait_for_future(writer.write_slice(&bytes)).expect("async write succeeds");
        let payload = writer
            .finish()
            .expect("finish async payload")
            .expect("non-empty async payload");

        assert_eq!(payload.to_vec(), bytes);
    }

    #[test]
    fn async_egress_writer_stays_behind_queued_waiters() {
        init_test_logger();

        let config = IoPoolConfig {
            max_chunk_count: 1,
            ..tiny_config()
        };
        let pool = EgressPool::new(config, default_runtime_logger());

        let first =
            wait_for_request(pool.reserve(64).expect("reserve first")).expect("first ready");
        let first_lease = first.copy_bytes(b"held").expect("write first");

        let mut second = pool.reserve(64).expect("reserve second");
        assert!(second.try_receive().expect("second pending").is_none());

        let mut writer = pool.writer(None);
        let mut write_future = Box::pin(writer.write_slice(b"later"));
        assert!(poll_future_once(write_future.as_mut()).is_none());

        drop(first_lease);

        let second = wait_for_request(second).expect("second becomes ready first");
        assert!(poll_future_once(write_future.as_mut()).is_none());

        drop(second);

        wait_for_future(write_future).expect("async write completes");
        let payload = writer
            .finish()
            .expect("finish async payload")
            .expect("non-empty async payload");
        assert_eq!(payload.to_vec().as_slice(), b"later");
    }

    #[test]
    fn payload_writer_typed_helpers_encode_expected_bytes() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let mut writer = pool.writer(Some(32));

        wait_for_future(async {
            writer.write_bool(true).await?;
            writer.write_u16_be(0x1234).await?;
            writer.write_u16_le(0x5678).await?;
            writer.write_str("ok").await
        })
        .expect("typed payload writes succeed");

        let payload = writer
            .finish()
            .expect("finish typed helper payload")
            .expect("typed helper payload is non-empty");
        assert_eq!(
            payload.to_vec(),
            vec![1, 0x12, 0x34, 0x78, 0x56, b'o', b'k']
        );
    }

    #[test]
    fn reserved_sync_writer_can_interleave_with_async_writes() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let mut writer = pool.writer(Some(32));

        wait_for_future(async {
            {
                let mut reserved = writer
                    .write_with_reserved(3)
                    .await
                    .expect("reserve sync prefix");
                reserved.put_slice(b"abc");
            }
            writer.write_slice(b"de").await?;
            {
                let mut reserved = writer
                    .write_with_reserved(3)
                    .await
                    .expect("reserve sync suffix");
                reserved.put_slice(b"fgh");
            }
            Ok::<_, Error>(())
        })
        .expect("mixed sync/async writes succeed");

        let payload = writer
            .finish()
            .expect("finish mixed payload")
            .expect("mixed payload is non-empty");
        assert_eq!(payload.to_vec().as_slice(), b"abcdefgh");
    }

    #[test]
    fn reserved_sync_writer_leaves_unused_capacity_for_later_writes() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let mut writer = pool.writer(Some(16));

        wait_for_future(async {
            {
                let mut reserved = writer
                    .write_with_reserved(4)
                    .await
                    .expect("reserve sync bytes");
                reserved.put_slice(b"ab");
                assert_eq!(reserved.reserved_remaining(), 2);
            }
            writer.write_slice(b"cd").await
        })
        .expect("writer should reuse unused reserved capacity");

        let payload = writer
            .finish()
            .expect("finish payload with reused reservation")
            .expect("payload is non-empty");
        assert_eq!(payload.to_vec().as_slice(), b"abcd");
    }

    #[test]
    #[should_panic(expected = "EgressReservedWriter overflowed its reserved budget")]
    fn reserved_sync_writer_panics_when_written_past_reserved_budget() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let mut writer = pool.writer(Some(8));

        wait_for_future(async {
            let mut reserved = writer
                .write_with_reserved(2)
                .await
                .expect("reserve sync bytes");
            reserved.put_slice(b"toolong");
            Ok::<_, Error>(())
        })
        .expect("overflow panic should unwind through future");
    }

    #[test]
    fn payload_writer_splice_helpers_append_separate_zero_copy_fragments() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let mut writer = pool.writer(Some(8));

        wait_for_future(async {
            writer.write_slice(b"head").await?;
            writer.splice_bytes(Bytes::from_static(b"-")).await?;
            writer.splice_static(b"tail").await
        })
        .expect("splice helpers succeed");

        let payload = writer
            .finish()
            .expect("finish spliced payload")
            .expect("spliced payload is non-empty");
        assert_eq!(payload.to_vec().as_slice(), b"head-tail");
        let parts = match &payload {
            IoPayload::Chain(parts) => parts,
            other => panic!("expected chained payload after splice helpers, got {other:?}"),
        };
        assert_eq!(parts.len(), 3);
        assert!(matches!(&parts[0], IoPayload::Lease(_)));
        assert!(matches!(&parts[1], IoPayload::Bytes(bytes) if bytes.as_ref() == b"-"));
        assert!(matches!(&parts[2], IoPayload::Bytes(bytes) if bytes.as_ref() == b"tail"));
    }

    #[test]
    fn failed_adopt_keeps_current_pooled_fragment_open_for_copy_fallback() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let foreign_pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(foreign_pool.reserve(8).expect("reserve shared payload"))
                .expect("ready");
        let shared_lease = reservation
            .copy_bytes(b"-tail")
            .expect("write shared payload");
        let shared_payload = IoPayload::Lease(shared_lease.clone());

        let mut writer = pool.writer(Some(16));
        wait_for_future(async {
            writer.write_slice(b"head").await?;
            let error = writer
                .adopt_payload(shared_payload.clone())
                .await
                .expect_err("shared payload adoption must fail");
            assert!(matches!(error, Error::SharedIoPayloadOwnership));
            writer.copy_payload(&shared_payload).await
        })
        .expect("fallback to copied payload succeeds");

        let payload = writer
            .finish()
            .expect("finish fallback payload")
            .expect("fallback payload is non-empty");
        assert_eq!(payload.to_vec().as_slice(), b"head-tail");
        assert!(
            matches!(payload, IoPayload::Lease(_)),
            "failed adopt should not have sealed a separate payload part"
        );
    }

    #[test]
    fn shared_payload_already_owned_by_same_egress_pool_is_adopted() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(pool.reserve(8).expect("reserve shared payload")).expect("ready");
        let owned_lease = reservation
            .copy_bytes(b"tail")
            .expect("write owned payload");
        let shared_payload = IoPayload::Lease(owned_lease.clone());

        let adopted = pool
            .adopt_payload(shared_payload.clone())
            .expect("same-pool shared payload should adopt without copying");

        assert_eq!(adopted.to_vec().as_slice(), b"tail");
        assert_exactly_one_chunk_of_quota_remains(&pool);
    }

    #[test]
    fn shared_chain_already_owned_by_same_egress_pool_is_adopted() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(pool.reserve(8).expect("reserve pooled payload")).expect("ready");
        let owned_lease = reservation
            .copy_bytes(b"tail")
            .expect("write owned payload");
        let owned_payload = IoPayload::Lease(owned_lease.clone());
        let shared_chain = IoPayload::chain([
            IoPayload::from_static(b"head"),
            owned_payload.clone(),
            IoPayload::from_static(b"!"),
        ]);

        let adopted = pool
            .adopt_payload(shared_chain.clone())
            .expect("same-pool shared chain should adopt without copying");

        assert_eq!(adopted.to_vec().as_slice(), b"headtail!");
        assert_exactly_one_chunk_of_quota_remains(&pool);
    }

    #[test]
    fn writer_adopts_shared_payload_already_owned_by_same_egress_pool_zero_copy() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(pool.reserve(8).expect("reserve pooled payload")).expect("ready");
        let owned_lease = reservation
            .copy_bytes(b"tail")
            .expect("write owned payload");
        let shared_payload = IoPayload::Lease(owned_lease.clone());

        let mut writer = pool.writer(Some(16));
        wait_for_future(async { writer.adopt_payload(shared_payload.clone()).await })
            .expect("same-pool shared payload adopt should succeed");

        let payload = writer
            .finish()
            .expect("finish adopted payload")
            .expect("adopted payload is non-empty");
        assert_eq!(payload.to_vec().as_slice(), b"tail");
        assert!(
            matches!(payload, IoPayload::Lease(_)),
            "same-pool adopt should preserve the pooled lease without copying"
        );
        assert_exactly_one_chunk_of_quota_remains(&pool);
    }

    #[test]
    fn shared_chain_with_foreign_pooled_fragment_still_fails_adopt() {
        init_test_logger();

        let pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let foreign_pool = EgressPool::new(tiny_config(), default_runtime_logger());
        let reservation =
            wait_for_request(foreign_pool.reserve(8).expect("reserve foreign payload"))
                .expect("ready");
        let foreign_lease = reservation
            .copy_bytes(b"tail")
            .expect("write foreign payload");
        let shared_chain = IoPayload::chain([
            IoPayload::from_static(b"head"),
            IoPayload::Lease(foreign_lease.clone()),
        ]);

        let error = pool
            .adopt_payload(shared_chain.clone())
            .expect_err("shared foreign pooled fragment must still fail adoption");
        assert!(matches!(error, Error::SharedIoPayloadOwnership));
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
        assert_eq!(leased.to_vec().as_slice(), b"next");
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
