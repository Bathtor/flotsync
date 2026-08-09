//! Retry scheduling for the reliable-delivery component.

#[allow(
    clippy::wildcard_imports,
    reason = "The private delivery helper shares its parent's local implementation vocabulary."
)]
use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum RetryKey {
    /// A plaintext submission that has not yet passed security admission and
    /// therefore has no stored encrypted envelope.
    Unpersisted(MessageId),
    /// A persisted outbound envelope waiting for retry or semantic ack timeout.
    Sender(MessageId),
    /// A sender row whose removal failed and must be retried without making the
    /// corresponding message transport-eligible again.
    SenderRemoval(MessageId),
    /// A receiver-generated semantic acknowledgement waiting for a route retry.
    InboundAck(MessageId),
}

/// Monotonic scheduler shared by unpersisted admission, persisted sender,
/// stored-row cleanup, and recipient-ack retries.
///
/// This keeps per-message due times but only schedules one Kompact timer for the
/// earliest known retry, which scales better than holding one timer per
/// outstanding reliable-delivery work item.
#[derive(Debug)]
pub(super) struct RetryQueue {
    /// Authoritative due time for every currently scheduled retry key.
    due_by_key: HashMap<RetryKey, Instant>,
    /// Earliest-first candidates; rescheduling may leave stale entries that
    /// queue operations discard by comparing them with `due_by_key`.
    due_heap: BinaryHeap<Reverse<(Instant, RetryKey)>>,
}

impl RetryQueue {
    pub(super) fn new() -> Self {
        Self {
            due_by_key: HashMap::new(),
            due_heap: BinaryHeap::new(),
        }
    }

    pub(super) fn schedule(&mut self, key: RetryKey, due_at: Instant) {
        self.due_by_key.insert(key, due_at);
        self.due_heap.push(Reverse((due_at, key)));
    }

    pub(super) fn cancel(&mut self, key: RetryKey) {
        self.due_by_key.remove(&key);
    }

    pub(super) fn remove_stale_entries(&mut self) {
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            let Some(current_due_at) = self.due_by_key.get(&key).copied() else {
                self.due_heap.pop();
                continue;
            };
            if current_due_at != due_at {
                self.due_heap.pop();
                continue;
            }
            break;
        }
    }

    pub(super) fn next_due_at(&mut self) -> Option<Instant> {
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            match self.due_by_key.get(&key).copied() {
                Some(current_due_at) if current_due_at == due_at => return Some(due_at),
                _ => {
                    self.due_heap.pop();
                }
            }
        }
        None
    }

    pub(super) fn take_ready(&mut self, now: Instant) -> Vec<RetryKey> {
        let mut ready = Vec::new();
        while let Some(Reverse((due_at, key))) = self.due_heap.peek().copied() {
            let Some(current_due_at) = self.due_by_key.get(&key).copied() else {
                self.due_heap.pop();
                continue;
            };
            if current_due_at != due_at {
                self.due_heap.pop();
                continue;
            }
            if due_at > now {
                break;
            }
            self.due_heap.pop();
            self.due_by_key.remove(&key);
            ready.push(key);
        }
        ready
    }

    /// Return whether one retry key currently has a live due time.
    #[cfg(test)]
    pub(super) fn contains(&self, key: RetryKey) -> bool {
        self.due_by_key.contains_key(&key)
    }
}
