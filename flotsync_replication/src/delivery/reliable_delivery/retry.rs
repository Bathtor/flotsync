//! Retry scheduling for the reliable-delivery component.

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum RetryKey {
    Sender(MessageId),
    InboundAck(MessageId),
}

/// Monotonic retry scheduler shared across all sender and recipient-ack retries.
///
/// This keeps per-message due times but only arms one Kompact timer for the
/// earliest known retry, which scales better than holding one timer per
/// outstanding reliable-delivery work item.
#[derive(Debug)]
pub(super) struct RetryQueue {
    due_by_key: HashMap<RetryKey, Instant>,
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
}
