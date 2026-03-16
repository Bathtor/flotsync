use super::DriverToken;
use mio::Token;

const FIRST_RESOURCE_TOKEN_INDEX: usize = 1;

/// Dense slot registry backed by a vector plus a LIFO free-list.
///
/// Released slots are pushed onto `free` and reused from the top, so this behaves like a stack of
/// available slots rather than a full queue or set of indices.
#[derive(Debug)]
pub(super) struct SlotRegistry<T> {
    entries: Vec<Option<T>>,
    free: Vec<usize>,
}

impl<T> Default for SlotRegistry<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl<T> SlotRegistry<T> {
    pub(super) fn next_slot(&self) -> usize {
        self.free.last().copied().unwrap_or(self.entries.len())
    }

    pub(super) fn reserve(&mut self, value: T) -> usize {
        if let Some(slot) = self.free.pop() {
            self.entries[slot] = Some(value);
            return slot;
        }

        let slot = self.entries.len();
        self.entries.push(Some(value));
        slot
    }

    pub(super) fn remove(&mut self, slot: usize) -> Option<T> {
        if slot >= self.entries.len() {
            return None;
        }

        let value = self.entries[slot].take();
        if value.is_some() {
            self.free.push(slot);
        }
        value
    }

    pub(super) fn get(&self, slot: usize) -> Option<&T> {
        self.entries.get(slot).and_then(Option::as_ref)
    }

    pub(super) fn get_mut(&mut self, slot: usize) -> Option<&mut T> {
        self.entries.get_mut(slot).and_then(Option::as_mut)
    }

    #[cfg(test)]
    pub(super) fn iter(&self) -> impl Iterator<Item = (usize, &T)> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(slot, entry)| entry.as_ref().map(|value| (slot, value)))
    }

    pub(super) fn entries(&self) -> &[Option<T>] {
        &self.entries
    }
}

pub(super) fn readiness_slot_to_token(slot: usize) -> DriverToken {
    Token(slot + FIRST_RESOURCE_TOKEN_INDEX)
}

pub(super) fn token_to_readiness_slot(token: DriverToken) -> Option<usize> {
    token.0.checked_sub(FIRST_RESOURCE_TOKEN_INDEX)
}
