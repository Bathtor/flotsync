//! Small framed-ingress helpers built on top of chunked payload delivery.
//!
//! The collector here intentionally does only three things:
//! - bound how much data gets buffered before a delimiter is found;
//! - detect a delimiter across chunk boundaries;
//! - report where trailing bytes begin inside the current chunk once a frame completes.

/// Outcome of pushing one chunk into a [`BoundedCollector`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CollectUntil<'a> {
    /// The delimiter has not been observed yet and the caller should feed more bytes.
    NeedMore,
    /// The delimiter was found and the caller may inspect the completed frame bytes.
    Complete {
        /// Buffered frame bytes including the matched delimiter.
        frame: &'a [u8],
        /// Offset inside the just-pushed chunk where trailing bytes begin.
        trailing_offset: usize,
    },
    /// The configured buffer limit would have been exceeded before the delimiter appeared.
    LimitExceeded,
}

/// Bounded byte accumulator for delimiter-terminated framed protocols.
///
/// The collector never grows its internal buffer beyond `limit`. It is therefore suitable for
/// request-head parsing paths such as HTTP where the application wants to retain backpressure and
/// reject oversized control frames without first buffering them in full.
#[derive(Clone, Debug)]
pub struct BoundedCollector {
    buffer: Vec<u8>,
    limit: usize,
}

impl BoundedCollector {
    /// Creates one empty collector with the supplied maximum buffered size.
    pub fn new(limit: usize) -> Self {
        Self {
            buffer: Vec::new(),
            limit,
        }
    }

    /// Drops all buffered bytes so collection can start again from an empty state.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Returns the currently buffered bytes.
    ///
    /// After [`CollectUntil::Complete`], this contains exactly the completed frame bytes including
    /// the delimiter. Any trailing bytes from the current chunk are reported only through
    /// `trailing_offset` and are not retained in the collector buffer.
    pub fn buffered(&self) -> &[u8] {
        &self.buffer
    }

    /// Pushes one transport chunk and searches for `delimiter`.
    ///
    /// `trailing_offset` in [`CollectUntil::Complete`] refers to the current `chunk`, not the
    /// full buffered frame. This lets callers continue processing trailing bytes from the same
    /// payload without reparsing the frame. The collector does not reset itself automatically
    /// after completion; call [`BoundedCollector::clear`] before reusing it for the next frame.
    pub fn push_until<'a>(&'a mut self, chunk: &[u8], delimiter: &[u8]) -> CollectUntil<'a> {
        assert!(
            !delimiter.is_empty(),
            "BoundedCollector delimiter must not be empty"
        );

        let buffered_before = self.buffer.len();
        let search_start = buffered_before.saturating_sub(delimiter.len().saturating_sub(1));
        let appendable = chunk.len().min(self.limit.saturating_sub(buffered_before));
        self.buffer.extend_from_slice(&chunk[..appendable]);

        if let Some(relative_index) = find_subslice(&self.buffer[search_start..], delimiter) {
            let frame_end = search_start + relative_index + delimiter.len();
            let trailing_offset = frame_end.saturating_sub(buffered_before);
            self.buffer.truncate(frame_end);
            return CollectUntil::Complete {
                frame: &self.buffer[..frame_end],
                trailing_offset,
            };
        }

        if appendable < chunk.len() {
            return CollectUntil::LimitExceeded;
        }

        CollectUntil::NeedMore
    }
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::{BoundedCollector, CollectUntil};

    #[test]
    fn collector_finds_delimiter_across_chunk_boundaries() {
        let mut collector = BoundedCollector::new(32);

        assert!(matches!(
            collector.push_until(b"hello\r\n", b"\r\n\r\n"),
            CollectUntil::NeedMore
        ));

        match collector.push_until(b"\r\nrest", b"\r\n\r\n") {
            CollectUntil::Complete {
                frame,
                trailing_offset,
            } => {
                assert_eq!(frame, b"hello\r\n\r\n");
                assert_eq!(trailing_offset, 2);
            }
            other => panic!("expected completed frame, got {other:?}"),
        }
    }

    #[test]
    fn collector_stops_at_limit_without_buffering_beyond_it() {
        let mut collector = BoundedCollector::new(4);

        match collector.push_until(b"abcdef", b"\r\n") {
            CollectUntil::LimitExceeded => {
                assert_eq!(collector.buffered(), b"abcd");
            }
            other => panic!("expected limit-exceeded outcome, got {other:?}"),
        }
    }
}
