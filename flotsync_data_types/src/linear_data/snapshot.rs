//! Format-agnostic snapshot streaming API.
//!
//! Serializers can consume snapshot nodes in canonical order via [[SnapshotSink]]
//! without depending on internal storage types.

use snafu::prelude::*;

/// Snapshot stream header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Total number of nodes that will be emitted.
    pub node_count: usize,
}

/// A single snapshot node borrowed from an in-memory CRDT state.
///
/// Invariants for emitted streams:
/// - Nodes are emitted in canonical order.
/// - `index == 0` and `index == node_count - 1` are boundary nodes.
/// - Boundary nodes have `value == None` and `deleted == false`.
/// - Non-boundary nodes have `left/right/value == Some(_)`.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotNodeRef<'a, Id, Value: ?Sized> {
    pub id: &'a Id,
    pub left: Option<&'a Id>,
    pub right: Option<&'a Id>,
    /// `true` for tombstone nodes, `false` for visible insert nodes.
    pub deleted: bool,
    pub value: Option<&'a Value>,
}

/// An owned snapshot node used during deserialization.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotNode<Id, Value> {
    pub id: Id,
    pub left: Option<Id>,
    pub right: Option<Id>,
    /// `true` for tombstone nodes, `false` for visible insert nodes.
    pub deleted: bool,
    pub value: Option<Value>,
}

/// Errors while reading/reconstructing snapshots.
#[derive(Clone, Debug, PartialEq, Snafu)]
pub enum SnapshotReadError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    #[snafu(display("Snapshot source failed while reading nodes."))]
    Source { source: E },
    #[snafu(display("Snapshot is missing required boundary nodes."))]
    MissingBoundaryNodes,
    #[snafu(display("Boundary node at index {index} has an unexpected left link."))]
    BoundaryNodeHasLeft { index: usize },
    #[snafu(display("Boundary node at index {index} has an unexpected right link."))]
    BoundaryNodeHasRight { index: usize },
    #[snafu(display("Boundary node at index {index} has an unexpected value payload."))]
    BoundaryNodeHasValue { index: usize },
    #[snafu(display("Boundary node at index {index} is marked deleted."))]
    BoundaryNodeMarkedDeleted { index: usize },
    #[snafu(display("Non-boundary node at index {index} is missing left link."))]
    NonBoundaryNodeMissingLeft { index: usize },
    #[snafu(display("Non-boundary node at index {index} is missing right link."))]
    NonBoundaryNodeMissingRight { index: usize },
    #[snafu(display("Non-boundary node at index {index} is missing value payload."))]
    NonBoundaryNodeMissingValue { index: usize },
    #[snafu(display("Snapshot contains no visible values."))]
    NoVisibleValues,
}
impl<E> SnapshotReadError<E>
where
    E: snafu::Error + Send + Sync + 'static,
{
    pub fn from_source(source: E) -> Self {
        Self::Source { source }
    }
}

/// Sink that receives a snapshot stream.
pub trait SnapshotSink<Id, Value: ?Sized> {
    type Error;

    fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error>;

    fn node(
        &mut self,
        index: usize,
        node: SnapshotNodeRef<'_, Id, Value>,
    ) -> Result<(), Self::Error>;

    fn end(&mut self) -> Result<(), Self::Error>;
}

#[cfg(test)]
pub(crate) mod bytes_testkit {
    use super::*;
    use crate::IdWithIndex;
    use bytes::{Buf, BufMut, Bytes, BytesMut};
    use std::marker::PhantomData;

    const MAGIC: [u8; 4] = *b"SNAP";
    const END_MARKER: u8 = 0xEE;
    const FLAG_HAS_LEFT: u8 = 1 << 0;
    const FLAG_HAS_RIGHT: u8 = 1 << 1;
    const FLAG_HAS_VALUE: u8 = 1 << 2;
    const FLAG_DELETED: u8 = 1 << 3;

    pub struct ByteBufSink<Id, Value: ?Sized, IdEncoder, ValueEncoder>
    where
        IdEncoder: Fn(&Id) -> Vec<u8>,
        ValueEncoder: Fn(&Value) -> Vec<u8>,
    {
        bytes: BytesMut,
        expected_index: usize,
        node_count: Option<usize>,
        encode_id: IdEncoder,
        encode_value: ValueEncoder,
        _marker: PhantomData<fn(&Id, &Value)>,
    }
    impl<Id, Value: ?Sized, IdEncoder, ValueEncoder> ByteBufSink<Id, Value, IdEncoder, ValueEncoder>
    where
        IdEncoder: Fn(&Id) -> Vec<u8>,
        ValueEncoder: Fn(&Value) -> Vec<u8>,
    {
        pub fn new(encode_id: IdEncoder, encode_value: ValueEncoder) -> Self {
            Self {
                bytes: BytesMut::new(),
                expected_index: 0,
                node_count: None,
                encode_id,
                encode_value,
                _marker: PhantomData,
            }
        }

        pub fn into_bytes(self) -> Bytes {
            self.bytes.freeze()
        }
    }
    impl<Id, Value: ?Sized, IdEncoder, ValueEncoder> SnapshotSink<Id, Value>
        for ByteBufSink<Id, Value, IdEncoder, ValueEncoder>
    where
        IdEncoder: Fn(&Id) -> Vec<u8>,
        ValueEncoder: Fn(&Value) -> Vec<u8>,
    {
        type Error = String;

        fn begin(&mut self, header: SnapshotHeader) -> Result<(), Self::Error> {
            if self.node_count.is_some() {
                return Err("begin called twice".to_owned());
            }
            self.node_count = Some(header.node_count);
            self.bytes.put_slice(&MAGIC);
            self.bytes.put_u32_le(
                u32::try_from(header.node_count).map_err(|_| "too many nodes".to_owned())?,
            );
            Ok(())
        }

        fn node(
            &mut self,
            index: usize,
            node: SnapshotNodeRef<'_, Id, Value>,
        ) -> Result<(), Self::Error> {
            if self.node_count.is_none() {
                return Err("node called before begin".to_owned());
            }
            if self.expected_index != index {
                return Err(format!(
                    "unexpected index, expected {}, got {index}",
                    self.expected_index
                ));
            }
            self.expected_index += 1;

            self.bytes
                .put_u32_le(u32::try_from(index).map_err(|_| "index too large".to_owned())?);
            let mut flags = 0u8;
            if node.left.is_some() {
                flags |= FLAG_HAS_LEFT;
            }
            if node.right.is_some() {
                flags |= FLAG_HAS_RIGHT;
            }
            if node.value.is_some() {
                flags |= FLAG_HAS_VALUE;
            }
            if node.deleted {
                flags |= FLAG_DELETED;
            }
            self.bytes.put_u8(flags);

            write_bytes(&mut self.bytes, &(self.encode_id)(node.id))?;
            if let Some(left) = node.left {
                write_bytes(&mut self.bytes, &(self.encode_id)(left))?;
            }
            if let Some(right) = node.right {
                write_bytes(&mut self.bytes, &(self.encode_id)(right))?;
            }
            if let Some(value) = node.value {
                write_bytes(&mut self.bytes, &(self.encode_value)(value))?;
            }
            Ok(())
        }

        fn end(&mut self) -> Result<(), Self::Error> {
            let Some(node_count) = self.node_count else {
                return Err("end called before begin".to_owned());
            };
            if self.expected_index != node_count {
                return Err(format!(
                    "end called before all nodes were emitted: expected {node_count}, got {}",
                    self.expected_index
                ));
            }
            self.bytes.put_u8(END_MARKER);
            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct ParsedSnapshot {
        pub node_count: usize,
        pub nodes: Vec<ParsedNode>,
    }
    #[derive(Debug)]
    pub struct ParsedNode {
        pub index: usize,
        pub has_left: bool,
        pub has_right: bool,
        pub has_value: bool,
        pub deleted: bool,
    }

    pub fn assert_node_invariants(parsed: ParsedSnapshot) {
        assert_eq!(parsed.node_count, parsed.nodes.len());
        assert!(
            parsed.node_count >= 2,
            "snapshot must contain boundary nodes"
        );

        for (expected_index, node) in parsed.nodes.iter().enumerate() {
            assert_eq!(node.index, expected_index);
            let is_boundary = expected_index == 0 || expected_index + 1 == parsed.node_count;
            if is_boundary {
                if expected_index == 0 {
                    assert!(!node.has_left);
                    assert!(node.has_right);
                } else {
                    assert!(node.has_left);
                    assert!(!node.has_right);
                }
                assert!(!node.has_value);
                assert!(!node.deleted);
            } else {
                assert!(node.has_left);
                assert!(node.has_right);
                assert!(node.has_value);
            }
        }
    }

    pub fn parse_snapshot_shape(mut bytes: Bytes) -> Result<ParsedSnapshot, String> {
        assert_magic(&mut bytes)?;
        let node_count: usize = read_u32(&mut bytes)?.try_into().unwrap();

        let mut nodes = Vec::with_capacity(node_count);
        for _ in 0..node_count {
            let index: usize = read_u32(&mut bytes)?.try_into().unwrap();
            let flags = read_u8(&mut bytes)?;

            let _id = read_len_prefixed(&mut bytes)?;
            if flags & FLAG_HAS_LEFT != 0 {
                let _left = read_len_prefixed(&mut bytes)?;
            }
            if flags & FLAG_HAS_RIGHT != 0 {
                let _right = read_len_prefixed(&mut bytes)?;
            }
            if flags & FLAG_HAS_VALUE != 0 {
                let _value = read_len_prefixed(&mut bytes)?;
            }

            nodes.push(ParsedNode {
                index,
                has_left: flags & FLAG_HAS_LEFT != 0,
                has_right: flags & FLAG_HAS_RIGHT != 0,
                has_value: flags & FLAG_HAS_VALUE != 0,
                deleted: flags & FLAG_DELETED != 0,
            });
        }

        assert_end(&mut bytes)?;
        Ok(ParsedSnapshot { node_count, nodes })
    }

    pub fn parse_snapshot_nodes<Id, Value, IdDecoder, ValueDecoder>(
        mut bytes: Bytes,
        decode_id: IdDecoder,
        decode_value: ValueDecoder,
    ) -> Result<Vec<SnapshotNode<Id, Value>>, String>
    where
        IdDecoder: Fn(&[u8]) -> Result<Id, String>,
        ValueDecoder: Fn(&[u8]) -> Result<Value, String>,
    {
        assert_magic(&mut bytes)?;
        let node_count: usize = read_u32(&mut bytes)?.try_into().unwrap();

        let mut nodes = Vec::with_capacity(node_count);
        for expected_index in 0..node_count {
            let index: usize = read_u32(&mut bytes)?.try_into().unwrap();
            if index != expected_index {
                return Err(format!(
                    "unexpected node index {index}, expected {expected_index}"
                ));
            }
            let flags = read_u8(&mut bytes)?;

            let id = decode_id(read_len_prefixed(&mut bytes)?.as_ref())?;
            let left = if flags & FLAG_HAS_LEFT != 0 {
                Some(decode_id(read_len_prefixed(&mut bytes)?.as_ref())?)
            } else {
                None
            };
            let right = if flags & FLAG_HAS_RIGHT != 0 {
                Some(decode_id(read_len_prefixed(&mut bytes)?.as_ref())?)
            } else {
                None
            };
            let value = if flags & FLAG_HAS_VALUE != 0 {
                Some(decode_value(read_len_prefixed(&mut bytes)?.as_ref())?)
            } else {
                None
            };

            nodes.push(SnapshotNode {
                id,
                left,
                right,
                deleted: flags & FLAG_DELETED != 0,
                value,
            });
        }

        assert_end(&mut bytes)?;
        Ok(nodes)
    }

    fn assert_magic(input: &mut Bytes) -> Result<(), String> {
        if input.remaining() < MAGIC.len() {
            return Err("unexpected end of snapshot".to_owned());
        }
        let magic = input.copy_to_bytes(MAGIC.len());
        if magic.as_ref() != MAGIC.as_slice() {
            return Err("invalid snapshot magic".to_owned());
        }
        Ok(())
    }

    fn assert_end(input: &mut Bytes) -> Result<(), String> {
        let end = read_u8(input)?;
        if end != END_MARKER {
            return Err("invalid snapshot end marker".to_owned());
        }
        if input.has_remaining() {
            return Err("trailing bytes after snapshot".to_owned());
        }
        Ok(())
    }

    pub fn write_bytes(target: &mut BytesMut, bytes: &[u8]) -> Result<(), String> {
        target.put_u32_le(u32::try_from(bytes.len()).map_err(|_| "payload too large".to_owned())?);
        target.put_slice(bytes);
        Ok(())
    }

    pub fn read_u8(input: &mut Bytes) -> Result<u8, String> {
        if input.remaining() < 1 {
            return Err("unexpected end of snapshot".to_owned());
        }
        Ok(input.get_u8())
    }

    pub fn read_u32(input: &mut Bytes) -> Result<u32, String> {
        if input.remaining() < 4 {
            return Err("unexpected end of snapshot".to_owned());
        }
        Ok(input.get_u32_le())
    }

    pub fn read_len_prefixed(input: &mut Bytes) -> Result<Bytes, String> {
        let len: usize = read_u32(input)?.try_into().unwrap();
        if input.remaining() < len {
            return Err("unexpected end of snapshot".to_owned());
        }
        Ok(input.copy_to_bytes(len))
    }

    pub fn encode_id_with_index_u32(id: &IdWithIndex<u32>) -> Vec<u8> {
        let mut out = Vec::with_capacity(8);
        out.extend_from_slice(&id.id.to_le_bytes());
        out.extend_from_slice(&id.index.to_le_bytes());
        out
    }

    pub fn decode_id_with_index_u32(bytes: &[u8]) -> Result<IdWithIndex<u32>, String> {
        if bytes.len() != 8 {
            return Err("invalid IdWithIndex<u32> length".to_owned());
        }
        let id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let index = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        Ok(IdWithIndex { id, index })
    }

    pub fn encode_vec_i32(value: &[i32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + value.len() * 4);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        for element in value {
            out.extend_from_slice(&element.to_le_bytes());
        }
        out
    }

    pub fn decode_vec_i32(bytes: &[u8]) -> Result<Vec<i32>, String> {
        let mut input = Bytes::copy_from_slice(bytes);
        let len: usize = read_u32(&mut input)?.try_into().unwrap();
        if input.remaining() != len * 4 {
            return Err("invalid i32 vector payload length".to_owned());
        }

        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(input.get_i32_le());
        }
        Ok(values)
    }

    pub fn encode_utf8_str(value: &str) -> Vec<u8> {
        value.as_bytes().to_vec()
    }

    pub fn decode_utf8_string(bytes: &[u8]) -> Result<String, String> {
        String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8 value payload".to_owned())
    }

    pub fn encode_u32(value: &u32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    pub fn decode_u32(bytes: &[u8]) -> Result<u32, String> {
        if bytes.len() != 4 {
            return Err("invalid u32 payload length".to_owned());
        }
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn encode_u64(value: &u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    pub fn decode_u64(bytes: &[u8]) -> Result<u64, String> {
        if bytes.len() != 8 {
            return Err("invalid u64 payload length".to_owned());
        }
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::bytes_testkit::*;
    use crate::{
        IdWithIndex,
        any_data::{LinearLatestValueWins, list::LinearList},
        text::LinearString,
    };
    use itertools::Itertools;

    #[test]
    fn linear_list_snapshot_stream_has_expected_shape() {
        let mut id_generator = 0u32..;
        let mut list = LinearList::with_values([10i32, 20, 30], id_generator.next().unwrap());
        list.append(IdWithIndex::zero(100), [40, 41]);
        let _ = list.delete_at(1);

        let mut sink = ByteBufSink::new(encode_id_with_index_u32, encode_vec_i32);
        list.encode_snapshot(&mut sink).unwrap();
        let parsed = parse_snapshot_shape(sink.into_bytes()).unwrap();
        assert!(parsed.nodes.iter().any(|n| n.deleted));

        assert_node_invariants(parsed);
    }

    #[test]
    fn linear_string_snapshot_stream_has_expected_shape() {
        let mut text = LinearString::with_value("alpha beta".to_owned(), 0u32);
        text.append(IdWithIndex::zero(100), " gamma".to_owned());
        let range = text.ids_in_range(1..=5).unwrap();
        range.delete(&mut text).unwrap();

        let mut sink = ByteBufSink::new(encode_id_with_index_u32, encode_utf8_str);
        text.encode_snapshot(&mut sink).unwrap();
        let parsed = parse_snapshot_shape(sink.into_bytes()).unwrap();
        assert!(parsed.nodes.iter().any(|n| n.deleted));

        assert_node_invariants(parsed);
    }

    #[test]
    fn latest_value_snapshot_stream_has_expected_shape() {
        let mut id_generator = 0u32..;
        let mut reg = LinearLatestValueWins::new(5u64, id_generator.next_array().unwrap());
        reg.update(10, 6);
        reg.update(11, 7);

        let mut sink = ByteBufSink::new(encode_u32, encode_u64);
        reg.encode_snapshot(&mut sink).unwrap();
        let parsed = parse_snapshot_shape(sink.into_bytes()).unwrap();
        assert!(parsed.nodes.iter().all(|n| !n.deleted));

        assert_node_invariants(parsed);
    }

    #[test]
    fn linear_list_snapshot_roundtrips_via_bytebuf() {
        let mut id_generator = 0u32..;
        let mut original = LinearList::with_values([1i32, 2, 3, 4], id_generator.next().unwrap());
        original.append(IdWithIndex::zero(100), [8, 9]);
        let _ = original.delete_at(2);

        let mut sink = ByteBufSink::new(encode_id_with_index_u32, encode_vec_i32);
        original.encode_snapshot(&mut sink).unwrap();
        let nodes =
            parse_snapshot_nodes(sink.into_bytes(), decode_id_with_index_u32, decode_vec_i32)
                .unwrap();

        let roundtrip = LinearList::from_snapshot_nodes(
            nodes.into_iter().map(Ok::<_, std::convert::Infallible>),
        )
        .unwrap();
        assert_eq!(roundtrip, original);
    }

    #[test]
    fn linear_string_snapshot_roundtrips_via_bytebuf() {
        let mut original = LinearString::with_value("alpha".to_owned(), 0u32);
        original.append(IdWithIndex::zero(100), " beta".to_owned());
        let range = original.ids_in_range(1..=3).unwrap();
        range.delete(&mut original).unwrap();

        let mut sink = ByteBufSink::new(encode_id_with_index_u32, encode_utf8_str);
        original.encode_snapshot(&mut sink).unwrap();
        let nodes = parse_snapshot_nodes(
            sink.into_bytes(),
            decode_id_with_index_u32,
            decode_utf8_string,
        )
        .unwrap();

        let roundtrip = LinearString::from_snapshot_nodes(
            nodes.into_iter().map(Ok::<_, std::convert::Infallible>),
        )
        .unwrap();
        assert_eq!(roundtrip, original);
    }

    #[test]
    fn latest_value_snapshot_roundtrips_via_bytebuf() {
        let mut id_generator = 0u32..;
        let mut original = LinearLatestValueWins::new(11u64, id_generator.next_array().unwrap());
        original.update(10, 12);
        original.update(20, 13);

        let mut sink = ByteBufSink::new(encode_u32, encode_u64);
        original.encode_snapshot(&mut sink).unwrap();
        let nodes = parse_snapshot_nodes(sink.into_bytes(), decode_u32, decode_u64).unwrap();

        let roundtrip = LinearLatestValueWins::from_snapshot_nodes(
            nodes.into_iter().map(Ok::<_, std::convert::Infallible>),
        )
        .unwrap();
        assert_eq!(roundtrip, original);
    }
}
