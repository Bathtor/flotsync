use crate::{buffa::Message as BuffaMessage, proto::EncodeProto};
use flotsync_io::prelude::{EgressAsyncWriter, EgressPool, Error as IoError, IoPayload};
use flotsync_utils::{BoxFuture, IString};
use futures_util::FutureExt;
use snafu::{ResultExt, Snafu};

/// Size hint returned by one serialisable network message.
///
/// Transports can use this to choose between bounded sync reservation and fully async/growable
/// encoding paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeHint {
    /// Absolutely no idea about the required size.
    Unknown,
    /// Given value is the exact size required.
    Exact(usize),
    /// Given value is an upper bound on the size required.
    UpperBound(usize),
    /// Given value is an estimate that may be too large or too small.
    Estimate(usize),
}

/// Dyn-safe payload contract for flotsync network messages.
pub trait FlotsyncSerializable: Send + Sync + 'static {
    /// Best-effort size information used before allocating pooled egress buffers.
    fn serialized_size_hint(&self) -> SizeHint;

    /// Encode the logical network message into one pooled writer owned by the concrete transport.
    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>>;
}

/// Transport-serialisable wrapper for values with a generated protobuf encoder.
///
/// This wrapper is explicit so it does not overlap with the blanket
/// [`FlotsyncSerializable`] implementation for generated protobuf messages.
pub struct ProtoSerializable<T> {
    /// Runtime value that knows how to produce its generated protobuf shape.
    value: T,
}

impl<T> ProtoSerializable<T> {
    /// Wrap a value with an [`EncodeProto`] implementation for transport serialisation.
    #[must_use]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Return the wrapped value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> FlotsyncSerializable for ProtoSerializable<T>
where
    T: EncodeProto + Send + Sync + 'static,
{
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Unknown
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        async move {
            let proto = self.value.encode_proto();
            let reserved_bytes = proto.encoded_len() as usize;
            let mut reserved = writer
                .write_with_reserved(reserved_bytes)
                .await
                .context(IoSnafu)?;
            proto.encode(&mut reserved);
            Ok(())
        }
        .boxed()
    }
}

impl<M> FlotsyncSerializable for M
where
    M: BuffaMessage + Send + Sync + 'static,
{
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Exact(self.encoded_len() as usize)
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        async move {
            let reserved_bytes = self.encoded_len() as usize;
            let mut reserved = writer
                .write_with_reserved(reserved_bytes)
                .await
                .context(IoSnafu)?;
            self.encode(&mut reserved);
            Ok(())
        }
        .boxed()
    }
}

/// Serialisation failure while writing a logical message into an egress payload.
#[derive(Debug, Snafu)]
pub enum FlotsyncSerializeError {
    /// The egress writer could not provide or finish the requested payload buffer.
    #[snafu(display("writer failure while serialising flotsync message"))]
    Io { source: IoError },
    /// The message itself could not be encoded.
    #[snafu(display("message serialisation failed: {message}"))]
    Encoding { message: IString },
}

/// Encode one serialisable message into an egress-pool-backed payload lease.
///
/// # Errors
///
/// Returns [`FlotsyncSerializeError`] when the egress writer cannot reserve or finish the payload,
/// or when the message implementation reports an encoding failure.
pub async fn encode_message_payload(
    egress_pool: &EgressPool,
    message: &(impl FlotsyncSerializable + ?Sized),
) -> Result<IoPayload, FlotsyncSerializeError> {
    let hint = match message.serialized_size_hint() {
        SizeHint::Unknown => None,
        SizeHint::Exact(bytes) | SizeHint::UpperBound(bytes) | SizeHint::Estimate(bytes) => {
            Some(bytes)
        }
    };
    let mut writer = egress_pool.writer(hint);
    message.serialize_into(&mut writer).await?;
    let payload = writer.finish().context(IoSnafu)?;
    Ok(payload.unwrap_or_else(|| IoPayload::from_static(b"")))
}

#[cfg(test)]
mod tests {
    use super::{FlotsyncSerializable, ProtoSerializable, SizeHint, encode_message_payload};
    use crate::{proto::EncodeProto, versions as versions_proto};
    use flotsync_io::prelude::{IoBufferConfig, IoBufferPools};
    use futures_util::FutureExt as _;

    struct SerializableVersion(u64);

    impl EncodeProto for SerializableVersion {
        type Proto = versions_proto::VersionVector;

        fn encode_proto(&self) -> Self::Proto {
            versions_proto::VersionVector {
                versions: Some(versions_proto::version_vector::Versions::Synced(Box::new(
                    versions_proto::SyncedVersionVector {
                        group_version: self.0,
                        ..versions_proto::SyncedVersionVector::default()
                    },
                ))),
                ..versions_proto::VersionVector::default()
            }
        }
    }

    #[test]
    fn proto_serializable_encodes_through_transport_payload_path() {
        let egress_pool = IoBufferPools::new(IoBufferConfig::default())
            .expect("test IO buffers should build")
            .egress();
        let wrapper = ProtoSerializable::new(SerializableVersion(55));

        assert_eq!(wrapper.serialized_size_hint(), SizeHint::Unknown);
        let payload = encode_message_payload(&egress_pool, &wrapper)
            .now_or_never()
            .expect("test egress pool should reserve immediately")
            .expect("payload should encode");

        assert_eq!(
            payload.create_byte_clone(),
            SerializableVersion(55).encode_proto_to_bytes()
        );
    }

    #[test]
    fn encode_proto_convenience_builds_dynamic_serializable_payload() {
        let egress_pool = IoBufferPools::new(IoBufferConfig::default())
            .expect("test IO buffers should build")
            .egress();
        let serializable = SerializableVersion(89).into_flotsync_serializable();

        let payload = encode_message_payload(&egress_pool, serializable.as_ref())
            .now_or_never()
            .expect("test egress pool should reserve immediately")
            .expect("payload should encode");

        assert_eq!(
            payload.create_byte_clone(),
            SerializableVersion(89).encode_proto_to_bytes()
        );
    }
}
