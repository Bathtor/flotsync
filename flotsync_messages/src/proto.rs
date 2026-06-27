//! Shared traits for converting between runtime types and generated protobuf messages.

use crate::buffa::{self, Message as _};
use std::sync::Arc;

/// Decode failure at a protobuf byte or buffer boundary.
///
/// The `Decode` variant means the generated protobuf message or view could not
/// be read from bytes. The `Convert` variant means protobuf decoding succeeded,
/// but the generated shape could not satisfy the runtime type's invariants.
#[derive(Debug)]
pub enum ProtoInputDecodeError<E> {
    /// Generated protobuf decoding failed before runtime conversion started.
    Decode { source: buffa::DecodeError },
    /// Runtime conversion failed after generated protobuf decoding succeeded.
    Convert { source: E },
}

impl<E> ProtoInputDecodeError<E> {
    /// Map a generated protobuf decode error into the boundary error.
    #[must_use]
    pub fn decode(source: buffa::DecodeError) -> Self {
        Self::Decode { source }
    }

    /// Map a runtime conversion error into the boundary error.
    #[must_use]
    pub fn convert(source: E) -> Self {
        Self::Convert { source }
    }
}

/// Convert a generated protobuf byte-decode failure into a runtime error type.
///
/// Implement this for error types that intentionally collapse byte decode and
/// runtime conversion failures into the same domain-level error. Use the
/// `try_*` helpers when callers must keep those phases separate.
pub trait FromProtoDecodeError {
    /// Build a runtime error from a generated protobuf decode failure.
    fn from_proto_decode_error(source: buffa::DecodeError) -> Self;
}

/// Encode a value into its owned generated protobuf representation.
///
/// Implementations may allocate or clone where generated protobuf messages own
/// their fields. Borrowed-source encoders should use explicit adapter types
/// instead of hiding those ownership decisions behind blanket implementations.
pub trait EncodeProto {
    /// The generated protobuf message type produced by this encoder.
    type Proto: buffa::Message;

    /// Encode this value into an owned generated protobuf message.
    fn encode_proto(&self) -> Self::Proto;

    /// Encode this value into owned bytes through its generated protobuf shape.
    fn encode_proto_to_bytes(&self) -> bytes::Bytes {
        self.encode_proto().encode_to_bytes()
    }

    /// Encode this value into a `Vec<u8>` through its generated protobuf shape.
    fn encode_proto_to_vec(&self) -> Vec<u8> {
        self.encode_proto().encode_to_vec()
    }

    /// Wrap this value so it can be serialised through Flotsync transport APIs.
    fn into_proto_serializable(self) -> crate::serialisation::ProtoSerializable<Self>
    where
        Self: Sized,
    {
        crate::serialisation::ProtoSerializable::new(self)
    }

    /// Wrap this value as a dynamic Flotsync transport payload.
    fn into_flotsync_serializable(self) -> Arc<dyn crate::serialisation::FlotsyncSerializable>
    where
        Self: Sized + Send + Sync + 'static,
    {
        Arc::new(self.into_proto_serializable())
    }
}

/// Decode a context-free runtime value from an owned generated protobuf message.
pub trait DecodeProto: Sized {
    /// The generated protobuf message type consumed by this decoder.
    type Proto: buffa::Message;

    /// Error returned when the protobuf message cannot become this value.
    type Error;

    /// Decode this value from an owned generated protobuf message.
    ///
    /// Use [`DecodeProtoWith`] instead when decoding needs runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the protobuf message is malformed or cannot
    /// satisfy this runtime type's invariants.
    fn decode_proto(proto: Self::Proto) -> Result<Self, Self::Error>;

    /// Decode this value from a generated protobuf message in a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// message cannot be decoded from `bytes`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_from_slice(
        bytes: &[u8],
    ) -> Result<Self, ProtoInputDecodeError<Self::Error>> {
        let proto = Self::Proto::decode_from_slice(bytes).map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto(proto).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf message in a byte slice.
    ///
    /// Use [`Self::try_decode_proto_from_slice`] when the caller needs to keep
    /// generated protobuf decode failures distinct from runtime conversion
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf byte decoding or runtime
    /// conversion fails.
    fn decode_proto_from_slice(bytes: &[u8]) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto =
            Self::Proto::decode_from_slice(bytes).map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto(proto)
    }

    /// Decode this value from a generated protobuf message in a buffer.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// message cannot be decoded from `buf`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_from_buf<B>(buf: &mut B) -> Result<Self, ProtoInputDecodeError<Self::Error>>
    where
        B: buffa::bytes::Buf,
    {
        let proto = Self::Proto::decode(buf).map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto(proto).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf message in a buffer.
    ///
    /// Use [`Self::try_decode_proto_from_buf`] when the caller needs to keep
    /// generated protobuf decode failures distinct from runtime conversion
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf buffer decoding or runtime
    /// conversion fails.
    fn decode_proto_from_buf<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: buffa::bytes::Buf,
        Self::Error: FromProtoDecodeError,
    {
        let proto = Self::Proto::decode(buf).map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto(proto)
    }
}

/// Decode a runtime value from an owned generated protobuf message with context.
///
/// This stays separate from [`DecodeProto`] so call sites must make required
/// runtime context explicit.
pub trait DecodeProtoWith<Context>: Sized {
    /// The generated protobuf message type consumed by this decoder.
    type Proto: buffa::Message;

    /// Error returned when the protobuf message cannot become this value.
    type Error;

    /// Decode this value from an owned generated protobuf message and context.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the protobuf message is malformed, when the
    /// context is incompatible with the message, or when the decoded value
    /// cannot satisfy this runtime type's invariants.
    fn decode_proto_with(proto: Self::Proto, context: Context) -> Result<Self, Self::Error>;

    /// Decode this value from a generated protobuf message in a byte slice with
    /// explicit runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// message cannot be decoded from `bytes`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_from_slice_with(
        bytes: &[u8],
        context: Context,
    ) -> Result<Self, ProtoInputDecodeError<Self::Error>> {
        let proto = Self::Proto::decode_from_slice(bytes).map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto_with(proto, context).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf message in a byte slice with
    /// explicit runtime context.
    ///
    /// Use [`Self::try_decode_proto_from_slice_with`] when the caller needs to
    /// keep generated protobuf decode failures distinct from runtime conversion
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf byte decoding or runtime
    /// conversion fails.
    fn decode_proto_from_slice_with(bytes: &[u8], context: Context) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto =
            Self::Proto::decode_from_slice(bytes).map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto_with(proto, context)
    }

    /// Decode this value from a generated protobuf message in a buffer with
    /// explicit runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// message cannot be decoded from `buf`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_from_buf_with<B>(
        buf: &mut B,
        context: Context,
    ) -> Result<Self, ProtoInputDecodeError<Self::Error>>
    where
        B: buffa::bytes::Buf,
    {
        let proto = Self::Proto::decode(buf).map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto_with(proto, context).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf message in a buffer with
    /// explicit runtime context.
    ///
    /// Use [`Self::try_decode_proto_from_buf_with`] when the caller needs to
    /// keep generated protobuf decode failures distinct from runtime conversion
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf buffer decoding or runtime
    /// conversion fails.
    fn decode_proto_from_buf_with<B>(buf: &mut B, context: Context) -> Result<Self, Self::Error>
    where
        B: buffa::bytes::Buf,
        Self::Error: FromProtoDecodeError,
    {
        let proto = Self::Proto::decode(buf).map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto_with(proto, context)
    }
}

/// Decode a runtime value from a generated borrowed protobuf view.
///
/// The associated view type is generic over the borrowed input lifetime so each
/// implementation can name the generated `FooView<'a>` type once without
/// making every implementation itself generic over `'a`.
pub trait DecodeProtoView: Sized {
    /// The generated protobuf view type borrowed by this decoder.
    type ProtoView<'a>: buffa::MessageView<'a>;

    /// Error returned when the protobuf view cannot become this value.
    type Error;

    /// Decode this value from a generated protobuf view.
    ///
    /// Use [`DecodeProtoViewWith`] instead when decoding needs runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the protobuf view is malformed or cannot
    /// satisfy this runtime type's invariants.
    fn decode_proto_view(proto: &Self::ProtoView<'_>) -> Result<Self, Self::Error>;

    /// Decode this value from a generated protobuf view over a byte slice.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// view cannot be decoded from `bytes`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_view_from_slice(
        bytes: &[u8],
    ) -> Result<Self, ProtoInputDecodeError<Self::Error>> {
        let proto = <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes)
            .map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto_view(&proto).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf view over a byte slice.
    ///
    /// Use [`Self::try_decode_proto_view_from_slice`] when the caller needs to
    /// keep generated protobuf decode failures distinct from runtime conversion
    /// failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf view decoding or runtime
    /// conversion fails.
    fn decode_proto_view_from_slice(bytes: &[u8]) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes)
            .map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto_view(&proto)
    }
}

/// Decode a runtime value from a generated borrowed protobuf view with context.
///
/// This mirrors [`DecodeProtoWith`] for generated views and keeps required
/// runtime context explicit at the call site.
pub trait DecodeProtoViewWith<Context>: Sized {
    /// The generated protobuf view type borrowed by this decoder.
    type ProtoView<'a>: buffa::MessageView<'a>;

    /// Error returned when the protobuf view cannot become this value.
    type Error;

    /// Decode this value from a generated protobuf view and context.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the protobuf view is malformed, when the
    /// context is incompatible with the message, or when the decoded value
    /// cannot satisfy this runtime type's invariants.
    fn decode_proto_view_with(
        proto: &Self::ProtoView<'_>,
        context: Context,
    ) -> Result<Self, Self::Error>;

    /// Decode this value from a generated protobuf view over a byte slice with
    /// explicit runtime context.
    ///
    /// # Errors
    ///
    /// Returns [`ProtoInputDecodeError::Decode`] when the generated protobuf
    /// view cannot be decoded from `bytes`, or
    /// [`ProtoInputDecodeError::Convert`] when runtime conversion fails.
    fn try_decode_proto_view_from_slice_with(
        bytes: &[u8],
        context: Context,
    ) -> Result<Self, ProtoInputDecodeError<Self::Error>> {
        let proto = <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes)
            .map_err(ProtoInputDecodeError::decode)?;
        Self::decode_proto_view_with(&proto, context).map_err(ProtoInputDecodeError::convert)
    }

    /// Decode this value from a generated protobuf view over a byte slice with
    /// explicit runtime context.
    ///
    /// Use [`Self::try_decode_proto_view_from_slice_with`] when the caller
    /// needs to keep generated protobuf decode failures distinct from runtime
    /// conversion failures.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when either protobuf view decoding or runtime
    /// conversion fails.
    fn decode_proto_view_from_slice_with(
        bytes: &[u8],
        context: Context,
    ) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes)
            .map_err(Self::Error::from_proto_decode_error)?;
        Self::decode_proto_view_with(&proto, context)
    }
}

/// Bidirectional owned protobuf conversion without external decode context.
///
/// Implement this trait when encoding and decoding use the same owned generated
/// protobuf message type. [`EncodeProto`] and [`DecodeProto`] are then provided
/// automatically.
pub trait ProtoCodec: Sized {
    /// The generated protobuf message type used by this codec.
    type Proto: buffa::Message;

    /// Error returned when the protobuf message cannot become this value.
    type DecodeError;

    /// Encode this value into an owned generated protobuf message.
    fn to_proto(&self) -> Self::Proto;

    /// Decode this value from an owned generated protobuf message.
    ///
    /// # Errors
    ///
    /// Returns [`Self::DecodeError`] when the protobuf message is malformed or
    /// cannot satisfy this runtime type's invariants.
    fn from_proto(proto: Self::Proto) -> Result<Self, Self::DecodeError>;
}

impl<T> EncodeProto for T
where
    T: ProtoCodec,
{
    type Proto = <T as ProtoCodec>::Proto;

    fn encode_proto(&self) -> Self::Proto {
        ProtoCodec::to_proto(self)
    }
}

impl<T> DecodeProto for T
where
    T: ProtoCodec,
{
    type Error = <T as ProtoCodec>::DecodeError;
    type Proto = <T as ProtoCodec>::Proto;

    fn decode_proto(proto: Self::Proto) -> Result<Self, Self::Error> {
        ProtoCodec::from_proto(proto)
    }
}

/// Contextual bidirectional owned protobuf conversion.
///
/// Implement this trait when decoding needs explicit runtime context while
/// encoding still uses the matching owned generated protobuf message type.
/// Rust cannot provide a coherent blanket [`EncodeProto`] implementation for
/// every possible `Context`, so implementations also provide `EncodeProto` and
/// this trait supplies [`DecodeProtoWith`].
pub trait ProtoCodecWith<Context>: EncodeProto + Sized {
    /// Error returned when the protobuf message cannot become this value.
    type DecodeError;

    /// Decode this value from an owned generated protobuf message and context.
    ///
    /// # Errors
    ///
    /// Returns [`Self::DecodeError`] when the protobuf message is malformed,
    /// when the context is incompatible with the message, or when the decoded
    /// value cannot satisfy this runtime type's invariants.
    fn from_proto_with(proto: Self::Proto, context: Context) -> Result<Self, Self::DecodeError>;
}

impl<T, Context> DecodeProtoWith<Context> for T
where
    T: ProtoCodecWith<Context>,
{
    type Error = <T as ProtoCodecWith<Context>>::DecodeError;
    type Proto = <T as EncodeProto>::Proto;

    fn decode_proto_with(proto: Self::Proto, context: Context) -> Result<Self, Self::Error> {
        ProtoCodecWith::from_proto_with(proto, context)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DecodeProto,
        DecodeProtoView,
        EncodeProto,
        FromProtoDecodeError,
        ProtoCodec,
        ProtoInputDecodeError,
    };
    use crate::{
        buffa::{self, Message as _},
        versions as versions_proto,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct SyncedVersion(u64);

    #[derive(Debug)]
    enum SyncedVersionError {
        Decode { _source: buffa::DecodeError },
        MissingVersions,
        UnsupportedVersions,
    }

    impl FromProtoDecodeError for SyncedVersionError {
        fn from_proto_decode_error(source: buffa::DecodeError) -> Self {
            Self::Decode { _source: source }
        }
    }

    impl ProtoCodec for SyncedVersion {
        type DecodeError = SyncedVersionError;
        type Proto = versions_proto::VersionVector;

        fn to_proto(&self) -> Self::Proto {
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

        fn from_proto(proto: Self::Proto) -> Result<Self, Self::DecodeError> {
            match proto.versions {
                Some(versions_proto::version_vector::Versions::Synced(synced)) => {
                    Ok(Self(synced.group_version))
                }
                Some(_) => Err(SyncedVersionError::UnsupportedVersions),
                None => Err(SyncedVersionError::MissingVersions),
            }
        }
    }

    impl DecodeProtoView for SyncedVersion {
        type Error = SyncedVersionError;
        type ProtoView<'a> = versions_proto::VersionVectorView<'a>;

        fn decode_proto_view(proto: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
            match proto.versions.as_ref() {
                Some(versions_proto::version_vector::VersionsView::Synced(synced)) => {
                    Ok(Self(synced.group_version))
                }
                Some(_) => Err(SyncedVersionError::UnsupportedVersions),
                None => Err(SyncedVersionError::MissingVersions),
            }
        }
    }

    #[test]
    fn encode_proto_helpers_return_owned_bytes() {
        let version = SyncedVersion(13);
        let proto = version.encode_proto();

        assert_eq!(version.encode_proto_to_bytes(), proto.encode_to_bytes());
        assert_eq!(version.encode_proto_to_vec(), proto.encode_to_vec());
    }

    #[test]
    fn owned_decode_helpers_preserve_input_and_conversion_phases() {
        let bytes = SyncedVersion(21).encode_proto_to_bytes();
        let mut buf = bytes.clone();

        assert_eq!(
            SyncedVersion::decode_proto_from_slice(&bytes).expect("slice should decode"),
            SyncedVersion(21)
        );
        assert_eq!(
            SyncedVersion::decode_proto_from_buf(&mut buf).expect("buf should decode"),
            SyncedVersion(21)
        );

        let invalid_wire = [0x0a];
        assert!(matches!(
            SyncedVersion::try_decode_proto_from_slice(&invalid_wire),
            Err(ProtoInputDecodeError::Decode { .. })
        ));
        assert!(matches!(
            SyncedVersion::decode_proto_from_slice(&invalid_wire),
            Err(SyncedVersionError::Decode { .. })
        ));

        let missing_versions = versions_proto::VersionVector::default().encode_to_vec();
        assert!(matches!(
            SyncedVersion::try_decode_proto_from_slice(&missing_versions),
            Err(ProtoInputDecodeError::Convert {
                source: SyncedVersionError::MissingVersions
            })
        ));
    }

    #[test]
    fn view_decode_helpers_preserve_input_and_conversion_phases() {
        let bytes = SyncedVersion(34).encode_proto_to_bytes();

        assert_eq!(
            SyncedVersion::decode_proto_view_from_slice(&bytes).expect("view should decode"),
            SyncedVersion(34)
        );

        let invalid_wire = [0x0a];
        assert!(matches!(
            SyncedVersion::try_decode_proto_view_from_slice(&invalid_wire),
            Err(ProtoInputDecodeError::Decode { .. })
        ));

        let missing_versions = versions_proto::VersionVector::default().encode_to_vec();
        assert!(matches!(
            SyncedVersion::try_decode_proto_view_from_slice(&missing_versions),
            Err(ProtoInputDecodeError::Convert {
                source: SyncedVersionError::MissingVersions
            })
        ));
    }
}
