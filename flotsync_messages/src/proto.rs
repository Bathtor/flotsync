//! Shared traits for converting between runtime types and generated protobuf messages.

use crate::buffa::{self, Message as _, MessageField};
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
    #[track_caller]
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

    /// Encode borrowed values into a collection of owned generated protobuf messages.
    fn encode_proto_collection<'a, C>(values: impl IntoIterator<Item = &'a Self>) -> C
    where
        Self: 'a,
        C: FromIterator<Self::Proto>,
    {
        values.into_iter().map(Self::encode_proto).collect()
    }

    /// Encode this value into a boxed owned generated protobuf message.
    ///
    /// Generated `oneof` variants frequently box message payloads, so this
    /// avoids repeating `Box::new(value.encode_proto())` at each call site.
    fn encode_proto_boxed(&self) -> Box<Self::Proto> {
        Box::new(self.encode_proto())
    }

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

/// Encode a value into one selected generated protobuf `oneof` variant.
///
/// Generated protobuf `oneof` enums are not messages, so this is deliberately
/// separate from [`EncodeProto`]. Implement this for runtime values that model
/// the selected variant of a wrapper message.
pub trait EncodeProtoOneof {
    /// The generated protobuf `oneof` enum produced by this encoder.
    type Proto;

    /// Encode this value into one selected generated protobuf `oneof` variant.
    fn encode_proto(&self) -> Self::Proto;
}

/// Construct a decode error for a missing required protobuf value.
///
/// `Context` is codec-specific. Simple codecs can use a field name; codecs with
/// more precise error variants can use an enum.
pub trait MissingRequiredProto {
    /// Context needed to choose or construct the missing-required error.
    type Context;

    /// Build the missing-required error for `context`.
    fn missing_required(context: Self::Context) -> Self;
}

/// Extract a required generated protobuf message field.
pub trait RequiredProtoField<T> {
    /// Take this required field from the generated message.
    ///
    /// # Errors
    ///
    /// Returns `E` when the field is absent.
    fn take_required_proto_field<E>(&mut self, context: E::Context) -> Result<T, E>
    where
        E: MissingRequiredProto;
}

impl<T: Default> RequiredProtoField<T> for MessageField<T> {
    fn take_required_proto_field<E>(&mut self, context: E::Context) -> Result<T, E>
    where
        E: MissingRequiredProto,
    {
        match self.take() {
            Some(field) => Ok(field),
            None => Err(E::missing_required(context)),
        }
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

    /// Decode owned generated protobuf messages into a runtime collection.
    ///
    /// Elements retain their input order and conversion stops at the first
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when any protobuf message cannot satisfy this
    /// runtime type's invariants.
    fn decode_proto_collection<C>(
        protos: impl IntoIterator<Item = Self::Proto>,
    ) -> Result<C, Self::Error>
    where
        C: FromIterator<Self>,
    {
        protos.into_iter().map(Self::decode_proto).collect()
    }

    /// Decode this value from a required generated protobuf message field.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the field is absent or the field payload
    /// cannot satisfy this runtime type's invariants.
    fn decode_required_proto_field(
        field: &mut MessageField<Self::Proto>,
        context: <Self::Error as MissingRequiredProto>::Context,
    ) -> Result<Self, Self::Error>
    where
        Self::Error: MissingRequiredProto,
        Self::Proto: Default,
    {
        let proto = field.take_required_proto_field::<Self::Error>(context)?;
        Self::decode_proto(proto)
    }

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
    #[track_caller]
    fn decode_proto_from_slice(bytes: &[u8]) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = match Self::Proto::decode_from_slice(bytes) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
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
    #[track_caller]
    fn decode_proto_from_buf<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: buffa::bytes::Buf,
        Self::Error: FromProtoDecodeError,
    {
        let proto = match Self::Proto::decode(buf) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
        Self::decode_proto(proto)
    }
}

/// Decode a runtime value from one selected generated protobuf `oneof` variant.
///
/// Generated protobuf `oneof` enums are not messages, so this is deliberately
/// separate from [`DecodeProto`]. Implement this for runtime values that model
/// the selected variant of a wrapper message.
pub trait DecodeProtoOneof: Sized {
    /// The generated protobuf `oneof` enum consumed by this decoder.
    type Proto;

    /// Error returned when the `oneof` variant cannot become this value.
    type Error: MissingRequiredProto;

    /// Decode this value from one selected generated protobuf `oneof` variant.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the selected variant cannot satisfy this
    /// runtime type's invariants.
    fn decode_proto(proto: Self::Proto) -> Result<Self, Self::Error>;

    /// Decode this value from a required generated protobuf `oneof` field.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the oneof is unset or the selected variant
    /// cannot satisfy this runtime type's invariants.
    fn decode_required_proto(
        proto: Option<Self::Proto>,
        context: <Self::Error as MissingRequiredProto>::Context,
    ) -> Result<Self, Self::Error> {
        match proto {
            Some(proto) => Self::decode_proto(proto),
            None => Err(Self::Error::missing_required(context)),
        }
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

    /// Decode owned generated protobuf messages with one reusable context.
    ///
    /// Collection decoding must duplicate the context for every element.
    /// Requiring [`Copy`] restricts this convenience to cheap context values or
    /// references instead of hiding a potentially expensive clone per member.
    ///
    /// Elements retain their input order and conversion stops at the first
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the context is incompatible with any
    /// message or a message cannot satisfy this runtime type's invariants.
    fn decode_proto_collection_with<C>(
        protos: impl IntoIterator<Item = Self::Proto>,
        context: Context,
    ) -> Result<C, Self::Error>
    where
        Context: Copy,
        C: FromIterator<Self>,
    {
        protos
            .into_iter()
            .map(|proto| Self::decode_proto_with(proto, context))
            .collect()
    }

    /// Decode this value from a required generated protobuf message field.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the field is absent, when the context is
    /// incompatible with the message, or when the decoded value cannot satisfy
    /// this runtime type's invariants.
    fn decode_required_proto_field_with(
        field: &mut MessageField<Self::Proto>,
        missing_context: <Self::Error as MissingRequiredProto>::Context,
        decode_context: Context,
    ) -> Result<Self, Self::Error>
    where
        Self::Error: MissingRequiredProto,
        Self::Proto: Default,
    {
        let proto = field.take_required_proto_field::<Self::Error>(missing_context)?;
        Self::decode_proto_with(proto, decode_context)
    }

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
    #[track_caller]
    fn decode_proto_from_slice_with(bytes: &[u8], context: Context) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = match Self::Proto::decode_from_slice(bytes) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
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
    #[track_caller]
    fn decode_proto_from_buf_with<B>(buf: &mut B, context: Context) -> Result<Self, Self::Error>
    where
        B: buffa::bytes::Buf,
        Self::Error: FromProtoDecodeError,
    {
        let proto = match Self::Proto::decode(buf) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
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

    /// Decode generated borrowed protobuf views into a runtime collection.
    ///
    /// Elements retain their input order and conversion stops at the first
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when any protobuf view cannot satisfy this
    /// runtime type's invariants.
    fn decode_proto_view_collection<'proto, 'view, C>(
        protos: impl IntoIterator<Item = &'view Self::ProtoView<'proto>>,
    ) -> Result<C, Self::Error>
    where
        'proto: 'view,
        Self::ProtoView<'proto>: 'view,
        C: FromIterator<Self>,
    {
        protos.into_iter().map(Self::decode_proto_view).collect()
    }

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
    #[track_caller]
    fn decode_proto_view_from_slice(bytes: &[u8]) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = match <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
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

    /// Decode generated borrowed protobuf views with one reusable context.
    ///
    /// Collection decoding must duplicate the context for every element.
    /// Requiring [`Copy`] restricts this convenience to cheap context values or
    /// references instead of hiding a potentially expensive clone per member.
    ///
    /// Elements retain their input order and conversion stops at the first
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the context is incompatible with any view
    /// or a view cannot satisfy this runtime type's invariants.
    fn decode_proto_view_collection_with<'proto, 'view, C>(
        protos: impl IntoIterator<Item = &'view Self::ProtoView<'proto>>,
        context: Context,
    ) -> Result<C, Self::Error>
    where
        'proto: 'view,
        Self::ProtoView<'proto>: 'view,
        Context: Copy,
        C: FromIterator<Self>,
    {
        protos
            .into_iter()
            .map(|proto| Self::decode_proto_view_with(proto, context))
            .collect()
    }

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
    #[track_caller]
    fn decode_proto_view_from_slice_with(
        bytes: &[u8],
        context: Context,
    ) -> Result<Self, Self::Error>
    where
        Self::Error: FromProtoDecodeError,
    {
        let proto = match <Self::ProtoView<'_> as buffa::MessageView<'_>>::decode_view(bytes) {
            Ok(proto) => proto,
            Err(error) => return Err(Self::Error::from_proto_decode_error(error)),
        };
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
        DecodeProtoViewWith,
        DecodeProtoWith,
        EncodeProto,
        FromProtoDecodeError,
        ProtoCodec,
        ProtoInputDecodeError,
    };
    use crate::{
        buffa::{self, Message as _, MessageView as _},
        versions as versions_proto,
    };
    use std::collections::VecDeque;

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
        type Proto = versions_proto::CompactVersionVector;

        fn to_proto(&self) -> Self::Proto {
            versions_proto::CompactVersionVector {
                versions: Some(versions_proto::compact_version_vector::Versions::Synced(
                    Box::new(versions_proto::SyncedVersionVector {
                        group_version: self.0,
                        ..versions_proto::SyncedVersionVector::default()
                    }),
                )),
                ..versions_proto::CompactVersionVector::default()
            }
        }

        fn from_proto(proto: Self::Proto) -> Result<Self, Self::DecodeError> {
            match proto.versions {
                Some(versions_proto::compact_version_vector::Versions::Synced(synced)) => {
                    Ok(Self(synced.group_version))
                }
                Some(_) => Err(SyncedVersionError::UnsupportedVersions),
                None => Err(SyncedVersionError::MissingVersions),
            }
        }
    }

    impl DecodeProtoView for SyncedVersion {
        type Error = SyncedVersionError;
        type ProtoView<'a> = versions_proto::CompactVersionVectorView<'a>;

        fn decode_proto_view(proto: &Self::ProtoView<'_>) -> Result<Self, Self::Error> {
            match proto.versions.as_ref() {
                Some(versions_proto::compact_version_vector::VersionsView::Synced(synced)) => {
                    Ok(Self(synced.group_version))
                }
                Some(_) => Err(SyncedVersionError::UnsupportedVersions),
                None => Err(SyncedVersionError::MissingVersions),
            }
        }
    }

    impl DecodeProtoWith<u64> for SyncedVersion {
        type Error = SyncedVersionError;
        type Proto = versions_proto::CompactVersionVector;

        fn decode_proto_with(proto: Self::Proto, offset: u64) -> Result<Self, Self::Error> {
            let decoded = Self::decode_proto(proto)?;
            Ok(Self(decoded.0 + offset))
        }
    }

    impl DecodeProtoViewWith<u64> for SyncedVersion {
        type Error = SyncedVersionError;
        type ProtoView<'a> = versions_proto::CompactVersionVectorView<'a>;

        fn decode_proto_view_with(
            proto: &Self::ProtoView<'_>,
            offset: u64,
        ) -> Result<Self, Self::Error> {
            let decoded = Self::decode_proto_view(proto)?;
            Ok(Self(decoded.0 + offset))
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
    fn proto_collection_helpers_preserve_order_and_collection_type() {
        let versions = [SyncedVersion(3), SyncedVersion(5), SyncedVersion(8)];
        let protos: VecDeque<_> = SyncedVersion::encode_proto_collection(&versions);
        let decoded: VecDeque<_> = SyncedVersion::decode_proto_collection(protos)
            .expect("owned protobuf collection should decode");

        assert_eq!(decoded, VecDeque::from(versions));

        let no_versions: [SyncedVersion; 0] = [];
        let no_protos: Vec<versions_proto::CompactVersionVector> =
            SyncedVersion::encode_proto_collection(&no_versions);
        assert!(no_protos.is_empty());

        let empty: Vec<SyncedVersion> = SyncedVersion::decode_proto_collection(Vec::new())
            .expect("empty protobuf collection should decode");
        assert!(empty.is_empty());
    }

    #[test]
    fn proto_collection_helpers_return_the_first_conversion_error() {
        let missing = versions_proto::CompactVersionVector::default();
        let unsupported = versions_proto::CompactVersionVector {
            versions: Some(versions_proto::compact_version_vector::Versions::Full(
                Box::default(),
            )),
            ..versions_proto::CompactVersionVector::default()
        };

        let result = SyncedVersion::decode_proto_collection::<Vec<_>>([missing, unsupported]);

        assert!(matches!(result, Err(SyncedVersionError::MissingVersions)));
    }

    #[test]
    fn contextual_proto_collection_helpers_reuse_context_for_owned_and_views() {
        let versions = [SyncedVersion(1), SyncedVersion(2)];
        let owned_protos: Vec<_> = SyncedVersion::encode_proto_collection(&versions);
        let owned: Vec<_> = SyncedVersion::decode_proto_collection_with(owned_protos, 10)
            .expect("contextual owned collection should decode");
        assert_eq!(owned, [SyncedVersion(11), SyncedVersion(12)]);

        let payloads = versions
            .iter()
            .map(EncodeProto::encode_proto_to_vec)
            .collect::<Vec<_>>();
        let views = payloads
            .iter()
            .map(|payload| {
                versions_proto::CompactVersionVectorView::decode_view(payload)
                    .expect("protobuf view should decode")
            })
            .collect::<Vec<_>>();
        let viewed: Vec<_> = SyncedVersion::decode_proto_view_collection_with(&views, 20)
            .expect("contextual view collection should decode");
        assert_eq!(viewed, [SyncedVersion(21), SyncedVersion(22)]);

        let plain_viewed: Vec<_> = SyncedVersion::decode_proto_view_collection(&views)
            .expect("plain view collection should decode");
        assert_eq!(plain_viewed, versions);
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

        let missing_versions = versions_proto::CompactVersionVector::default().encode_to_vec();
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

        let missing_versions = versions_proto::CompactVersionVector::default().encode_to_vec();
        assert!(matches!(
            SyncedVersion::try_decode_proto_view_from_slice(&missing_versions),
            Err(ProtoInputDecodeError::Convert {
                source: SyncedVersionError::MissingVersions
            })
        ));
    }
}
