//! Shared traits for converting between runtime types and generated protobuf messages.

use crate::buffa;

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
