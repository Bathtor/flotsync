use crate::buffa::Message as BuffaMessage;
use flotsync_io::prelude::{EgressAsyncWriter, EgressPool, Error as IoError, IoPayload};
use flotsync_utils::{BoxFuture, IString};
use futures_util::FutureExt;
use snafu::Snafu;

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

impl<M> FlotsyncSerializable for M
where
    M: BuffaMessage + Send + Sync + 'static,
{
    fn serialized_size_hint(&self) -> SizeHint {
        SizeHint::Exact(self.compute_size() as usize)
    }

    fn serialize_into<'a>(
        &'a self,
        writer: &'a mut EgressAsyncWriter,
    ) -> BoxFuture<'a, Result<(), FlotsyncSerializeError>> {
        async move {
            let reserved_bytes = self.compute_size() as usize;
            let mut reserved = writer
                .write_with_reserved(reserved_bytes)
                .await
                .map_err(|source| FlotsyncSerializeError::Io { source })?;
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
    let payload = writer
        .finish()
        .map_err(|source| FlotsyncSerializeError::Io { source })?;
    Ok(payload.unwrap_or_else(|| IoPayload::from_static(b"")))
}
