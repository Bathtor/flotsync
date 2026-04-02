#[cfg(test)]
use crate::roaring_helpers::serialize_bitmap;
use crate::{
    roaring_helpers::{
        RoaringBitmapError,
        deserialize_bitmap_from,
        serialize_bitmap_into,
        split_bitmap_to_serialized_bounds,
    },
    types::*,
};
use bytes::{Buf, BufMut};
#[cfg(test)]
use bytes::{Bytes, BytesMut};
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use snafu::prelude::*;

/// Fixed protocol header size in bytes.
pub const FRAME_HEADER_LEN: usize = 20;

/// Decodes one frame from an `IoPayload`.
///
/// The header is copied into a small local buffer for parsing. The payload body
/// of a `Payload` frame stays in `IoPayload` form.
pub(crate) fn decode_frame(payload: IoPayload) -> Result<UDPourFrame, CodecError> {
    ensure!(
        payload.len() >= FRAME_HEADER_LEN,
        FrameTooShortSnafu { len: payload.len() }
    );
    let header = decode_header_payload(&payload)?
        .validate()
        .context(TypeSnafu)?;
    let body_len = payload.len() - FRAME_HEADER_LEN;
    match header.frame_type {
        FrameType::Payload => {
            let body = payload
                .clone()
                .try_slice(FRAME_HEADER_LEN, body_len)
                .context(InvalidPayloadSliceSnafu {
                    payload_len: payload.len(),
                    offset: FRAME_HEADER_LEN,
                    len: body_len,
                })?;
            Ok(UDPourFrame::Payload(PayloadFrame {
                header,
                payload: body,
            }))
        }
        FrameType::Ack => {
            ensure!(
                body_len == 0,
                UnexpectedBodySnafu {
                    frame_type: header.frame_type
                }
            );
            Ok(UDPourFrame::Ack(AckFrame { header }))
        }
        FrameType::NoLongerAvailable => {
            ensure!(
                body_len == 0,
                UnexpectedBodySnafu {
                    frame_type: header.frame_type
                }
            );
            Ok(UDPourFrame::NoLongerAvailable(NoLongerAvailableFrame {
                header,
            }))
        }
        FrameType::NeedParts => {
            ensure!(body_len > 0, EmptyNeedPartsSnafu);
            let body = payload
                .clone()
                .try_slice(FRAME_HEADER_LEN, body_len)
                .context(InvalidPayloadSliceSnafu {
                    payload_len: payload.len(),
                    offset: FRAME_HEADER_LEN,
                    len: body_len,
                })?;
            let missing_parts =
                deserialize_bitmap_from(body.cursor().reader()).context(BitmapSnafu)?;
            ensure!(!missing_parts.is_empty(), EmptyNeedPartsSnafu);
            Ok(UDPourFrame::NeedParts(NeedPartsFrame {
                header,
                missing_parts,
            }))
        }
    }
}

/// Splits one missing-part set into as many `NeedParts` frames as needed to fit
/// within `max_frame_len`.
pub(crate) fn split_need_parts_frames(
    message_id: MessageId,
    part_count: PartCount,
    checksum: Checksum,
    missing_parts: &RoaringBitmap,
    max_frame_len: usize,
) -> Result<Vec<NeedPartsFrame>, CodecError> {
    ensure!(
        max_frame_len > FRAME_HEADER_LEN,
        MaxFrameLenTooSmallSnafu { max_frame_len }
    );
    ensure!(!missing_parts.is_empty(), EmptyNeedPartsSnafu);

    let max_body_len = max_frame_len - FRAME_HEADER_LEN;
    let header = UDPourHeader::control(FrameType::NeedParts, message_id, part_count, checksum)
        .context(TypeSnafu)?;
    let mut remaining = Some(missing_parts.clone());
    let mut frames = Vec::new();
    while let Some(bitmap) = remaining.take() {
        let step = split_bitmap_to_serialized_bounds(bitmap, max_body_len).context(BitmapSnafu)?;
        frames.push(NeedPartsFrame {
            header,
            missing_parts: step.chunk,
        });
        remaining = step.rest;
    }
    Ok(frames)
}

/// Returns the exact encoded frame length in bytes.
pub(crate) fn encoded_frame_len(frame: &UDPourFrame) -> usize {
    match frame {
        UDPourFrame::Payload(frame) => FRAME_HEADER_LEN + frame.payload.len(),
        UDPourFrame::Ack(_) | UDPourFrame::NoLongerAvailable(_) => FRAME_HEADER_LEN,
        UDPourFrame::NeedParts(frame) => FRAME_HEADER_LEN + frame.missing_parts.serialized_size(),
    }
}

/// Encodes one validated fixed header into any synchronous `BufMut`.
pub(crate) fn encode_header_into<B>(header: UDPourHeader, out: &mut B)
where
    B: BufMut,
{
    debug_assert!(
        header.validate().is_ok(),
        "encode_header_into expects a validated header"
    );
    out.put_u8(u8::from(header.frame_type));
    out.put_u8(header.version);
    out.put_u8(header.flags.bits());
    out.put_u8(header.reserved);
    out.put_u32(header.message_id.0);
    out.put_u32(header.part_number.0);
    out.put_u32(u32::from(header.part_count));
    out.put_u32(header.checksum.0);
}

/// Encodes one `NeedParts` body into an existing synchronous writer.
pub(crate) fn encode_need_parts_body_into<W>(
    missing_parts: &RoaringBitmap,
    writer: &mut W,
) -> Result<(), RoaringBitmapError>
where
    W: std::io::Write,
{
    serialize_bitmap_into(missing_parts, writer)?;
    Ok(())
}

/// Encodes one frame into an `IoPayload` for tests.
///
/// Payload frames preserve their payload storage as-is and prepend only the
/// fixed 20-byte header. Control frames emit only owned header/body bytes.
#[cfg(test)]
pub(crate) fn encode_frame(frame: &UDPourFrame) -> Result<IoPayload, CodecError> {
    let header = frame.header();
    header.validate().context(TypeSnafu)?;
    let header_bytes = encode_header(header).freeze();
    let header_payload = IoPayload::from(header_bytes);
    match frame {
        UDPourFrame::Payload(frame) => {
            Ok(IoPayload::chain([header_payload, frame.payload.clone()]))
        }
        UDPourFrame::Ack(_) | UDPourFrame::NoLongerAvailable(_) => Ok(header_payload),
        UDPourFrame::NeedParts(frame) => {
            let body = serialize_bitmap(&frame.missing_parts).context(BitmapSnafu)?;
            Ok(IoPayload::chain([
                header_payload,
                IoPayload::from(Bytes::from(body)),
            ]))
        }
    }
}

#[cfg(test)]
fn encode_header(header: UDPourHeader) -> BytesMut {
    let mut bytes = BytesMut::with_capacity(FRAME_HEADER_LEN);
    encode_header_into(header, &mut bytes);
    bytes
}

fn decode_header_payload(payload: &IoPayload) -> Result<UDPourHeader, CodecError> {
    let mut header_bytes = [0_u8; FRAME_HEADER_LEN];
    let mut cursor = payload.cursor();
    cursor.copy_to_slice(&mut header_bytes);
    decode_header(&header_bytes)
}

fn decode_header(bytes: &[u8]) -> Result<UDPourHeader, CodecError> {
    let frame_type = FrameType::try_from(bytes[0]).context(TypeSnafu)?;
    let part_count = PartCount::new(u32::from_be_bytes(
        bytes[12..16].try_into().expect("part_count slice"),
    ))
    .context(TypeSnafu)?;
    Ok(UDPourHeader {
        frame_type,
        version: bytes[1],
        flags: FrameFlags::from_bits(bytes[2]),
        reserved: bytes[3],
        message_id: MessageId(u32::from_be_bytes(
            bytes[4..8].try_into().expect("message_id slice"),
        )),
        part_number: PartNumber(u32::from_be_bytes(
            bytes[8..12].try_into().expect("part_number slice"),
        )),
        part_count,
        checksum: Checksum(u32::from_be_bytes(
            bytes[16..20].try_into().expect("checksum slice"),
        )),
    })
}

/// Codec-level errors.
#[derive(Debug, Snafu)]
pub(crate) enum CodecError {
    #[snafu(display("Frame length {len} is shorter than the {FRAME_HEADER_LEN}-byte header"))]
    FrameTooShort { len: usize },
    #[snafu(display("Frame {:?} must not carry a body", frame_type))]
    UnexpectedBody { frame_type: FrameType },
    #[snafu(display("NeedParts must encode at least one missing part"))]
    EmptyNeedParts,
    #[snafu(display("max frame length {max_frame_len} must be larger than the fixed header"))]
    MaxFrameLenTooSmall { max_frame_len: usize },
    #[snafu(display("Invalid roaring bitmap payload"))]
    Bitmap { source: RoaringBitmapError },
    #[snafu(display("Invalid datagram frame"))]
    Type { source: UDPourTypeError },
    #[snafu(display(
        "Invalid IoPayload slice range offset={offset}, len={len}, payload_len={payload_len}"
    ))]
    InvalidPayloadSlice {
        payload_len: usize,
        offset: usize,
        len: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn raw_frame(bytes: Vec<u8>) -> IoPayload {
        IoPayload::from(Bytes::from(bytes))
    }

    fn encode_control_header(frame_type: FrameType) -> Vec<u8> {
        encode_header(
            UDPourHeader::control(
                frame_type,
                MessageId(99),
                PartCount::new(4).unwrap(),
                Checksum(777),
            )
            .unwrap(),
        )
        .to_vec()
    }

    #[test]
    fn payload_round_trip() {
        let frame = UDPourFrame::Payload(PayloadFrame {
            header: UDPourHeader::payload(
                MessageId(7),
                PartNumber(2),
                PartCount::new(4).unwrap(),
                Checksum(123),
            ),
            payload: IoPayload::from_static(b"hello"),
        });
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn need_parts_round_trip() {
        let mut missing_parts = RoaringBitmap::new();
        missing_parts.insert(1);
        missing_parts.insert(3);
        let frame = UDPourFrame::NeedParts(NeedPartsFrame {
            header: UDPourHeader::control(
                FrameType::NeedParts,
                MessageId(8),
                PartCount::new(10).unwrap(),
                Checksum(77),
            )
            .unwrap(),
            missing_parts,
        });
        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(encoded).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn need_parts_fragmentation_respects_frame_budget() {
        let mut missing_parts = RoaringBitmap::new();
        for part_number in 0..4096u32 {
            missing_parts.insert(part_number * 3);
        }

        let frames = split_need_parts_frames(
            MessageId(42),
            PartCount::new(u32::MAX).unwrap(),
            Checksum(101),
            &missing_parts,
            96,
        )
        .unwrap();

        assert!(frames.len() > 1);
        for frame in frames {
            let encoded = encode_frame(&UDPourFrame::NeedParts(frame)).unwrap();
            assert!(encoded.len() <= 96);
        }
    }

    #[test]
    fn rejects_short_header() {
        let error = decode_frame(IoPayload::from_static(b"too short")).unwrap_err();
        assert!(matches!(error, CodecError::FrameTooShort { .. }));
    }

    #[test]
    fn rejects_body_on_ack() {
        let mut bytes = encode_control_header(FrameType::Ack);
        bytes.push(0xAA);

        let error = decode_frame(raw_frame(bytes)).unwrap_err();
        assert!(matches!(
            error,
            CodecError::UnexpectedBody {
                frame_type: FrameType::Ack,
            }
        ));
    }

    #[test]
    fn rejects_body_on_no_longer_available() {
        let mut bytes = encode_control_header(FrameType::NoLongerAvailable);
        bytes.push(0xAA);

        let error = decode_frame(raw_frame(bytes)).unwrap_err();
        assert!(matches!(
            error,
            CodecError::UnexpectedBody {
                frame_type: FrameType::NoLongerAvailable,
            }
        ));
    }

    #[test]
    fn rejects_empty_need_parts() {
        let bytes = encode_control_header(FrameType::NeedParts);

        let error = decode_frame(raw_frame(bytes)).unwrap_err();
        assert!(matches!(error, CodecError::EmptyNeedParts));
    }

    #[test]
    fn rejects_bad_version() {
        let mut bytes = encode_control_header(FrameType::Ack);
        bytes[1] = PROTOCOL_VERSION + 1;

        let error = decode_frame(raw_frame(bytes)).unwrap_err();
        assert!(matches!(
            error,
            CodecError::Type {
                source: UDPourTypeError::UnsupportedVersion { .. },
            }
        ));
    }

    #[test]
    fn rejects_bad_control_part_number() {
        let mut bytes = encode_control_header(FrameType::Ack);
        bytes[8..12].copy_from_slice(&1u32.to_be_bytes());

        let error = decode_frame(raw_frame(bytes)).unwrap_err();
        assert!(matches!(
            error,
            CodecError::Type {
                source: UDPourTypeError::ControlPartNumberMustBeZero { part_number: 1 },
            }
        ));
    }
}
