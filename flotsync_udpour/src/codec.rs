use crate::{
    roaring_helpers::{
        MIN_ENCODED_NON_EMPTY_BITMAP_LEN, RoaringBitmapError, select_bitmap_chunk,
    },
    types::*,
    wire::{DecodeFromBuf, EncodeToBufMut},
};
#[cfg(test)]
use bytes::BytesMut;
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use snafu::prelude::*;

/// Fixed protocol header size in bytes.
pub const FRAME_HEADER_LEN: usize = 20;

/// Returns the exact encoded frame length in bytes.
pub(crate) fn encoded_frame_len(frame: &UDPourFrame) -> usize {
    match frame {
        UDPourFrame::Payload(frame) => frame.header.encoded_len() + frame.payload.len(),
        UDPourFrame::Ack(frame) => frame.header.encoded_len(),
        UDPourFrame::NoLongerAvailable(frame) => frame.header.encoded_len(),
        UDPourFrame::NeedParts(frame) => {
            frame.header.encoded_len() + frame.missing_parts.encoded_len()
        }
    }
}

/// Decodes one frame from an `IoPayload`.
pub(crate) fn decode_frame(payload: IoPayload) -> Result<UDPourFrame, CodecError> {
    ensure!(
        payload.len() >= FRAME_HEADER_LEN,
        FrameTooShortSnafu { len: payload.len() }
    );
    let mut cursor = payload.cursor();
    let header = UDPourHeader::decode_from_buf(&mut cursor).context(TypeSnafu)?;
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
            let mut body_cursor = body.cursor();
            let missing_parts =
                RoaringBitmap::decode_from_buf(&mut body_cursor).context(BitmapSnafu)?;
            ensure!(!missing_parts.is_empty(), EmptyNeedPartsSnafu);
            Ok(UDPourFrame::NeedParts(NeedPartsFrame {
                header,
                missing_parts,
            }))
        }
    }
}

/// Fits one missing-part chunk into one `NeedParts` frame within `max_frame_len`.
///
/// Returns `Ok(None)` only when `missing_parts` still contains values overall but none remain
/// strictly after `after_exclusive`. Callers should then decide whether to wrap their repair
/// cursor back to the beginning of the missing-part set.
pub(crate) fn fit_one_need_parts_frame(
    message_id: MessageId,
    part_count: PartCount,
    checksum: Checksum,
    missing_parts: &RoaringBitmap,
    after_exclusive: Option<u32>,
    max_frame_len: usize,
) -> Result<Option<NeedPartsFrame>, CodecError> {
    ensure!(
        max_frame_len >= FRAME_HEADER_LEN + MIN_ENCODED_NON_EMPTY_BITMAP_LEN,
        MaxFrameLenTooSmallSnafu { max_frame_len }
    );
    ensure!(!missing_parts.is_empty(), EmptyNeedPartsSnafu);

    let max_body_len = max_frame_len - FRAME_HEADER_LEN;
    let header = UDPourHeader::control(FrameType::NeedParts, message_id, part_count, checksum)
        .context(TypeSnafu)?;
    let Some(chunk) =
        select_bitmap_chunk(missing_parts, after_exclusive, max_body_len).context(BitmapSnafu)?
    else {
        return Ok(None);
    };
    Ok(Some(NeedPartsFrame {
        header,
        missing_parts: chunk,
    }))
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
    #[snafu(display(
        "max frame length {max_frame_len} is too small; need at least {} bytes for the fixed header plus one non-empty roaring bitmap body",
        FRAME_HEADER_LEN + MIN_ENCODED_NON_EMPTY_BITMAP_LEN
    ))]
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

/*
 * Test Helpers
 */

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
            let body = frame.missing_parts.encode_to_bytes().context(BitmapSnafu)?;
            Ok(IoPayload::chain([
                header_payload,
                IoPayload::from(body.freeze()),
            ]))
        }
    }
}

#[cfg(test)]
fn encode_header(header: UDPourHeader) -> BytesMut {
    header
        .encode_to_bytes()
        .expect("UDPourHeader::encode_into_buf is infallible")
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
    fn need_parts_chunk_respects_frame_budget() {
        let mut missing_parts = RoaringBitmap::new();
        for part_number in 0..4096u32 {
            missing_parts.insert(part_number * 3);
        }

        let frame = fit_one_need_parts_frame(
            MessageId(42),
            PartCount::new(u32::MAX).unwrap(),
            Checksum(101),
            &missing_parts,
            None,
            96,
        )
        .unwrap()
        .unwrap();

        let encoded = encode_frame(&UDPourFrame::NeedParts(frame)).unwrap();
        assert!(encoded.len() <= 96);
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
