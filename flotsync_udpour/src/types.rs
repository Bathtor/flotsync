use bytes::Buf;
use flotsync_io::prelude::IoPayload;
use roaring::RoaringBitmap;
use snafu::prelude::*;
use std::num::NonZeroU32;

/// Current UDPour protocol version.
pub(crate) const PROTOCOL_VERSION: u8 = 1;

/// Bitflags carried in the shared UDPour header.
///
/// This is a small wrapper rather than a raw `u8` so call sites can describe
/// intent directly and we have one place to grow future payload/control flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub(crate) struct FrameFlags(u8);

impl FrameFlags {
    /// Default header flags for frames without any optional signalling bits.
    pub const DEFAULT: Self = Self(0);
    const RETRANSMIT_MASK: u8 = 0x01;

    /// Wraps raw on-the-wire bits.
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Returns these flags with the retransmit bit enabled.
    pub fn with_retransmit(mut self) -> Self {
        self.0 |= Self::RETRANSMIT_MASK;
        self
    }

    /// Returns the raw on-the-wire bits.
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns whether this frame is marked as a sender-side payload retransmission.
    pub const fn is_retransmit(self) -> bool {
        (self.0 & Self::RETRANSMIT_MASK) != 0
    }
}

/// Whole-message CRC32C checksum.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Checksum(pub u32);

/// Sender-local logical message id.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub u32);

/// Zero-based part number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PartNumber(pub(crate) u32);

/// Total number of parts in one logical message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartCount(NonZeroU32);

impl PartCount {
    /// Wraps one raw part count while enforcing the protocol's non-zero rule.
    pub(crate) fn new(value: u32) -> Result<Self, UDPourTypeError> {
        let value = NonZeroU32::new(value).context(ZeroPartCountSnafu)?;
        Ok(Self(value))
    }

    /// Returns the raw count.
    pub fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns the zero-based number of the final part.
    pub(crate) fn last_part_number(self) -> PartNumber {
        PartNumber(self.get() - 1)
    }
}

impl From<PartCount> for u32 {
    fn from(value: PartCount) -> Self {
        value.get()
    }
}

/// Fixed-header frame kind.
///
/// The top bit is reserved to distinguish who originated a frame:
///
/// - sender-originated frames use `0x01..0x7F`
/// - receiver-originated frames use `0x80..0xFE`
///
/// Note that `0x00` and `0xFF` are reserved for future signalling.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FrameType {
    /// One logical message part produced by the sender.
    Payload = 0x01,
    /// One sender-side rejection that a previously sent logical message can no
    /// longer be repaired because its retained parts are gone.
    NoLongerAvailable = 0x02,
    /// One receiver-side advisory acknowledgment that all parts were received
    /// and whole-message checksum verification succeeded.
    Ack = 0x81,
    /// One receiver-side repair request carrying a set of missing part numbers.
    NeedParts = 0x82,
}

impl TryFrom<u8> for FrameType {
    type Error = UDPourTypeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Payload),
            0x02 => Ok(Self::NoLongerAvailable),
            0x81 => Ok(Self::Ack),
            0x82 => Ok(Self::NeedParts),
            _ => UnknownFrameTypeSnafu { value }.fail(),
        }
    }
}

impl From<FrameType> for u8 {
    fn from(value: FrameType) -> Self {
        value as u8
    }
}

/// Shared fixed 20-byte datagram header.
///
/// This header is present on every protocol frame, including pure control
/// traffic such as `Ack` and `NeedParts`.
///
/// The header deliberately carries enough metadata to let the receiver decide
/// whether two packets really belong to the same logical message without first
/// parsing any variable-length body:
///
/// - `message_id` identifies the sender-local logical message instance
/// - `part_count` states how many `Payload` parts make up that logical message
/// - `checksum` is the whole-message CRC32C over the fully reassembled payload
///
/// Together with the forwarded UDP source address, that gives the runtime the
/// effective transfer key `(source, message_id)` and the extra guards needed to
/// detect confusion between late packets and a later reuse of the same message
/// id.
///
/// `Payload` frames use `part_number` to identify one concrete payload slice.
/// Control frames do not refer to one concrete slice and therefore always carry
/// `part_number = 0`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct UDPourHeader {
    /// Fixed frame kind discriminator.
    pub frame_type: FrameType,
    /// Protocol version for this frame format.
    pub version: u8,
    /// Per-frame feature bits.
    ///
    /// The low bit currently marks a sender-side payload retransmission that
    /// was emitted in response to `NeedParts`.
    pub flags: FrameFlags,
    /// Reserved for future header extensions.
    pub reserved: u8,
    /// Sender-local logical message identifier.
    pub message_id: MessageId,
    /// Zero-based payload part number.
    ///
    /// This is meaningful only for `Payload` frames. Control frames must leave
    /// it at zero.
    pub part_number: PartNumber,
    /// Total number of payload parts in the logical message.
    pub part_count: PartCount,
    /// Whole-message CRC32C checksum over the fully reassembled payload bytes.
    pub checksum: Checksum,
}

impl UDPourHeader {
    /// Builds one payload header for tests and local fixtures.
    #[cfg(test)]
    pub fn payload(
        message_id: MessageId,
        part_number: PartNumber,
        part_count: PartCount,
        checksum: Checksum,
    ) -> Self {
        Self::payload_with_flags(
            message_id,
            part_number,
            part_count,
            checksum,
            FrameFlags::DEFAULT,
        )
    }

    pub(crate) fn payload_with_flags(
        message_id: MessageId,
        part_number: PartNumber,
        part_count: PartCount,
        checksum: Checksum,
        flags: FrameFlags,
    ) -> Self {
        Self {
            frame_type: FrameType::Payload,
            version: PROTOCOL_VERSION,
            flags,
            reserved: 0,
            message_id,
            part_number,
            part_count,
            checksum,
        }
    }

    /// Builds one control-frame header.
    ///
    /// Control frames do not carry one concrete part. Their `part_number`
    /// remains zero on the wire.
    pub fn control(
        frame_type: FrameType,
        message_id: MessageId,
        part_count: PartCount,
        checksum: Checksum,
    ) -> Result<Self, UDPourTypeError> {
        ensure!(frame_type != FrameType::Payload, PayloadControlHeaderSnafu);
        Ok(Self {
            frame_type,
            version: PROTOCOL_VERSION,
            flags: FrameFlags::DEFAULT,
            reserved: 0,
            message_id,
            part_number: PartNumber(0),
            part_count,
            checksum,
        })
    }

    /// Validates one header against protocol invariants.
    pub fn validate(self) -> Result<Self, UDPourTypeError> {
        ensure!(
            self.version == PROTOCOL_VERSION,
            UnsupportedVersionSnafu {
                version: self.version
            }
        );
        match self.frame_type {
            FrameType::Payload => {
                ensure!(
                    self.part_number.0 < self.part_count.get(),
                    PartNumberOutOfRangeSnafu {
                        part_number: self.part_number.0,
                        part_count: self.part_count.get(),
                    }
                );
            }
            FrameType::Ack | FrameType::NeedParts | FrameType::NoLongerAvailable => {
                ensure!(
                    self.part_number.0 == 0,
                    ControlPartNumberMustBeZeroSnafu {
                        part_number: self.part_number.0,
                    }
                );
            }
        }
        Ok(self)
    }

    /// Returns whether this header marks a sender-side payload retransmission.
    pub fn is_retransmit(self) -> bool {
        self.frame_type == FrameType::Payload && self.flags.is_retransmit()
    }
}

/// One payload-carrying datagram.
#[derive(Clone, Debug)]
pub(crate) struct PayloadFrame {
    pub header: UDPourHeader,
    pub payload: IoPayload,
}

impl PartialEq for PayloadFrame {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && io_payload_eq(&self.payload, &other.payload)
    }
}

/// One receiver acknowledgment datagram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AckFrame {
    pub header: UDPourHeader,
}

/// One receiver repair request datagram.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NeedPartsFrame {
    pub header: UDPourHeader,
    pub missing_parts: RoaringBitmap,
}

impl Eq for NeedPartsFrame {}

/// One sender-side late-repair rejection datagram.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NoLongerAvailableFrame {
    pub header: UDPourHeader,
}

/// Any datagram frame.
#[derive(Clone, Debug)]
pub(crate) enum UDPourFrame {
    Payload(PayloadFrame),
    Ack(AckFrame),
    NeedParts(NeedPartsFrame),
    NoLongerAvailable(NoLongerAvailableFrame),
}

impl UDPourFrame {
    /// Returns the shared fixed header.
    pub fn header(&self) -> UDPourHeader {
        match self {
            Self::Payload(frame) => frame.header,
            Self::Ack(frame) => frame.header,
            Self::NeedParts(frame) => frame.header,
            Self::NoLongerAvailable(frame) => frame.header,
        }
    }
}

impl PartialEq for UDPourFrame {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Payload(left), Self::Payload(right)) => left == right,
            (Self::Ack(left), Self::Ack(right)) => left == right,
            (Self::NeedParts(left), Self::NeedParts(right)) => left == right,
            (Self::NoLongerAvailable(left), Self::NoLongerAvailable(right)) => left == right,
            _ => false,
        }
    }
}

pub(crate) fn io_payload_eq(left: &IoPayload, right: &IoPayload) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut left_cursor = left.cursor();
    let mut right_cursor = right.cursor();
    while left_cursor.has_remaining() {
        let left_chunk = left_cursor.chunk();
        let right_chunk = right_cursor.chunk();
        let compared_len = left_chunk.len().min(right_chunk.len());
        debug_assert!(
            compared_len > 0,
            "IoPayload cursors must not yield empty chunks while bytes remain"
        );
        if left_chunk[..compared_len] != right_chunk[..compared_len] {
            return false;
        }
        left_cursor.advance(compared_len);
        right_cursor.advance(compared_len);
    }

    true
}

/// Local type validation and parse errors.
#[derive(Debug, Snafu)]
pub(crate) enum UDPourTypeError {
    #[snafu(display("Unsupported protocol version {version}"))]
    UnsupportedVersion { version: u8 },
    #[snafu(display("Frame type 0x{value:02X} is not assigned"))]
    UnknownFrameType { value: u8 },
    #[snafu(display("part_count must be greater than zero"))]
    ZeroPartCount,
    #[snafu(display("Payload part_number {part_number} is outside part_count {part_count}"))]
    PartNumberOutOfRange { part_number: u32, part_count: u32 },
    #[snafu(display("Control frame part_number must be zero, got {part_number}"))]
    ControlPartNumberMustBeZero { part_number: u32 },
    #[snafu(display("control() must not be used for payload frames"))]
    PayloadControlHeader,
}
