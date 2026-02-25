use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use itertools::Itertools;
use snafu::prelude::*;
use uuid::Uuid;

/// UUID segment encodings supported by flotsync.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UuidEncoding {
    /// Standard UUID string form like `123e4567-e89b-12d3-a456-426614174000`.
    Hyphenated,
    /// Niceware word-list encoding using `:` as the separator in a single identifier segment.
    Words,
    /// URL-safe base64 without padding.
    Base64,
}

#[derive(Debug, Snafu)]
pub enum UuidEncodingError {
    #[snafu(display("Failed to decode hyphenated UUID segment '{input}': {source}"))]
    DecodeHyphenatedError { input: String, source: uuid::Error },
    #[snafu(display("Failed to encode UUID as words: {source}"))]
    EncodeWordsError { source: niceware::Error },
    #[snafu(display("Failed to decode words UUID segment '{input}': {source}"))]
    DecodeWordsError {
        input: String,
        source: niceware::Error,
    },
    #[snafu(display("Failed to decode base64 UUID segment '{input}': {source}"))]
    DecodeBase64Error {
        input: String,
        source: base64::DecodeError,
    },
    #[snafu(display(
        "Decoded UUID bytes from segment '{input}' had invalid length {actual}, expected 16."
    ))]
    InvalidByteLengthError { input: String, actual: usize },
}

pub trait UuidEncodingExt {
    fn encode_words(&self) -> Result<String, UuidEncodingError>;
    fn encode_base64(&self) -> String;

    fn decode_words(input: &str) -> Result<Uuid, UuidEncodingError>;
    fn decode_base64(input: &str) -> Result<Uuid, UuidEncodingError>;
}

impl UuidEncodingExt for Uuid {
    fn encode_words(&self) -> Result<String, UuidEncodingError> {
        let words = niceware::bytes_to_passphrase(self.as_bytes()).context(EncodeWordsSnafu)?;
        Ok(words.join(":"))
    }

    fn encode_base64(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.as_bytes())
    }

    fn decode_words(input: &str) -> Result<Uuid, UuidEncodingError> {
        let words = input.split(':').collect_vec();
        let bytes = niceware::passphrase_to_bytes(&words).context(DecodeWordsSnafu {
            input: input.to_owned(),
        })?;
        uuid_from_decoded_bytes(input, bytes)
    }

    fn decode_base64(input: &str) -> Result<Uuid, UuidEncodingError> {
        let bytes = URL_SAFE_NO_PAD.decode(input).context(DecodeBase64Snafu {
            input: input.to_owned(),
        })?;
        uuid_from_decoded_bytes(input, bytes)
    }
}

fn uuid_from_decoded_bytes(input: &str, bytes: Vec<u8>) -> Result<Uuid, UuidEncodingError> {
    if bytes.len() != 16 {
        return InvalidByteLengthSnafu {
            input: input.to_owned(),
            actual: bytes.len(),
        }
        .fail();
    }
    let mut data = [0u8; 16];
    data.copy_from_slice(&bytes);
    Ok(Uuid::from_bytes(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn base64_literal_roundtrip() {
        let uuid = Uuid::from_bytes([0u8; 16]);
        let encoded = uuid.encode_base64();
        assert_eq!(encoded, "AAAAAAAAAAAAAAAAAAAAAA");
        let decoded = <Uuid as UuidEncodingExt>::decode_base64(&encoded).unwrap();
        assert_eq!(decoded, uuid);
    }

    #[test]
    fn words_literal_roundtrip() {
        let uuid = Uuid::from_bytes([255u8; 16]);
        let encoded = uuid.encode_words().unwrap();
        assert_eq!(
            encoded,
            "zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva:zyzzyva"
        );
        let decoded = <Uuid as UuidEncodingExt>::decode_words(&encoded).unwrap();
        assert_eq!(decoded, uuid);
    }

    proptest! {
        #[test]
        fn base64_roundtrips(bytes in any::<[u8; 16]>()) {
            let uuid = Uuid::from_bytes(bytes);
            let encoded = uuid.encode_base64();
            let decoded = <Uuid as UuidEncodingExt>::decode_base64(&encoded).expect("base64 decode should succeed");
            prop_assert_eq!(decoded, uuid);
            prop_assert_eq!(encoded.len(), 22);
        }
    }

    proptest! {
        #[test]
        fn words_roundtrips(bytes in any::<[u8; 16]>()) {
            let uuid = Uuid::from_bytes(bytes);
            let encoded = uuid.encode_words().expect("word encoding should succeed");
            let decoded = <Uuid as UuidEncodingExt>::decode_words(&encoded).expect("word decode should succeed");
            prop_assert_eq!(decoded, uuid);
            prop_assert!(encoded.contains(':'));
        }
    }
}
