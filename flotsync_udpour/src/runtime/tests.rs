//! Tests for the UDPour runtime adapter.

use super::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    #[test]
    fn rejects_need_parts_frame_budget_that_cannot_fit_a_header() {
        let sender = SenderConfig::new(
            NonZeroUsize::new(256).unwrap(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let receiver = ReceiverConfig {
            repair_interval: Duration::from_millis(10),
            give_up_timeout: Duration::from_secs(1),
            max_need_parts_frame_len: FRAME_HEADER_LEN,
            delivered_tombstone_timeout: Duration::ZERO,
        };

        let error = UDPourConfig::new(sender, receiver).unwrap_err();
        assert!(matches!(
            error,
            UDPourConfigError::NeedPartsFrameLenTooSmall {
                max_need_parts_frame_len: FRAME_HEADER_LEN
            }
        ));
    }
}
