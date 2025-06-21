use base64::engine::{GeneralPurpose, general_purpose::URL_SAFE_NO_PAD};
use std::fmt;

/// Shorthand for [[base64::display::Base64Display]] with fixed engine.
pub struct Base64Display<'a> {
    inner: base64::display::Base64Display<'a, 'static, GeneralPurpose>,
}
impl<'a> Base64Display<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            inner: base64::display::Base64Display::new(bytes, &URL_SAFE_NO_PAD),
        }
    }
}
impl fmt::Display for Base64Display<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
