use derive_more::{Deref, Display, From};

pub use uuid;
pub mod errors;
pub mod services;

/// A new-type wrapper for socket ports.
#[derive(Clone, Copy, Debug, Deref, Display, PartialEq, Eq, Hash, From, PartialOrd, Ord)]
pub struct Port(u16);

#[cfg(test)]
mod tests {
    use super::*;
}
