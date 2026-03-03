#![feature(vec_from_fn)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
pub use chrono;
use snafu::{Location, prelude::*};
use std::borrow::Cow;

pub mod any_data;
#[allow(unused, reason = "Might re-use some already implemented things later.")]
mod linear_data;
pub mod schema;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
pub mod text;
pub mod snapshot {
    pub use crate::linear_data::snapshot::*;
}

pub use linear_data::{DataOperation, IdWithIndex, IdWithIndexRange, IntegrityError};

#[derive(Debug, Snafu)]
#[snafu(display("The diff could not be applied due to a logic error at {location}: {context}"))]
pub struct InternalError {
    context: String,
    #[snafu(implicit)]
    location: Location,
}

#[derive(Debug, Snafu)]
pub enum OperationError {
    #[snafu(display(
        "An unusupported operation variant was encountered at {location}. {explanation}"
    ))]
    UnsupportedOperationVariant {
        explanation: Cow<'static, str>,
        #[snafu(implicit)]
        location: Location,
    },
}
