#![feature(linked_list_cursors)]
#![feature(assert_matches)]
#![feature(vec_from_fn)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
use snafu::{Location, prelude::*};

pub mod any_data;
#[allow(unused, reason = "Might re-use some already implemented things later.")]
mod linear_data;
pub mod snapshot;
pub mod text;

pub use linear_data::{DataOperation, IdWithIndex, IdWithIndexRange};

#[derive(Debug, Snafu)]
#[snafu(display("The diff could not be applied due to a logic error at {location}: {context}"))]
pub struct InternalError {
    context: String,
    #[snafu(implicit)]
    location: Location,
}
