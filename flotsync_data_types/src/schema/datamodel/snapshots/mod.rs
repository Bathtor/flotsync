#[cfg(test)]
use super::validation::{ensure_nullable_basic_type, ensure_primitive_array_type};
#[cfg(test)]
use super::{BasicValue, BasicValueRef, DecodeError};
use super::{
    CounterValue,
    CounterValueRef,
    DataModelValueError,
    NullableBasicDataType,
    NullableBasicValue,
    NullableBasicValueRef,
    NullablePrimitiveType,
    NullablePrimitiveValue,
    NullablePrimitiveValueArray,
    NullablePrimitiveValueRef,
    PrimitiveType,
    PrimitiveValue,
    PrimitiveValueArray,
    PrimitiveValueArrayRef,
    PrimitiveValueRef,
    ReplicatedDataType,
    Schema,
    SchemaValueError,
    SchemaVisitError,
    VisitError,
    validation::{ensure_counter_type, ensure_finite_state_value, ensure_primitive_type},
};
use crate::{
    IdWithIndex,
    snapshot::{SnapshotNode, SnapshotSink},
};
use std::{collections::HashSet, convert::Infallible, marker::PhantomData};

mod codec;
mod contracts;
mod reader;
#[cfg(test)]
mod tests;
mod writer;

pub use contracts::{SchemaSnapshotEncoder, StateSnapshotFieldValue, StateSnapshotFieldValueRef};
pub(crate) use reader::SnapshotNodeSourceIter;
pub use reader::{
    DataSnapshotDecoder,
    DataSnapshotEncoder,
    SchemaSnapshotDecoder,
    SnapshotNodeSource,
};
pub(crate) use writer::{SchemaSnapshotEncodingWriter, prepare_schema_snapshot_encoder};
