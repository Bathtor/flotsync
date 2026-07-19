//! Coverage tests for exhaustive schema and operation generation.

use super::*;
use crate::schema::datamodel::validate_schema_operation_fields;
use std::{assert_matches, collections::BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PrimitiveCoverageKey {
    String,
    UInt,
    Int,
    Byte,
    Float,
    Boolean,
    Binary,
    Date,
    Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NullableCoverageKey {
    NonNull,
    Nullable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum DirectionCoverageKey {
    Ascending,
    Descending,
}

fn count_as_u32(count: usize) -> u32 {
    u32::try_from(count).expect("exhaustive schema test count must fit into u32")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BasicCoverageKey {
    Primitive(PrimitiveCoverageKey),
    Array(PrimitiveCoverageKey),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum SchemaCoverageKey {
    LatestValueWins {
        nullable: NullableCoverageKey,
        value: BasicCoverageKey,
    },
    LinearString,
    LinearList {
        value_type: PrimitiveCoverageKey,
    },
    MonotonicCounter {
        small_range: bool,
    },
    TotalOrderRegister {
        direction: DirectionCoverageKey,
        value_type: PrimitiveCoverageKey,
    },
    TotalOrderFiniteStateRegister {
        nullable: NullableCoverageKey,
        value_type: PrimitiveCoverageKey,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OperationCoverageKey {
    LatestValueWinsValue {
        nullable: NullableCoverageKey,
        value: BasicCoverageKey,
    },
    LatestValueWinsNull,
    LinearStringInsert,
    LinearStringDelete,
    LinearListInsert {
        value_type: PrimitiveCoverageKey,
    },
    LinearListDelete,
    MonotonicCounterIncrement {
        small_range: bool,
    },
    TotalOrderRegisterSet {
        direction: DirectionCoverageKey,
        value_type: PrimitiveCoverageKey,
    },
    TotalOrderFiniteStateRegisterValue {
        nullable: NullableCoverageKey,
        value_type: PrimitiveCoverageKey,
    },
    TotalOrderFiniteStateRegisterNull,
}

fn primitive_coverage_key(primitive_type: PrimitiveType) -> PrimitiveCoverageKey {
    match primitive_type {
        PrimitiveType::String => PrimitiveCoverageKey::String,
        PrimitiveType::UInt => PrimitiveCoverageKey::UInt,
        PrimitiveType::Int => PrimitiveCoverageKey::Int,
        PrimitiveType::Byte => PrimitiveCoverageKey::Byte,
        PrimitiveType::Float => PrimitiveCoverageKey::Float,
        PrimitiveType::Boolean => PrimitiveCoverageKey::Boolean,
        PrimitiveType::Binary => PrimitiveCoverageKey::Binary,
        PrimitiveType::Date => PrimitiveCoverageKey::Date,
        PrimitiveType::Timestamp => PrimitiveCoverageKey::Timestamp,
    }
}

fn nullable_coverage_key(nullable: bool) -> NullableCoverageKey {
    if nullable {
        NullableCoverageKey::Nullable
    } else {
        NullableCoverageKey::NonNull
    }
}

fn direction_coverage_key(direction: &Direction) -> DirectionCoverageKey {
    match direction {
        Direction::Ascending => DirectionCoverageKey::Ascending,
        Direction::Descending => DirectionCoverageKey::Descending,
    }
}

fn basic_coverage_key(value_type: &BasicDataType) -> BasicCoverageKey {
    match value_type {
        BasicDataType::Primitive(primitive_type) => {
            BasicCoverageKey::Primitive(primitive_coverage_key(*primitive_type))
        }
        BasicDataType::Array(array_type) => {
            BasicCoverageKey::Array(primitive_coverage_key(array_type.element_type))
        }
    }
}

fn nullable_basic_coverage_key(value_type: &NullableBasicDataType) -> SchemaCoverageKey {
    match value_type {
        NullableBasicDataType::NonNull(value_type) => SchemaCoverageKey::LatestValueWins {
            nullable: NullableCoverageKey::NonNull,
            value: basic_coverage_key(value_type),
        },
        NullableBasicDataType::Nullable(value_type) => SchemaCoverageKey::LatestValueWins {
            nullable: NullableCoverageKey::Nullable,
            value: basic_coverage_key(value_type),
        },
    }
}

fn nullable_primitive_coverage_parts(
    value_type: NullablePrimitiveType,
) -> (NullableCoverageKey, PrimitiveCoverageKey) {
    match value_type {
        NullablePrimitiveType::NonNull(value_type) => (
            NullableCoverageKey::NonNull,
            primitive_coverage_key(value_type),
        ),
        NullablePrimitiveType::Nullable(value_type) => (
            NullableCoverageKey::Nullable,
            primitive_coverage_key(value_type),
        ),
    }
}

fn classify_linear_operation_batch<Id, Value>(
    values: &[DataOperation<Id, Value>],
) -> &DataOperation<Id, Value> {
    let first = values
        .first()
        .expect("expected non-empty linear operation batch");
    let first_is_insert = matches!(first, DataOperation::Insert { .. });
    assert!(
        values
            .iter()
            .all(|value| matches!(value, DataOperation::Insert { .. }) == first_is_insert),
        "expected homogeneous linear operation batch in coverage example"
    );
    first
}

fn schema_coverage_key(data_type: &ReplicatedDataType) -> SchemaCoverageKey {
    match data_type {
        ReplicatedDataType::LatestValueWins { value_type } => {
            nullable_basic_coverage_key(value_type)
        }
        ReplicatedDataType::LinearString => SchemaCoverageKey::LinearString,
        ReplicatedDataType::LinearList { value_type } => SchemaCoverageKey::LinearList {
            value_type: primitive_coverage_key(*value_type),
        },
        ReplicatedDataType::MonotonicCounter { small_range } => {
            SchemaCoverageKey::MonotonicCounter {
                small_range: *small_range,
            }
        }
        ReplicatedDataType::TotalOrderRegister {
            value_type,
            direction,
        } => SchemaCoverageKey::TotalOrderRegister {
            direction: direction_coverage_key(direction),
            value_type: primitive_coverage_key(*value_type),
        },
        ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. } => {
            let (nullable, value_type) = nullable_primitive_coverage_parts(*value_type);
            SchemaCoverageKey::TotalOrderFiniteStateRegister {
                nullable,
                value_type,
            }
        }
    }
}

fn operation_coverage_key<Id>(
    data_type: &ReplicatedDataType,
    value: &OperationValue<Id>,
) -> OperationCoverageKey {
    match (data_type, value) {
        (
            ReplicatedDataType::LatestValueWins { value_type },
            OperationValue::LatestValueWins(UpdateOperation { value, .. }),
        ) => {
            let (nullable, basic_type) = match value_type {
                NullableBasicDataType::NonNull(value_type) => {
                    (NullableCoverageKey::NonNull, basic_coverage_key(value_type))
                }
                NullableBasicDataType::Nullable(value_type) => (
                    NullableCoverageKey::Nullable,
                    basic_coverage_key(value_type),
                ),
            };
            match value {
                NullableBasicValue::Null => OperationCoverageKey::LatestValueWinsNull,
                NullableBasicValue::Value(_) => OperationCoverageKey::LatestValueWinsValue {
                    nullable,
                    value: basic_type,
                },
            }
        }
        (ReplicatedDataType::LinearString, OperationValue::LinearString(values)) => {
            match classify_linear_operation_batch(values) {
                DataOperation::Insert { .. } => OperationCoverageKey::LinearStringInsert,
                DataOperation::Delete { .. } => OperationCoverageKey::LinearStringDelete,
            }
        }
        (ReplicatedDataType::LinearList { value_type }, OperationValue::LinearList(values)) => {
            match classify_linear_operation_batch(values) {
                DataOperation::Insert { .. } => OperationCoverageKey::LinearListInsert {
                    value_type: primitive_coverage_key(*value_type),
                },
                DataOperation::Delete { .. } => OperationCoverageKey::LinearListDelete,
            }
        }
        (
            ReplicatedDataType::MonotonicCounter { small_range },
            OperationValue::MonotonicCounterIncrement(_),
        ) => OperationCoverageKey::MonotonicCounterIncrement {
            small_range: *small_range,
        },
        (
            ReplicatedDataType::TotalOrderRegister {
                value_type,
                direction,
            },
            OperationValue::TotalOrderRegisterSet(_),
        ) => OperationCoverageKey::TotalOrderRegisterSet {
            direction: direction_coverage_key(direction),
            value_type: primitive_coverage_key(*value_type),
        },
        (
            ReplicatedDataType::TotalOrderFiniteStateRegister { value_type, .. },
            OperationValue::TotalOrderFiniteStateRegisterSet(value),
        ) => {
            let (nullable, value_type) = nullable_primitive_coverage_parts(*value_type);
            match value {
                NullablePrimitiveValue::Null => {
                    OperationCoverageKey::TotalOrderFiniteStateRegisterNull
                }
                NullablePrimitiveValue::Value(_) => {
                    OperationCoverageKey::TotalOrderFiniteStateRegisterValue {
                        nullable,
                        value_type,
                    }
                }
            }
        }
        (
            ReplicatedDataType::LatestValueWins { .. }
            | ReplicatedDataType::LinearString
            | ReplicatedDataType::LinearList { .. }
            | ReplicatedDataType::MonotonicCounter { .. }
            | ReplicatedDataType::TotalOrderRegister { .. }
            | ReplicatedDataType::TotalOrderFiniteStateRegister { .. },
            _,
        ) => {
            panic!("operation does not match schema data type")
        }
    }
}

fn expected_schema_coverage() -> BTreeSet<SchemaCoverageKey> {
    let mut coverage = BTreeSet::new();

    for primitive_type in primitive_types() {
        let primitive_type = primitive_coverage_key(primitive_type);
        for nullable in [false, true] {
            coverage.insert(SchemaCoverageKey::LatestValueWins {
                nullable: nullable_coverage_key(nullable),
                value: BasicCoverageKey::Primitive(primitive_type),
            });
            coverage.insert(SchemaCoverageKey::LatestValueWins {
                nullable: nullable_coverage_key(nullable),
                value: BasicCoverageKey::Array(primitive_type),
            });
        }

        coverage.insert(SchemaCoverageKey::LinearList {
            value_type: primitive_type,
        });

        for direction in [Direction::Ascending, Direction::Descending] {
            coverage.insert(SchemaCoverageKey::TotalOrderRegister {
                direction: direction_coverage_key(&direction),
                value_type: primitive_type,
            });
        }

        for nullable in [false, true] {
            coverage.insert(SchemaCoverageKey::TotalOrderFiniteStateRegister {
                nullable: nullable_coverage_key(nullable),
                value_type: primitive_type,
            });
        }
    }

    coverage.insert(SchemaCoverageKey::LinearString);
    coverage.insert(SchemaCoverageKey::MonotonicCounter { small_range: true });
    coverage.insert(SchemaCoverageKey::MonotonicCounter { small_range: false });
    coverage
}

fn expected_operation_coverage() -> BTreeSet<OperationCoverageKey> {
    let mut coverage = BTreeSet::new();

    for primitive_type in primitive_types() {
        let primitive_type = primitive_coverage_key(primitive_type);
        for nullable in [false, true] {
            coverage.insert(OperationCoverageKey::LatestValueWinsValue {
                nullable: nullable_coverage_key(nullable),
                value: BasicCoverageKey::Primitive(primitive_type),
            });
            coverage.insert(OperationCoverageKey::LatestValueWinsValue {
                nullable: nullable_coverage_key(nullable),
                value: BasicCoverageKey::Array(primitive_type),
            });
            coverage.insert(OperationCoverageKey::TotalOrderFiniteStateRegisterValue {
                nullable: nullable_coverage_key(nullable),
                value_type: primitive_type,
            });
        }

        coverage.insert(OperationCoverageKey::LinearListInsert {
            value_type: primitive_type,
        });

        for direction in [Direction::Ascending, Direction::Descending] {
            coverage.insert(OperationCoverageKey::TotalOrderRegisterSet {
                direction: direction_coverage_key(&direction),
                value_type: primitive_type,
            });
        }
    }

    coverage.insert(OperationCoverageKey::LatestValueWinsNull);
    coverage.insert(OperationCoverageKey::LinearStringInsert);
    coverage.insert(OperationCoverageKey::LinearStringDelete);
    coverage.insert(OperationCoverageKey::LinearListDelete);
    coverage.insert(OperationCoverageKey::MonotonicCounterIncrement { small_range: true });
    coverage.insert(OperationCoverageKey::MonotonicCounterIncrement { small_range: false });
    coverage.insert(OperationCoverageKey::TotalOrderFiniteStateRegisterNull);
    coverage
}

#[test]
fn exhaustive_schema_has_expected_number_of_fields() {
    let schema = exhaustive_schema();
    assert_eq!(schema.columns.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
}

#[test]
fn exhaustive_schema_covers_all_supported_schema_shapes() {
    let schema = exhaustive_schema();
    let actual = schema
        .columns
        .values()
        .map(|field| schema_coverage_key(&field.data_type))
        .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected_schema_coverage());
}

#[test]
fn exhaustive_schema_operations_validate_against_exhaustive_schema() {
    let schema = exhaustive_schema();
    let operations = exhaustive_schema_operations(0u32.., 10_000u32..).unwrap();

    assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    for (index, operation) in operations.iter().enumerate() {
        let RowOperation::Update { fields, .. } = &operation.operation else {
            panic!("expected single-field update operation");
        };
        validate_schema_operation_fields(&schema, fields).unwrap_or_else(|error| {
            panic!("single-field operation {index} failed validation: {error:?}; fields={fields:?}")
        });
    }
}

#[test]
fn exhaustive_schema_operations_cover_all_supported_operation_shapes() {
    let schema = exhaustive_schema();
    let operations = exhaustive_schema_operations(0u32.., 10_000u32..).unwrap();
    let actual = operations
        .iter()
        .map(|operation| {
            let RowOperation::Update { fields, .. } = &operation.operation else {
                panic!("expected single-field update operation");
            };
            let [field] = fields.as_slice() else {
                panic!("expected single-field operation");
            };
            let data_type = &schema
                .columns
                .get(field.field_name.as_ref())
                .expect("example field exists in exhaustive schema")
                .data_type;
            operation_coverage_key(data_type, &field.value)
        })
        .collect::<BTreeSet<_>>();

    assert_eq!(actual, expected_operation_coverage());
}

#[test]
fn exhaustive_schema_operation_validates_against_exhaustive_schema() {
    let schema = exhaustive_schema();
    let operation = exhaustive_schema_operation(7u32, 10_000u32..).unwrap();

    let RowOperation::Update { fields, .. } = &operation.operation else {
        panic!("expected combined update operation");
    };
    assert_eq!(fields.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
    operation
        .validate_against_schema(&schema)
        .unwrap_or_else(|error| {
            panic!("combined operation failed validation: {error:?}; fields={fields:?}")
        });
}

#[test]
fn exhaustive_schema_operation_accepts_exact_minimum_id_count() {
    let ids = 0u32..count_as_u32(EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT);
    let operation = exhaustive_schema_operation(7u32, ids).unwrap();
    let RowOperation::Update { fields, .. } = &operation.operation else {
        panic!("expected combined update operation");
    };
    assert_eq!(fields.len(), EXHAUSTIVE_SCHEMA_FIELD_COUNT);
}

#[test]
fn exhaustive_schema_operations_accept_exact_minimum_id_count() {
    let row_ids = 0u32..count_as_u32(EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    let change_ids = 10_000u32..10_000u32 + count_as_u32(EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT);
    let operations = exhaustive_schema_operations(row_ids, change_ids).unwrap();
    assert_eq!(operations.len(), EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
}

#[test]
fn exhaustive_schema_operation_reports_insufficient_ids() {
    let ids = 0u32..(count_as_u32(EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT) - 1);
    let err = exhaustive_schema_operation(7u32, ids).unwrap_err();
    assert_matches!(
        err,
        ExampleBuildError::InsufficientIds {
            builder: "exhaustive_schema_operation",
            required_minimum: EXHAUSTIVE_SCHEMA_OPERATION_MIN_ID_COUNT,
            ..
        }
    );
}

#[test]
fn exhaustive_schema_operations_report_insufficient_ids() {
    let row_ids = 0u32..count_as_u32(EXHAUSTIVE_SCHEMA_OPERATION_COUNT);
    let change_ids =
        10_000u32..10_000u32 + (count_as_u32(EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT) - 1);
    let err = exhaustive_schema_operations(row_ids, change_ids).unwrap_err();
    assert_matches!(
        err,
        ExampleBuildError::InsufficientIds {
            builder: "exhaustive_schema_operations",
            required_minimum: EXHAUSTIVE_SCHEMA_OPERATIONS_MIN_ID_COUNT,
            ..
        }
    );
}
