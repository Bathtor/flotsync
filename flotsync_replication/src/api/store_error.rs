//! Classified replication-store failures.
//!
//! [`StoreErrorClassification`] is the authoritative in-memory representation. Its opaque
//! [`Display`](std::fmt::Display) code is intended for diagnostics and stable external references,
//! not for in-process policy decisions. Callers should inspect its typed [`StoreErrorClass`],
//! [`StoreErrorScope`], and [`StoreErrorResolution`] fields directly.
//!
//! The initial code is seven characters wide:
//!
//! | Positions | Dimension | Encoding |
//! |---|---|---|
//! | 1–2 | [`StoreErrorScope`] | uppercase alphabetic |
//! | 3–5 | [`StoreErrorClass`] | decimal numeric |
//! | 6–7 | [`StoreErrorResolution`] | uppercase alphabetic |
//!
//! Current top-level alphabetic values use their first position and pad the second with `X`;
//! current top-level numeric values use their first position and pad later positions with `0`.
//! Later positions are reserved for future subcategories. An entirely unknown alphabetic or
//! numeric dimension is encoded as `XX` or `000`, respectively. Assigned positions and values are
//! permanent. Future dimensions must append fixed-width padded suffixes without changing this
//! seven-character prefix. Any future external code matching must tolerate unrecognised trailing
//! padding and may match the established prefix.

use flotsync_utils::BoxError;
use snafu::prelude::*;
use std::{error::Error as StdError, fmt};

/// Define one typed classification dimension and its canonical stable-code registry.
///
/// The code literal appears only in the invocation. The generated enum documentation, variant
/// documentation, [`code`](StoreErrorScope::code)-style accessor, and assignment registry all use
/// that same literal.
macro_rules! define_store_error_dimension {
    (
        $(#[$enum_meta:meta])*
        pub enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident => $code:literal,
            )+
        }
    ) => {
        $(#[$enum_meta])*
        #[doc = concat!(
            "\n\n# Stable code assignments\n\n",
            "| Variant | Code |\n",
            "|---|---|\n",
            $(
                "| [`Self::", stringify!($variant), "`] | `", $code, "` |\n",
            )+
        )]
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[non_exhaustive]
        pub enum $name {
            $(
                $(#[$variant_meta])*
                #[doc = concat!("\n\nStable code segment: `", $code, "`.")]
                $variant,
            )+
        }

        impl $name {
            /// Every value assigned by this release, in declaration order.
            ///
            /// This registry supports programmatic enumeration, documentation, and validation.
            /// Its membership may grow in later compatible releases because the enum is
            /// non-exhaustive. Neither declaration order nor slice indices are stable protocol
            /// values; consumers must identify entries by their typed value or permanent
            /// [`Self::code`] segment and tolerate newly appended entries.
            pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

            /// Return this value's permanent fixed-width code segment.
            #[must_use]
            pub const fn code(self) -> &'static str {
                match self {
                    $(Self::$variant => $code,)+
                }
            }
        }
    };
}

/// Errors capable of exposing a replication-store failure classification.
///
/// Higher-level error types can implement this object-safe trait by delegating through variants
/// that retain a classified store source. Returning `None` means the error was not caused by a
/// classified store failure.
pub trait StoreErrorClassificationSource: StdError + Send + Sync {
    /// Return the store classification retained by this error chain, when present.
    fn store_error_classification(&self) -> Option<StoreErrorClassification>;
}

/// Classification attached to a [`StoreError`].
///
/// Display renders the stable code as `SSCCCRR`, where `SS` is [`Self::scope`], `CCC` is
/// [`Self::class`], and `RR` is [`Self::resolution`]. The typed fields, rather than this rendered
/// representation, are authoritative for control flow. The canonical assignment tables are on
/// [`StoreErrorScope`], [`StoreErrorClass`], and [`StoreErrorResolution`]; each table and its enum
/// accessor are generated from the same declarations.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub struct StoreErrorClassification {
    /// Smallest state domain known to be affected.
    pub scope: StoreErrorScope,
    /// Nature of the failure.
    pub class: StoreErrorClass,
    /// Suggested way to address the failure.
    pub resolution: StoreErrorResolution,
}

impl StoreErrorClassification {
    /// Conservative classification for a failure with no recognised dimensions.
    pub const UNKNOWN: Self = Self {
        scope: StoreErrorScope::Unknown,
        class: StoreErrorClass::Unknown,
        resolution: StoreErrorResolution::Unknown,
    };

    /// Return this classification with the affected scope set to `scope`.
    #[must_use]
    pub const fn with_scope(mut self, scope: StoreErrorScope) -> Self {
        self.scope = scope;
        self
    }

    /// Return this classification with the failure class set to `class`.
    #[must_use]
    pub const fn with_class(mut self, class: StoreErrorClass) -> Self {
        self.class = class;
        self
    }

    /// Return this classification with the suggested resolution set to `resolution`.
    #[must_use]
    pub const fn with_resolution(mut self, resolution: StoreErrorResolution) -> Self {
        self.resolution = resolution;
        self
    }
}

impl Default for StoreErrorClassification {
    fn default() -> Self {
        Self::UNKNOWN
    }
}

impl fmt::Display for StoreErrorClassification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.scope.code())?;
        formatter.write_str(self.class.code())?;
        formatter.write_str(self.resolution.code())
    }
}

define_store_error_dimension! {
    /// Nature of a replication-store failure.
    pub enum StoreErrorClass {
        /// Concurrent access temporarily prevented the requested work.
        ///
        /// For example, SQLite may report a busy database while another transaction holds a lock.
        ConcurrentAccess => "100",
        /// The store could not currently provide the requested access.
        ///
        /// For example, `SQLx` may report that a connection pool is closed.
        Unavailable => "200",
        /// A finite execution resource was exhausted.
        ///
        /// For example, SQLite may report that the filesystem is full or memory is exhausted.
        ResourceExhaustion => "300",
        /// Stored bytes or records violated their required representation or invariants.
        ///
        /// For example, a stored local member index may lie outside its stored member set.
        InvalidData => "400",
        /// Authoritative stored state conflicts with the requested state transition or material.
        ///
        /// For example, a store may already be provisioned for a different local identity.
        ConflictingState => "500",
        /// Store construction or environmental configuration is unsuitable.
        ///
        /// For example, a database URL may be invalid or its directory may not be writable.
        Configuration => "600",
        /// A store implementation or caller violated the persistence contract.
        ///
        /// For example, a caller may provide an illegal value.
        Contract => "700",
        /// The failure does not have a recognised class.
        Unknown => "000",
    }
}

define_store_error_dimension! {
    /// Smallest state domain known to be affected by a replication-store failure.
    pub enum StoreErrorScope {
        /// One store operation is affected.
        Operation => "OX",
        /// The containing transaction must be abandoned or restarted.
        Transaction => "TX",
        /// One connection or its connection pool is affected.
        Connection => "CX",
        /// One identifiable stored record is affected.
        Record => "RX",
        /// The store as a whole is affected.
        Store => "SX",
        /// Storage facilities outside the store process are affected.
        ///
        /// Examples include filesystem capacity, mount availability, and access permissions.
        Environment => "EX",
        /// The affected scope is not recognised.
        Unknown => "XX",
    }
}

define_store_error_dimension! {
    /// Suggested way to address a replication-store failure.
    pub enum StoreErrorResolution {
        /// Retry after the relevant transient condition clears.
        ///
        /// For example, repeat a complete transaction after SQLite reports a busy database.
        Retry => "RX",
        /// Wait until the affected scope is known to accept work again.
        ///
        /// For example, an interrupted connection may become usable again after store quiescence.
        /// Consumers decide whether availability is detected through lifecycle signals, timed
        /// probes, or another policy appropriate to their context.
        WaitForResume => "WX",
        /// Discard and recreate the affected runtime resource.
        ///
        /// For example, replace a closed or broken database connection.
        Recreate => "CX",
        /// Repair or remove invalid persisted state deliberately.
        ///
        /// For example, replace or remove a corrupted stored record.
        Repair => "PX",
        /// Correct store or environmental configuration.
        ///
        /// For example, correct a database URL or filesystem permissions.
        Reconfigure => "GX",
        /// Resolve the conflict against authoritative current state.
        ///
        /// For example, reconcile requested local identity material with the identity already
        /// provisioned in the store.
        ResolveConflict => "VX",
        /// Fix a bug in the caller or store implementation.
        ///
        /// For example, correct caller code that supplied invalid material or a backend query that
        /// violates its decoding contract.
        FixBug => "FX",
        /// The suggested resolution is not recognised.
        Unknown => "XX",
    }
}

/// Failure reported through the project-wide replication-store API.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
#[non_exhaustive]
pub enum StoreError {
    /// A backend failure carrying a project-wide typed classification.
    #[snafu(display("Replication store failed [{classification}]: {source}"))]
    StoreExternal {
        /// Typed failure classification preserved across the store boundary.
        classification: StoreErrorClassification,
        /// Concrete backend error retained for diagnostics and downcasting.
        source: BoxError,
    },
}

impl StoreError {
    /// Construct a classified store error from any boxed-error-compatible source.
    pub fn new<E>(classification: StoreErrorClassification, source: E) -> Self
    where
        E: Into<BoxError>,
    {
        Self::StoreExternal {
            classification,
            source: source.into(),
        }
    }

    /// Construct a store error by extracting the classification retained by `source`.
    ///
    /// A source returning `None` is mapped to [`StoreErrorClassification::UNKNOWN`].
    pub fn from_classification_source<E>(source: E) -> Self
    where
        E: StoreErrorClassificationSource + Into<BoxError>,
    {
        let classification = source
            .store_error_classification()
            .unwrap_or(StoreErrorClassification::UNKNOWN);
        Self::new(classification, source)
    }

    /// Return this failure's authoritative typed classification.
    #[must_use]
    pub const fn classification(&self) -> StoreErrorClassification {
        match self {
            Self::StoreExternal { classification, .. } => *classification,
        }
    }
}

impl StoreErrorClassificationSource for StoreError {
    fn store_error_classification(&self) -> Option<StoreErrorClassification> {
        Some(self.classification())
    }
}

/// Snafu context for a store failure without a recognised classification.
#[cfg(test)]
pub(crate) const STORE_EXTERNAL_UNCLASSIFIED_SNAFU: StoreExternalSnafu<StoreErrorClassification> =
    StoreExternalSnafu {
        classification: StoreErrorClassification::UNKNOWN,
    };

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Test source exposing an optional store classification.
    #[derive(Debug)]
    struct TestClassificationSource {
        classification: Option<StoreErrorClassification>,
    }

    impl fmt::Display for TestClassificationSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("injected classification source")
        }
    }

    impl StdError for TestClassificationSource {}

    impl StoreErrorClassificationSource for TestClassificationSource {
        fn store_error_classification(&self) -> Option<StoreErrorClassification> {
            self.classification
        }
    }

    #[test]
    fn classification_code_has_fixed_segment_shapes() {
        for scope in StoreErrorScope::ALL {
            assert_eq!(scope.code().len(), 2);
            assert!(scope.code().bytes().all(|byte| byte.is_ascii_uppercase()));
        }
        for class in StoreErrorClass::ALL {
            assert_eq!(class.code().len(), 3);
            assert!(class.code().bytes().all(|byte| byte.is_ascii_digit()));
        }
        for resolution in StoreErrorResolution::ALL {
            assert_eq!(resolution.code().len(), 2);
            assert!(
                resolution
                    .code()
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase())
            );
        }

        let classification = StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Operation)
            .with_class(StoreErrorClass::ConcurrentAccess)
            .with_resolution(StoreErrorResolution::Retry);
        assert_eq!(classification.to_string(), "OX100RX");

        assert_eq!(StoreErrorScope::Unknown.code(), "XX");
        assert_eq!(StoreErrorClass::Unknown.code(), "000");
        assert_eq!(StoreErrorResolution::Unknown.code(), "XX");
    }

    #[test]
    fn dimension_codes_are_unique() {
        assert_unique_codes(StoreErrorScope::ALL.iter().map(|scope| scope.code()));
        assert_unique_codes(StoreErrorClass::ALL.iter().map(|class| class.code()));
        assert_unique_codes(
            StoreErrorResolution::ALL
                .iter()
                .map(|resolution| resolution.code()),
        );
    }

    #[test]
    fn store_error_preserves_typed_classification_and_prints_its_code() {
        let classification = StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Record)
            .with_class(StoreErrorClass::InvalidData)
            .with_resolution(StoreErrorResolution::Repair);
        let error = StoreError::new(
            classification,
            std::io::Error::other("injected invalid record"),
        );
        let classified: &dyn StoreErrorClassificationSource = &error;

        assert_eq!(
            classified.store_error_classification(),
            Some(classification)
        );
        assert_eq!(
            error.to_string(),
            "Replication store failed [RX400PX]: injected invalid record"
        );
        assert!(error.source().is_some());
    }

    #[test]
    fn classification_source_conversion_preserves_a_retained_classification() {
        let classification = StoreErrorClassification::UNKNOWN
            .with_scope(StoreErrorScope::Store)
            .with_class(StoreErrorClass::Configuration)
            .with_resolution(StoreErrorResolution::Reconfigure);
        let source = TestClassificationSource {
            classification: Some(classification),
        };

        let error = StoreError::from_classification_source(source);

        assert_eq!(error.classification(), classification);
        assert_eq!(
            error.to_string(),
            "Replication store failed [SX600GX]: injected classification source"
        );
    }

    #[test]
    fn classification_source_conversion_maps_none_to_unknown() {
        let source = TestClassificationSource {
            classification: None,
        };

        let error = StoreError::from_classification_source(source);

        assert_eq!(error.classification(), StoreErrorClassification::UNKNOWN);
        assert_eq!(
            error.to_string(),
            "Replication store failed [XX000XX]: injected classification source"
        );
    }

    /// Assert that `codes` contains no repeated assignment.
    fn assert_unique_codes(codes: impl IntoIterator<Item = &'static str>) {
        let mut assigned = HashSet::new();
        for code in codes {
            assert!(assigned.insert(code), "duplicate stable code '{code}'");
        }
    }
}
