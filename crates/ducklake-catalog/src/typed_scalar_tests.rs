#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::super::{
        CatalogScalarType, compare_typed_scalars, normalize_catalog_scalar_type,
        typed_extrema_are_invalid,
    };

    fn compare(declaration: &str, left: &str, right: &str) -> Ordering {
        let scalar_type = normalize_catalog_scalar_type(declaration);
        compare_typed_scalars(scalar_type, left, right).unwrap()
    }

    #[test]
    fn given_timestamp_values_when_compared_then_chronology_and_precision_are_preserved() {
        for declaration in ["TIMESTAMP", "timestamp_s", "TIMESTAMP_MS", "TIMESTAMP_NS"] {
            for (left, right) in [
                ("2026-10-15 01:02:03", "2026-10-15 01:02:03.000000001"),
                ("2026-10-15 01:02:03.09", "2026-10-15 01:02:03.1"),
                ("0001-01-01 00:00:00 (BC)", "0001-01-01 00:00:00"),
                ("0002-01-01 00:00:00 (BC)", "0001-01-01 00:00:00 (BC)"),
                ("9999-12-31 23:59:59", "10000-01-01 00:00:00"),
                ("-infinity", "0001-01-01 00:00:00 (BC)"),
                ("2026-10-15 00:00:00", "infinity"),
            ] {
                assert_eq!(compare(declaration, left, right), Ordering::Less);
                assert_eq!(compare(declaration, right, left), Ordering::Greater);
                assert_eq!(compare(declaration, left, left), Ordering::Equal);
            }
            assert_eq!(
                compare(
                    declaration,
                    "2026-10-15 01:02:03",
                    "2026-10-15 01:02:03.000"
                ),
                Ordering::Equal
            );
            for invalid in [
                "not-a-date",
                "2026-02-30 00:00:00",
                "2026-10-15 25:00:00",
                "2026-10-15 01:02:03+01",
            ] {
                assert_eq!(
                    compare_typed_scalars(
                        normalize_catalog_scalar_type(declaration),
                        invalid,
                        invalid
                    ),
                    None
                );
            }
        }
        for declaration in [
            "TIMESTAMPTZ",
            "TIMESTAMP_TZ",
            "TIMESTAMP_TZ_NS",
            "TIMESTAMP WITH TIME ZONE",
        ] {
            assert_eq!(
                compare(
                    declaration,
                    "2026-10-15 01:02:03+01",
                    "2026-10-15 00:02:03+00"
                ),
                Ordering::Equal
            );
            assert_eq!(
                compare(
                    declaration,
                    "2026-10-15 01:02:03.000000001+01:30",
                    "2026-10-14 23:32:03+00"
                ),
                Ordering::Greater
            );
            assert_eq!(
                compare_typed_scalars(
                    normalize_catalog_scalar_type(declaration),
                    "2026-10-15 00:00:00",
                    "2026-10-15 00:00:00"
                ),
                None
            );
        }
    }

    #[test]
    fn given_numeric_aliases_when_normalized_then_widths_are_preserved() {
        for (declaration, expected) in [
            ("TINYINT", CatalogScalarType::SignedInteger { bits: 8 }),
            ("INT1", CatalogScalarType::SignedInteger { bits: 8 }),
            ("SMALLINT", CatalogScalarType::SignedInteger { bits: 16 }),
            ("INT2", CatalogScalarType::SignedInteger { bits: 16 }),
            ("INT16", CatalogScalarType::SignedInteger { bits: 16 }),
            ("SHORT", CatalogScalarType::SignedInteger { bits: 16 }),
            ("INTEGER", CatalogScalarType::SignedInteger { bits: 32 }),
            ("INT", CatalogScalarType::SignedInteger { bits: 32 }),
            ("INT4", CatalogScalarType::SignedInteger { bits: 32 }),
            ("INT32", CatalogScalarType::SignedInteger { bits: 32 }),
            ("SIGNED", CatalogScalarType::SignedInteger { bits: 32 }),
            ("BIGINT", CatalogScalarType::SignedInteger { bits: 64 }),
            ("INT8", CatalogScalarType::SignedInteger { bits: 64 }),
            ("INT64", CatalogScalarType::SignedInteger { bits: 64 }),
            ("LONG", CatalogScalarType::SignedInteger { bits: 64 }),
            ("HUGEINT", CatalogScalarType::SignedInteger { bits: 128 }),
            ("INT128", CatalogScalarType::SignedInteger { bits: 128 }),
            ("UTINYINT", CatalogScalarType::UnsignedInteger { bits: 8 }),
            ("UINT8", CatalogScalarType::UnsignedInteger { bits: 8 }),
            ("USMALLINT", CatalogScalarType::UnsignedInteger { bits: 16 }),
            ("UINT16", CatalogScalarType::UnsignedInteger { bits: 16 }),
            ("UINTEGER", CatalogScalarType::UnsignedInteger { bits: 32 }),
            ("UINT32", CatalogScalarType::UnsignedInteger { bits: 32 }),
            ("UBIGINT", CatalogScalarType::UnsignedInteger { bits: 64 }),
            ("UINT64", CatalogScalarType::UnsignedInteger { bits: 64 }),
            ("UHUGEINT", CatalogScalarType::UnsignedInteger { bits: 128 }),
            ("UINT128", CatalogScalarType::UnsignedInteger { bits: 128 }),
        ] {
            assert_eq!(
                normalize_catalog_scalar_type(declaration),
                expected,
                "{declaration}"
            );
        }
        assert_eq!(
            normalize_catalog_scalar_type("HUGEINT"),
            CatalogScalarType::SignedInteger { bits: 128 }
        );
        assert_eq!(
            normalize_catalog_scalar_type("UHUGEINT"),
            CatalogScalarType::UnsignedInteger { bits: 128 }
        );
        assert_eq!(
            normalize_catalog_scalar_type("NUMERIC(38, 10)"),
            CatalogScalarType::Decimal
        );
        assert_eq!(
            normalize_catalog_scalar_type("REAL"),
            CatalogScalarType::Float32
        );
    }

    #[test]
    fn given_int8_alias_when_compared_then_i64_range_is_accepted() {
        let int8 = normalize_catalog_scalar_type("INT8");
        assert_eq!(
            compare_typed_scalars(int8, "9223372036854775806", "9223372036854775807"),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_typed_scalars(int8, "9223372036854775807", "0"),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare_typed_scalars(int8, "9223372036854775808", "0"),
            None
        );
    }

    #[test]
    fn given_decimal_values_when_compared_then_order_is_exact() {
        assert_eq!(compare("DECIMAL(38, 17)", "9.9", "10.0"), Ordering::Less);
        assert_eq!(
            compare("DECIMAL(38, 17)", "9007199254740992", "9007199254740993"),
            Ordering::Less
        );
        assert_eq!(compare("DECIMAL", "-10", "-9.9"), Ordering::Less);
        assert_eq!(compare("DECIMAL", "1e100", "9e99"), Ordering::Greater);
        assert_eq!(compare("DECIMAL", "1e-100", "9e-101"), Ordering::Greater);
        for equivalent in ["1.2", "1.20", "+01.200e0", "0.12E1"] {
            assert_eq!(compare("NUMERIC", equivalent, "1.2000"), Ordering::Equal);
        }
    }

    #[test]
    fn given_integer_limits_when_compared_then_full_width_values_are_exact() {
        assert_eq!(
            compare("HUGEINT", "-170141183460469231731687303715884105728", "0"),
            Ordering::Less
        );
        assert_eq!(
            compare("HUGEINT", "170141183460469231731687303715884105727", "0"),
            Ordering::Greater
        );
        assert_eq!(
            compare("UBIGINT", "18446744073709551614", "18446744073709551615"),
            Ordering::Less
        );
        assert_eq!(
            compare(
                "UHUGEINT",
                "340282366920938463463374607431768211454",
                "340282366920938463463374607431768211455"
            ),
            Ordering::Less
        );
    }

    #[test]
    fn given_invalid_numeric_values_when_compared_then_comparison_is_unavailable() {
        let decimal = normalize_catalog_scalar_type("DECIMAL");
        let hugeint = normalize_catalog_scalar_type("HUGEINT");
        let double = normalize_catalog_scalar_type("DOUBLE");
        assert_eq!(compare_typed_scalars(decimal, "bad", "1"), None);
        assert_eq!(compare_typed_scalars(hugeint, "1.0", "1"), None);
        assert_eq!(compare_typed_scalars(double, "NaN", "1"), None);
        assert_eq!(compare_typed_scalars(double, "inf", "1"), None);
        assert_eq!(compare_typed_scalars(double, "1", "not-a-number"), None);
    }

    #[test]
    fn given_inverted_numeric_bounds_when_validated_then_extrema_are_invalid() {
        assert!(typed_extrema_are_invalid(
            normalize_catalog_scalar_type("DECIMAL"),
            Some(1),
            0,
            Some("10.0"),
            Some("9.9"),
        ));
        assert!(!typed_extrema_are_invalid(
            normalize_catalog_scalar_type("UHUGEINT"),
            Some(1),
            0,
            Some("0"),
            Some("340282366920938463463374607431768211455"),
        ));
        assert!(typed_extrema_are_invalid(
            normalize_catalog_scalar_type("BIGINT"),
            None,
            0,
            None,
            None,
        ));
        assert!(!typed_extrema_are_invalid(
            normalize_catalog_scalar_type("BIGINT"),
            Some(0),
            0,
            None,
            None,
        ));
    }

    #[test]
    fn given_finite_float_values_when_compared_then_declared_precision_is_used() {
        assert_eq!(compare("REAL", "1.5", "1.25"), Ordering::Greater);
        assert_eq!(
            compare("DOUBLE", "0.3", "0.30000000000000004"),
            Ordering::Less
        );
    }

    #[test]
    fn given_text_values_when_compared_then_only_text_uses_lexical_order() {
        assert_eq!(compare("VARCHAR", "10", "9"), Ordering::Less);
        assert_eq!(
            compare_typed_scalars(
                normalize_catalog_scalar_type("DATE"),
                "2024-01-01",
                "2024-01-02"
            ),
            None
        );
        assert_eq!(
            normalize_catalog_scalar_type("VARCHAR[]"),
            CatalogScalarType::Unsupported
        );
    }
    #[test]
    fn given_zero_and_fractional_decimals_when_compared_then_numeric_order_is_preserved() {
        for (left, right, expected) in [
            ("0", "0.001", Ordering::Less),
            ("-0.0", "-0.001", Ordering::Greater),
            ("0e100", "-0e-100", Ordering::Equal),
        ] {
            assert_eq!(compare("DECIMAL", left, right), expected);
        }
    }
}
