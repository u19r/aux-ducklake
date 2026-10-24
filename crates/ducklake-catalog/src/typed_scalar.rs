use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum CatalogScalarType {
    SignedInteger {
        bits: u32,
    },
    UnsignedInteger {
        bits: u32,
    },
    Decimal,
    Float32,
    Float64,
    Text,
    Timestamp,
    TimestampTz,
    #[default]
    Unsupported,
}

pub(crate) fn normalize_catalog_scalar_type(declaration: &str) -> CatalogScalarType {
    let declaration = declaration.trim().to_ascii_uppercase();
    if declaration.contains('[')
        || declaration.starts_with("STRUCT")
        || declaration.starts_with("MAP")
    {
        return CatalogScalarType::Unsupported;
    }
    if declaration == "TIMESTAMP WITH TIME ZONE" {
        return CatalogScalarType::TimestampTz;
    }
    let base = declaration
        .split(['(', ' ', ')'])
        .next()
        .unwrap_or_default();
    match base {
        "TINYINT" | "INT1" => CatalogScalarType::SignedInteger { bits: 8 },
        "SMALLINT" | "INT2" | "INT16" | "SHORT" => CatalogScalarType::SignedInteger { bits: 16 },
        "INTEGER" | "INT" | "INT4" | "INT32" | "SIGNED" => {
            CatalogScalarType::SignedInteger { bits: 32 }
        }
        "BIGINT" | "INT8" | "INT64" | "LONG" => CatalogScalarType::SignedInteger { bits: 64 },
        "HUGEINT" | "INT128" => CatalogScalarType::SignedInteger { bits: 128 },
        "UTINYINT" | "UINT8" => CatalogScalarType::UnsignedInteger { bits: 8 },
        "USMALLINT" | "UINT16" => CatalogScalarType::UnsignedInteger { bits: 16 },
        "UINTEGER" | "UINT32" => CatalogScalarType::UnsignedInteger { bits: 32 },
        "UBIGINT" | "UINT64" => CatalogScalarType::UnsignedInteger { bits: 64 },
        "UHUGEINT" | "UINT128" => CatalogScalarType::UnsignedInteger { bits: 128 },
        "TIMESTAMP" | "TIMESTAMP_S" | "TIMESTAMP_MS" | "TIMESTAMP_NS" => {
            CatalogScalarType::Timestamp
        }
        "TIMESTAMPTZ" | "TIMESTAMP_TZ" | "TIMESTAMP_TZ_NS" => CatalogScalarType::TimestampTz,
        "DECIMAL" | "NUMERIC" => CatalogScalarType::Decimal,
        "FLOAT" | "REAL" | "FLOAT4" => CatalogScalarType::Float32,
        "DOUBLE" | "FLOAT8" => CatalogScalarType::Float64,
        "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => CatalogScalarType::Text,
        _ => CatalogScalarType::Unsupported,
    }
}

pub(crate) fn compare_typed_scalars(
    scalar_type: CatalogScalarType,
    left: &str,
    right: &str,
) -> Option<Ordering> {
    match scalar_type {
        CatalogScalarType::SignedInteger { bits } | CatalogScalarType::UnsignedInteger { bits } => {
            let signed = matches!(scalar_type, CatalogScalarType::SignedInteger { .. });
            let left = parse_integer(left, bits, signed)?;
            let right = parse_integer(right, bits, signed)?;
            Some(compare_integer(left, right))
        }
        CatalogScalarType::Decimal => {
            Some(ExactDecimal::parse(left)?.cmp(&ExactDecimal::parse(right)?))
        }
        CatalogScalarType::Float32 => {
            let left = left.trim().parse::<f32>().ok()?;
            let right = right.trim().parse::<f32>().ok()?;
            left.is_finite().then_some(())?;
            right.is_finite().then_some(())?;
            left.partial_cmp(&right)
        }
        CatalogScalarType::Float64 => {
            let left = left.trim().parse::<f64>().ok()?;
            let right = right.trim().parse::<f64>().ok()?;
            left.is_finite().then_some(())?;
            right.is_finite().then_some(())?;
            left.partial_cmp(&right)
        }
        CatalogScalarType::Text => Some(left.cmp(right)),
        CatalogScalarType::Timestamp | CatalogScalarType::TimestampTz => Some(
            parse_catalog_timestamp(scalar_type, left)?
                .cmp(&parse_catalog_timestamp(scalar_type, right)?),
        ),
        CatalogScalarType::Unsupported => None,
    }
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum CatalogTimestamp {
    NegativeInfinity,
    Finite(chrono::NaiveDateTime),
    PositiveInfinity,
}

fn parse_catalog_timestamp(
    scalar_type: CatalogScalarType,
    value: &str,
) -> Option<CatalogTimestamp> {
    match value {
        "-infinity" => return Some(CatalogTimestamp::NegativeInfinity),
        "infinity" => return Some(CatalogTimestamp::PositiveInfinity),
        _ => {}
    }
    parse_finite_timestamp(scalar_type, value).map(CatalogTimestamp::Finite)
}

fn parse_finite_timestamp(
    scalar_type: CatalogScalarType,
    value: &str,
) -> Option<chrono::NaiveDateTime> {
    use chrono::Datelike;

    let (value, is_bc) = value
        .strip_suffix(" (BC)")
        .map_or((value, false), |value| (value, true));
    // DuckDB prints large positive years without chrono's required leading sign.
    let value = if value.find('-').is_some_and(|index| index > 4) {
        format!("+{value}")
    } else {
        value.to_owned()
    };
    let (local, offset_seconds) = match scalar_type {
        CatalogScalarType::Timestamp => (
            chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f").ok()?,
            0,
        ),
        CatalogScalarType::TimestampTz => {
            let timestamp =
                chrono::DateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S%.f%#z").ok()?;
            (
                timestamp.naive_local(),
                timestamp.offset().local_minus_utc(),
            )
        }
        _ => return None,
    };
    let local = if is_bc {
        local.with_year(1 - local.year())?
    } else {
        local
    };
    local.checked_sub_signed(chrono::Duration::seconds(i64::from(offset_seconds)))
}

pub(crate) fn typed_extrema_are_invalid(
    scalar_type: CatalogScalarType,
    value_count: Option<u64>,
    null_count: u64,
    min_value: Option<&str>,
    max_value: Option<&str>,
) -> bool {
    let min_valid =
        min_value.is_none_or(|value| compare_typed_scalars(scalar_type, value, value).is_some());
    let max_valid =
        max_value.is_none_or(|value| compare_typed_scalars(scalar_type, value, value).is_some());
    let bounds_ordered = match (min_value, max_value) {
        (Some(min_value), Some(max_value)) => {
            compare_typed_scalars(scalar_type, min_value, max_value)
                .is_some_and(|ordering| !ordering.is_gt())
        }
        _ => true,
    };
    let all_values_are_known_null =
        value_count.is_some_and(|value_count| value_count <= null_count);
    let bounds_are_missing = min_value.is_none() && max_value.is_none();
    !min_valid
        || !max_valid
        || !bounds_ordered
        || min_value.is_some() != max_value.is_some()
        || (bounds_are_missing && !all_values_are_known_null)
        || (all_values_are_known_null && !bounds_are_missing)
}

#[derive(Clone, Copy)]
struct ExactInteger {
    negative: bool,
    magnitude: u128,
}

fn parse_integer(value: &str, bits: u32, signed: bool) -> Option<ExactInteger> {
    let value = value.trim();
    let (negative, digits) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let magnitude = digits.parse::<u128>().ok()?;
    let limit = if signed {
        let negative_limit = 1_u128.checked_shl(bits.saturating_sub(1))?;
        if negative {
            negative_limit
        } else {
            negative_limit.checked_sub(1)?
        }
    } else {
        if negative {
            return None;
        }
        if bits == 128 {
            u128::MAX
        } else {
            1_u128.checked_shl(bits)?.checked_sub(1)?
        }
    };
    if magnitude > limit {
        return None;
    }
    Some(ExactInteger {
        negative: negative && magnitude != 0,
        magnitude,
    })
}

fn compare_integer(left: ExactInteger, right: ExactInteger) -> Ordering {
    match (left.negative, right.negative) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (true, true) => right.magnitude.cmp(&left.magnitude),
        (false, false) => left.magnitude.cmp(&right.magnitude),
    }
}

#[derive(Eq, PartialEq)]
struct ExactDecimal {
    negative: bool,
    digits: String,
    power: i64,
}

impl ExactDecimal {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        let (mantissa, exponent) = split_decimal_exponent(unsigned)?;
        let (mut digits, fractional_digits) = decimal_digits(mantissa)?;
        let mut power = exponent.checked_sub(i64::try_from(fractional_digits).ok()?)?;
        let first_nonzero = digits.find(|digit| digit != '0').unwrap_or(digits.len());
        digits.drain(..first_nonzero);
        if digits.is_empty() {
            return Some(Self {
                negative: false,
                digits: "0".to_owned(),
                power: 0,
            });
        }
        while digits.ends_with('0') {
            digits.pop();
            power = power.checked_add(1)?;
        }
        Some(Self {
            negative,
            digits,
            power,
        })
    }

    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        let left_places = self.digits.len() as i128 + i128::from(self.power);
        let right_places = other.digits.len() as i128 + i128::from(other.power);
        left_places
            .cmp(&right_places)
            .then_with(|| compare_padded_digits(&self.digits, &other.digits))
    }
}

impl Ord for ExactDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, true) => self.magnitude_cmp(other).reverse(),
            (false, false) => self.magnitude_cmp(other),
        }
    }
}

impl PartialOrd for ExactDecimal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn split_decimal_exponent(value: &str) -> Option<(&str, i64)> {
    let mut parts = value.split(['e', 'E']);
    let mantissa = parts.next()?;
    let exponent = match parts.next() {
        Some(exponent) => exponent.parse().ok()?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    Some((mantissa, exponent))
}

fn decimal_digits(mantissa: &str) -> Option<(String, usize)> {
    let mut parts = mantissa.split('.');
    let integer = parts.next()?;
    let fractional = parts.next().unwrap_or_default();
    if parts.next().is_some() || (integer.is_empty() && fractional.is_empty()) {
        return None;
    }
    if !integer
        .bytes()
        .chain(fractional.bytes())
        .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    Some((format!("{integer}{fractional}"), fractional.len()))
}

fn compare_padded_digits(left: &str, right: &str) -> Ordering {
    let width = left.len().max(right.len());
    let mut left = left.bytes().chain(std::iter::repeat(b'0')).take(width);
    let mut right = right.bytes().chain(std::iter::repeat(b'0')).take(width);
    left.by_ref().cmp(right.by_ref())
}

#[cfg(test)]
#[path = "typed_scalar_tests.rs"]
mod typed_scalar_tests;
