use std::cmp::Ordering;

use jiff::{
    civil::{Date, DateTime},
    fmt::strtime::BrokenDownTime,
};

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
    let declaration = declaration.trim();
    if declaration.contains('[') {
        return CatalogScalarType::Unsupported;
    }
    if declaration.eq_ignore_ascii_case("TIMESTAMP WITH TIME ZONE") {
        return CatalogScalarType::TimestampTz;
    }
    let base = declaration
        .split(['(', ' ', ')'])
        .next()
        .unwrap_or_default();
    let mut storage = [0_u8; crate::constants::SCALAR_TYPE_TOKEN_BYTES];
    let Some(token) = storage.get_mut(..base.len()) else {
        return CatalogScalarType::Unsupported;
    };
    for (destination, source) in token.iter_mut().zip(base.bytes()) {
        *destination = source.to_ascii_uppercase();
    }
    let base = std::str::from_utf8(token).unwrap_or_default();
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
    Finite(i64, i32),
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
    let (value, has_bc_suffix) = value
        .strip_suffix(" (BC)")
        .map_or((value, false), |value| (value, true));
    let year_end = value.get(1..)?.find('-')? + 1;
    let year = i64::from(value[..year_end].parse::<i32>().ok()?);
    let month_day_and_time = &value[year_end + 1..];

    // Jiff normalizes leap seconds to :59. An unrecognized bound must stay
    // incomparable so a partition containing it is still scanned.
    if month_day_and_time
        .split(':')
        .nth(2)?
        .trim_start()
        .starts_with("60")
    {
        return None;
    }
    let has_bc_after_date = month_day_and_time.contains(" (BC) ");
    let format = if has_bc_after_date {
        "%m-%d (BC) %H:%M:%S%.f"
    } else {
        "%m-%d %H:%M:%S%.f"
    };
    let (parsed, consumed) = BrokenDownTime::parse_prefix(format, month_day_and_time).ok()?;
    let year = if has_bc_after_date || has_bc_suffix {
        1 - year
    } else {
        year
    };
    let suffix = &month_day_and_time[consumed..];
    let offset_seconds = match scalar_type {
        CatalogScalarType::Timestamp if suffix.is_empty() => 0,
        CatalogScalarType::TimestampTz => parse_timestamp_offset(suffix)?,
        _ => return None,
    };

    // DuckDB's years can exceed Jiff's range. Every 400 Gregorian years
    // repeat the same calendar and contain exactly 146,097 days.
    let cycle_year = i16::try_from(year.rem_euclid(400)).ok()?;
    let date = Date::new(cycle_year, parsed.month()?, parsed.day()?).ok()?;
    let time = parsed.to_time().ok()?;
    let seconds = year.div_euclid(400) * (146_097 * 86_400)
        + date
            .to_datetime(time)
            .duration_since(DateTime::ZERO)
            .as_secs()
        - offset_seconds;
    Some(CatalogTimestamp::Finite(seconds, time.subsec_nanosecond()))
}

fn parse_timestamp_offset(suffix: &str) -> Option<i64> {
    let suffix = suffix.trim_start();
    if suffix.eq_ignore_ascii_case("Z") {
        return Some(0);
    }
    let offset = BrokenDownTime::parse("%:::z", suffix)
        .or_else(|_| BrokenDownTime::parse("%z", suffix))
        .ok()?
        .offset()?;
    Some(i64::from(offset.seconds()))
}

pub(crate) fn typed_extrema_are_invalid(
    scalar_type: CatalogScalarType,
    value_count: Option<u64>,
    null_count: u64,
    min_value: Option<&str>,
    max_value: Option<&str>,
) -> bool {
    let bounds_ordered = match (min_value, max_value) {
        (Some(minimum), Some(maximum)) => compare_typed_scalars(scalar_type, minimum, maximum)
            .is_some_and(|ordering| !ordering.is_gt()),
        (None, None) => true,
        _ => false,
    };
    let all_values_are_known_null =
        value_count.is_some_and(|value_count| value_count <= null_count);
    let bounds_are_missing = min_value.is_none() && max_value.is_none();
    !bounds_ordered
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

struct ExactDecimal<'a> {
    negative: bool,
    digits: &'a [u8],
    digit_count: usize,
    power: i64,
}

impl<'a> ExactDecimal<'a> {
    fn parse(value: &'a str) -> Option<Self> {
        let value = value.trim();
        let (negative, unsigned) = match value.as_bytes().first() {
            Some(b'-') => (true, &value[1..]),
            Some(b'+') => (false, &value[1..]),
            _ => (false, value),
        };
        let (mantissa, exponent) = split_decimal_exponent(unsigned)?;
        let fractional_digits = decimal_fractional_digits(mantissa)?;
        let power = exponent.checked_sub(i64::try_from(fractional_digits).ok()?)?;
        let bytes = mantissa.as_bytes();
        let is_nonzero = |byte: &u8| *byte != b'0' && *byte != b'.';
        let Some(first) = bytes.iter().position(is_nonzero) else {
            return Some(Self {
                negative: false,
                digits: b"0",
                digit_count: 1,
                power: 0,
            });
        };
        let last = bytes.iter().rposition(is_nonzero)?;
        let trailing = bytes[last + 1..]
            .iter()
            .filter(|byte| **byte == b'0')
            .count();
        let digits = &bytes[first..=last];
        Some(Self {
            negative,
            digits,
            digit_count: digits.len() - usize::from(digits.contains(&b'.')),
            power: power.checked_add(i64::try_from(trailing).ok()?)?,
        })
    }

    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        match (self.digits == b"0", other.digits == b"0") {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            _ => {}
        }
        let left_places = self.digit_count as i128 + i128::from(self.power);
        let right_places = other.digit_count as i128 + i128::from(other.power);
        left_places.cmp(&right_places).then_with(|| {
            let width = self.digit_count.max(other.digit_count);
            let digits = |value: &'a [u8]| {
                value
                    .iter()
                    .copied()
                    .filter(u8::is_ascii_digit)
                    .chain(std::iter::repeat(b'0'))
                    .take(width)
            };
            digits(self.digits).cmp(digits(other.digits))
        })
    }
}

impl Ord for ExactDecimal<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (true, true) => self.magnitude_cmp(other).reverse(),
            (false, false) => self.magnitude_cmp(other),
        }
    }
}

impl PartialOrd for ExactDecimal<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ExactDecimal<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for ExactDecimal<'_> {}

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

fn decimal_fractional_digits(mantissa: &str) -> Option<usize> {
    let mut parts = mantissa.split('.');
    let integer = parts.next()?;
    let fractional = parts.next().unwrap_or_default();
    if parts.next().is_some() || (integer.is_empty() && fractional.is_empty()) {
        return None;
    }
    integer
        .bytes()
        .chain(fractional.bytes())
        .all(|byte| byte.is_ascii_digit())
        .then_some(fractional.len())
}

#[cfg(test)]
#[path = "typed_scalar_tests.rs"]
mod typed_scalar_tests;
