use std::cmp::Ordering;

use crate::{CatalogResult, TableRow};

use crate::runtime_inline_rows::*;

pub(super) enum InlineFlushFileOrder {
    Default,
    Terms(Vec<InlineSortTerm>),
}

pub(super) struct InlineSortTerm {
    key: InlineSortKey,
    descending: bool,
    nulls_first: bool,
}

pub(super) enum InlineSortKey {
    RowId,
    BeginSnapshot,
    Column(usize),
    ColumnSquare(usize),
}

pub(super) enum InlineSortValue {
    Null,
    Comparable(String),
}

impl InlineFlushFileOrder {
    pub(super) fn parse(order: Option<&str>, schema_table: &TableRow) -> CatalogResult<Self> {
        let Some(order) = order.map(str::trim).filter(|order| !order.is_empty()) else {
            return Ok(Self::Default);
        };
        let mut terms = Vec::new();
        for term in split_inline_top_level(order) {
            terms.push(InlineSortTerm::parse(term, schema_table)?);
        }
        Ok(Self::Terms(terms))
    }

    pub(super) fn compare(&self, left: &InlineFlushRow, right: &InlineFlushRow) -> Ordering {
        match self {
            Self::Default => compare_inline_flush_tie_breakers(left, right),
            Self::Terms(terms) => {
                for term in terms {
                    let ordering = term.compare(left, right);
                    if !ordering.is_eq() {
                        return ordering;
                    }
                }
                compare_inline_flush_tie_breakers(left, right)
            }
        }
    }
}

impl InlineSortTerm {
    fn parse(term: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        let (without_nulls, nulls_first) = strip_inline_order_nulls(term);
        let (expression, descending) = strip_inline_order_direction(without_nulls);
        let key = InlineSortKey::parse(expression, schema_table)?;
        Ok(Self {
            key,
            descending,
            nulls_first,
        })
    }

    fn compare(&self, left: &InlineFlushRow, right: &InlineFlushRow) -> Ordering {
        let ordering =
            compare_inline_sort_values(self.value(left), self.value(right), self.nulls_first);
        if self.descending {
            ordering.reverse()
        } else {
            ordering
        }
    }

    fn value(&self, row: &InlineFlushRow) -> InlineSortValue {
        match self.key {
            InlineSortKey::RowId => InlineSortValue::Comparable(row.row_id.to_string()),
            InlineSortKey::BeginSnapshot => {
                InlineSortValue::Comparable(row.begin_snapshot.to_string())
            }
            InlineSortKey::Column(index) => inline_sort_value(&row.values[index]),
            InlineSortKey::ColumnSquare(index) => inline_square_sort_value(&row.values[index]),
        }
    }
}

impl InlineSortKey {
    fn parse(expression: &str, schema_table: &TableRow) -> CatalogResult<Self> {
        let normalized = normalize_inline_sort_expression(expression);
        if normalized.eq_ignore_ascii_case("row_id") {
            return Ok(Self::RowId);
        }
        if normalized.eq_ignore_ascii_case("begin_snapshot") {
            return Ok(Self::BeginSnapshot);
        }
        if let Some(column_name) = parse_inline_square_sort_expression(&normalized) {
            let Some((index, _)) = schema_table
                .columns
                .iter()
                .filter(|column| column.parent_id.is_none())
                .enumerate()
                .find(|(_, column)| column.name.eq_ignore_ascii_case(&column_name))
            else {
                return Err(crate::CatalogError::InvalidMutation(format!(
                    "unsupported inline flush file order term: {expression}"
                )));
            };
            return Ok(Self::ColumnSquare(index));
        }
        let Some((index, _)) = schema_table
            .columns
            .iter()
            .filter(|column| column.parent_id.is_none())
            .enumerate()
            .find(|(_, column)| column.name.eq_ignore_ascii_case(&normalized))
        else {
            return Err(crate::CatalogError::InvalidMutation(format!(
                "unsupported inline flush file order term: {expression}"
            )));
        };
        Ok(Self::Column(index))
    }
}

pub(super) fn parse_inline_square_sort_expression(expression: &str) -> Option<String> {
    let (left, right) = expression.split_once('*')?;
    let left = normalize_inline_sort_expression(left);
    let right = normalize_inline_sort_expression(right);
    if left.eq_ignore_ascii_case(&right) {
        Some(left)
    } else {
        None
    }
}

pub(super) fn compare_inline_flush_tie_breakers(
    left: &InlineFlushRow,
    right: &InlineFlushRow,
) -> Ordering {
    left.row_id
        .cmp(&right.row_id)
        .then(left.begin_snapshot.cmp(&right.begin_snapshot))
}

pub(super) fn compare_inline_sort_values(
    left: InlineSortValue,
    right: InlineSortValue,
    nulls_first: bool,
) -> Ordering {
    match (left, right) {
        (InlineSortValue::Null, InlineSortValue::Null) => Ordering::Equal,
        (InlineSortValue::Null, InlineSortValue::Comparable(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (InlineSortValue::Comparable(_), InlineSortValue::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (InlineSortValue::Comparable(left), InlineSortValue::Comparable(right)) => {
            stats_value_cmp(&left, &right)
        }
    }
}

pub(super) fn inline_sort_value(encoded: &str) -> InlineSortValue {
    match decode_inline_stats_value(encoded) {
        Ok(InlineStatsValue::Null) => InlineSortValue::Null,
        Ok(InlineStatsValue::Comparable(value)) => InlineSortValue::Comparable(value),
        Ok(InlineStatsValue::Opaque) | Err(_) => InlineSortValue::Comparable(encoded.to_owned()),
    }
}

pub(super) fn inline_square_sort_value(encoded: &str) -> InlineSortValue {
    match decode_inline_stats_value(encoded) {
        Ok(InlineStatsValue::Null) => InlineSortValue::Null,
        Ok(InlineStatsValue::Comparable(value)) => match value.parse::<i128>() {
            Ok(value) => InlineSortValue::Comparable(value.saturating_mul(value).to_string()),
            Err(_) => InlineSortValue::Comparable(value),
        },
        Ok(InlineStatsValue::Opaque) | Err(_) => InlineSortValue::Comparable(encoded.to_owned()),
    }
}

pub(super) fn strip_inline_order_nulls(term: &str) -> (&str, bool) {
    let trimmed = term.trim();
    if let Some(value) = strip_ascii_suffix(trimmed, " NULLS FIRST") {
        return (value.trim(), true);
    }
    if let Some(value) = strip_ascii_suffix(trimmed, " NULLS LAST") {
        return (value.trim(), false);
    }
    (trimmed, false)
}

pub(super) fn strip_inline_order_direction(term: &str) -> (&str, bool) {
    let trimmed = term.trim();
    if let Some(value) = strip_ascii_suffix(trimmed, " DESC") {
        return (value.trim(), true);
    }
    if let Some(value) = strip_ascii_suffix(trimmed, " ASC") {
        return (value.trim(), false);
    }
    (trimmed, false)
}

pub(super) fn strip_ascii_suffix<'a>(value: &'a str, suffix: &str) -> Option<&'a str> {
    let start = value.len().checked_sub(suffix.len())?;
    if value[start..].eq_ignore_ascii_case(suffix) {
        Some(&value[..start])
    } else {
        None
    }
}

pub(super) fn normalize_inline_sort_expression(expression: &str) -> String {
    let mut normalized = strip_balanced_parentheses(expression.trim())
        .trim()
        .to_owned();
    if let Some(prefix) = strip_ascii_suffix(&normalized, " + 0") {
        normalized = strip_balanced_parentheses(prefix.trim()).trim().to_owned();
    }
    unquote_inline_identifier(&normalized)
}

pub(super) fn strip_balanced_parentheses(value: &str) -> &str {
    let mut current = value.trim();
    while current.starts_with('(')
        && current.ends_with(')')
        && parentheses_wrap_entire_value(current)
    {
        current = current[1..current.len() - 1].trim();
    }
    current
}

pub(super) fn parentheses_wrap_entire_value(value: &str) -> bool {
    let mut depth = 0i32;
    let mut quote = false;
    for (index, ch) in value.char_indices() {
        match ch {
            '"' => quote = !quote,
            '(' if !quote => depth += 1,
            ')' if !quote => {
                depth -= 1;
                if depth == 0 && index != value.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0
}

pub(super) fn unquote_inline_identifier(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_owned()
    }
}
