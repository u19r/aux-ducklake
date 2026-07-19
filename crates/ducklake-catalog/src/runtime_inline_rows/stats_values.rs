use std::{cmp::Ordering, collections::BTreeMap};

use crate::CatalogResult;

use crate::runtime_inline_rows::*;

pub(super) fn accumulate_child_inline_stats(
    stats: &mut BTreeMap<u64, LiveColumnStats>,
    column: &InlineStatsColumn,
    encoded: &str,
) -> CatalogResult<()> {
    if column.children.is_empty() {
        return Ok(());
    }
    let value = match decode_inline_stats_value(encoded)? {
        InlineStatsValue::Null => {
            for child in &column.children {
                stats.entry(child.column_id).or_default().accumulate_null();
            }
            return Ok(());
        }
        InlineStatsValue::Opaque => return Ok(()),
        InlineStatsValue::Comparable(value) => value,
    };
    let type_name = column.column_type.to_ascii_lowercase();
    if type_name == "list" || type_name.ends_with("[]") {
        if let Some(child) = column.children.first() {
            for item in parse_inline_list_values(&value) {
                accumulate_inline_text_value(stats, child.column_id, item);
            }
        }
        return Ok(());
    }
    if type_name == "struct" || type_name.starts_with("struct(") {
        let fields = parse_inline_struct_values(&value);
        for child in &column.children {
            match fields.iter().find(|(name, _)| name == &child.name) {
                Some((_, value)) => accumulate_inline_text_value(stats, child.column_id, value),
                None => stats.entry(child.column_id).or_default().accumulate_null(),
            }
        }
    }
    Ok(())
}

pub(super) fn accumulate_inline_text_value(
    stats: &mut BTreeMap<u64, LiveColumnStats>,
    column_id: u64,
    value: &str,
) {
    let value = value.trim();
    if value.eq_ignore_ascii_case("NULL") {
        stats.entry(column_id).or_default().accumulate_null();
        return;
    }
    stats
        .entry(column_id)
        .or_default()
        .accumulate_comparable(unquote_inline_scalar(value));
}

pub(super) fn parse_inline_list_values(value: &str) -> Vec<&str> {
    let trimmed = value.trim();
    let Some(inner) = trimmed
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
    else {
        return Vec::new();
    };
    split_inline_top_level(inner)
}

pub(super) fn parse_inline_struct_values(value: &str) -> Vec<(String, &str)> {
    let trimmed = value.trim();
    let Some(inner) = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    else {
        return Vec::new();
    };
    split_inline_top_level(inner)
        .into_iter()
        .filter_map(|field| {
            let (name, value) = field.split_once(':')?;
            Some((unquote_inline_scalar(name.trim()), value.trim()))
        })
        .collect()
}

pub(super) fn split_inline_top_level(value: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quote = false;
    let mut depth = 0u32;
    for (index, ch) in value.char_indices() {
        match ch {
            '\'' => quote = !quote,
            '[' | '{' if !quote => depth += 1,
            ']' | '}' if !quote && depth > 0 => depth -= 1,
            ',' if !quote && depth == 0 => {
                parts.push(value[start..index].trim());
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = value[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    parts
}

pub(super) fn unquote_inline_scalar(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        trimmed[1..trimmed.len() - 1].replace("''", "'")
    } else {
        trimmed.to_owned()
    }
}

pub(super) enum InlineStatsValue {
    Null,
    Opaque,
    Comparable(String),
}

pub(super) fn decode_inline_stats_value(encoded: &str) -> CatalogResult<InlineStatsValue> {
    if encoded == "n:" {
        return Ok(InlineStatsValue::Null);
    }
    if let Some(value) = encoded.strip_prefix("i:") {
        return Ok(InlineStatsValue::Comparable(value.to_owned()));
    }
    if let Some(value) = encoded.strip_prefix("s:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if let Some(value) = encoded.strip_prefix("v:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if let Some(value) = encoded.strip_prefix("x:") {
        return Ok(InlineStatsValue::Comparable(hex_decode(value)?));
    }
    if encoded.starts_with("d:") {
        return Ok(InlineStatsValue::Opaque);
    }
    match encoded {
        "b:0" => Ok(InlineStatsValue::Comparable("false".to_owned())),
        "b:1" => Ok(InlineStatsValue::Comparable("true".to_owned())),
        _ => Err(crate::CatalogError::Decode(format!(
            "inline stats payload has invalid value: {encoded}"
        ))),
    }
}

pub(super) fn hex_decode(value: &str) -> CatalogResult<String> {
    if !value.len().is_multiple_of(2) {
        return Err(crate::CatalogError::Decode(
            "inline stats payload has odd-length hex".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let mut chars = value.as_bytes().chunks_exact(2);
    for chunk in &mut chars {
        let high = hex_nibble(chunk[0])?;
        let low = hex_nibble(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    String::from_utf8(bytes).map_err(|error| {
        crate::CatalogError::Decode(format!("inline stats payload is not utf8: {error}"))
    })
}

pub(super) fn hex_nibble(byte: u8) -> CatalogResult<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crate::CatalogError::Decode(
            "inline stats payload has invalid hex".to_owned(),
        )),
    }
}

pub(super) fn stats_value_cmp(left: &str, right: &str) -> Ordering {
    match (left.parse::<i64>(), right.parse::<i64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}
