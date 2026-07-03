use crate::{
    CatalogError, CatalogId, CatalogResult,
    runtime_cleanup::OldFilesCleanupRequest,
    runtime_foundationdb::{
        runtime_list_foundationdb_known_files_for_cleanup,
        runtime_list_foundationdb_old_files_for_cleanup,
    },
    runtime_protocol::RuntimeCatalogBackend,
};

pub(crate) fn list_old_files_for_cleanup(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
    payload: &[u8],
) -> CatalogResult<Vec<u8>> {
    let request = old_files_cleanup_request_from_payload(payload)?;
    runtime_list_foundationdb_old_files_for_cleanup(catalog, request)
}

fn old_files_cleanup_request_from_payload(payload: &[u8]) -> CatalogResult<OldFilesCleanupRequest> {
    if payload.is_empty() {
        return Ok(OldFilesCleanupRequest {
            cleanup_all: true,
            schedule_before_micros: None,
        });
    }
    let text = std::str::from_utf8(payload).map_err(|err| {
        CatalogError::Decode(format!("ListOldFilesForCleanup payload is not utf8: {err}"))
    })?;
    let mut cleanup_all = false;
    let mut schedule_before_micros = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("cleanup_all=") {
            cleanup_all = match value {
                "true" => true,
                "false" | "" => false,
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid cleanup_all value {other}"
                    )));
                }
            };
        } else if let Some(filter) = line.strip_prefix("filter=") {
            schedule_before_micros = cleanup_schedule_before_micros(filter)?;
        }
    }
    Ok(OldFilesCleanupRequest {
        cleanup_all,
        schedule_before_micros,
    })
}

fn cleanup_schedule_before_micros(filter: &str) -> CatalogResult<Option<i64>> {
    if filter.trim().is_empty() {
        return Ok(None);
    }
    let Some(start) = filter.find('\'') else {
        return Err(CatalogError::Decode(format!(
            "cleanup timestamp filter is missing opening quote: {filter}"
        )));
    };
    let Some(end) = filter[start + 1..].find('\'') else {
        return Err(CatalogError::Decode(format!(
            "cleanup timestamp filter is missing closing quote: {filter}"
        )));
    };
    let timestamp = &filter[start + 1..start + 1 + end];
    Ok(Some(parse_ducklake_utc_timestamp_micros(timestamp)?))
}

fn parse_ducklake_utc_timestamp_micros(timestamp: &str) -> CatalogResult<i64> {
    let timestamp = timestamp
        .strip_suffix("+00:00")
        .or_else(|| timestamp.strip_suffix("+00"))
        .or_else(|| timestamp.strip_suffix('Z'))
        .unwrap_or(timestamp);
    let (date, time) = timestamp
        .split_once('T')
        .or_else(|| timestamp.split_once(' '))
        .ok_or_else(|| {
            CatalogError::Decode(format!(
                "cleanup timestamp is missing date/time separator: {timestamp}"
            ))
        })?;
    let mut date_parts = date.split('-');
    let year = parse_i64_part(date_parts.next(), "year", timestamp)?;
    let month = parse_i64_part(date_parts.next(), "month", timestamp)?;
    let day = parse_i64_part(date_parts.next(), "day", timestamp)?;
    if date_parts.next().is_some() {
        return Err(CatalogError::Decode(format!(
            "cleanup timestamp has too many date fields: {timestamp}"
        )));
    }
    let mut time_parts = time.split(':');
    let hour = parse_i64_part(time_parts.next(), "hour", timestamp)?;
    let minute = parse_i64_part(time_parts.next(), "minute", timestamp)?;
    let second_text = time_parts.next().ok_or_else(|| {
        CatalogError::Decode(format!("cleanup timestamp is missing seconds: {timestamp}"))
    })?;
    if time_parts.next().is_some() {
        return Err(CatalogError::Decode(format!(
            "cleanup timestamp has too many time fields: {timestamp}"
        )));
    }
    let (second, micros) = parse_second_micros(second_text, timestamp)?;
    let days = days_from_civil(year, month, day)?;
    Ok(days
        .saturating_mul(86_400_000_000)
        .saturating_add(hour.saturating_mul(3_600_000_000))
        .saturating_add(minute.saturating_mul(60_000_000))
        .saturating_add(second.saturating_mul(1_000_000))
        .saturating_add(micros))
}

fn parse_i64_part(part: Option<&str>, field: &str, timestamp: &str) -> CatalogResult<i64> {
    part.ok_or_else(|| {
        CatalogError::Decode(format!("cleanup timestamp is missing {field}: {timestamp}"))
    })?
    .parse::<i64>()
    .map_err(|err| {
        CatalogError::Decode(format!(
            "cleanup timestamp has invalid {field} in {timestamp}: {err}"
        ))
    })
}

fn parse_second_micros(second_text: &str, timestamp: &str) -> CatalogResult<(i64, i64)> {
    let (second, fraction) = second_text
        .split_once('.')
        .map_or((second_text, ""), |(second, fraction)| (second, fraction));
    let second = second.parse::<i64>().map_err(|err| {
        CatalogError::Decode(format!(
            "cleanup timestamp has invalid seconds in {timestamp}: {err}"
        ))
    })?;
    let mut micros_text = fraction.chars().take(6).collect::<String>();
    while micros_text.len() < 6 {
        micros_text.push('0');
    }
    let micros = if micros_text.is_empty() {
        0
    } else {
        micros_text.parse::<i64>().map_err(|err| {
            CatalogError::Decode(format!(
                "cleanup timestamp has invalid fractional seconds in {timestamp}: {err}"
            ))
        })?
    };
    Ok((second, micros))
}

fn days_from_civil(year: i64, month: i64, day: i64) -> CatalogResult<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(CatalogError::Decode(format!(
            "cleanup timestamp date is out of range: {year:04}-{month:02}-{day:02}"
        )));
    }
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Ok(era * 146_097 + doe - 719_468)
}

pub(crate) fn list_known_files_for_cleanup(
    _backend: RuntimeCatalogBackend,
    catalog: CatalogId,
) -> CatalogResult<Vec<u8>> {
    runtime_list_foundationdb_known_files_for_cleanup(catalog)
}

#[cfg(test)]
#[path = "runtime_cleanup_ops_tests.rs"]
mod runtime_cleanup_ops_tests;
