use crate::CatalogResult;

pub(crate) fn payload_u32_value(payload: &[u8], key: &str, missing: &str) -> CatalogResult<u32> {
    let value = payload_u64_value(payload, key, missing)?;
    value.try_into().map_err(|error| {
        crate::CatalogError::Decode(format!("invalid runtime {key} {value}: {error}"))
    })
}

pub(crate) fn payload_string_value(
    payload: &[u8],
    key: &str,
    missing: &str,
) -> CatalogResult<String> {
    Ok(payload_str_value(payload, key, missing)?.to_owned())
}

pub(crate) fn payload_string_values(payload: &[u8], key: &str) -> CatalogResult<Vec<String>> {
    Ok(payload_values(payload, key)?.map(str::to_owned).collect())
}

pub(crate) fn payload_str_value<'a>(
    payload: &'a [u8],
    key: &str,
    missing: &str,
) -> CatalogResult<&'a str> {
    for value in payload_values(payload, key)? {
        return Ok(value);
    }
    Err(crate::CatalogError::Decode(missing.to_owned()))
}

pub(crate) fn payload_u64_value(payload: &[u8], key: &str, missing: &str) -> CatalogResult<u64> {
    for value in payload_values(payload, key)? {
        return value.parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid runtime {key} {value}: {error}"))
        });
    }
    Err(crate::CatalogError::Decode(missing.to_owned()))
}

pub(crate) fn payload_i64_value(payload: &[u8], key: &str, missing: &str) -> CatalogResult<i64> {
    for value in payload_values(payload, key)? {
        return value.parse::<i64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid runtime {key} {value}: {error}"))
        });
    }
    Err(crate::CatalogError::Decode(missing.to_owned()))
}

pub(crate) fn optional_payload_u64_value(payload: &[u8], key: &str) -> CatalogResult<Option<u64>> {
    for value in payload_values(payload, key)? {
        let parsed = value.parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid runtime {key} {value}: {error}"))
        })?;
        return Ok(Some(parsed));
    }
    Ok(None)
}

pub(crate) fn optional_payload_string_value(
    payload: &[u8],
    key: &str,
) -> CatalogResult<Option<String>> {
    Ok(optional_payload_str_value(payload, key)?.map(str::to_owned))
}

pub(crate) fn optional_payload_str_value<'a>(
    payload: &'a [u8],
    key: &str,
) -> CatalogResult<Option<&'a str>> {
    for value in payload_values(payload, key)? {
        return Ok(Some(value));
    }
    Ok(None)
}

pub(crate) fn optional_payload_i64_value(payload: &[u8], key: &str) -> CatalogResult<Option<i64>> {
    for value in payload_values(payload, key)? {
        let parsed = value.parse::<i64>().map_err(|error| {
            crate::CatalogError::Decode(format!("invalid runtime {key} {value}: {error}"))
        })?;
        return Ok(Some(parsed));
    }
    Ok(None)
}

fn payload_utf8(payload: &[u8]) -> CatalogResult<&str> {
    std::str::from_utf8(payload).map_err(|error| {
        crate::CatalogError::Decode(format!("runtime payload is not utf-8: {error}"))
    })
}

fn payload_values<'a, 'key>(
    payload: &'a [u8],
    key: &'key str,
) -> CatalogResult<PayloadValues<'a, 'key>> {
    Ok(PayloadValues {
        lines: payload_utf8(payload)?.lines(),
        key,
    })
}

struct PayloadValues<'a, 'key> {
    lines: std::str::Lines<'a>,
    key: &'key str,
}

impl<'a> Iterator for PayloadValues<'a, '_> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        for line in self.lines.by_ref() {
            let Some((candidate, value)) = line.split_once('=') else {
                continue;
            };
            if candidate == self.key {
                return Some(value);
            }
        }
        None
    }
}

#[cfg(test)]
#[path = "runtime_payload_tests.rs"]
mod runtime_payload_tests;
