use crate::{CatalogError, CatalogResult, error::hex, keys::KeyFamily};

pub fn decode_key(key: &[u8]) -> CatalogResult<String> {
    let (catalog, rest) = parse_catalog_prefix(key)?;
    let Some((&family_code, tail)) = rest.split_first() else {
        return Err(CatalogError::InvalidKey("missing family".to_owned()));
    };
    let family = KeyFamily::from_code(family_code)?;
    let tail = tail.strip_prefix(b"/").unwrap_or(tail);
    Ok(format!(
        "catalog={catalog}/family={}/tail={}",
        family.label(),
        hex(tail)
    ))
}

fn parse_catalog_prefix(key: &[u8]) -> CatalogResult<(u64, &[u8])> {
    if key.len() < 10 {
        return Err(CatalogError::InvalidKey(format!(
            "key too short: {} bytes",
            key.len()
        )));
    }
    let catalog = u64::from_be_bytes(
        key[..8]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("catalog prefix is truncated".to_owned()))?,
    );
    if key[8] != b'/' {
        return Err(CatalogError::InvalidKey(
            "missing catalog separator".to_owned(),
        ));
    }
    Ok((catalog, &key[9..]))
}
