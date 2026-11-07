use crate::{
    CatalogId, CatalogResult, KvBatch, MutableCatalogKv, OrderedCatalogKv, RangeDirection,
    keys::{KeyFamily, family_prefix},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataSettingScope {
    Global,
    Schema(u64),
    Table(u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSettingRow {
    pub key: String,
    pub value: String,
    pub scope: MetadataSettingScope,
}

impl MetadataSettingRow {
    #[must_use]
    pub fn global(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            scope: MetadataSettingScope::Global,
        }
    }

    #[must_use]
    pub fn schema(key: impl Into<String>, value: impl Into<String>, schema_id: u64) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            scope: MetadataSettingScope::Schema(schema_id),
        }
    }

    #[must_use]
    pub fn table(key: impl Into<String>, value: impl Into<String>, table_id: u64) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            scope: MetadataSettingScope::Table(table_id),
        }
    }
}

pub fn set_metadata_setting(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    row: MetadataSettingRow,
) -> CatalogResult<()> {
    let mut batch = KvBatch::new();
    stage_metadata_setting(&mut batch, catalog, &row);
    kv.commit(batch)
}

pub fn list_metadata_settings(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
) -> CatalogResult<Vec<MetadataSettingRow>> {
    kv.scan_prefix(
        &family_prefix(catalog, KeyFamily::MetadataSetting),
        RangeDirection::Forward,
        usize::MAX,
    )?
    .into_iter()
    .map(|item| {
        let (scope, key) = decode_metadata_setting_key(&item.key, catalog)?;
        let value = String::from_utf8(item.value).map_err(|error| {
            crate::CatalogError::Decode(format!("metadata setting value is not utf8: {error}"))
        })?;
        Ok(MetadataSettingRow { key, value, scope })
    })
    .collect()
}

fn stage_metadata_setting(batch: &mut KvBatch, catalog: CatalogId, row: &MetadataSettingRow) {
    batch.put(
        metadata_setting_key(catalog, row.scope, &row.key),
        row.value.as_bytes().to_vec(),
    );
}

pub(crate) fn metadata_setting_key(
    catalog: CatalogId,
    scope: MetadataSettingScope,
    key: &str,
) -> Vec<u8> {
    let mut out = family_prefix(catalog, KeyFamily::MetadataSetting);
    match scope {
        MetadataSettingScope::Global => out.push(b'g'),
        MetadataSettingScope::Schema(id) => {
            out.push(b's');
            out.extend_from_slice(&id.to_be_bytes());
        }
        MetadataSettingScope::Table(id) => {
            out.push(b't');
            out.extend_from_slice(&id.to_be_bytes());
        }
    }
    out.push(b'/');
    out.extend_from_slice(key.as_bytes());
    out
}

fn decode_metadata_setting_key(
    bytes: &[u8],
    catalog: CatalogId,
) -> CatalogResult<(MetadataSettingScope, String)> {
    let prefix = family_prefix(catalog, KeyFamily::MetadataSetting);
    let rest = bytes.strip_prefix(prefix.as_slice()).ok_or_else(|| {
        crate::CatalogError::Decode("metadata setting key has invalid prefix".to_owned())
    })?;
    let (scope, key_start) = match rest.first().copied() {
        Some(b'g') if rest.get(1) == Some(&b'/') => (MetadataSettingScope::Global, 2),
        Some(b's') if rest.len() >= 10 && rest.get(9) == Some(&b'/') => (
            MetadataSettingScope::Schema(u64::from_be_bytes(rest[1..9].try_into().map_err(
                |_| crate::CatalogError::Decode("metadata schema scope is truncated".to_owned()),
            )?)),
            10,
        ),
        Some(b't') if rest.len() >= 10 && rest.get(9) == Some(&b'/') => (
            MetadataSettingScope::Table(u64::from_be_bytes(rest[1..9].try_into().map_err(
                |_| crate::CatalogError::Decode("metadata table scope is truncated".to_owned()),
            )?)),
            10,
        ),
        _ => {
            return Err(crate::CatalogError::Decode(
                "metadata setting key has invalid scope".to_owned(),
            ));
        }
    };
    let key = String::from_utf8(rest[key_start..].to_vec()).map_err(|error| {
        crate::CatalogError::Decode(format!("metadata setting key is not utf8: {error}"))
    })?;
    Ok((scope, key))
}

#[cfg(test)]
#[path = "metadata_settings_tests.rs"]
mod metadata_settings_tests;
