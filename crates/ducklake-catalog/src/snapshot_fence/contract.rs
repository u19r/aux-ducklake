use crate::{CatalogError, CatalogOrderId, CatalogOrderKind, CatalogResult};

/// Exact catalog order captured by one DuckLake transaction before effect
/// reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotFence(CatalogOrderId);

impl TryFrom<&str> for SnapshotFence {
    type Error = CatalogError;

    fn try_from(value: &str) -> CatalogResult<Self> {
        if value.len() != CatalogOrderId::LEN * 2
            || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(CatalogError::Decode(
                "snapshot fence must be a 16-byte hexadecimal catalog order".to_owned(),
            ));
        }
        let order = u128::from_str_radix(value, 16)
            .map_err(|_| CatalogError::Decode("invalid snapshot fence".to_owned()))?;
        Ok(Self(CatalogOrderId::from_bytes(
            CatalogOrderKind::FdbVersionstamp,
            order.to_be_bytes(),
        )))
    }
}

impl std::fmt::Display for SnapshotFence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

#[cfg(feature = "foundationdb")]
impl SnapshotFence {
    pub(super) fn order(self) -> CatalogOrderId {
        self.0
    }
}
