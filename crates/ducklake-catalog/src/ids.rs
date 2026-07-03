use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitAttemptId(pub u128);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DuckLakeSnapshotId(pub u64);

impl DuckLakeSnapshotId {
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for DuckLakeSnapshotId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawSnapshotSequence(pub u64);

impl RawSnapshotSequence {
    #[must_use]
    pub const fn initial() -> Self {
        Self(0)
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    #[must_use]
    pub const fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

impl fmt::Display for RawSnapshotSequence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl CommitAttemptId {
    pub const LEN: usize = 16;

    #[must_use]
    pub const fn as_bytes(self) -> [u8; Self::LEN] {
        self.0.to_be_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CatalogOrderKind {
    FdbVersionstamp,
    UuidV7,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CatalogOrderId {
    kind: CatalogOrderKind,
    bytes: [u8; Self::LEN],
}

impl CatalogOrderId {
    pub const LEN: usize = 16;
    pub const FDB_VERSIONSTAMP_LEN: usize = 10;

    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self::uuid_v7(value)
    }

    #[must_use]
    pub const fn uuid_v7(value: u128) -> Self {
        Self {
            kind: CatalogOrderKind::UuidV7,
            bytes: value.to_be_bytes(),
        }
    }

    #[must_use]
    pub const fn from_bytes(kind: CatalogOrderKind, bytes: [u8; Self::LEN]) -> Self {
        Self { kind, bytes }
    }

    pub fn fdb_versionstamp(
        versionstamp: [u8; Self::FDB_VERSIONSTAMP_LEN],
        user_suffix: u16,
    ) -> Self {
        let mut bytes = [0; Self::LEN];
        let mut index = 0;
        while index < Self::FDB_VERSIONSTAMP_LEN {
            bytes[index] = versionstamp[index];
            index += 1;
        }
        let suffix = user_suffix.to_be_bytes();
        bytes[Self::FDB_VERSIONSTAMP_LEN] = suffix[0];
        bytes[Self::FDB_VERSIONSTAMP_LEN + 1] = suffix[1];
        Self {
            kind: CatalogOrderKind::FdbVersionstamp,
            bytes,
        }
    }

    #[must_use]
    pub const fn as_bytes(self) -> [u8; Self::LEN] {
        self.bytes
    }

    #[must_use]
    pub const fn kind(self) -> CatalogOrderKind {
        self.kind
    }
}

pub(crate) fn incomplete_fdb_order() -> CatalogOrderId {
    CatalogOrderId::fdb_versionstamp([0; CatalogOrderId::FDB_VERSIONSTAMP_LEN], 0)
}

impl PartialOrd for CatalogOrderId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CatalogOrderId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl fmt::Debug for CatalogOrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "CatalogOrderId({self})")
    }
}

impl fmt::Display for CatalogOrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.bytes {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MacroId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataFileId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeleteFileId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionKeyIndex(pub u32);
