use std::collections::BTreeMap;

use crate::{
    CatalogError, CatalogResult, DataFileId, DataFileRow, DeleteFileId, DeleteFileRow, TableId,
    ids::CatalogOrderId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedDataFile {
    pub data_file: DataFileRow,
    pub delete_file: Option<DeleteFileRow>,
}

impl AttachedDataFile {
    #[must_use]
    pub fn new(data_file: DataFileRow, delete_file: Option<DeleteFileRow>) -> Self {
        Self {
            data_file,
            delete_file,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFileChangeKind {
    Added,
    Removed,
}

impl DataFileChangeKind {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Added => b'a',
            Self::Removed => b'r',
        }
    }

    pub fn from_code(code: u8) -> CatalogResult<Self> {
        match code {
            b'a' => Ok(Self::Added),
            b'r' => Ok(Self::Removed),
            _ => Err(CatalogError::Decode(format!(
                "unknown data file change kind 0x{code:02x}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileChange {
    pub table_id: TableId,
    pub order: CatalogOrderId,
    pub kind: DataFileChangeKind,
    pub data_file_id: DataFileId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileChange {
    pub table_id: TableId,
    pub order: CatalogOrderId,
    pub delete_file_id: DeleteFileId,
}

impl DeleteFileChange {
    #[must_use]
    pub const fn new(
        table_id: TableId,
        order: CatalogOrderId,
        delete_file_id: DeleteFileId,
    ) -> Self {
        Self {
            table_id,
            order,
            delete_file_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteScanFile {
    pub data_file: DataFileRow,
    pub delete_file: Option<DeleteFileRow>,
    pub previous_delete_file: Option<DeleteFileRow>,
    pub snapshot_order: CatalogOrderId,
    pub full_file_delete: bool,
    pub inline_file_deletions: BTreeMap<u64, CatalogOrderId>,
}

impl DeleteScanFile {
    #[must_use]
    pub const fn partial(
        data_file: DataFileRow,
        delete_file: DeleteFileRow,
        previous_delete_file: Option<DeleteFileRow>,
        snapshot_order: CatalogOrderId,
    ) -> Self {
        Self {
            snapshot_order,
            data_file,
            delete_file: Some(delete_file),
            previous_delete_file,
            full_file_delete: false,
            inline_file_deletions: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn full(
        data_file: DataFileRow,
        previous_delete_file: Option<DeleteFileRow>,
        snapshot_order: CatalogOrderId,
    ) -> Self {
        Self {
            data_file,
            delete_file: None,
            previous_delete_file,
            snapshot_order,
            full_file_delete: true,
            inline_file_deletions: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn inline(
        data_file: DataFileRow,
        snapshot_order: CatalogOrderId,
        inline_file_deletions: BTreeMap<u64, CatalogOrderId>,
    ) -> Self {
        Self {
            data_file,
            delete_file: None,
            previous_delete_file: None,
            snapshot_order,
            full_file_delete: false,
            inline_file_deletions,
        }
    }
}

impl DataFileChange {
    #[must_use]
    pub const fn new(
        table_id: TableId,
        order: CatalogOrderId,
        kind: DataFileChangeKind,
        data_file_id: DataFileId,
    ) -> Self {
        Self {
            table_id,
            order,
            kind,
            data_file_id,
        }
    }
}
