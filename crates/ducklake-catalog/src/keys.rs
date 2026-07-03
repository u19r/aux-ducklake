use crate::{
    CatalogError, CatalogOrderKind, CatalogResult,
    ids::{
        CatalogId, CatalogOrderId, ColumnId, DataFileId, DeleteFileId, PartitionKeyIndex, SchemaId,
        TableId,
    },
};

pub use crate::change_keys::{
    decode_order_delete_file_change_table_id, inline_table_change_key, inline_table_change_prefix,
    order_delete_file_change_key, order_delete_file_change_prefix,
    order_delete_file_change_scan_end, order_delete_file_change_scan_start,
    snapshot_data_file_change_key, snapshot_data_file_change_prefix, table_data_file_change_key,
    table_data_file_change_prefix, table_data_file_change_scan_end,
    table_data_file_change_scan_start, table_delete_file_change_key,
    table_delete_file_change_prefix, table_delete_file_change_scan_end,
    table_delete_file_change_scan_start, table_inline_row_change_key,
    table_inline_row_change_prefix, table_inline_row_change_scan_end,
    table_inline_row_change_scan_start, table_schema_kind_inline_row_change_key,
    table_schema_kind_inline_row_change_prefix, table_schema_kind_inline_row_change_scan_end,
    table_schema_kind_inline_row_change_scan_start,
};
pub use crate::key_debug::decode_key;
pub use crate::object_keys::{
    conflict_fence_key, current_table_name_key, current_table_row_key, current_table_row_prefix,
    macro_object_key, macro_object_prefix, macro_object_scan_prefix, schema_object_key,
    schema_object_prefix, schema_object_scan_prefix, table_object_key, table_object_prefix,
    table_object_scan_prefix, table_visibility_key, table_visibility_prefix,
    table_visibility_scan_end, view_object_key, view_object_prefix, view_object_scan_prefix,
};
pub use crate::snapshot_keys::{
    decode_snapshot_timestamp_key, snapshot_key, snapshot_prefix, snapshot_timestamp_key,
    snapshot_timestamp_prefix,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyFamily {
    Snapshot,
    SnapshotByTimestamp,
    SnapshotChanges,
    TableChanges,
    Object,
    DataFile,
    CurrentDataFile,
    DataFileBegin,
    EndOrder,
    DeleteFile,
    CurrentDeleteFile,
    DeleteFileTimeline,
    DeleteFileChange,
    DeleteFileChangeByOrder,
    FileColumnStats,
    FileColumnStatsLookup,
    FilePartitionValue,
    PartitionValueLookup,
    InlineTable,
    InlineDeletion,
    InlineFileDeletion,
    InlineRowChange,
    InlineTableChange,
    InlineRowChangeBySchemaKind,
    MetadataSetting,
    MetadataVersion,
    ColumnMapping,
    ConflictFence,
    CommitAttempt,
    ScheduledCleanup,
    SnapshotOperation,
}

impl KeyFamily {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Snapshot => b's',
            Self::SnapshotByTimestamp => b'S',
            Self::SnapshotChanges => b'c',
            Self::TableChanges => b'C',
            Self::Object => b'o',
            Self::DataFile => b'd',
            Self::CurrentDataFile => b'D',
            Self::DataFileBegin => b'b',
            Self::EndOrder => b'E',
            Self::DeleteFile => b'x',
            Self::CurrentDeleteFile => b'X',
            Self::DeleteFileTimeline => b'y',
            Self::DeleteFileChange => b'Y',
            Self::DeleteFileChangeByOrder => b'k',
            Self::FileColumnStats => b'f',
            Self::FileColumnStatsLookup => b'F',
            Self::FilePartitionValue => b'P',
            Self::PartitionValueLookup => b'q',
            Self::InlineTable => b'i',
            Self::InlineDeletion => b'I',
            Self::InlineFileDeletion => b'J',
            Self::InlineRowChange => b'R',
            Self::InlineTableChange => b'H',
            Self::InlineRowChangeBySchemaKind => b'K',
            Self::MetadataSetting => b'm',
            Self::MetadataVersion => b'v',
            Self::ColumnMapping => b'M',
            Self::ConflictFence => b'w',
            Self::CommitAttempt => b'z',
            Self::ScheduledCleanup => b'G',
            Self::SnapshotOperation => b'O',
        }
    }

    pub fn from_code(code: u8) -> CatalogResult<Self> {
        match code {
            b's' => Ok(Self::Snapshot),
            b'S' => Ok(Self::SnapshotByTimestamp),
            b'c' => Ok(Self::SnapshotChanges),
            b'C' => Ok(Self::TableChanges),
            b'o' => Ok(Self::Object),
            b'd' => Ok(Self::DataFile),
            b'D' => Ok(Self::CurrentDataFile),
            b'b' => Ok(Self::DataFileBegin),
            b'E' => Ok(Self::EndOrder),
            b'x' => Ok(Self::DeleteFile),
            b'X' => Ok(Self::CurrentDeleteFile),
            b'y' => Ok(Self::DeleteFileTimeline),
            b'Y' => Ok(Self::DeleteFileChange),
            b'k' => Ok(Self::DeleteFileChangeByOrder),
            b'f' => Ok(Self::FileColumnStats),
            b'F' => Ok(Self::FileColumnStatsLookup),
            b'P' => Ok(Self::FilePartitionValue),
            b'q' => Ok(Self::PartitionValueLookup),
            b'i' => Ok(Self::InlineTable),
            b'I' => Ok(Self::InlineDeletion),
            b'J' => Ok(Self::InlineFileDeletion),
            b'R' => Ok(Self::InlineRowChange),
            b'H' => Ok(Self::InlineTableChange),
            b'K' => Ok(Self::InlineRowChangeBySchemaKind),
            b'm' => Ok(Self::MetadataSetting),
            b'v' => Ok(Self::MetadataVersion),
            b'M' => Ok(Self::ColumnMapping),
            b'w' => Ok(Self::ConflictFence),
            b'z' => Ok(Self::CommitAttempt),
            b'G' => Ok(Self::ScheduledCleanup),
            b'O' => Ok(Self::SnapshotOperation),
            _ => Err(CatalogError::InvalidKey(format!(
                "unknown family byte 0x{code:02x}"
            ))),
        }
    }

    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::SnapshotByTimestamp => "snapshot-by-timestamp",
            Self::SnapshotChanges => "snapshot-changes",
            Self::TableChanges => "table-changes",
            Self::Object => "object",
            Self::DataFile => "data-file",
            Self::CurrentDataFile => "current-data-file",
            Self::DataFileBegin => "data-file-begin",
            Self::EndOrder => "end-order",
            Self::DeleteFile => "delete-file",
            Self::CurrentDeleteFile => "current-delete-file",
            Self::DeleteFileTimeline => "delete-file-timeline",
            Self::DeleteFileChange => "delete-file-change",
            Self::DeleteFileChangeByOrder => "delete-file-change-by-order",
            Self::FileColumnStats => "file-column-stats",
            Self::FileColumnStatsLookup => "file-column-stats-lookup",
            Self::FilePartitionValue => "file-partition-value",
            Self::PartitionValueLookup => "partition-value-lookup",
            Self::InlineTable => "inline-table",
            Self::InlineDeletion => "inline-deletion",
            Self::InlineFileDeletion => "inline-file-deletion",
            Self::InlineRowChange => "inline-row-change",
            Self::InlineTableChange => "inline-table-change",
            Self::InlineRowChangeBySchemaKind => "inline-row-change-by-schema-kind",
            Self::MetadataSetting => "metadata-setting",
            Self::MetadataVersion => "metadata-version",
            Self::ColumnMapping => "column-mapping",
            Self::ConflictFence => "conflict-fence",
            Self::CommitAttempt => "commit-attempt",
            Self::ScheduledCleanup => "scheduled-cleanup",
            Self::SnapshotOperation => "snapshot-operation",
        }
    }
}

#[must_use]
pub fn catalog_prefix(catalog: CatalogId) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(&catalog.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn family_prefix(catalog: CatalogId, family: KeyFamily) -> Vec<u8> {
    let mut key = catalog_prefix(catalog);
    key.push(family.code());
    key.push(b'/');
    key
}

#[must_use]
pub fn data_file_key(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DataFile);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn table_file_stats_version_key(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"table-file-stats/");
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn catalog_file_stats_version_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"catalog-file-stats");
    key
}

#[must_use]
pub fn conflict_max_catalog_id_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"conflict-max-catalog-id");
    key
}

#[must_use]
pub fn conflict_max_file_id_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"conflict-max-file-id");
    key
}

#[must_use]
pub fn current_schema_version_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"current-schema-version");
    key
}

#[must_use]
pub fn catalog_snapshot_version_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"catalog-snapshot-version");
    key
}

#[must_use]
pub fn latest_snapshot_row_key(catalog: CatalogId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::MetadataVersion);
    key.extend_from_slice(b"latest-snapshot-row");
    key
}

#[must_use]
pub fn current_data_file_key(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = current_data_file_prefix(catalog, table_id);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn current_data_file_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::CurrentDataFile);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn data_file_begin_key(
    catalog: CatalogId,
    table_id: TableId,
    begin_order: CatalogOrderId,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DataFileBegin);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn data_file_begin_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DataFileBegin);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn data_file_begin_scan_end(
    catalog: CatalogId,
    table_id: TableId,
    snapshot_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = data_file_begin_prefix(catalog, table_id);
    key.extend_from_slice(&snapshot_order.as_bytes());
    key.push(0xff);
    key
}

#[must_use]
pub fn data_file_end_key(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::EndOrder);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&end_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn delete_file_end_key(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
    delete_file_id: DeleteFileId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::EndOrder);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&end_order.as_bytes());
    key.push(b'/');
    key.push(b'x');
    key.push(b'/');
    key.extend_from_slice(&delete_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn inline_table_end_key(
    catalog: CatalogId,
    table_id: TableId,
    end_order: CatalogOrderId,
    schema_id: SchemaId,
    begin_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::EndOrder);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&end_order.as_bytes());
    key.push(b'/');
    key.push(b'i');
    key.push(b'/');
    key.extend_from_slice(&schema_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&begin_order.as_bytes());
    key
}

#[must_use]
pub fn inline_file_deletion_key(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
    begin_order: CatalogOrderId,
    row_id: u64,
) -> Vec<u8> {
    let mut key = inline_file_deletion_file_prefix(catalog, table_id, data_file_id);
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&row_id.to_be_bytes());
    key
}

#[must_use]
pub fn inline_file_deletion_file_prefix(
    catalog: CatalogId,
    table_id: TableId,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = inline_file_deletion_table_prefix(catalog, table_id);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn inline_file_deletion_table_prefix(catalog: CatalogId, table_id: TableId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::InlineFileDeletion);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn delete_file_key(catalog: CatalogId, delete_file_id: DeleteFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DeleteFile);
    key.extend_from_slice(&delete_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn current_delete_file_key(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::CurrentDeleteFile);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn delete_file_timeline_key(
    catalog: CatalogId,
    data_file_id: DataFileId,
    begin_order: CatalogOrderId,
    delete_file_id: DeleteFileId,
) -> Vec<u8> {
    let mut key = delete_file_timeline_prefix(catalog, data_file_id);
    key.extend_from_slice(&begin_order.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&delete_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn delete_file_timeline_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::DeleteFileTimeline);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn delete_file_timeline_scan_end(
    catalog: CatalogId,
    data_file_id: DataFileId,
    snapshot_order: CatalogOrderId,
) -> Vec<u8> {
    let mut key = delete_file_timeline_prefix(catalog, data_file_id);
    key.extend_from_slice(&snapshot_order.as_bytes());
    key.push(0xff);
    key
}

pub fn delete_file_timeline_order_from_key(
    catalog: CatalogId,
    data_file_id: DataFileId,
    key: &[u8],
    kind: CatalogOrderKind,
) -> CatalogResult<CatalogOrderId> {
    let prefix = delete_file_timeline_prefix(catalog, data_file_id);
    let Some(tail) = key.strip_prefix(prefix.as_slice()) else {
        return Err(CatalogError::InvalidKey(
            "delete file timeline key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8;
    if tail.len() != minimum_len || tail[CatalogOrderId::LEN] != b'/' {
        return Err(CatalogError::InvalidKey(format!(
            "delete file timeline key tail must be {minimum_len} bytes with separator, got {}",
            tail.len()
        )));
    }
    let bytes: [u8; CatalogOrderId::LEN] =
        tail[..CatalogOrderId::LEN].try_into().map_err(|_| {
            CatalogError::InvalidKey("delete file timeline order is truncated".to_owned())
        })?;
    Ok(CatalogOrderId::from_bytes(kind, bytes))
}

#[must_use]
pub fn file_column_stats_key(
    catalog: CatalogId,
    data_file_id: DataFileId,
    column_id: ColumnId,
) -> Vec<u8> {
    let mut key = file_column_stats_data_file_prefix(catalog, data_file_id);
    key.push(b'/');
    key.extend_from_slice(&column_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn file_column_stats_data_file_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::FileColumnStats);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn file_column_stats_lookup_key(
    catalog: CatalogId,
    table_id: TableId,
    column_id: ColumnId,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key = file_column_stats_lookup_prefix(catalog, table_id, column_id);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn file_column_stats_lookup_prefix(
    catalog: CatalogId,
    table_id: TableId,
    column_id: ColumnId,
) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::FileColumnStatsLookup);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&column_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn file_partition_value_key(
    catalog: CatalogId,
    data_file_id: DataFileId,
    partition_key_index: PartitionKeyIndex,
) -> Vec<u8> {
    let mut key = file_partition_value_prefix(catalog, data_file_id);
    key.extend_from_slice(&partition_key_index.0.to_be_bytes());
    key
}

#[must_use]
pub fn file_partition_value_prefix(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::FilePartitionValue);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key.push(b'/');
    key
}

#[must_use]
pub fn partition_value_lookup_key(
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
    data_file_id: DataFileId,
) -> Vec<u8> {
    let mut key =
        partition_value_lookup_prefix(catalog, table_id, partition_key_index, partition_value);
    key.extend_from_slice(&data_file_id.0.to_be_bytes());
    key
}

#[must_use]
pub fn partition_value_lookup_prefix(
    catalog: CatalogId,
    table_id: TableId,
    partition_key_index: PartitionKeyIndex,
    partition_value: &str,
) -> Vec<u8> {
    let value_bytes = partition_value.as_bytes();
    let mut key = family_prefix(catalog, KeyFamily::PartitionValueLookup);
    key.extend_from_slice(&table_id.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&partition_key_index.0.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(value_bytes);
    key.push(b'/');
    key
}

#[must_use]
pub fn scheduled_data_file_cleanup_key(catalog: CatalogId, data_file_id: DataFileId) -> Vec<u8> {
    scheduled_cleanup_key(catalog, b'd', data_file_id.0)
}

#[must_use]
pub fn scheduled_delete_file_cleanup_key(
    catalog: CatalogId,
    delete_file_id: DeleteFileId,
) -> Vec<u8> {
    scheduled_cleanup_key(catalog, b'x', delete_file_id.0)
}

#[must_use]
pub fn scheduled_data_file_cleanup_prefix(catalog: CatalogId) -> Vec<u8> {
    scheduled_cleanup_prefix(catalog, b'd')
}

#[must_use]
pub fn scheduled_delete_file_cleanup_prefix(catalog: CatalogId) -> Vec<u8> {
    scheduled_cleanup_prefix(catalog, b'x')
}

fn scheduled_cleanup_key(catalog: CatalogId, kind: u8, id: u64) -> Vec<u8> {
    let mut key = scheduled_cleanup_prefix(catalog, kind);
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn scheduled_cleanup_prefix(catalog: CatalogId, kind: u8) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::ScheduledCleanup);
    key.push(kind);
    key.push(b'/');
    key
}

pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for byte in end.iter_mut().rev() {
        if *byte < 0xff {
            *byte += 1;
            return end;
        }
        *byte = 0;
    }
    end.push(0);
    end
}
