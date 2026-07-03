use crate::{
    CatalogError, CatalogId, CatalogResult, DataFileChange, DataFileChangeKind, DataFileId,
    KvBatch, OrderedCatalogKv, RangeDirection, TableId, TableRow,
    data_file_changes::list_data_file_changes,
    ids::{CatalogOrderId, CommitAttemptId},
    keys::{
        KeyFamily, family_prefix, snapshot_data_file_change_key, snapshot_data_file_change_prefix,
        table_data_file_change_key,
    },
    rows::{STORED_ORDER_LEN, decode_stored_order, encode_stored_order},
    table_store::load_table_at,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitAttemptRow {
    pub attempt_id: CommitAttemptId,
    pub commit_order: CatalogOrderId,
}

impl CommitAttemptRow {
    const VERSION: u8 = 2;
    #[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
    pub(crate) const COMMIT_ORDER_BYTES_OFFSET: usize = 1 + CommitAttemptId::LEN + 1;

    #[must_use]
    pub const fn new(attempt_id: CommitAttemptId, commit_order: CatalogOrderId) -> Self {
        Self {
            attempt_id,
            commit_order,
        }
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + CommitAttemptId::LEN + STORED_ORDER_LEN);
        out.push(Self::VERSION);
        out.extend_from_slice(&self.attempt_id.as_bytes());
        encode_stored_order(&mut out, self.commit_order);
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        match bytes.first().copied() {
            Some(Self::VERSION) => {}
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported commit attempt row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "commit attempt row is too short: 0 bytes".to_owned(),
                ));
            }
        }
        let expected_len = 1 + CommitAttemptId::LEN + STORED_ORDER_LEN;
        if bytes.len() != expected_len {
            return Err(CatalogError::Decode(format!(
                "commit attempt row must be {expected_len} bytes, got {}",
                bytes.len()
            )));
        }
        let attempt_start = 1;
        let order_start = attempt_start + CommitAttemptId::LEN;
        Ok(Self {
            attempt_id: CommitAttemptId(u128::from_be_bytes(
                bytes[attempt_start..order_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("attempt id is truncated".to_owned()))?,
            )),
            commit_order: decode_stored_order(&bytes[order_start..], "commit order")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitAttemptDecision {
    FirstCommit(CommitAttemptRow),
    AlreadyCommitted(CommitAttemptRow),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataCommitIntent {
    AppendFiles,
    RewriteOrDeleteFiles,
}

impl DataCommitIntent {
    const fn conflicts_with(self, kind: DataFileChangeKind) -> bool {
        match self {
            Self::AppendFiles => matches!(kind, DataFileChangeKind::Removed),
            Self::RewriteOrDeleteFiles => true,
        }
    }
}

pub fn stage_commit_attempt(
    kv: &impl OrderedCatalogKv,
    batch: &mut KvBatch,
    catalog: CatalogId,
    attempt_id: CommitAttemptId,
    commit_order: CatalogOrderId,
) -> CatalogResult<CommitAttemptDecision> {
    if let Some(row) = load_commit_attempt(kv, catalog, attempt_id)? {
        if row.commit_order == commit_order {
            return Ok(CommitAttemptDecision::AlreadyCommitted(row));
        }
        return Err(CatalogError::CommitAttemptOrderChanged { attempt_id });
    }

    let row = CommitAttemptRow::new(attempt_id, commit_order);
    batch.put(commit_attempt_key(catalog, attempt_id), row.encode());
    Ok(CommitAttemptDecision::FirstCommit(row))
}

pub fn load_commit_attempt(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    attempt_id: CommitAttemptId,
) -> CatalogResult<Option<CommitAttemptRow>> {
    kv.get(&commit_attempt_key(catalog, attempt_id))?
        .map(|bytes| CommitAttemptRow::decode(&bytes))
        .transpose()
}

pub fn write_data_file_change(
    batch: &mut KvBatch,
    catalog: CatalogId,
    table_id: TableId,
    order: CatalogOrderId,
    kind: DataFileChangeKind,
    data_file_id: DataFileId,
) {
    batch.put(
        table_data_file_change_key(catalog, table_id, order, kind, data_file_id),
        Vec::new(),
    );
    batch.put(
        snapshot_data_file_change_key(catalog, table_id, order, kind, data_file_id),
        Vec::new(),
    );
}

pub fn list_data_file_changes_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<Vec<DataFileChange>> {
    if through_order < base_order {
        return Err(CatalogError::InvalidMutation(
            "logical conflict end order cannot precede base order".to_owned(),
        ));
    }
    let prefix = snapshot_data_file_change_prefix(catalog);
    let order_kind = scan_order_kind(base_order, through_order);
    let mut changes = Vec::new();
    for item in kv.scan_range(
        &snapshot_change_scan_start_after(catalog, base_order),
        &snapshot_change_scan_end(catalog, through_order),
        RangeDirection::Forward,
        usize::MAX,
    )? {
        changes.push(decode_snapshot_data_file_change_key(
            &prefix, &item.key, order_kind,
        )?);
    }
    Ok(changes)
}

pub fn list_data_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    intent: DataCommitIntent,
) -> CatalogResult<Vec<DataFileChange>> {
    let changes = list_data_file_changes(kv, catalog, table_id, base_order, through_order)?;
    Ok(changes
        .into_iter()
        .filter(|change| change.order > base_order && intent.conflicts_with(change.kind))
        .collect())
}

pub fn reject_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    intent: DataCommitIntent,
) -> CatalogResult<()> {
    if through_order < base_order {
        return Err(CatalogError::InvalidMutation(
            "logical conflict end order cannot precede base order".to_owned(),
        ));
    }
    if through_order == base_order {
        return Ok(());
    }
    let base_table = load_table_at(kv, catalog, table_id, base_order)?;
    let mut through_table = None;
    if let Some(dropped_at) = table_drop_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )? {
        return Err(CatalogError::TableLogicalConflict {
            table_id,
            dropped_at,
        });
    }
    let conflicts =
        list_data_conflicts_since_base(kv, catalog, table_id, base_order, through_order, intent)?;
    if !conflicts.is_empty() {
        return Err(CatalogError::LogicalConflict {
            table_id,
            conflicting_changes: conflicts,
        });
    }
    if let Some(changed_at) = table_schema_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )? {
        return Err(CatalogError::TableSchemaConflict {
            table_id,
            changed_at,
        });
    }
    Ok(())
}

pub(crate) fn reject_table_metadata_conflicts_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<()> {
    if through_order < base_order {
        return Err(CatalogError::InvalidMutation(
            "table metadata conflict end order cannot precede base order".to_owned(),
        ));
    }
    if through_order == base_order {
        return Ok(());
    }
    let base_table = load_table_at(kv, catalog, table_id, base_order)?;
    let mut through_table = None;
    if let Some(dropped_at) = table_drop_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )? {
        return Err(CatalogError::TableLogicalConflict {
            table_id,
            dropped_at,
        });
    }
    if let Some(changed_at) = table_schema_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )? {
        return Err(CatalogError::TableSchemaConflict {
            table_id,
            changed_at,
        });
    }
    Ok(())
}

pub fn table_drop_conflict_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<Option<CatalogOrderId>> {
    let base_table = load_table_at(kv, catalog, table_id, base_order)?;
    let mut through_table = None;
    table_drop_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )
}

fn table_drop_conflict_for_base_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    table: Option<&TableRow>,
    through_table: &mut Option<Option<TableRow>>,
) -> CatalogResult<Option<CatalogOrderId>> {
    let Some(table) = table else {
        return Ok(None);
    };
    let Some(drop_order) = table.validity.end_order else {
        return Ok(None);
    };
    if !(base_order < drop_order && drop_order <= through_order) {
        return Ok(None);
    }
    let exists_after_change =
        cached_through_table(kv, catalog, table_id, through_order, through_table)?.is_some();
    Ok((!exists_after_change).then_some(drop_order))
}

pub fn table_schema_conflict_since_base(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
) -> CatalogResult<Option<CatalogOrderId>> {
    let base_table = load_table_at(kv, catalog, table_id, base_order)?;
    let mut through_table = None;
    table_schema_conflict_for_base_table(
        kv,
        catalog,
        table_id,
        base_order,
        through_order,
        base_table.as_ref(),
        &mut through_table,
    )
}

fn table_schema_conflict_for_base_table(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    base_order: CatalogOrderId,
    through_order: CatalogOrderId,
    table: Option<&TableRow>,
    through_table: &mut Option<Option<TableRow>>,
) -> CatalogResult<Option<CatalogOrderId>> {
    let Some(table) = table else {
        return Ok(None);
    };
    let Some(changed_at) = table.validity.end_order else {
        return Ok(None);
    };
    if !(base_order < changed_at && changed_at <= through_order) {
        return Ok(None);
    }
    let Some(current_table) =
        cached_through_table(kv, catalog, table_id, through_order, through_table)?
    else {
        return Ok(None);
    };
    Ok((!current_table.same_user_visible_schema_as(&table)).then_some(changed_at))
}

fn cached_through_table<'a>(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    through_order: CatalogOrderId,
    cached: &'a mut Option<Option<TableRow>>,
) -> CatalogResult<Option<&'a TableRow>> {
    if cached.is_none() {
        *cached = Some(load_table_at(kv, catalog, table_id, through_order)?);
    }
    Ok(cached.as_ref().and_then(Option::as_ref))
}

pub(crate) fn commit_attempt_key(catalog: CatalogId, attempt_id: CommitAttemptId) -> Vec<u8> {
    let mut key = family_prefix(catalog, KeyFamily::CommitAttempt);
    key.extend_from_slice(&attempt_id.as_bytes());
    key
}

fn snapshot_change_scan_start_after(catalog: CatalogId, base_order: CatalogOrderId) -> Vec<u8> {
    let mut key = snapshot_data_file_change_prefix(catalog);
    key.extend_from_slice(&base_order.as_bytes());
    key.push(0xff);
    key
}

fn snapshot_change_scan_end(catalog: CatalogId, through_order: CatalogOrderId) -> Vec<u8> {
    let mut key = snapshot_data_file_change_prefix(catalog);
    key.extend_from_slice(&through_order.as_bytes());
    key.push(0xff);
    key
}

fn decode_snapshot_data_file_change_key(
    prefix: &[u8],
    key: &[u8],
    order_kind: crate::CatalogOrderKind,
) -> CatalogResult<DataFileChange> {
    let Some(tail) = key.strip_prefix(prefix) else {
        return Err(CatalogError::InvalidKey(
            "snapshot data-file change key has wrong prefix".to_owned(),
        ));
    };
    let minimum_len = CatalogOrderId::LEN + 1 + 8 + 1 + 1 + 1 + 8;
    if tail.len() != minimum_len {
        return Err(CatalogError::InvalidKey(format!(
            "snapshot data-file change key tail must be {minimum_len} bytes, got {}",
            tail.len()
        )));
    }

    let order_end = CatalogOrderId::LEN;
    let table_start = order_end + 1;
    let table_end = table_start + 8;
    let kind_index = table_end + 1;
    let data_file_start = kind_index + 2;
    if tail[order_end] != b'/' || tail[table_end] != b'/' || tail[kind_index + 1] != b'/' {
        return Err(CatalogError::InvalidKey(
            "snapshot data-file change key separators are invalid".to_owned(),
        ));
    }

    let order = CatalogOrderId::from_bytes(
        order_kind,
        tail[..order_end]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("change order is truncated".to_owned()))?,
    );
    let table_id = TableId(u64::from_be_bytes(
        tail[table_start..table_end]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("table id is truncated".to_owned()))?,
    ));
    let kind = DataFileChangeKind::from_code(tail[kind_index])?;
    let data_file_id = DataFileId(u64::from_be_bytes(
        tail[data_file_start..]
            .try_into()
            .map_err(|_| CatalogError::InvalidKey("data file id is truncated".to_owned()))?,
    ));
    Ok(DataFileChange::new(table_id, order, kind, data_file_id))
}

fn scan_order_kind(
    start_order: CatalogOrderId,
    end_order: CatalogOrderId,
) -> crate::CatalogOrderKind {
    if start_order.kind() == end_order.kind() {
        return end_order.kind();
    }
    crate::CatalogOrderKind::UuidV7
}

#[cfg(test)]
#[path = "conflict_tests.rs"]
mod conflict_tests;
