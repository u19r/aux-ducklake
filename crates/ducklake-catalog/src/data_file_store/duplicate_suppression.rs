use std::collections::{BTreeMap, BTreeSet};

use crate::data_file_store::*;
use crate::runtime_read_context::CatalogCurrentFilePartitionValuesContext;
use crate::{
    CatalogId, CatalogResult, DataFileId, DataFileRow, FilePartitionValueRow, OrderedCatalogKv,
    TableId, file_partitions::list_file_partition_values_for_data_files, ids::CatalogOrderId,
};
pub(crate) fn data_file_visible_at(row: &DataFileRow, snapshot_order: CatalogOrderId) -> bool {
    row.validity.is_visible_at(snapshot_order)
}

pub(crate) fn without_backfilled_source_duplicates(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: Vec<DataFileRow>,
) -> CatalogResult<Vec<DataFileRow>> {
    let started = RuntimeMetricStage::start();
    let replacements = rows
        .iter()
        .filter(|row| is_backfilled_replacement_file(row))
        .map(DataFileCoverage::from)
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        record_runtime_method_stage(
            "method.data_file_store.without_backfilled_source_duplicates",
            started,
        );
        return Ok(rows);
    }
    let data_file_ids = rows
        .iter()
        .map(|row| row.data_file_id)
        .collect::<BTreeSet<_>>();
    let partition_keys = if replacements_need_partition_values(&replacements, &rows) {
        let comparison_count = replacements.len().saturating_mul(rows.len());
        PartitionKeyLookup::new(
            partition_values_for_duplicate_suppression(kv, catalog, &rows, &data_file_ids)?,
            comparison_count,
        )
    } else {
        PartitionKeyLookup::default()
    };

    let rows = rows
        .into_iter()
        .filter(|row| {
            replacements.iter().all(|replacement| {
                row.data_file_id == replacement.data_file_id
                    || !replacement.covers(row, &partition_keys)
            })
        })
        .collect();
    record_runtime_method_stage(
        "method.data_file_store.without_backfilled_source_duplicates",
        started,
    );
    Ok(rows)
}

pub(super) fn partition_values_for_duplicate_suppression(
    kv: &impl OrderedCatalogKv,
    catalog: CatalogId,
    rows: &[DataFileRow],
    data_file_ids: &BTreeSet<DataFileId>,
) -> CatalogResult<Vec<FilePartitionValueRow>> {
    if rows.iter().all(|row| row.validity.end_order.is_none()) {
        return Ok(
            CatalogCurrentFilePartitionValuesContext::for_current_file_partition_values(
                kv, catalog,
            )?
            .current_file_partition_values()
            .iter()
            .filter(|row| data_file_ids.contains(&row.data_file_id))
            .cloned()
            .collect(),
        );
    }
    list_file_partition_values_for_data_files(kv, catalog, data_file_ids)
}

pub(super) fn replacements_need_partition_values(
    replacements: &[DataFileCoverage],
    rows: &[DataFileRow],
) -> bool {
    replacements.iter().any(|replacement| {
        rows.iter().any(|row| {
            row.data_file_id != replacement.data_file_id
                && replacement.might_cover_by_partition(row)
        })
    })
}

pub(super) fn is_backfilled_replacement_file(row: &DataFileRow) -> bool {
    row.max_partial_order
        .is_some_and(|max_partial_order| max_partial_order != row.validity.begin_order)
}

#[derive(Debug, Clone)]
pub(super) struct DataFileCoverage {
    data_file_id: DataFileId,
    table_id: TableId,
    path: String,
    begin_order: crate::CatalogOrderId,
    row_id_start: u64,
    row_id_end: u64,
    row_id_start_known: bool,
    record_count: u64,
    max_partial_order: CatalogOrderId,
}

impl DataFileCoverage {
    fn covers(&self, row: &DataFileRow, partition_keys: &PartitionKeyLookup) -> bool {
        if row.table_id != self.table_id || row.record_count == 0 {
            return false;
        }
        if row.validity.begin_order > self.max_partial_order {
            return false;
        }
        if self.covers_row_id_range(row) {
            return !partition_paths_are_known_and_different(&self.path, &row.path);
        }
        if self.row_id_start_known && row.row_id_start_known {
            return false;
        }
        if self.record_count <= row.record_count {
            return false;
        }
        if row.validity.begin_order < self.begin_order {
            return false;
        }
        partition_keys.same_non_empty_key(self.data_file_id, row.data_file_id)
    }

    fn might_cover_by_partition(&self, row: &DataFileRow) -> bool {
        row.table_id == self.table_id
            && row.record_count != 0
            && row.validity.begin_order <= self.max_partial_order
            && row.validity.begin_order >= self.begin_order
            && !self.covers_row_id_range(row)
            && (!self.row_id_start_known || !row.row_id_start_known)
            && self.record_count > row.record_count
    }

    fn covers_row_id_range(&self, row: &DataFileRow) -> bool {
        if !self.row_id_start_known || !row.row_id_start_known {
            return false;
        }
        let row_end = row.row_id_start.saturating_add(row.record_count);
        self.row_id_start <= row.row_id_start && row_end <= self.row_id_end
    }
}

impl From<&DataFileRow> for DataFileCoverage {
    fn from(row: &DataFileRow) -> Self {
        Self {
            data_file_id: row.data_file_id,
            table_id: row.table_id,
            path: row.path.clone(),
            begin_order: row.validity.begin_order,
            row_id_start: row.row_id_start,
            row_id_end: row.row_id_start.saturating_add(row.record_count),
            row_id_start_known: row.row_id_start_known,
            record_count: row.record_count,
            max_partial_order: row.max_partial_order.unwrap_or(row.validity.begin_order),
        }
    }
}

pub(super) fn partition_paths_are_known_and_different(left: &str, right: &str) -> bool {
    let left = hive_partition_path_key(left);
    let right = hive_partition_path_key(right);
    !left.is_empty() && !right.is_empty() && left != right
}

pub(super) fn hive_partition_path_key(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|component| {
            component
                .split_once('=')
                .is_some_and(|(key, value)| !key.is_empty() && !value.is_empty())
        })
        .collect()
}

const PARTITION_KEY_LOOKUP_INDEX_SCAN_MAX_PRODUCT: usize = 256;

#[derive(Default)]
pub(super) enum PartitionKeyLookup {
    #[default]
    Empty,
    Linear(Vec<FilePartitionValueRow>),
    Indexed(BTreeMap<DataFileId, Vec<(u32, String)>>),
}

impl PartitionKeyLookup {
    pub(super) fn new(
        partition_values: Vec<FilePartitionValueRow>,
        comparison_count: usize,
    ) -> Self {
        if partition_values.is_empty() {
            return Self::Empty;
        }
        let scan_product = partition_values.len().saturating_mul(comparison_count);
        if scan_product <= PARTITION_KEY_LOOKUP_INDEX_SCAN_MAX_PRODUCT {
            return Self::Linear(partition_values);
        }
        let mut keys_by_file = BTreeMap::<DataFileId, Vec<(u32, String)>>::new();
        for row in partition_values {
            keys_by_file
                .entry(row.data_file_id)
                .or_default()
                .push((row.partition_key_index.0, row.partition_value));
        }
        for values in keys_by_file.values_mut() {
            values.sort();
        }
        Self::Indexed(keys_by_file)
    }

    pub(super) fn same_non_empty_key(&self, left: DataFileId, right: DataFileId) -> bool {
        match self {
            Self::Empty => false,
            Self::Linear(partition_values) => {
                let left_key = partition_key_from_rows(partition_values, left);
                !left_key.is_empty() && left_key == partition_key_from_rows(partition_values, right)
            }
            Self::Indexed(keys_by_file) => keys_by_file.get(&left).is_some_and(|left_key| {
                !left_key.is_empty() && Some(left_key) == keys_by_file.get(&right)
            }),
        }
    }
}

pub(super) fn partition_key_from_rows(
    partition_values: &[FilePartitionValueRow],
    data_file_id: DataFileId,
) -> Vec<(u32, String)> {
    let mut values = partition_values
        .iter()
        .filter(|row| row.data_file_id == data_file_id)
        .map(|row| (row.partition_key_index.0, row.partition_value.clone()))
        .collect::<Vec<_>>();
    values.sort();
    values
}
