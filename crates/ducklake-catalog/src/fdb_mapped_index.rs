use foundationdb::{KeySelector, RangeOption, Transaction, options::StreamingMode, tuple};

use crate::{
    CatalogError, CatalogResult, FdbOrderedCatalogKv, RangeItem,
    constants::{FDB_MAPPED_INDEX_PAGE_ROWS, FDB_READ_PAGE_TARGET_BYTES},
    fdb_key_codec::decode_owned_value,
    keys::prefix_end,
    kv::{CatalogIndexJoin, CatalogIndexRow},
};

struct MappedIndexRow {
    index: RangeItem,
    children: Vec<RangeItem>,
}

impl From<&foundationdb::mapped_key_values::FdbMappedKeyValue> for MappedIndexRow {
    fn from(item: &foundationdb::mapped_key_values::FdbMappedKeyValue) -> Self {
        Self {
            index: RangeItem {
                key: item.parent_key().to_vec(),
                value: item.parent_value().to_vec(),
            },
            children: item
                .key_values()
                .iter()
                .map(|child| RangeItem {
                    key: child.key().to_vec(),
                    value: child.value().to_vec(),
                })
                .collect(),
        }
    }
}

impl FdbOrderedCatalogKv {
    pub(super) fn fetch_mapped_index_rows(
        &self,
        join: &CatalogIndexJoin,
    ) -> CatalogResult<Vec<CatalogIndexRow>> {
        let kv = self.with_read_version()?;
        let transaction = kv.create_transaction()?;
        let rows = Self::retry_sync_fdb_read(
            "get_mapped_range",
            transaction,
            |transaction| fetch_mapped_pages(&kv, transaction, join),
            |transaction, error| kv.reset_read_transaction_after_error(transaction, error),
        )?;
        rows.into_iter()
            .map(|row| kv.decode_mapped_index_row(join, row))
            .collect()
    }

    fn decode_mapped_index_row(
        &self,
        join: &CatalogIndexJoin,
        row: MappedIndexRow,
    ) -> CatalogResult<CatalogIndexRow> {
        let key = self.strip_namespace(&row.index.key)?;
        let value = decode_owned_value(&key, row.index.value)?;
        let expected_primary = join.primary_key(&key)?;
        if row.children.len() > 1 {
            return Err(CatalogError::Decode(
                "point index join returned multiple primary rows".to_owned(),
            ));
        }
        let primary = row
            .children
            .into_iter()
            .next()
            .map(|child| {
                let key = self.strip_namespace(&child.key)?;
                if key != expected_primary {
                    return Err(CatalogError::Decode(
                        "mapped primary row does not match its index".to_owned(),
                    ));
                }
                let value = decode_owned_value(&key, child.value)?;
                Ok(RangeItem { key, value })
            })
            .transpose()?;
        Ok(CatalogIndexRow {
            index: RangeItem { key, value },
            primary,
        })
    }
}

fn fetch_mapped_pages(
    kv: &FdbOrderedCatalogKv,
    transaction: &Transaction,
    join: &CatalogIndexJoin,
) -> Result<Vec<MappedIndexRow>, foundationdb::FdbError> {
    // The C binding exposes the GetRange variant. The fixed-width tuple ID
    // makes this primary-key prefix contain exactly one logical row.
    let mapper = tuple::pack(&(kv.key_prefix(), "{V[0]}", "{V[1]}", "{...}"));
    let mut range = RangeOption::from(
        kv.namespaced_range_bound(&join.index_prefix)
            ..kv.namespaced_range_bound(&prefix_end(&join.index_prefix)),
    );
    range.mode = StreamingMode::WantAll;
    range.limit = Some(FDB_MAPPED_INDEX_PAGE_ROWS);
    range.target_bytes = FDB_READ_PAGE_TARGET_BYTES;
    let mut rows = Vec::new();
    loop {
        let page = crate::fdb_runtime::wait_for_fdb(
            "wait.fdb.get_mapped_range",
            transaction.get_mapped_range(&range, &mapper, 1, false),
        )?;
        crate::fdb_runtime::record_transaction_usage(transaction);
        if page.is_empty() {
            return Ok(rows);
        }
        let last = page
            .last()
            .expect("nonempty mapped page")
            .parent_key()
            .to_vec();
        let is_complete = !page.more() && page.len() < FDB_MAPPED_INDEX_PAGE_ROWS;
        rows.extend(page.iter().map(MappedIndexRow::from));
        #[cfg(feature = "runtime-metrics")]
        record_mapped_page(&page);
        if is_complete {
            return Ok(rows);
        }
        range.begin = KeySelector::first_greater_than(last);
    }
}

#[cfg(feature = "runtime-metrics")]
fn record_mapped_page(page: &foundationdb::future::MappedKeyValues) {
    crate::runtime_metrics::record_runtime_measurement(
        "fdb.mapped_range.primary_rows",
        page.len() as u64,
        page.iter()
            .map(|row| (row.parent_key().len() + row.parent_value().len()) as u64)
            .sum(),
    );
    crate::runtime_metrics::record_runtime_measurement(
        "fdb.mapped_range.child_rows",
        page.iter().map(|row| row.key_values().len() as u64).sum(),
        page.iter()
            .flat_map(|row| row.key_values())
            .map(|row| (row.key().len() + row.value().len()) as u64)
            .sum(),
    );
}
