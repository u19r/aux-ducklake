use std::collections::{BTreeMap, HashMap};

use crate::{
    CatalogError, CatalogId, CatalogResult, RawSnapshotSequence, SnapshotRow, TableId, TableRow,
    ValidityWindow,
    conflict_watermarks::stage_max_catalog_id_watermark,
    ids::CatalogOrderId,
    keys::{current_table_row_key, prefix_end, table_object_key, table_visibility_key},
    schema_version_state::{stage_next_catalog_snapshot_version, stage_next_schema_version},
    store::stage_snapshot,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeDirection {
    Forward,
    Reverse,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct KvBatch {
    checks: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    writes: Vec<(Vec<u8>, Vec<u8>)>,
    deletes: Vec<Vec<u8>>,
    fence_writes: Vec<Vec<u8>>,
}

impl KvBatch {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check_value(&mut self, key: Vec<u8>, expected: Option<Vec<u8>>) {
        self.checks.push((key, expected));
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.writes.push((key, value));
    }

    pub(crate) fn put_max_u64(&mut self, key: Vec<u8>, value: u64) -> CatalogResult<()> {
        for (staged_key, staged_value) in &mut self.writes {
            if staged_key != &key {
                continue;
            }
            let bytes: [u8; 8] = staged_value.as_slice().try_into().map_err(|_| {
                let key_label =
                    crate::keys::decode_key(&key).unwrap_or_else(|_| "<invalid-key>".to_owned());
                CatalogError::InvalidKey(format!(
                    "staged max-u64 value for key {} is not 8 bytes",
                    key_label
                ))
            })?;
            let max_value = u64::from_be_bytes(bytes).max(value);
            *staged_value = max_value.to_be_bytes().to_vec();
            return Ok(());
        }
        self.put(key, value.to_be_bytes().to_vec());
        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.deletes.push(key);
    }

    pub fn write_conflict_fence(&mut self, key: Vec<u8>) {
        self.fence_writes.push(key);
    }

    #[must_use]
    #[cfg(feature = "foundationdb")]
    pub(crate) fn checks(&self) -> &[(Vec<u8>, Option<Vec<u8>>)] {
        &self.checks
    }

    #[must_use]
    #[cfg(feature = "foundationdb")]
    pub(crate) fn writes(&self) -> &[(Vec<u8>, Vec<u8>)] {
        &self.writes
    }

    #[must_use]
    #[cfg(feature = "foundationdb")]
    pub(crate) fn deletes(&self) -> &[Vec<u8>] {
        &self.deletes
    }

    #[must_use]
    #[cfg(feature = "foundationdb")]
    pub(crate) fn fence_writes(&self) -> &[Vec<u8>] {
        &self.fence_writes
    }

    #[must_use]
    pub fn estimated_mutation_bytes(&self) -> usize {
        let write_bytes = self
            .writes
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum::<usize>();
        let delete_bytes = self.deletes.iter().map(Vec::len).sum::<usize>();
        let check_bytes = self
            .checks
            .iter()
            .map(|(key, value)| {
                key.len()
                    .saturating_add(value.as_ref().map_or(0, std::vec::Vec::len))
            })
            .sum::<usize>();
        let fence_bytes = self.fence_writes.iter().map(Vec::len).sum::<usize>();
        write_bytes
            .saturating_add(delete_bytes)
            .saturating_add(check_bytes)
            .saturating_add(fence_bytes)
    }
}

pub trait OrderedCatalogKv {
    fn catalog_cache_namespace(&self) -> CatalogCacheNamespace
    where
        Self: Sized,
    {
        CatalogCacheNamespace::process_local(self as *const Self as usize)
    }

    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>>;
    fn batch_get(&self, keys: &[Vec<u8>]) -> CatalogResult<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(key)).collect()
    }
    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>>;
    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>>;
    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CatalogCacheNamespace {
    kind: u8,
    primary: u64,
    secondary: u64,
}

impl CatalogCacheNamespace {
    #[must_use]
    pub fn process_local(address: usize) -> Self {
        Self {
            kind: 0,
            primary: address as u64,
            secondary: 0,
        }
    }

    #[must_use]
    pub fn foundationdb(database_address: usize, key_prefix_hash: u64) -> Self {
        Self {
            kind: 1,
            primary: database_address as u64,
            secondary: key_prefix_hash,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableVersionReplacement {
    pub table_id: TableId,
    pub previous: TableRow,
    pub next: TableRow,
}

impl TableVersionReplacement {
    #[must_use]
    pub fn new(table_id: TableId, previous: TableRow, next: TableRow) -> Self {
        Self {
            table_id,
            previous,
            next,
        }
    }
}

pub trait MutableCatalogKv: OrderedCatalogKv {
    fn generated_order_id(&mut self) -> CatalogResult<CatalogOrderId>;
    fn commit(&mut self, batch: KvBatch) -> CatalogResult<()>;

    fn commit_table_replacements(
        &mut self,
        catalog: CatalogId,
        previous_sequence: RawSnapshotSequence,
        replacements: Vec<TableVersionReplacement>,
    ) -> CatalogResult<()> {
        if replacements.is_empty() {
            return Ok(());
        }
        let order = self.generated_order_id()?;
        let snapshot = SnapshotRow::new(order, previous_sequence.next());
        let mut batch = KvBatch::new();
        stage_snapshot(&mut batch, catalog, &snapshot);
        if replacements.iter().any(|replacement| {
            !replacement
                .previous
                .same_user_visible_schema_as(&replacement.next)
        }) {
            stage_next_schema_version(self, &mut batch, catalog)?;
        } else {
            stage_next_catalog_snapshot_version(self, &mut batch, catalog)?;
        }

        for replacement in replacements {
            let mut previous = replacement.previous;
            let mut next = replacement.next;
            previous.validity.end_order = Some(order);
            next.validity = ValidityWindow::new(order, None);
            batch.put(
                table_object_key(catalog, replacement.table_id, previous.validity.begin_order),
                previous.encode(),
            );
            batch.put(
                table_visibility_key(catalog, previous.validity.begin_order, replacement.table_id),
                previous.encode(),
            );
            batch.put(
                table_object_key(catalog, replacement.table_id, order),
                next.encode(),
            );
            batch.put(
                table_visibility_key(catalog, order, replacement.table_id),
                next.encode(),
            );
            batch.put(
                current_table_row_key(catalog, replacement.table_id),
                next.encode(),
            );
            stage_max_catalog_id_watermark(self, &mut batch, catalog, replacement.table_id.0)?;
        }

        self.commit(batch)
    }
}

#[derive(Debug, Default)]
pub struct FakeOrderedCatalogKv {
    items: BTreeMap<Vec<u8>, Vec<u8>>,
    fence_versions: HashMap<Vec<u8>, u64>,
    next_order: u128,
}

impl FakeOrderedCatalogKv {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.items.get(key).cloned()
    }

    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> Vec<RangeItem> {
        self.scan_range(prefix, &prefix_end(prefix), direction, limit)
    }

    pub fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> Vec<RangeItem> {
        let range = self
            .items
            .range(start.to_vec()..end.to_vec())
            .map(|(key, value)| RangeItem {
                key: key.clone(),
                value: value.clone(),
            });
        match direction {
            RangeDirection::Forward => range.take(limit).collect(),
            RangeDirection::Reverse => range.rev().take(limit).collect(),
        }
    }

    #[must_use]
    pub fn read_conflict_fence(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.fence_versions
            .get(key)
            .map(|version| version.to_be_bytes().to_vec())
    }

    pub fn write_conflict_fence(&mut self, key: Vec<u8>) {
        let version = self.fence_versions.entry(key).or_insert(0);
        *version = version.saturating_add(1);
    }

    pub fn generated_order_id(&mut self) -> CatalogOrderId {
        self.next_order = self.next_order.saturating_add(1);
        CatalogOrderId::from_u128(self.next_order)
    }

    pub fn commit(&mut self, batch: KvBatch) -> CatalogResult<()> {
        for (key, expected) in &batch.checks {
            let actual = self.read_conflict_fence(key);
            if actual != *expected {
                return Err(CatalogError::ConflictFenceChanged { fence: key.clone() });
            }
        }
        for key in batch.deletes {
            self.items.remove(&key);
        }
        for (key, value) in batch.writes {
            self.items.insert(key, value);
        }
        for key in batch.fence_writes {
            self.write_conflict_fence(key);
        }
        Ok(())
    }
}

impl OrderedCatalogKv for FakeOrderedCatalogKv {
    fn get(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(Self::get(self, key))
    }

    fn scan_prefix(
        &self,
        prefix: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        Ok(Self::scan_prefix(self, prefix, direction, limit))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> CatalogResult<Vec<RangeItem>> {
        Ok(Self::scan_range(self, start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> CatalogResult<Option<Vec<u8>>> {
        Ok(Self::read_conflict_fence(self, key))
    }
}

impl MutableCatalogKv for FakeOrderedCatalogKv {
    fn generated_order_id(&mut self) -> CatalogResult<CatalogOrderId> {
        Ok(Self::generated_order_id(self))
    }

    fn commit(&mut self, batch: KvBatch) -> CatalogResult<()> {
        Self::commit(self, batch)
    }
}
