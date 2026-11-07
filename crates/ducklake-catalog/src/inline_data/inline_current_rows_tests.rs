use std::cell::Cell;

use super::inline_current_rows::{InlineCurrentRow, list_inline_current_rows_bounded};
use crate::{
    CatalogError, CatalogId, CatalogOrderId, FakeOrderedCatalogKv, KvBatch, OrderedCatalogKv,
    RangeDirection, RangeItem, RawSnapshotSequence, SchemaId, TableId,
    keys::inline_current_row_key,
};

#[test]
fn inline_current_row_round_trips_order_sequence_and_payload() {
    let row = InlineCurrentRow::new(
        CatalogOrderId::from_u128(123),
        RawSnapshotSequence(17),
        b"row\t7\ti:91".to_vec(),
    );

    assert_eq!(InlineCurrentRow::decode(&row.encode()).unwrap(), row);
}

#[test]
fn inline_current_row_rejects_truncated_and_unknown_versions() {
    assert!(InlineCurrentRow::decode(&[1]).is_err());

    let mut encoded = InlineCurrentRow::new(
        CatalogOrderId::from_u128(2),
        RawSnapshotSequence(1),
        Vec::new(),
    )
    .encode();
    encoded[0] = 99;

    assert!(InlineCurrentRow::decode(&encoded).is_err());
}

#[test]
fn bounded_current_row_scan_rejects_rows_over_capture_budget() {
    let catalog = CatalogId(1);
    let table = TableId(2);
    let schema = SchemaId(3);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for row_id in 0..2 {
        batch.put(
            inline_current_row_key(catalog, table, schema, row_id),
            InlineCurrentRow::new(
                CatalogOrderId::uuid_v7(row_id as u128),
                RawSnapshotSequence(1),
                format!("row\t{row_id}\tvalue").into_bytes(),
            )
            .encode(),
        );
    }
    kv.commit(batch).unwrap();

    let error = list_inline_current_rows_bounded(&kv, catalog, table, schema, 1, 1024)
        .expect_err("a truncated scan must fail closed");
    assert!(error.to_string().contains("more than 1 current rows"));
}

#[test]
fn bounded_current_row_scan_rejects_payload_bytes_over_capture_budget() {
    let catalog = CatalogId(4);
    let table = TableId(5);
    let schema = SchemaId(6);
    let mut kv = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    batch.put(
        inline_current_row_key(catalog, table, schema, 0),
        InlineCurrentRow::new(
            CatalogOrderId::uuid_v7(1),
            RawSnapshotSequence(1),
            b"row\t0\tvalue".to_vec(),
        )
        .encode(),
    );
    kv.commit(batch).unwrap();

    let error = list_inline_current_rows_bounded(&kv, catalog, table, schema, 1, 4)
        .expect_err("payloads over the capture budget must fail closed");
    assert!(error.to_string().contains("over capture budget 4"));
}

struct RejectsBulkPrefixScan {
    inner: FakeOrderedCatalogKv,
    range_calls: Cell<usize>,
}

impl OrderedCatalogKv for RejectsBulkPrefixScan {
    fn get(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.get(key))
    }

    fn scan_prefix(
        &self,
        _prefix: &[u8],
        _direction: RangeDirection,
        _limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        Err(CatalogError::Backend(
            "unbounded prefix scans are forbidden at this boundary".to_owned(),
        ))
    }

    fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        direction: RangeDirection,
        limit: usize,
    ) -> crate::CatalogResult<Vec<RangeItem>> {
        self.range_calls
            .set(self.range_calls.get().saturating_add(1));
        Ok(self.inner.scan_range(start, end, direction, limit))
    }

    fn read_conflict_fence(&self, key: &[u8]) -> crate::CatalogResult<Option<Vec<u8>>> {
        Ok(self.inner.read_conflict_fence(key))
    }
}

#[test]
fn bounded_current_row_scan_pages_before_decoding_a_large_over_budget_range() {
    let catalog = CatalogId(7);
    let table = TableId(8);
    let schema = SchemaId(9);
    let mut inner = FakeOrderedCatalogKv::new();
    let mut batch = KvBatch::new();
    for row_id in 0..2 {
        let mut payload = format!("row\t{row_id}\t").into_bytes();
        payload.resize(crate::inline_data::INLINE_PAYLOAD_LIMIT_BYTES - 1, b'x');
        batch.put(
            inline_current_row_key(catalog, table, schema, row_id),
            InlineCurrentRow::new(
                CatalogOrderId::uuid_v7(row_id as u128),
                RawSnapshotSequence(1),
                payload,
            )
            .encode(),
        );
    }
    inner.commit(batch).unwrap();
    let kv = RejectsBulkPrefixScan {
        inner,
        range_calls: Cell::new(0),
    };

    let error = list_inline_current_rows_bounded(
        &kv,
        catalog,
        table,
        schema,
        2,
        crate::inline_data::INLINE_PAYLOAD_LIMIT_BYTES,
    )
    .expect_err("the second near-limit row must exceed the cumulative bound");
    assert!(error.to_string().contains("over capture budget"));
    assert!(
        kv.range_calls.get() <= 2,
        "bounded range scan made too many requests: {}",
        kv.range_calls.get()
    );
}
