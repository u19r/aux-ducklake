use super::inline_current_rows::InlineCurrentRow;
use crate::{CatalogOrderId, RawSnapshotSequence};

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
