use crate::{CatalogOrderId, SchemaId, SchemaRow};

#[test]
fn given_nested_schema_when_encoded_then_parent_and_validity_round_trip() {
    let mut row = SchemaRow::new(
        SchemaId(3),
        "uuid",
        "child",
        "parent/child",
        CatalogOrderId::uuid_v7(1),
    );
    row.parent_schema_id = Some(SchemaId(2));
    assert_eq!(SchemaRow::decode(&row.encode()).unwrap(), row);
    let mut truncated = row.encode();
    truncated.pop();
    assert!(SchemaRow::decode(&truncated).is_err());
}

#[test]
fn given_persisted_version_one_schema_when_decoded_then_parent_is_absent() {
    let row = SchemaRow::new(
        SchemaId(3),
        "uuid",
        "root",
        "root",
        CatalogOrderId::uuid_v7(1),
    );
    let mut bytes = row.encode();
    bytes[0] = 1;
    bytes.pop();
    assert_eq!(SchemaRow::decode(&bytes).unwrap(), row);
    bytes.extend_from_slice(&10_u32.to_le_bytes());
    assert_eq!(SchemaRow::decode(&bytes).unwrap(), row);
}
