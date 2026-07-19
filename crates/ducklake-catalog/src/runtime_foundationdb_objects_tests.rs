#[cfg(test)]
mod tests {
    use crate::{
        CatalogId, CatalogOrderId, DuckLakeSnapshotId, FakeOrderedCatalogKv, SchemaId, TableId,
        ViewRow, commit_create_view_row, initialize_catalog_if_absent,
    };

    use super::super::reject_stale_object_commit;

    #[test]
    fn given_same_public_snapshot_when_validating_object_helper_commit_then_it_is_not_stale() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_view_row(
            &mut kv,
            catalog,
            ViewRow::new(
                TableId(2),
                SchemaId(0),
                crate::ViewDefinition::new("view-uuid", "v", "duckdb", "SELECT 1", Vec::new()),
                CatalogOrderId::uuid_v7(0),
            ),
        )
        .unwrap();

        reject_stale_object_commit(
            &kv,
            catalog,
            Some(DuckLakeSnapshotId(1)),
            "change view comment",
        )
        .unwrap();
        let error = reject_stale_object_commit(
            &kv,
            catalog,
            Some(DuckLakeSnapshotId(0)),
            "change view comment",
        )
        .unwrap_err();

        assert!(error.to_string().contains("proposed snapshot 0 is stale"));
    }
}
