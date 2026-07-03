#[cfg(test)]
mod tests {
    use super::super::commit_create_macro_rows;
    use crate::{
        CatalogId, CatalogOrderId, FakeOrderedCatalogKv, MacroId, MacroImplementationRow, MacroRow,
        RawSnapshotSequence, SchemaId, initialize_catalog_if_absent,
    };

    #[test]
    fn given_existing_scalar_macro_when_creating_same_scalar_macro_then_commit_conflicts() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(1, "simple", "scalar")],
            Some(RawSnapshotSequence(1)),
        )
        .unwrap();

        let err = commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(2, "simple", "scalar")],
            Some(RawSnapshotSequence(2)),
        )
        .unwrap_err();

        assert!(err.to_string().contains("conflict creating macro simple"));
    }

    #[test]
    fn given_existing_scalar_macro_when_creating_table_macro_with_same_name_then_commit_succeeds() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(1, "same_name", "scalar")],
            Some(RawSnapshotSequence(1)),
        )
        .unwrap();

        let created = commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(2, "same_name", "table")],
            Some(RawSnapshotSequence(2)),
        )
        .unwrap();

        assert_eq!(created.len(), 1);
    }

    #[test]
    fn given_existing_macro_id_when_creating_macro_with_same_id_then_commit_conflicts() {
        let catalog = CatalogId(1);
        let mut kv = FakeOrderedCatalogKv::new();
        initialize_catalog_if_absent(&mut kv, catalog).unwrap();
        commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(1, "first", "scalar")],
            Some(RawSnapshotSequence(1)),
        )
        .unwrap();

        let err = commit_create_macro_rows(
            &mut kv,
            catalog,
            vec![macro_row(1, "second", "table")],
            Some(RawSnapshotSequence(2)),
        )
        .unwrap_err();

        assert!(err.to_string().contains("macro id 1 already exists"));
    }

    fn macro_row(id: u64, name: &str, macro_type: &str) -> MacroRow {
        MacroRow::new(
            MacroId(id),
            SchemaId(0),
            name,
            vec![MacroImplementationRow {
                dialect: "duckdb".to_owned(),
                sql: "SELECT 1".to_owned(),
                macro_type: macro_type.to_owned(),
                parameters: Vec::new(),
            }],
            CatalogOrderId::uuid_v7(0),
        )
    }
}
