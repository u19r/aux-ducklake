#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn given_settings_with_scopes_when_listed_then_round_trips_rows() {
        let mut kv = crate::FakeOrderedCatalogKv::new();
        let catalog = CatalogId(1);

        set_metadata_setting(
            &mut kv,
            catalog,
            MetadataSettingRow::global("target_file_size", "1024"),
        )
        .expect("global");
        set_metadata_setting(
            &mut kv,
            catalog,
            MetadataSettingRow::schema("data_inlining_row_limit", "0", 7),
        )
        .expect("schema");
        set_metadata_setting(
            &mut kv,
            catalog,
            MetadataSettingRow::table("sort_on_insert", "true", 9),
        )
        .expect("table");

        let rows = list_metadata_settings(&kv, catalog).expect("settings");

        assert_eq!(
            rows,
            vec![
                MetadataSettingRow::global("target_file_size", "1024"),
                MetadataSettingRow::schema("data_inlining_row_limit", "0", 7),
                MetadataSettingRow::table("sort_on_insert", "true", 9),
            ]
        );
    }
}
