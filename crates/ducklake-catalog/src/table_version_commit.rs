use crate::{
    CatalogId, CatalogResult, MutableCatalogKv, RawSnapshotSequence, TableId, TableRow,
    TableVersionReplacement,
};

pub(crate) fn commit_replaced_table_version(
    kv: &mut impl MutableCatalogKv,
    catalog: CatalogId,
    table_id: TableId,
    previous_sequence: RawSnapshotSequence,
    previous: TableRow,
    next: TableRow,
) -> CatalogResult<()> {
    kv.commit_table_replacements(
        catalog,
        previous_sequence,
        vec![TableVersionReplacement::new(table_id, previous, next)],
    )
}
