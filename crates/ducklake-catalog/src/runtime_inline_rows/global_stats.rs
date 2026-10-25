use crate::{CatalogOrderId, CatalogResult, runtime_inline_rows::*};

pub(super) struct GlobalInlineStatsInput<'a> {
    pub(super) payload: &'a [u8],
    pub(super) begin_order: CatalogOrderId,
    pub(super) include_deleted: bool,
    pub(super) include_materialized: bool,
    pub(super) stats_mode: InlineStatsMode,
    pub(super) materialized_files: &'a InlineMaterializedFiles,
    pub(super) deleted_rows: Option<&'a RawInlineDeletionIndex>,
}

pub(super) fn accumulate_global_inline_stats(
    input: GlobalInlineStatsInput<'_>,
    catalog_stats: &mut InlineCatalogStats,
) -> CatalogResult<()> {
    let rows = std::str::from_utf8(input.payload).map_err(|error| {
        crate::CatalogError::Decode(format!("inline row payload is not utf8: {error}"))
    })?;
    for line in rows.lines().filter(|line| !line.is_empty()) {
        let fields = inline_row_fields(line)?;
        let row_id = fields[1].parse::<u64>().map_err(|error| {
            crate::CatalogError::Decode(format!("inline row id {} is invalid: {error}", fields[1]))
        })?;
        catalog_stats.observe_row_id(row_id);
        if !input.include_materialized
            && input
                .materialized_files
                .materializes(row_id, input.begin_order)
        {
            continue;
        }
        if matches!(input.stats_mode, InlineStatsMode::ExactVisible)
            && !input.include_deleted
            && input
                .deleted_rows
                .is_some_and(|index| index.hides_row_version(row_id, input.begin_order))
        {
            continue;
        }
        catalog_stats.accumulate_visible_row(row_id, &fields[2..])?;
    }
    Ok(())
}
