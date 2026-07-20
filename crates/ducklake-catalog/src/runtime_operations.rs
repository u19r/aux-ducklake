use crate::{
    CatalogResult,
    runtime_change_feed_ops::{list_data_file_changes, list_table_deletions},
    runtime_cleanup_ops::{list_known_files_for_cleanup, list_old_files_for_cleanup},
    runtime_commit_attempt_ops::commit_attempt,
    runtime_compaction_ops::{merge_adjacent_files, rewrite_delete_files},
    runtime_data_mutation_ops::commit_data_mutation,
    runtime_file_ops::{
        list_current_partition_files, list_current_partition_files_batch,
        list_current_partition_prune_files, list_data_files_at, list_partition_files_at,
        list_partition_files_at_batch, list_partition_prune_files_at,
        list_removed_data_files_after,
    },
    runtime_inline_ops::{
        delete_inline_rows, inline_file_deletions_exist,
        list_current_inline_flush_delete_positions, list_inline_file_deletions,
        list_inline_file_deletions_for_flush, list_inline_flush_delete_positions,
        list_inline_row_deletions, list_inline_row_insertions, read_inline_rows,
        read_inline_rows_for_aggregate_stats, read_inline_rows_for_flush,
        read_inline_rows_for_global_stats, read_inline_rows_for_global_stats_batch,
        register_inline_rows, register_inline_tables,
    },
    runtime_maintenance_ops::remove_cleanup_files,
    runtime_metadata_ops::{
        attach_metadata, commit_column_mappings, commit_metadata_batch, initialize_ducklake,
        list_column_mapping_rows, list_config_options, list_current_metadata_data_file_rows,
        list_current_metadata_data_file_rows_for_data_file_ids,
        list_current_metadata_file_column_stats_rows,
        list_current_metadata_file_column_stats_rows_for_data_file_ids,
        list_current_metadata_file_partition_value_rows, list_data_file_rows,
        list_delete_file_rows, list_delete_file_rows_for_delete_file_ids,
        list_file_column_stats_rows, list_file_partition_value_rows_for_data_file_ids,
        list_global_stats_for_snapshot, list_global_stats_inputs_for_snapshot,
        list_snapshot_stats_and_changes_inputs, lookup_begin_snapshot_for_schema_version,
        metadata_exists, render_bounded_append_mirror_sql, render_bounded_delete_file_mirror_sql,
        render_current_metadata_data_file_mirror_sql,
        render_current_metadata_file_column_stats_mirror_sql,
        render_current_metadata_file_partition_value_mirror_sql, render_delete_file_mirror_sql,
        render_scheduled_cleanup_mirror_sql, resolve_catalog_id, set_config_option,
    },
    runtime_object_ops::{
        change_view_comment, create_macros, create_views, drop_macros, drop_tables, drop_views,
        rename_views,
    },
    runtime_protocol::RuntimeRequest,
    runtime_schema_change_ops::{
        add_columns, change_column_defaults, change_column_types, change_comments,
        change_partition_keys, change_sort_keys, drop_columns, rename_columns, rename_tables,
    },
    runtime_schema_ops::{create_schemas, drop_schemas},
    runtime_snapshot_maintenance_ops::expire_snapshots_for_runtime,
    runtime_snapshot_ops::{
        get_catalog_for_snapshot, get_conflict_snapshot, get_snapshot, get_snapshot_at,
        get_snapshot_at_timestamp, list_snapshot_changes_after, list_snapshots,
    },
    runtime_table_ops::{
        create_tables, get_next_column_id, is_column_created_with_table, replace_tables,
    },
};

pub(crate) fn payload_for_request(request: &RuntimeRequest) -> CatalogResult<Vec<u8>> {
    let catalog = request.catalog_id;
    match request.operation.as_str() {
        "ResolveCatalogId" => resolve_catalog_id(request.backend, catalog, &request.payload),
        "AttachMetadata" => attach_metadata(request.backend, catalog, &request.payload),
        "MetadataExists" => metadata_exists(request.backend, catalog),
        "InitializeDuckLake" => initialize_ducklake(request.backend, catalog, &request.payload),
        "CommitMetadataBatch" => commit_metadata_batch(request.backend, catalog),
        "SetConfigOption" => set_config_option(request.backend, catalog, &request.payload),
        "ListConfigOptions" => list_config_options(request.backend, catalog),
        "CommitColumnMappings" => {
            commit_column_mappings(request.backend, catalog, &request.payload)
        }
        "ListColumnMappings" => {
            list_column_mapping_rows(request.backend, catalog, &request.payload)
        }
        "ListFilePartitionValuesForDataFileIds" => {
            list_file_partition_value_rows_for_data_file_ids(
                request.backend,
                catalog,
                &request.payload,
            )
        }
        "ListFileColumnStats" => list_file_column_stats_rows(request.backend, catalog),
        "ListCurrentMetadataFilePartitionValues" => {
            list_current_metadata_file_partition_value_rows(request.backend, catalog)
        }
        "RenderCurrentMetadataFilePartitionValueMirrorSql" => {
            render_current_metadata_file_partition_value_mirror_sql(request.backend, catalog)
        }
        "ListCurrentMetadataFileColumnStats" => {
            list_current_metadata_file_column_stats_rows(request.backend, catalog)
        }
        "RenderCurrentMetadataFileColumnStatsMirrorSql" => {
            render_current_metadata_file_column_stats_mirror_sql(request.backend, catalog)
        }
        "ListCurrentMetadataFileColumnStatsForDataFileIds" => {
            list_current_metadata_file_column_stats_rows_for_data_file_ids(
                request.backend,
                catalog,
                &request.payload,
            )
        }
        "ListGlobalStatsInputsForSnapshot" => {
            list_global_stats_inputs_for_snapshot(request.backend, catalog, &request.payload)
        }
        "ListGlobalStatsForSnapshot" => {
            list_global_stats_for_snapshot(request.backend, catalog, &request.payload)
        }
        "ListSnapshotStatsAndChangesInputs" => {
            list_snapshot_stats_and_changes_inputs(request.backend, catalog, &request.payload)
        }
        "ListCurrentMetadataDataFiles" => {
            list_current_metadata_data_file_rows(request.backend, catalog)
        }
        "RenderCurrentMetadataDataFileMirrorSql" => {
            render_current_metadata_data_file_mirror_sql(request.backend, catalog)
        }
        "RenderBoundedAppendMirrorSql" => {
            render_bounded_append_mirror_sql(request.backend, catalog, &request.payload)
        }
        "ListCurrentMetadataDataFilesForDataFileIds" => {
            list_current_metadata_data_file_rows_for_data_file_ids(
                request.backend,
                catalog,
                &request.payload,
            )
        }
        "ListDataFiles" => list_data_file_rows(request.backend, catalog),
        "ListDeleteFiles" => list_delete_file_rows(request.backend, catalog),
        "RenderDeleteFileMirrorSql" => render_delete_file_mirror_sql(request.backend, catalog),
        "RenderScheduledCleanupMirrorSql" => {
            render_scheduled_cleanup_mirror_sql(request.backend, catalog)
        }
        "RenderBoundedDeleteFileMirrorSql" => {
            render_bounded_delete_file_mirror_sql(request.backend, catalog, &request.payload)
        }
        "ListDeleteFilesForDeleteFileIds" => {
            list_delete_file_rows_for_delete_file_ids(request.backend, catalog, &request.payload)
        }
        "GetBeginSnapshotForSchemaVersion" => {
            lookup_begin_snapshot_for_schema_version(request.backend, catalog, &request.payload)
        }
        "CreateSchemas" => create_schemas(request.backend, catalog, &request.payload),
        "DropSchemas" => drop_schemas(request.backend, catalog, &request.payload),
        "CreateTables" => create_tables(request.backend, catalog, &request.payload),
        "ReplaceTables" => replace_tables(request.backend, catalog, &request.payload),
        "DropTables" => drop_tables(request.backend, catalog, &request.payload),
        "CreateViews" => create_views(request.backend, catalog, &request.payload),
        "RenameViews" => rename_views(request.backend, catalog, &request.payload),
        "DropViews" => drop_views(request.backend, catalog, &request.payload),
        "ChangeViewComment" => change_view_comment(request.backend, catalog, &request.payload),
        "CreateMacros" => create_macros(request.backend, catalog, &request.payload),
        "DropMacros" => drop_macros(request.backend, catalog, &request.payload),
        "AddColumns" => add_columns(request.backend, catalog, &request.payload),
        "RenameColumns" => rename_columns(request.backend, catalog, &request.payload),
        "ChangeColumnTypes" => change_column_types(request.backend, catalog, &request.payload),
        "ChangeColumnDefaults" => {
            change_column_defaults(request.backend, catalog, &request.payload)
        }
        "ChangeComments" => change_comments(request.backend, catalog, &request.payload),
        "ChangePartitionKeys" => change_partition_keys(request.backend, catalog, &request.payload),
        "ChangeSortKeys" => change_sort_keys(request.backend, catalog, &request.payload),
        "DropColumns" => drop_columns(request.backend, catalog, &request.payload),
        "RenameTables" => rename_tables(request.backend, catalog, &request.payload),
        "CommitDataMutation" => commit_data_mutation(request.backend, catalog, &request.payload),
        "CommitAttempt" => commit_attempt(request.backend, catalog, &request.payload),
        "GetSnapshot" => get_snapshot(request.backend, catalog),
        "GetConflictSnapshot" => get_conflict_snapshot(request.backend, catalog),
        "GetSnapshotAt" => get_snapshot_at(request.backend, catalog, &request.payload),
        "GetSnapshotAtTimestamp" => {
            get_snapshot_at_timestamp(request.backend, catalog, &request.payload)
        }
        "GetCatalogForSnapshot" => {
            get_catalog_for_snapshot(request.backend, catalog, &request.payload)
        }
        "ListSnapshots" => list_snapshots(request.backend, catalog, &request.payload),
        "ListSnapshotChangesAfter" => {
            list_snapshot_changes_after(request.backend, catalog, &request.payload)
        }
        "ListDataFilesAt" => list_data_files_at(request.backend, catalog, &request.payload),
        "ListRemovedDataFilesAfter" => {
            list_removed_data_files_after(request.backend, catalog, &request.payload)
        }
        "ListCurrentDataFilesForPartitionScan" => {
            list_current_partition_files(request.backend, catalog, &request.payload)
        }
        "ListCurrentDataFilesForPartitionScans" => {
            list_current_partition_files_batch(request.backend, catalog, &request.payload)
        }
        "ListCurrentDataFilesForPartitionPrune" => {
            list_current_partition_prune_files(request.backend, catalog, &request.payload)
        }
        "ListDataFilesForPartitionScanAt" => {
            list_partition_files_at(request.backend, catalog, &request.payload)
        }
        "ListDataFilesForPartitionScansAt" => {
            list_partition_files_at_batch(request.backend, catalog, &request.payload)
        }
        "ListDataFilesForPartitionPruneAt" => {
            list_partition_prune_files_at(request.backend, catalog, &request.payload)
        }
        "ReadInlineRows" => read_inline_rows(request.backend, catalog, &request.payload),
        "ReadInlineRowsForFlush" => {
            read_inline_rows_for_flush(request.backend, catalog, &request.payload)
        }
        "ReadInlineRowsForGlobalStats" => {
            read_inline_rows_for_global_stats(request.backend, catalog, &request.payload)
        }
        "ReadInlineRowsForAggregateStats" => {
            read_inline_rows_for_aggregate_stats(request.backend, catalog, &request.payload)
        }
        "ReadInlineRowsForGlobalStatsBatch" => {
            read_inline_rows_for_global_stats_batch(request.backend, catalog, &request.payload)
        }
        "RegisterInlineRows" => register_inline_rows(request.backend, catalog, &request.payload),
        "RegisterInlineTables" => {
            register_inline_tables(request.backend, catalog, &request.payload)
        }
        "DeleteInlineRows" => delete_inline_rows(request.backend, catalog, &request.payload),
        "InlineFileDeletionsExist" => {
            inline_file_deletions_exist(request.backend, catalog, &request.payload)
        }
        "ListInlineFileDeletions" => {
            list_inline_file_deletions(request.backend, catalog, &request.payload)
        }
        "ListInlineFileDeletionsForFlush" => {
            list_inline_file_deletions_for_flush(request.backend, catalog, &request.payload)
        }
        "ListInlineFlushDeletePositions" => {
            list_inline_flush_delete_positions(request.backend, catalog, &request.payload)
        }
        "ListCurrentInlineFlushDeletePositions" => {
            list_current_inline_flush_delete_positions(request.backend, catalog, &request.payload)
        }
        "ListInlineRowInsertions" => {
            list_inline_row_insertions(request.backend, catalog, &request.payload)
        }
        "ListInlineRowDeletions" => {
            list_inline_row_deletions(request.backend, catalog, &request.payload)
        }
        "ListDataFileChanges" => list_data_file_changes(request.backend, catalog, &request.payload),
        "ListTableDeletions" => list_table_deletions(request.backend, catalog, &request.payload),
        "ListOldFilesForCleanup" => {
            list_old_files_for_cleanup(request.backend, catalog, &request.payload)
        }
        "ListKnownFilesForCleanup" => list_known_files_for_cleanup(request.backend, catalog),
        "ExpireSnapshots" => {
            expire_snapshots_for_runtime(request.backend, catalog, &request.payload)
        }
        "RemoveCleanupFiles" => remove_cleanup_files(request.backend, catalog, &request.payload),
        "MergeAdjacentFiles" => merge_adjacent_files(request.backend, catalog, &request.payload),
        "RewriteDeleteFiles" => rewrite_delete_files(request.backend, catalog, &request.payload),
        "GetNextColumnId" => get_next_column_id(request.backend, catalog, &request.payload),
        "IsColumnCreatedWithTable" => {
            is_column_created_with_table(request.backend, catalog, &request.payload)
        }
        _ => Ok(format!(
            "runtime_ffi=ok\noperation={}\nbackend={}\npayload_bytes={}\n",
            request.operation,
            request.backend.as_str(),
            request.payload.len()
        )
        .into_bytes()),
    }
}
