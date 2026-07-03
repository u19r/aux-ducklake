#![cfg_attr(not(feature = "foundationdb"), allow(dead_code))]

pub mod append_commits;
mod bounded_cache;
mod change_keys;
pub mod column_mappings;
pub mod compaction_store;
pub mod conflict;
mod conflict_watermarks;
mod data_file_changes;
pub mod data_file_store;
mod data_mutation_intents;
pub mod data_mutation_store;
pub mod debug_export;
pub mod delete_change_feed;
pub mod error;
#[cfg(feature = "foundationdb")]
pub mod fdb;
#[cfg(feature = "foundationdb")]
mod fdb_append_commits;
#[cfg(feature = "foundationdb")]
mod fdb_async;
#[cfg(feature = "foundationdb")]
mod fdb_async_cleanup;
#[cfg(feature = "foundationdb")]
mod fdb_async_read;
#[cfg(feature = "foundationdb")]
mod fdb_cleanup;
#[cfg(feature = "foundationdb")]
mod fdb_compaction;
#[cfg(feature = "foundationdb")]
mod fdb_data_mutation_staging;
#[cfg(feature = "foundationdb")]
mod fdb_data_mutations;
#[cfg(feature = "foundationdb")]
mod fdb_inline_flushes;
#[cfg(feature = "foundationdb")]
mod fdb_inline_tables;
#[cfg(feature = "foundationdb")]
mod fdb_kv;
#[cfg(feature = "foundationdb")]
mod fdb_macros;
#[cfg(feature = "foundationdb")]
mod fdb_runtime;
#[cfg(feature = "foundationdb")]
mod fdb_schemas;
#[cfg(feature = "foundationdb")]
mod fdb_table_drop;
#[cfg(feature = "foundationdb")]
mod fdb_tables;
#[cfg(feature = "foundationdb")]
mod fdb_versionstamp;
#[cfg(feature = "foundationdb")]
mod fdb_views;
pub mod file_listing;
pub mod file_partitions;
pub mod file_stats;
mod file_visibility;
pub mod ids;
mod immutable_file_metadata;
pub mod inline_change_feed;
pub mod inline_column_types;
pub mod inline_data;
pub mod inline_read_schema;
mod key_debug;
pub mod keys;
pub mod kv;
mod lru_cache;
mod macro_rows;
mod macro_store;
pub mod maintenance;
pub mod metadata_settings;
mod object_keys;
pub mod orphan_cleanup;
pub mod rows;
mod runtime_catalog_snapshot;
mod runtime_change_feed;
mod runtime_change_feed_ops;
mod runtime_cleanup;
mod runtime_cleanup_ops;
mod runtime_commit_attempt_ops;
mod runtime_compaction_ops;
mod runtime_data_mutation_ops;
pub mod runtime_ffi;
mod runtime_file_listing;
mod runtime_file_ops;
mod runtime_foundationdb;
mod runtime_foundationdb_inline;
mod runtime_foundationdb_objects;
mod runtime_inline_ops;
mod runtime_inline_rows;
mod runtime_maintenance_ops;
mod runtime_metadata_ops;
mod runtime_metrics;
mod runtime_object_ops;
mod runtime_operations;
mod runtime_payload;
pub mod runtime_protocol;
mod runtime_read_context;
mod runtime_schema_change_ops;
mod runtime_schema_change_payload;
mod runtime_schema_ops;
mod runtime_snapshot_maintenance_ops;
mod runtime_snapshot_ops;
mod runtime_snapshot_range;
mod runtime_snapshots;
mod runtime_table_ops;
mod runtime_tabular_payload;
mod schema_rows;
mod schema_store;
mod schema_version_state;
mod snapshot_keys;
mod snapshot_operations;
pub mod store;
mod table_column_defaults;
pub mod table_columns;
mod table_comments;
pub mod table_drop;
mod table_partition;
mod table_partition_rows;
pub mod table_rows;
mod table_sort;
mod table_sort_rows;
pub mod table_store;
mod table_version_commit;
mod view_rows;
mod view_store;
pub mod workload;

pub use append_commits::{AppendCommitResult, commit_append_data_file};
pub use column_mappings::{
    ColumnMappingRow, NameMappingColumnRow, list_column_mappings, put_column_mappings,
};
pub use compaction_store::{
    MergeAdjacentCompaction, RewriteDeleteCompaction, commit_merge_adjacent_data_files,
    commit_merge_adjacent_data_files_with_conflict_check, commit_rewrite_delete_data_files,
    commit_rewrite_delete_data_files_with_conflict_check,
};
pub use conflict::{
    CommitAttemptDecision, CommitAttemptRow, DataCommitIntent, list_data_conflicts_since_base,
    list_data_file_changes_since_base, load_commit_attempt, stage_commit_attempt,
};
pub use data_file_changes::list_data_file_changes;
pub use data_file_store::{
    append_data_file, commit_append_data_files, commit_append_data_files_with_inline_flushes,
    commit_register_delete_files, expire_data_file, list_current_data_files,
    list_current_data_files_with_deletes, list_data_files, list_data_files_at,
    list_data_files_with_deletes_at, register_delete_file,
};
pub use data_mutation_store::{
    DataMutationCommit, commit_data_mutation, commit_data_mutation_with_file_partitions,
    commit_data_mutation_with_file_partitions_and_inline_deletes,
    commit_data_mutation_with_file_partitions_inline_deletes_and_dropped_files,
    commit_data_mutation_with_file_partitions_inline_deletes_stats_and_dropped_files,
};
pub use debug_export::{
    CatalogDebugRow, list_catalog_debug_rows, list_inline_deletion_debug_chunks,
    list_inline_table_debug_chunks,
};
pub use delete_change_feed::list_table_deletion_scan_files;
pub use error::{CatalogError, CatalogResult, FoundationDbErrorClass};
#[cfg(feature = "foundationdb")]
pub use fdb::FdbOrderedCatalogKv;
#[cfg(feature = "foundationdb")]
pub use fdb_runtime::shutdown_foundationdb_if_booted;
pub use file_listing::{
    AttachedDataFile, DataFileChange, DataFileChangeKind, DeleteFileChange, DeleteScanFile,
};
pub use file_partitions::{
    FilePartitionValueRow, list_current_data_files_by_partition_value,
    list_current_data_files_by_partition_value_with_deletes,
    list_current_data_files_for_partition_scan_with_deletes,
    list_data_files_for_partition_scan_at_with_deletes, list_file_partition_values,
    register_file_partition_value,
};
pub use file_stats::{
    FileColumnStatsRow, list_file_column_stats, list_file_column_stats_for_table_column,
    register_file_column_stats, register_file_column_stats_batch,
};
pub use ids::{
    CatalogId, CatalogOrderId, CatalogOrderKind, ColumnId, CommitAttemptId, DataFileId,
    DeleteFileId, DuckLakeSnapshotId, MacroId, PartitionKeyIndex, RawSnapshotSequence, SchemaId,
    TableId,
};
pub use inline_change_feed::{
    InlineRowChange, InlineRowChangeKind, InlineRowPayloadChange, list_inline_row_changes,
    list_inline_row_payload_changes,
};
pub(crate) use inline_data::list_inline_file_deletion_rows_for_table_at;
pub use inline_data::{
    INLINE_PAYLOAD_LIMIT_BYTES, InlineDeletionChunkRow, InlineFileDeletionRow, InlineTableChunkRow,
    InlineTableDeleteCommit, InlineTableFlush, InlineTablePayloadCommit, InlineTablePayloadRow,
    commit_delete_inline_table_rows, commit_delete_inline_table_rows_at_snapshot,
    commit_inline_file_deletions, list_inline_file_deletions_at,
    list_inline_file_deletions_between, list_inline_table_payloads_at,
    load_inline_deletion_payload_at, load_inline_table_payload_at,
    register_inline_deletion_payload, register_inline_table_payload,
    register_inline_table_payload_with_table, register_inline_table_payload_with_table_at_snapshot,
    route_inline_table_payload_or_data_file,
};
pub use kv::{
    CatalogCacheNamespace, FakeOrderedCatalogKv, KvBatch, MutableCatalogKv, OrderedCatalogKv,
    RangeDirection, RangeItem, TableVersionReplacement,
};
pub use macro_rows::{MacroImplementationRow, MacroParameterRow, MacroRow};
pub use macro_store::{
    DroppedMacro, commit_create_macro_rows, commit_drop_macros, list_macros_at, load_macro_at,
};
pub use maintenance::{
    DeleteFileCleanupRow, InlineTableCleanupId, InlineTableCleanupRow,
    list_old_data_files_for_cleanup, list_old_delete_files_for_cleanup,
    list_old_inline_table_payloads_for_cleanup, remove_old_data_files, remove_old_delete_files,
    remove_old_inline_table_payloads,
};
pub use metadata_settings::{
    MetadataSettingRow, MetadataSettingScope, list_metadata_settings, set_metadata_setting,
};
pub use orphan_cleanup::{KnownCleanupFileRow, list_known_files_for_orphan_cleanup};
pub use rows::{DataFileRow, DeleteFileRow, SnapshotCommitMetadata, SnapshotRow, ValidityWindow};
pub use runtime_change_feed::{insertion_files, user_visible_data_file_changes};
pub use runtime_snapshots::{
    next_public_snapshot_sequence, public_snapshot_sequence_for_order,
    snapshot_by_ducklake_sequence, snapshot_by_public_sequence, snapshot_changes_made,
    snapshot_schema_version,
};
pub use schema_rows::SchemaRow;
pub use schema_store::{
    commit_create_schema_rows, commit_drop_schema_rows, list_schemas_at, load_schema_at,
};
pub use store::{
    SUPPORTED_DUCKLAKE_COMMIT, SnapshotTimestampBound, expire_snapshots,
    initialize_catalog_if_absent, initialize_empty_catalog, latest_snapshot, list_all_snapshots,
    list_snapshots, list_snapshots_older_than, snapshot_by_raw_sequence, snapshot_by_timestamp,
};
pub use table_column_defaults::{
    ChangedColumnDefault, ColumnDefaultChange, commit_change_table_column_defaults,
    commit_change_table_column_defaults_with_conflict_check,
};
pub use table_columns::{
    ChangedColumnType, ColumnDrop, ColumnRename, ColumnTypeChange, DroppedColumn, RenamedColumn,
    commit_change_table_column_types, commit_change_table_column_types_with_conflict_check,
    commit_drop_table_columns, commit_drop_table_columns_with_conflict_check,
    commit_rename_table_columns, commit_rename_table_columns_with_conflict_check,
    normalize_column_renames_to_current_shape,
};
pub use table_comments::{
    ChangedTableComments, ColumnCommentChange, TableCommentChange, commit_change_table_comments,
    commit_change_table_comments_with_conflict_check,
};
pub use table_drop::{DroppedTable, commit_drop_tables, commit_drop_tables_at};
pub use table_partition::{
    ChangedTablePartition, TablePartitionChange, commit_change_table_partition,
    commit_change_table_partition_with_conflict_check,
};
pub use table_partition_rows::{TablePartitionFieldRow, TablePartitionRow};
pub use table_rows::{InlinedTableRow, TableColumnRow, TableRow};
pub use table_sort::{
    ChangedTableSort, TableSortChange, commit_change_table_sort,
    commit_change_table_sort_with_conflict_check,
};
pub use table_sort_rows::{TableSortFieldRow, TableSortRow};
pub use table_store::{
    RenamedTable, TableRename, commit_append_table_columns,
    commit_append_table_columns_with_conflict_check, commit_create_table, commit_create_table_row,
    commit_rename_tables, commit_rename_tables_with_conflict_check, create_table_version,
    list_tables_at, load_table_at,
};
pub use view_rows::ViewRow;
pub use view_store::{
    ChangedViewComment, DroppedView, RenamedView, ViewCommentChange, ViewRename,
    commit_change_view_comment, commit_create_view_row, commit_drop_views, commit_rename_views,
    list_views_at, load_view_at,
};
