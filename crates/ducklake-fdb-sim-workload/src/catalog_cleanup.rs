use ducklake_catalog::{
    CatalogId, ColumnId, CommitAttemptId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, PartitionKeyIndex, TableId,
};
use ducklake_fdb_sim_model::{CatalogCleanupScenario, require_exactly_one_committed_file};
use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::common::{
    OPTION_ACTIVE_CLIENT_COUNT, OPTION_PROFILE, option_or_default, option_or_default_string,
};
use crate::metrics::metric;

pub(crate) struct CatalogCleanupWorkload {
    name: String,
    profile: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    append_count: u64,
    metadata_count: u64,
    reuse_count: u64,
    cleanup_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl CatalogCleanupWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        let profile = option_or_default_string(&context, OPTION_PROFILE, "smoke");
        let client_id = context.client_id();
        let client_count = context.client_count();
        let active_client_count =
            option_or_default(&context, OPTION_ACTIVE_CLIENT_COUNT, 1_i32).clamp(1, client_count);
        Self {
            name,
            profile,
            client_id,
            client_count,
            active_client_count,
            setup_count: 0,
            start_count: 0,
            check_count: 0,
            append_count: 0,
            metadata_count: 0,
            reuse_count: 0,
            cleanup_count: 0,
            error_count: 0,
            context,
        }
    }

    fn catalog(&self, db: SimDatabase) -> FdbOrderedCatalogKv {
        FdbOrderedCatalogKv::from_shared_database_with_prefix(db, self.key_prefix())
    }

    fn key_prefix(&self) -> Vec<u8> {
        format!(
            "aux-ducklake/fdb-sim/{}/client-{}/{}",
            self.profile, self.client_id, self.name
        )
        .into_bytes()
    }

    fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    fn trace_step(&self, step: &'static str) {
        self.context.trace(
            Severity::Info,
            "DuckLakeFdbSimCatalogCleanupStep",
            details![
                "Layer" => "aux-ducklake",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Step" => step,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
            ],
        );
    }

    fn trace_error(&mut self, step: &'static str, error: impl Into<String>) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "DuckLakeFdbSimCatalogCleanupError",
            details![
                "Layer" => "aux-ducklake",
                "Workload" => &self.name,
                "Profile" => &self.profile,
                "Step" => step,
                "ClientId" => self.client_id,
                "ClientCount" => self.client_count,
                "ActiveClientCount" => self.active_client_count,
                "Error" => error.into(),
            ],
        );
    }

    async fn run_catalog_cleanup(&mut self, db: SimDatabase) -> Result<(), String> {
        let scenario = CatalogCleanupScenario::for_client(&self.profile, self.client_id);
        let catalog = CatalogId(scenario.catalog_id);
        let table = TableId(scenario.table_id);
        let data_file = DataFileId(scenario.data_file_id);
        let partition_key = PartitionKeyIndex(scenario.partition_key_index);
        let column = ColumnId(scenario.column_id);
        let kv = self.catalog(db);
        let metadata = CleanupMetadataCoordinates {
            catalog,
            table,
            data_file,
            partition_key,
            partition_value: scenario.partition_value.clone(),
            column,
        };

        let initial = kv
            .initialize_catalog_if_absent_versionstamped_async(catalog)
            .await
            .map_err(|err| format!("initialize catalog: {err}"))?;
        self.trace_step("catalog_initialized");
        let committed = kv
            .commit_data_files_versionstamped_async(
                catalog,
                Some(CommitAttemptId(scenario.attempt_id)),
                vec![DataFileRow::new(
                    data_file,
                    table,
                    &scenario.path,
                    10,
                    100,
                    initial.order,
                )],
            )
            .await
            .map_err(|err| format!("append before cleanup: {err}"))?;
        let file = require_exactly_one_committed_file(&committed.data_files)?.clone();
        self.append_count += 1;
        self.trace_step("file_appended");

        register_cleanup_metadata(&kv, &metadata, "1").await?;
        self.metadata_count += 1;
        self.trace_step("metadata_registered");
        require_cleanup_metadata_count(&kv, &metadata, 1, "before cleanup").await?;
        self.trace_step("metadata_verified");
        register_cleanup_metadata(&kv, &metadata, "2").await?;
        self.trace_step("metadata_reused");
        require_cleanup_metadata_count(&kv, &metadata, 1, "after reuse").await?;
        self.reuse_count += 1;
        self.trace_step("reuse_verified");

        kv.expire_data_file_versionstamped_async(catalog, file.data_file_id)
            .await
            .map_err(|err| format!("expire before cleanup: {err}"))?;
        self.trace_step("file_expired");
        let removed = kv
            .remove_expired_data_file_metadata_async(catalog, file.data_file_id)
            .await
            .map_err(|err| format!("remove expired metadata: {err}"))?;
        if !removed {
            return Err("cleanup did not remove the expired data file".to_owned());
        }
        self.cleanup_count += 1;
        self.trace_step("file_removed");
        require_cleanup_metadata_count(&kv, &metadata, 0, "after cleanup").await?;
        self.trace_step("cleanup_verified");
        Ok(())
    }
}

struct CleanupMetadataCoordinates {
    catalog: CatalogId,
    table: TableId,
    data_file: DataFileId,
    partition_key: PartitionKeyIndex,
    partition_value: String,
    column: ColumnId,
}

async fn register_cleanup_metadata(
    kv: &FdbOrderedCatalogKv,
    metadata: &CleanupMetadataCoordinates,
    min_value: &str,
) -> Result<(), String> {
    kv.register_file_cleanup_metadata_async(
        metadata.catalog,
        FilePartitionValueRow::new(
            metadata.data_file,
            metadata.table,
            metadata.partition_key,
            &metadata.partition_value,
        ),
        FileColumnStatsRow::new(
            metadata.data_file,
            metadata.table,
            metadata.column,
            0,
            Some(min_value.to_owned()),
            None,
        ),
    )
    .await
    .map_err(|err| format!("register cleanup metadata: {err}"))
}

async fn require_cleanup_metadata_count(
    kv: &FdbOrderedCatalogKv,
    metadata: &CleanupMetadataCoordinates,
    expected: usize,
    phase: &str,
) -> Result<(), String> {
    let actual = kv
        .file_cleanup_metadata_counts_async(
            metadata.catalog,
            metadata.data_file,
            metadata.table,
            metadata.partition_key,
            &metadata.partition_value,
            metadata.column,
        )
        .await
        .map_err(|err| format!("count metadata {phase}: {err}"))?;
    if actual.partition_values == expected
        && actual.partition_lookups == expected
        && actual.column_stats == expected
        && actual.column_stats_lookups == expected
    {
        return Ok(());
    }
    Err(format!("unexpected metadata count {phase}: {actual:?}"))
}

impl RustWorkload for CatalogCleanupWorkload {
    async fn setup(&mut self, _db: SimDatabase) {
        self.setup_count += 1;
        self.trace_step("setup");
    }

    async fn start(&mut self, db: SimDatabase) {
        self.start_count += 1;
        self.trace_step("start");
        if !self.is_active_client() {
            return;
        }
        if let Err(err) = self.run_catalog_cleanup(db).await {
            self.trace_error("start", err);
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_step("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric("ducklake_catalog_cleanup_setup_count", self.setup_count),
            metric("ducklake_catalog_cleanup_start_count", self.start_count),
            metric("ducklake_catalog_cleanup_check_count", self.check_count),
            metric("ducklake_catalog_cleanup_append_count", self.append_count),
            metric(
                "ducklake_catalog_cleanup_metadata_count",
                self.metadata_count,
            ),
            metric("ducklake_catalog_cleanup_reuse_count", self.reuse_count),
            metric("ducklake_catalog_cleanup_cleanup_count", self.cleanup_count),
            metric("ducklake_catalog_cleanup_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
