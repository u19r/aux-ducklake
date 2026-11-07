use std::collections::BTreeMap;

use ducklake_catalog::{
    CatalogId, ColumnId, CommitAttemptId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    FileColumnStatsRow, FilePartitionValueRow, PartitionKeyIndex, TableId,
};
use ducklake_fdb_sim_model::{CatalogScaleFile, CatalogScaleMaintenanceScenario};
use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::common::{
    OPTION_ACTIVE_CLIENT_COUNT, OPTION_PROFILE, option_or_default, option_or_default_string,
};
use crate::metrics::metric;

pub(crate) struct CatalogScaleMaintenanceWorkload {
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
    expire_count: u64,
    cleanup_count: u64,
    verified_table_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl CatalogScaleMaintenanceWorkload {
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
            expire_count: 0,
            cleanup_count: 0,
            verified_table_count: 0,
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
            "DuckLakeFdbSimCatalogScaleMaintenanceStep",
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
            "DuckLakeFdbSimCatalogScaleMaintenanceError",
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

    async fn run_catalog_scale_maintenance(&mut self, db: SimDatabase) -> Result<(), String> {
        let scenario = CatalogScaleMaintenanceScenario::for_client(&self.profile, self.client_id);
        let catalog = CatalogId(scenario.catalog_id);
        let files = scenario.files();
        let kv = self.catalog(db);
        let initial = kv
            .initialize_catalog_if_absent_versionstamped_async(catalog)
            .await
            .map_err(|err| format!("initialize catalog: {err}"))?;
        let committed = kv
            .commit_data_files_versionstamped_async(
                catalog,
                Some(CommitAttemptId(scenario.attempt_id)),
                files
                    .iter()
                    .map(|file| {
                        DataFileRow::new(
                            DataFileId(file.data_file_id),
                            TableId(file.table_id),
                            &file.path,
                            10,
                            100,
                            initial.order,
                        )
                    })
                    .collect(),
            )
            .await
            .map_err(|err| format!("multi-table append: {err}"))?;
        if committed.data_files.len() != files.len() {
            return Err(format!(
                "expected {} committed files, got {}",
                files.len(),
                committed.data_files.len()
            ));
        }
        self.append_count += committed.data_files.len() as u64;
        self.trace_step("multi_table_append_committed");

        let files_by_id: BTreeMap<_, _> =
            files.iter().map(|file| (file.data_file_id, file)).collect();
        for file in &committed.data_files {
            let scenario_file = files_by_id
                .get(&file.data_file_id.0)
                .ok_or_else(|| format!("unexpected committed file {}", file.data_file_id.0))?;
            register_metadata(&kv, catalog, scenario_file).await?;
            self.metadata_count += 1;
        }
        self.trace_step("partition_metadata_registered");

        for file in files.iter().filter(|file| file.expires) {
            kv.expire_data_file_versionstamped_async(catalog, DataFileId(file.data_file_id))
                .await
                .map_err(|err| format!("expire file {}: {err}", file.data_file_id))?;
            self.expire_count += 1;
            let removed = kv
                .remove_expired_data_file_metadata_async(catalog, DataFileId(file.data_file_id))
                .await
                .map_err(|err| format!("cleanup file {}: {err}", file.data_file_id))?;
            if !removed {
                return Err(format!("cleanup did not remove file {}", file.data_file_id));
            }
            self.cleanup_count += 1;
            require_metadata_count(&kv, catalog, file, 0).await?;
        }
        self.trace_step("expired_partitions_cleaned");

        for table_offset in 0..scenario.table_count {
            let table = TableId(scenario.first_table_id + table_offset as u64);
            let current = kv
                .list_current_data_files_async(catalog, table)
                .await
                .map_err(|err| format!("list table {}: {err}", table.0))?;
            if current.len() != scenario.expected_remaining_files_per_table() {
                return Err(format!(
                    "table {} expected {} current files, got {}",
                    table.0,
                    scenario.expected_remaining_files_per_table(),
                    current.len()
                ));
            }
            if current.iter().any(|file| file.table_id != table) {
                return Err(format!(
                    "table {} read returned a foreign table row",
                    table.0
                ));
            }
            self.verified_table_count += 1;
        }

        let retained = files
            .iter()
            .find(|file| !file.expires)
            .ok_or_else(|| "scale scenario has no retained file".to_owned())?;
        require_metadata_count(&kv, catalog, retained, 1).await?;
        self.trace_step("unaffected_tables_verified");
        Ok(())
    }
}

async fn register_metadata(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    file: &CatalogScaleFile,
) -> Result<(), String> {
    kv.register_file_cleanup_metadata_async(
        catalog,
        FilePartitionValueRow::new(
            DataFileId(file.data_file_id),
            TableId(file.table_id),
            PartitionKeyIndex(0),
            &file.partition_value,
        ),
        FileColumnStatsRow::new(
            DataFileId(file.data_file_id),
            TableId(file.table_id),
            ColumnId(1),
            0,
            Some("1".to_owned()),
            Some("2".to_owned()),
        ),
    )
    .await
    .map_err(|err| format!("register metadata for file {}: {err}", file.data_file_id))
}

async fn require_metadata_count(
    kv: &FdbOrderedCatalogKv,
    catalog: CatalogId,
    file: &CatalogScaleFile,
    expected: usize,
) -> Result<(), String> {
    let actual = kv
        .file_cleanup_metadata_counts_async(
            catalog,
            DataFileId(file.data_file_id),
            TableId(file.table_id),
            PartitionKeyIndex(0),
            &file.partition_value,
            ColumnId(1),
        )
        .await
        .map_err(|err| format!("count metadata for file {}: {err}", file.data_file_id))?;
    if actual.partition_values == expected
        && actual.partition_lookups == expected
        && actual.column_stats == expected
        && actual.column_stats_lookups == expected
    {
        return Ok(());
    }
    Err(format!(
        "file {} expected metadata count {expected}, got {actual:?}",
        file.data_file_id
    ))
}

impl RustWorkload for CatalogScaleMaintenanceWorkload {
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
        if let Err(err) = self.run_catalog_scale_maintenance(db).await {
            self.trace_error("start", err);
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_step("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric(
                "ducklake_catalog_scale_maintenance_setup_count",
                self.setup_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_start_count",
                self.start_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_check_count",
                self.check_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_append_count",
                self.append_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_metadata_count",
                self.metadata_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_expire_count",
                self.expire_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_cleanup_count",
                self.cleanup_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_verified_table_count",
                self.verified_table_count,
            ),
            metric(
                "ducklake_catalog_scale_maintenance_error_count",
                self.error_count,
            ),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        120.0
    }
}
