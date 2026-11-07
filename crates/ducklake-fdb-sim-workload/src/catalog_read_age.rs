use ducklake_catalog::{
    CatalogId, CommitAttemptId, DataFileId, DataFileRow, FdbOrderedCatalogKv, TableId,
};
use ducklake_fdb_sim_model::{CatalogReadAgeScenario, require_bounded_scan_transactions};
use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::common::{
    OPTION_ACTIVE_CLIENT_COUNT, OPTION_PROFILE, option_or_default, option_or_default_string,
};
use crate::metrics::metric;

pub(crate) struct CatalogReadAgeWorkload {
    name: String,
    profile: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    append_count: u64,
    bounded_read_count: u64,
    scan_transaction_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl CatalogReadAgeWorkload {
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
            bounded_read_count: 0,
            scan_transaction_count: 0,
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
            "DuckLakeFdbSimCatalogReadAgeStep",
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
            "DuckLakeFdbSimCatalogReadAgeError",
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

    async fn run_catalog_read_age(&mut self, db: SimDatabase) -> Result<(), String> {
        let scenario = CatalogReadAgeScenario::for_client(&self.profile, self.client_id);
        let catalog = CatalogId(scenario.catalog_id);
        let table = TableId(scenario.table_id);
        let kv = self.catalog(db);

        let initial = kv
            .initialize_catalog_if_absent_versionstamped_async(catalog)
            .await
            .map_err(|err| format!("initialize catalog: {err}"))?;
        let mut rows = Vec::new();
        for offset in 0..scenario.file_count {
            let file_id = scenario.first_data_file_id + offset as u64;
            rows.push(DataFileRow::new(
                DataFileId(file_id),
                table,
                format!("{}-{offset}.parquet", scenario.path_prefix),
                10,
                100,
                initial.order,
            ));
        }
        kv.commit_data_files_versionstamped_async(
            catalog,
            Some(CommitAttemptId(scenario.attempt_id)),
            rows,
        )
        .await
        .map_err(|err| format!("append read-age fixture: {err}"))?;
        self.append_count += scenario.file_count as u64;

        let bounded = kv
            .count_data_files_bounded_async(catalog, table, scenario.scan_chunk_size)
            .await
            .map_err(|err| format!("bounded data-file count: {err}"))?;
        if bounded.count != scenario.file_count {
            return Err(format!(
                "expected {} current file(s), got {}",
                scenario.file_count, bounded.count
            ));
        }
        require_bounded_scan_transactions(
            bounded.scan_transaction_count,
            scenario.minimum_scan_transactions(),
        )?;
        self.bounded_read_count += 1;
        self.scan_transaction_count += bounded.scan_transaction_count as u64;

        let files = kv
            .list_current_data_files_bounded_async(catalog, table, scenario.scan_chunk_size)
            .await
            .map_err(|err| format!("bounded data-file read: {err}"))?;
        if files.data_files.len() != scenario.file_count {
            return Err(format!(
                "expected {} bounded file row(s), got {}",
                scenario.file_count,
                files.data_files.len()
            ));
        }
        require_bounded_scan_transactions(
            files.scan_transaction_count,
            scenario.minimum_scan_transactions(),
        )?;
        self.bounded_read_count += 1;
        self.scan_transaction_count += files.scan_transaction_count as u64;
        Ok(())
    }
}

impl RustWorkload for CatalogReadAgeWorkload {
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
        if let Err(err) = self.run_catalog_read_age(db).await {
            self.trace_error("start", err);
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_step("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric("ducklake_catalog_read_age_setup_count", self.setup_count),
            metric("ducklake_catalog_read_age_start_count", self.start_count),
            metric("ducklake_catalog_read_age_check_count", self.check_count),
            metric("ducklake_catalog_read_age_append_count", self.append_count),
            metric(
                "ducklake_catalog_read_age_bounded_read_count",
                self.bounded_read_count,
            ),
            metric(
                "ducklake_catalog_read_age_scan_transaction_count",
                self.scan_transaction_count,
            ),
            metric("ducklake_catalog_read_age_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
