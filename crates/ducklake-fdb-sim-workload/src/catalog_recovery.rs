use ducklake_catalog::{
    CatalogId, CatalogOrderKind, CommitAttemptId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    TableId,
};
use ducklake_fdb_sim_model::{
    CatalogRecoveryScenario, require_exactly_one_committed_file, require_no_retry_publication,
};
use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::common::{
    OPTION_ACTIVE_CLIENT_COUNT, OPTION_PROFILE, option_or_default, option_or_default_string,
};
use crate::metrics::metric;

pub(crate) struct CatalogRecoveryWorkload {
    name: String,
    profile: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    commit_count: u64,
    recovery_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl CatalogRecoveryWorkload {
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
            commit_count: 0,
            recovery_count: 0,
            error_count: 0,
            context,
        }
    }

    fn catalog(&self, db: SimDatabase) -> FdbOrderedCatalogKv {
        FdbOrderedCatalogKv::from_shared_database_with_prefix(db, self.key_prefix())
    }

    fn key_prefix(&self) -> Vec<u8> {
        format!(
            "aux-ducklake/fdb-sim/{}/client-{}/recovery/",
            self.profile, self.client_id
        )
        .into_bytes()
    }

    fn is_active_client(&self) -> bool {
        self.client_id < self.active_client_count
    }

    fn trace_step(&self, step: &'static str) {
        self.context.trace(
            Severity::Info,
            "DuckLakeFdbSimCatalogRecoveryStep",
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
            "DuckLakeFdbSimCatalogRecoveryError",
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

    async fn run_unknown_outcome_recovery(&mut self, db: SimDatabase) -> Result<(), String> {
        let scenario = CatalogRecoveryScenario::for_client(&self.profile, self.client_id);
        let catalog = CatalogId(scenario.catalog_id);
        let table = TableId(scenario.table_id);
        let attempt = CommitAttemptId(scenario.attempt_id);
        let first_kv = self.catalog(db.clone());
        let initial = first_kv
            .initialize_catalog_if_absent_versionstamped_async(catalog)
            .await
            .map_err(|err| format!("initialize catalog: {err}"))?;
        let committed = first_kv
            .commit_data_files_versionstamped_async(
                catalog,
                Some(attempt),
                vec![DataFileRow::new(
                    DataFileId(scenario.first_file_id),
                    table,
                    &scenario.first_path,
                    10,
                    100,
                    initial.order,
                )],
            )
            .await
            .map_err(|err| format!("commit before unknown outcome: {err}"))?;
        let file = require_exactly_one_committed_file(&committed.data_files)?.clone();
        if file.validity.begin_order.kind() != CatalogOrderKind::FdbVersionstamp {
            return Err("unknown-outcome file begin order is not an FDB versionstamp".to_owned());
        }
        self.commit_count += 1;
        drop(first_kv);

        let reopened_kv = self.catalog(db);
        let recovered = reopened_kv
            .commit_data_files_versionstamped_async(
                catalog,
                Some(attempt),
                vec![DataFileRow::new(
                    DataFileId(scenario.retry_file_id),
                    table,
                    &scenario.retry_path,
                    1,
                    10,
                    initial.order,
                )],
            )
            .await
            .map_err(|err| format!("recover unknown outcome: {err}"))?;
        require_no_retry_publication(&recovered.data_files)?;
        self.recovery_count += 1;

        let attempt_row = reopened_kv
            .load_commit_attempt_async(catalog, Some(attempt))
            .await
            .map_err(|err| format!("load recovered commit attempt: {err}"))?
            .ok_or_else(|| {
                "commit attempt row missing after unknown-outcome recovery".to_owned()
            })?;
        if attempt_row.commit_order != file.validity.begin_order {
            return Err("recovered commit attempt order does not match data file order".to_owned());
        }

        let files = reopened_kv
            .list_current_data_files_async(catalog, table)
            .await
            .map_err(|err| format!("list recovered current files: {err}"))?;
        if files != vec![file] {
            return Err(format!(
                "unknown-outcome recovery expected one original file, got {}",
                files.len()
            ));
        }
        Ok(())
    }
}

impl RustWorkload for CatalogRecoveryWorkload {
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
        if let Err(err) = self.run_unknown_outcome_recovery(db).await {
            self.trace_error("start", err);
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_step("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric("ducklake_catalog_recovery_setup_count", self.setup_count),
            metric("ducklake_catalog_recovery_start_count", self.start_count),
            metric("ducklake_catalog_recovery_check_count", self.check_count),
            metric("ducklake_catalog_recovery_commit_count", self.commit_count),
            metric(
                "ducklake_catalog_recovery_recovery_count",
                self.recovery_count,
            ),
            metric("ducklake_catalog_recovery_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
