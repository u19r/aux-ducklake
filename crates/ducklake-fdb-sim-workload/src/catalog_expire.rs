use ducklake_catalog::{
    CatalogId, CatalogOrderKind, CommitAttemptId, DataFileId, DataFileRow, FdbOrderedCatalogKv,
    TableId,
};
use ducklake_fdb_sim_model::{CatalogExpireScenario, require_exactly_one_committed_file};
use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::common::{
    OPTION_ACTIVE_CLIENT_COUNT, OPTION_PROFILE, option_or_default, option_or_default_string,
};
use crate::metrics::metric;

pub(crate) struct CatalogExpireWorkload {
    name: String,
    profile: String,
    client_id: i32,
    client_count: i32,
    active_client_count: i32,
    setup_count: u64,
    start_count: u64,
    check_count: u64,
    append_count: u64,
    expire_count: u64,
    error_count: u64,
    context: WorkloadContext,
}

impl CatalogExpireWorkload {
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
            expire_count: 0,
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
            "DuckLakeFdbSimCatalogExpireStep",
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
            "DuckLakeFdbSimCatalogExpireError",
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

    async fn run_catalog_expire(&mut self, db: SimDatabase) -> Result<(), String> {
        let scenario = CatalogExpireScenario::for_client(&self.profile, self.client_id);
        let catalog = CatalogId(scenario.catalog_id);
        let table = TableId(scenario.table_id);
        let attempt = CommitAttemptId(scenario.attempt_id);
        let kv = self.catalog(db);
        let initial = kv
            .initialize_catalog_if_absent_versionstamped_async(catalog)
            .await
            .map_err(|err| format!("initialize catalog: {err}"))?;
        let committed = kv
            .commit_data_files_versionstamped_async(
                catalog,
                Some(attempt),
                vec![DataFileRow::new(
                    DataFileId(scenario.data_file_id),
                    table,
                    &scenario.path,
                    10,
                    100,
                    initial.order,
                )],
            )
            .await
            .map_err(|err| format!("append before expire: {err}"))?;
        let file = require_exactly_one_committed_file(&committed.data_files)?.clone();
        self.append_count += 1;

        let expired = kv
            .expire_data_file_versionstamped_async(catalog, file.data_file_id)
            .await
            .map_err(|err| format!("expire data file: {err}"))?;
        let Some(end_order) = expired.validity.end_order else {
            return Err("expired data file is missing end order".to_owned());
        };
        if end_order.kind() != CatalogOrderKind::FdbVersionstamp {
            return Err("expired data file end order is not an FDB versionstamp".to_owned());
        }
        self.expire_count += 1;

        let current = kv
            .list_current_data_files_async(catalog, table)
            .await
            .map_err(|err| format!("list after expire: {err}"))?;
        if !current.is_empty() {
            return Err(format!(
                "expected no current files after expire, got {}",
                current.len()
            ));
        }
        Ok(())
    }
}

impl RustWorkload for CatalogExpireWorkload {
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
        if let Err(err) = self.run_catalog_expire(db).await {
            self.trace_error("start", err);
        }
    }

    async fn check(&mut self, _db: SimDatabase) {
        self.check_count += 1;
        self.trace_step("check");
    }

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([
            metric("ducklake_catalog_expire_setup_count", self.setup_count),
            metric("ducklake_catalog_expire_start_count", self.start_count),
            metric("ducklake_catalog_expire_check_count", self.check_count),
            metric("ducklake_catalog_expire_append_count", self.append_count),
            metric("ducklake_catalog_expire_expire_count", self.expire_count),
            metric("ducklake_catalog_expire_error_count", self.error_count),
        ]);
    }

    fn get_check_timeout(&self) -> f64 {
        60.0
    }
}
