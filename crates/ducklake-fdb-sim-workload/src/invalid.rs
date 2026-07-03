use foundationdb_simulation::{
    Metrics, RustWorkload, Severity, SimDatabase, WorkloadContext, details,
};

use crate::metrics::metric;

pub(crate) struct InvalidWorkload {
    name: String,
    context: WorkloadContext,
    error_count: u64,
}

impl InvalidWorkload {
    pub(crate) fn new(name: String, context: WorkloadContext) -> Self {
        Self {
            name,
            context,
            error_count: 0,
        }
    }
}

impl RustWorkload for InvalidWorkload {
    async fn setup(&mut self, _db: SimDatabase) {}

    async fn start(&mut self, _db: SimDatabase) {
        self.error_count += 1;
        self.context.trace(
            Severity::Error,
            "DuckLakeFdbSimInvalidWorkload",
            details!["Workload" => &self.name],
        );
    }

    async fn check(&mut self, _db: SimDatabase) {}

    fn get_metrics(&self, mut out: Metrics) {
        out.extend([metric(
            "ducklake_invalid_workload_error_count",
            self.error_count,
        )]);
    }

    fn get_check_timeout(&self) -> f64 {
        1.0
    }
}
