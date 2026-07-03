mod catalog_cleanup;
mod catalog_expire;
mod catalog_read_age;
mod catalog_recovery;
mod catalog_smoke;
mod common;
mod invalid;
mod metrics;

use foundationdb_simulation::{
    RustWorkload, RustWorkloadFactory, WorkloadContext, WrappedWorkload, register_factory,
};

use crate::{
    catalog_cleanup::CatalogCleanupWorkload, catalog_expire::CatalogExpireWorkload,
    catalog_read_age::CatalogReadAgeWorkload, catalog_recovery::CatalogRecoveryWorkload,
    catalog_smoke::CatalogSmokeWorkload, invalid::InvalidWorkload,
};

const WORKLOAD_CATALOG_SMOKE: &str = "catalog_smoke";
const WORKLOAD_CATALOG_EXPIRE: &str = "catalog_expire";
const WORKLOAD_CATALOG_CLEANUP: &str = "catalog_cleanup";
const WORKLOAD_CATALOG_READ_AGE: &str = "catalog_read_age";
const WORKLOAD_CATALOG_RECOVERY: &str = "catalog_recovery";

struct DuckLakeFdbSimFactory;

impl RustWorkloadFactory for DuckLakeFdbSimFactory {
    fn create(name: String, context: WorkloadContext) -> WrappedWorkload {
        match name.as_str() {
            WORKLOAD_CATALOG_SMOKE => CatalogSmokeWorkload::new(name, context).wrap(),
            WORKLOAD_CATALOG_EXPIRE => CatalogExpireWorkload::new(name, context).wrap(),
            WORKLOAD_CATALOG_CLEANUP => CatalogCleanupWorkload::new(name, context).wrap(),
            WORKLOAD_CATALOG_READ_AGE => CatalogReadAgeWorkload::new(name, context).wrap(),
            WORKLOAD_CATALOG_RECOVERY => CatalogRecoveryWorkload::new(name, context).wrap(),
            _ => InvalidWorkload::new(name, context).wrap(),
        }
    }
}

register_factory!(DuckLakeFdbSimFactory);
