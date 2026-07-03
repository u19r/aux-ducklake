//! Deterministic model helpers for DuckLake FoundationDB simulation workloads.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSmokeScenario {
    pub catalog_id: u64,
    pub table_id: u64,
    pub first_attempt_id: u128,
    pub first_file_id: u64,
    pub first_path: String,
    pub retry_file_id: u64,
    pub retry_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogExpireScenario {
    pub catalog_id: u64,
    pub table_id: u64,
    pub attempt_id: u128,
    pub data_file_id: u64,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogCleanupScenario {
    pub catalog_id: u64,
    pub table_id: u64,
    pub attempt_id: u128,
    pub data_file_id: u64,
    pub path: String,
    pub partition_key_index: u32,
    pub partition_value: String,
    pub column_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogReadAgeScenario {
    pub catalog_id: u64,
    pub table_id: u64,
    pub attempt_id: u128,
    pub first_data_file_id: u64,
    pub file_count: usize,
    pub scan_chunk_size: usize,
    pub path_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRecoveryScenario {
    pub catalog_id: u64,
    pub table_id: u64,
    pub attempt_id: u128,
    pub first_file_id: u64,
    pub first_path: String,
    pub retry_file_id: u64,
    pub retry_path: String,
}

impl CatalogRecoveryScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        Self {
            catalog_id: 1,
            table_id: client + 40_001,
            attempt_id: u128::from(client + 40_001),
            first_file_id: client + 40_001,
            first_path: format!("sim-{profile}-client-{client}-unknown-outcome-first.parquet"),
            retry_file_id: client + 1_040_001,
            retry_path: format!("sim-{profile}-client-{client}-unknown-outcome-retry.parquet"),
        }
    }
}

impl CatalogReadAgeScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        Self {
            catalog_id: 1,
            table_id: client + 30_001,
            attempt_id: u128::from(client + 30_001),
            first_data_file_id: client.saturating_mul(1_000) + 30_001,
            file_count: 12,
            scan_chunk_size: 3,
            path_prefix: format!("sim-{profile}-client-{client}-read-age"),
        }
    }

    pub fn minimum_scan_transactions(&self) -> usize {
        self.file_count.div_ceil(self.scan_chunk_size)
    }
}

pub fn require_bounded_scan_transactions(actual: usize, minimum: usize) -> Result<(), String> {
    if actual >= minimum {
        return Ok(());
    }
    Err(format!(
        "expected at least {minimum} bounded scan transaction(s), got {actual}"
    ))
}

impl CatalogCleanupScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        Self {
            catalog_id: 1,
            table_id: client + 20_001,
            attempt_id: u128::from(client + 20_001),
            data_file_id: client + 20_001,
            path: format!("sim-{profile}-client-{client}-cleanup.parquet"),
            partition_key_index: 0,
            partition_value: format!("partition-{client}"),
            column_id: 1,
        }
    }
}

impl CatalogExpireScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        Self {
            catalog_id: 1,
            table_id: client + 10_001,
            attempt_id: u128::from(client + 10_001),
            data_file_id: client + 10_001,
            path: format!("sim-{profile}-client-{client}-expire.parquet"),
        }
    }
}

impl CatalogSmokeScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        Self {
            catalog_id: 1,
            table_id: client + 1,
            first_attempt_id: u128::from(client + 1),
            first_file_id: client + 1,
            first_path: format!("sim-{profile}-client-{client}-first.parquet"),
            retry_file_id: client + 1_000_001,
            retry_path: format!("sim-{profile}-client-{client}-retry.parquet"),
        }
    }

    pub fn expected_current_file_count(&self) -> usize {
        1
    }
}

pub fn require_exactly_one_committed_file<T>(files: &[T]) -> Result<&T, String> {
    files
        .first()
        .filter(|_| files.len() == 1)
        .ok_or_else(|| format!("expected exactly one committed file, got {}", files.len()))
}

pub fn require_no_retry_publication<T>(files: &[T]) -> Result<(), String> {
    if files.is_empty() {
        return Ok(());
    }
    Err(format!(
        "idempotent retry published {} duplicate file(s)",
        files.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_is_deterministic_per_profile_and_client() {
        let first = CatalogSmokeScenario::for_client("smoke", 3);
        let second = CatalogSmokeScenario::for_client("smoke", 3);

        assert_eq!(first, second);
        assert_eq!(first.catalog_id, 1);
        assert_eq!(first.table_id, 4);
        assert!(first.first_path.contains("client-3"));
    }

    #[test]
    fn retry_publication_must_be_empty() {
        assert!(require_no_retry_publication::<u8>(&[]).is_ok());
        assert!(require_no_retry_publication(&[1]).is_err());
    }

    #[test]
    fn expire_scenario_uses_distinct_table_and_attempt_space() {
        let smoke = CatalogSmokeScenario::for_client("smoke", 0);
        let expire = CatalogExpireScenario::for_client("smoke", 0);

        assert_ne!(smoke.table_id, expire.table_id);
        assert_ne!(smoke.first_attempt_id, expire.attempt_id);
        assert!(expire.path.contains("expire"));
    }

    #[test]
    fn cleanup_scenario_uses_distinct_table_and_attempt_space() {
        let expire = CatalogExpireScenario::for_client("smoke", 0);
        let cleanup = CatalogCleanupScenario::for_client("smoke", 0);

        assert_ne!(expire.table_id, cleanup.table_id);
        assert_ne!(expire.attempt_id, cleanup.attempt_id);
        assert!(cleanup.path.contains("cleanup"));
    }

    #[test]
    fn read_age_scenario_forces_multiple_scan_transactions() {
        let scenario = CatalogReadAgeScenario::for_client("smoke", 0);

        assert!(scenario.file_count > scenario.scan_chunk_size);
        assert_eq!(scenario.minimum_scan_transactions(), 4);
    }

    #[test]
    fn bounded_scan_requires_minimum_transaction_count() {
        assert!(require_bounded_scan_transactions(4, 4).is_ok());
        assert!(require_bounded_scan_transactions(3, 4).is_err());
    }

    #[test]
    fn recovery_scenario_uses_distinct_table_and_attempt_space() {
        let read_age = CatalogReadAgeScenario::for_client("smoke", 0);
        let recovery = CatalogRecoveryScenario::for_client("smoke", 0);

        assert_ne!(read_age.table_id, recovery.table_id);
        assert_ne!(read_age.attempt_id, recovery.attempt_id);
        assert!(recovery.first_path.contains("unknown-outcome-first"));
        assert!(recovery.retry_path.contains("unknown-outcome-retry"));
    }
}
