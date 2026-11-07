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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogScaleMaintenanceScenario {
    pub catalog_id: u64,
    pub first_table_id: u64,
    pub first_data_file_id: u64,
    pub attempt_id: u128,
    pub table_count: usize,
    pub partitions_per_table: usize,
    pub path_prefix: String,
    pub partition_value_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogScaleFile {
    pub table_id: u64,
    pub data_file_id: u64,
    pub path: String,
    pub partition_value: String,
    pub expires: bool,
}

impl CatalogScaleMaintenanceScenario {
    pub fn for_client(profile: &str, client_id: i32) -> Self {
        let client = client_id.max(0) as u64;
        let first_table_id = 50_001 + client.saturating_mul(100);
        let first_data_file_id = 5_000_001 + client.saturating_mul(1_000);
        Self {
            catalog_id: 1,
            first_table_id,
            first_data_file_id,
            attempt_id: u128::from(first_data_file_id),
            table_count: 4,
            partitions_per_table: 4,
            path_prefix: format!("sim-{profile}-client-{client}-scale"),
            partition_value_prefix: format!("tenant-{client}"),
        }
    }

    pub fn files(&self) -> Vec<CatalogScaleFile> {
        (0..self.table_count)
            .flat_map(|table_offset| {
                (0..self.partitions_per_table).map(move |partition_offset| {
                    let ordinal = table_offset * self.partitions_per_table + partition_offset;
                    CatalogScaleFile {
                        table_id: self.first_table_id + table_offset as u64,
                        data_file_id: self.first_data_file_id + ordinal as u64,
                        path: format!(
                            "{}-table-{table_offset}-partition-{partition_offset}.parquet",
                            self.path_prefix
                        ),
                        partition_value: format!(
                            "{}-table-{table_offset}-partition-{partition_offset}",
                            self.partition_value_prefix
                        ),
                        expires: partition_offset == 0,
                    }
                })
            })
            .collect()
    }

    pub fn expected_remaining_files_per_table(&self) -> usize {
        self.partitions_per_table.saturating_sub(1)
    }
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
#[path = "lib_tests.rs"]
mod tests;
