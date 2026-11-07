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

#[test]
fn scale_scenario_spreads_files_across_tables_and_partitions() {
    let scenario = CatalogScaleMaintenanceScenario::for_client("smoke", 2);
    let files = scenario.files();

    assert_eq!(
        files.len(),
        scenario.table_count * scenario.partitions_per_table
    );
    assert_eq!(
        files.iter().filter(|file| file.expires).count(),
        scenario.table_count
    );
    assert_eq!(
        scenario.expected_remaining_files_per_table(),
        scenario.partitions_per_table - 1
    );
    assert_eq!(files[0].table_id, scenario.first_table_id);
    assert_eq!(
        files.last().expect("last scale file").table_id,
        scenario.first_table_id + scenario.table_count as u64 - 1
    );
}
