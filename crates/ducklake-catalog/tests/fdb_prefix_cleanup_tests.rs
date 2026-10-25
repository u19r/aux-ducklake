#![cfg(feature = "foundationdb")]

use std::{
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use ducklake_catalog::{CatalogError, clear_foundationdb_prefix};
use foundationdb::{Database, options::TransactionOption};
use futures::executor::block_on;

#[test]
fn given_live_adjacent_keys_when_clearing_prefix_then_only_target_keys_are_deleted() {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
        return;
    }

    let cluster_file = std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
    let db = open_database(cluster_file.as_deref());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = format!("aux-ducklake/prefix-cleanup/{run_id}/");
    let target = format!("{root}target/");
    let entries = [
        (target.clone(), b"exact".as_slice()),
        (format!("{target}nested/key"), b"nested".as_slice()),
        (format!("{target}nested/deeper/key"), b"deep".as_slice()),
        (
            target.trim_end_matches('/').to_owned(),
            b"adjacent".as_slice(),
        ),
        (format!("{root}targeted/key"), b"sibling".as_slice()),
        (format!("{root}foreign/key"), b"foreign".as_slice()),
    ];
    put_entries(&db, &entries);

    let result = clear_and_verify_target(&db, &target, &entries);

    clear_foundationdb_prefix(&db, root.as_bytes()).unwrap();
    result.unwrap();
}

fn clear_and_verify_target(
    db: &Database,
    target: &str,
    entries: &[(String, &[u8])],
) -> Result<(), String> {
    clear_foundationdb_prefix(db, target.as_bytes()).map_err(|error| error.to_string())?;
    for (key, _) in &entries[..3] {
        if get_entry(db, key).is_some() {
            return Err(format!("target key survived: {key}"));
        }
    }
    for (key, value) in &entries[3..] {
        if get_entry(db, key) != Some(value.to_vec()) {
            return Err(format!("adjacent or foreign key changed: {key}"));
        }
    }
    Ok(())
}

#[test]
fn given_live_sentinel_when_clearing_hostile_prefixes_then_sentinel_is_preserved() {
    if std::env::var("AUX_DUCKLAKE_FDB_LIVE").as_deref() != Ok("1") {
        eprintln!("skipping live FoundationDB test; set AUX_DUCKLAKE_FDB_LIVE=1 to enable");
        return;
    }

    let cluster_file = std::env::var("AUX_DUCKLAKE_FDB_CLUSTER_FILE").ok();
    let db = open_database(cluster_file.as_deref());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = format!("aux-ducklake/prefix-cleanup-hostile/{run_id}/");
    let sentinel = format!("{root}sentinel/");
    put_entries(&db, &[(sentinel.clone(), b"preserve".as_slice())]);

    let cases = [
        b"".as_slice(),
        b"/".as_slice(),
        b"aux-ducklake/receipt/without-slash".as_slice(),
        b"aux-ducklake//receipt/".as_slice(),
        b"aux-ducklake/../receipt/".as_slice(),
        &[0xff, b'/'][..],
        &[u8::MAX; 10_000][..],
    ];
    for prefix in cases {
        let error = clear_foundationdb_prefix(&db, prefix).expect_err("hostile prefix accepted");
        assert!(matches!(error, CatalogError::InvalidMutation(_)));
    }
    assert_eq!(get_entry(&db, &sentinel), Some(b"preserve".to_vec()));
    clear_foundationdb_prefix(&db, root.as_bytes()).unwrap();
}

fn put_entries(db: &Database, entries: &[(String, &[u8])]) {
    let trx = db.create_trx().unwrap();
    trx.set_option(TransactionOption::Timeout(5_000)).unwrap();
    for (key, value) in entries {
        trx.set(key.as_bytes(), value);
    }
    block_on(trx.commit()).unwrap();
}

fn get_entry(db: &Database, key: &str) -> Option<Vec<u8>> {
    let trx = db.create_trx().unwrap();
    trx.set_option(TransactionOption::Timeout(5_000)).unwrap();
    block_on(trx.get(key.as_bytes(), false))
        .unwrap()
        .map(|value| value.to_vec())
}

static FDB_NETWORK: OnceLock<Mutex<Option<foundationdb::api::NetworkAutoStop>>> = OnceLock::new();

fn open_database(cluster_file: Option<&str>) -> Database {
    FDB_NETWORK.get_or_init(|| {
        Mutex::new(Some(
            foundationdb::boot().expect("boot FoundationDB network"),
        ))
    });
    Database::new(cluster_file).unwrap()
}
