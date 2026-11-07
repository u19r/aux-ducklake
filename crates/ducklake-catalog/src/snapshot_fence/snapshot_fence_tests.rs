use super::SnapshotFence;
use crate::{
    CatalogId,
    runtime_commit_attempt_ops::commit_attempt,
    runtime_operations::payload_for_request,
    runtime_protocol::{RuntimeCatalogBackend, RuntimeRequest},
};

#[test]
fn given_invalid_or_duplicate_fence_when_decoded_then_rejects() {
    for value in [
        "",
        "0",
        "0000000000000000000000000000000z",
        "-0000000000000000000000000000001",
    ] {
        assert!(SnapshotFence::try_from(value).is_err(), "{value}");
    }
    let fence = "00000000000000000001000000000000";
    let mut request = RuntimeRequest::new(
        "fenced",
        RuntimeCatalogBackend::FoundationDb,
        "CheckSnapshotFence",
        Vec::new(),
    )
    .unwrap();
    request.snapshot_fence = Some(SnapshotFence::try_from(fence).unwrap());
    let frame = request.encode().unwrap();
    assert_eq!(RuntimeRequest::decode(&frame).unwrap(), request);
    let text = String::from_utf8(frame).unwrap();
    let duplicate = text.replace(
        &format!("snapshot_fence={fence}\n"),
        &format!("snapshot_fence={fence}\nsnapshot_fence={fence}\n"),
    );
    assert!(RuntimeRequest::decode(duplicate.as_bytes()).is_err());
}

#[test]
fn given_fenced_request_when_nonatomic_mutation_then_rejects_before_catalog_access() {
    for operation in [
        "DropTables",
        "CreateViews",
        "RenameViews",
        "DropViews",
        "ChangeViewComment",
        "CreateMacros",
        "DropMacros",
        "SetConfigOption",
        "ExpireSnapshots",
        "RegisterInlineRows",
        "CommitDataMutation",
        "RemoveCleanupFiles",
    ] {
        let mut request = RuntimeRequest::new(
            "fenced",
            RuntimeCatalogBackend::FoundationDb,
            operation,
            Vec::new(),
        )
        .unwrap();
        request.snapshot_fence =
            Some(SnapshotFence::try_from("00000000000000000001000000000000").unwrap());
        assert!(
            payload_for_request(&(&request).into())
                .unwrap_err()
                .to_string()
                .contains("only atomic data"),
            "{operation}"
        );
    }
    for body in [
        "metadata\tCreateSchemas\nschema\t5\tfixture\tprobe\tmain/probe/\t\n",
        "inline\tRegisterInlineTables\ntable\t1\t0\tducklake_inlined_data_1_0\n",
    ] {
        let payload = format!("commit_snapshot\t1\n{body}");
        let fence = SnapshotFence::try_from("00000000000000000001000000000000").unwrap();
        assert!(
            commit_attempt(
                RuntimeCatalogBackend::FoundationDb,
                CatalogId(1),
                payload.as_bytes(),
                Some(fence)
            )
            .unwrap_err()
            .to_string()
            .contains("only atomic data")
        );
    }
}

#[cfg(feature = "foundationdb")]
#[test]
#[ignore = "requires an explicitly selected, isolated native FoundationDB catalog"]
fn given_native_catalog_when_fenced_effect_retries_or_races_then_publication_is_atomic() {
    use futures::executor::block_on;

    use crate::{
        CatalogId, CommitAttemptId, OrderedCatalogKv, latest_snapshot, load_commit_attempt,
    };
    let _guard = crate::FDB_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog().unwrap();
    let catalog = CatalogId(927);
    assert!(
        latest_snapshot(&kv, catalog).unwrap().is_none(),
        "test requires an empty dedicated catalog"
    );
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    let response = commit_attempt(crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb, catalog,
        b"commit_snapshot\t1\nread_snapshot\t0\nmetadata\tCreateTables\ntable\t1\t0\titems-uuid\titems\tmain/items/\t\ncolumn\t1\t1\tvalue\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\ntable\t2\t0\tcheckpoints-uuid\tcheckpoints\tmain/checkpoints/\t\ncolumn\t2\t1\tposition\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\ntable\t3\t0\treceipts-uuid\treceipts\tmain/receipts/\t\ncolumn\t3\t1\texecution\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\n", None).unwrap();
    let response = String::from_utf8(response).unwrap();
    let fields = response
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<std::collections::BTreeMap<_, _>>();
    for key in [
        "metadata_intent_count",
        "inline_intent_count",
        "compaction_intent_count",
        "data_mutation_payload_bytes",
        "inline_output_bytes",
        "compaction_output_bytes",
        "changed_table_count",
        "created_table_count",
    ] {
        assert!(fields.contains_key(key), "missing {key}: {response:?}");
    }
    assert_eq!(fields["created_table_count"], "3");
    let initial = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let initial_fence = Some(SnapshotFence::try_from(initial.order.to_string().as_str()).unwrap());
    let payload = String::from(
        "commit_snapshot\t2\nread_snapshot\t1\nrecovery_attempt\t00000000000000000000000000000001\ninline\tRegisterInlineRows\ntable\t1\t0\tducklake_inlined_data_1_0\nrow\t0\ts:6974656d\ntable\t2\t0\tducklake_inlined_data_2_0\nrow\t0\ts:31\ntable\t3\t0\tducklake_inlined_data_3_0\nrow\t0\ts:657865637574696f6e\n",
    );
    commit_attempt(
        crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb,
        catalog,
        payload.as_bytes(),
        initial_fence,
    )
    .unwrap();
    let accepted = latest_snapshot(&kv, catalog).unwrap().unwrap();
    assert_ne!(initial.order, accepted.order);
    let recovery = load_commit_attempt(&kv, catalog, CommitAttemptId(1))
        .unwrap()
        .unwrap();
    assert_eq!(recovery.commit_order, accepted.order);
    commit_attempt(
        crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb,
        catalog,
        payload.as_bytes(),
        initial_fence,
    )
    .unwrap();
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        accepted.order
    );
    let stale = payload
        .replace("commit_snapshot\t2", "commit_snapshot\t3")
        .replace("read_snapshot\t1", "read_snapshot\t2")
        .replace(
            "00000000000000000000000000000001",
            "00000000000000000000000000000002",
        )
        .replace("row\t0\t", "row\t1\t");
    let error = commit_attempt(
        crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb,
        catalog,
        stale.as_bytes(),
        initial_fence,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("snapshot fence changed"),
        "{error}"
    );
    assert!(
        load_commit_attempt(&kv, catalog, CommitAttemptId(2))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        accepted.order
    );
    let transaction = kv.create_transaction().unwrap();
    SnapshotFence::try_from(accepted.order.to_string().as_str())
        .unwrap()
        .stage(&kv, &transaction, catalog)
        .unwrap();
    let winner = stale;
    commit_attempt(
        crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb,
        catalog,
        winner.as_bytes(),
        Some(SnapshotFence::try_from(accepted.order.to_string().as_str()).unwrap()),
    )
    .unwrap();
    let marker = kv.namespaced_key(b"rejected-after-validation");
    transaction.set(&marker, b"must not commit");
    let error = block_on(transaction.commit()).unwrap_err();
    assert_eq!(
        error.code(),
        1020,
        "a competing catalog commit must conflict"
    );
    assert!(kv.get(b"rejected-after-validation").unwrap().is_none());
    assert!(
        load_commit_attempt(&kv, catalog, CommitAttemptId(2))
            .unwrap()
            .is_some()
    );
}

#[cfg(feature = "foundationdb")]
thread_local! {
    static BEFORE_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(feature = "foundationdb")]
pub(crate) fn invoke_before_commit() {
    let action = BEFORE_COMMIT.with_borrow_mut(Option::take);
    if let Some(action) = action {
        action();
    }
}

#[cfg(feature = "foundationdb")]
#[test]
#[ignore = "requires an explicitly selected, isolated native FoundationDB catalog"]
fn given_native_file_retries_and_flush_race_when_fenced_then_recovery_or_failure_is_explicit() {
    use crate::{
        CatalogId, CommitAttemptId, TableId, latest_snapshot, list_current_data_files,
        load_commit_attempt,
    };
    let _guard = crate::FDB_ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let kv = crate::runtime_foundationdb::open_foundationdb_catalog().unwrap();
    let catalog = CatalogId(928);
    assert!(latest_snapshot(&kv, catalog).unwrap().is_none());
    kv.initialize_catalog_if_absent_versionstamped(catalog)
        .unwrap();
    commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog,
        b"commit_snapshot\t1\nread_snapshot\t0\nmetadata\tCreateTables\ntable\t1\t0\titems-uuid\titems\tmain/items/\t\ncolumn\t1\t1\tvalue\tVARCHAR\ttrue\t\t\t\tNULL\tliteral\n", None).unwrap();
    let mut file_id = None;
    for sequence in 2..=4u64 {
        let before = latest_snapshot(&kv, catalog).unwrap().unwrap();
        let fence = Some(SnapshotFence::try_from(before.order.to_string().as_str()).unwrap());
        let mut payload = format!(
            "commit_snapshot\t{sequence}\nread_snapshot\t{}\nrecovery_attempt\t{sequence:032x}\ndata_mutation\n",
            sequence - 1
        );
        if sequence < 4 {
            payload += &format!("file\t0\t1\tmain/items/{sequence}.parquet\t1\t128\t0\n");
        }
        if let Some(id) = file_id {
            payload += &format!("dropped_data_file\t{id}\n");
        }
        commit_attempt(
            RuntimeCatalogBackend::FoundationDb,
            catalog,
            payload.as_bytes(),
            fence,
        )
        .unwrap();
        let published = latest_snapshot(&kv, catalog).unwrap().unwrap();
        assert_ne!(published.order, before.order);
        // Exact lost-response retry must recover before validating its own now-dropped
        // target.
        commit_attempt(
            RuntimeCatalogBackend::FoundationDb,
            catalog,
            payload.as_bytes(),
            fence,
        )
        .unwrap();
        assert_eq!(
            latest_snapshot(&kv, catalog).unwrap().unwrap().order,
            published.order
        );
        let files = list_current_data_files(&kv, catalog, TableId(1)).unwrap();
        assert_eq!(files.len(), usize::from(sequence < 4));
        file_id = files.first().map(|file| file.data_file_id.0);
    }
    commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog,
        b"commit_snapshot\t5\nread_snapshot\t4\ninline\tRegisterInlineRows\ntable\t1\t0\tducklake_inlined_data_1_0\nrow\t1\ts:666c757368\n", None).unwrap();
    let before = latest_snapshot(&kv, catalog).unwrap().unwrap();
    let fence = Some(SnapshotFence::try_from(before.order.to_string().as_str()).unwrap());
    BEFORE_COMMIT.with_borrow_mut(|hook| *hook = Some(Box::new(move || {
        commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog,
            b"commit_snapshot\t6\nread_snapshot\t5\nrecovery_attempt\t00000000000000000000000000000099\ninline\tRegisterInlineRows\ntable\t1\t0\tducklake_inlined_data_1_0\nrow\t2\ts:77696e6e6572\n", None).unwrap();
    })));
    let error = commit_attempt(RuntimeCatalogBackend::FoundationDb, catalog,
        b"commit_snapshot\t6\nread_snapshot\t5\nrecovery_attempt\t00000000000000000000000000000006\ndata_mutation\nfile\t0\t1\tmain/items/flushed.parquet\t1\t128\t1\ninline_table\tducklake_inlined_data_1_0\t5\n", fence).unwrap_err();
    assert!(
        error.to_string().contains("snapshot fence changed"),
        "{error}"
    );
    assert!(
        load_commit_attempt(&kv, catalog, CommitAttemptId(6))
            .unwrap()
            .is_none()
    );
    assert!(
        list_current_data_files(&kv, catalog, TableId(1))
            .unwrap()
            .is_empty()
    );
    assert_ne!(
        latest_snapshot(&kv, catalog).unwrap().unwrap().order,
        before.order
    );
    assert!(
        BEFORE_COMMIT.with_borrow(|hook| hook.is_none()),
        "race must execute after staging"
    );
}
