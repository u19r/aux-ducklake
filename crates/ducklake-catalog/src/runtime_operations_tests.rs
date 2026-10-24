use super::*;

#[test]
fn operation_policy_owns_mutability_and_cost_family() {
    for (operation, family) in [
        ("CommitAttempt", "data_mutation"),
        ("ChangePartitionKeys", "schema"),
        ("RegisterInlineRows", "inline"),
        ("ExpireSnapshots", "snapshot_maintenance"),
        ("MergeAdjacentFiles", "compaction"),
    ] {
        let policy = runtime_operation_policy(operation);
        assert!(policy.mutates_catalog, "{operation}");
        assert_eq!(policy.family, family, "{operation}");
    }

    for (operation, family) in [
        ("GetCatalogForSnapshot", "read"),
        ("ListCurrentDataFilesForPartitionScans", "read"),
        ("EndRuntimeReadContext", "read_context"),
        ("ReadInlineRowsForFlush", "inline"),
        ("RenderBoundedAppendMirrorSql", "metadata"),
        ("ListKnownFilesForCleanup", "cleanup"),
    ] {
        let policy = runtime_operation_policy(operation);
        assert!(!policy.mutates_catalog, "{operation}");
        assert_eq!(policy.family, family, "{operation}");
    }
}

#[test]
fn unknown_operation_is_non_mutating_and_visible_as_unknown_cost() {
    let policy = runtime_operation_policy("FutureOperation");

    assert!(!policy.mutates_catalog);
    assert_eq!(policy.family, "unknown");
}

#[test]
fn ending_runtime_read_context_requires_context_identity() {
    let request = RuntimeRequest::new(
        "end-runtime-read-context-without-id",
        crate::runtime_protocol::RuntimeCatalogBackend::FoundationDb,
        "EndRuntimeReadContext",
        Vec::new(),
    )
    .unwrap();

    let error = payload_for_request(&request).unwrap_err();

    assert!(error.to_string().contains("requires a read_context_id"));
}
