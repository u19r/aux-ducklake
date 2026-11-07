use super::validate_prefix_end;

#[test]
fn given_hostile_prefix_when_validating_then_mutation_is_rejected() {
    let cases = [
        (b"".as_slice(), "non-empty"),
        (b"/".as_slice(), "root"),
        (
            b"aux-ducklake/receipt/without-slash".as_slice(),
            "trailing slash",
        ),
        (b"aux-ducklake//receipt/".as_slice(), "empty or traversal"),
        (b"aux-ducklake/../receipt/".as_slice(), "empty or traversal"),
        (&[0xff, b'/'][..], "valid UTF-8"),
        (&[u8::MAX; 10_000][..], "successor overflow"),
    ];

    for (prefix, expected_message) in cases {
        let error = validate_prefix_end(prefix).expect_err("hostile prefix must be rejected");
        assert!(
            error.to_string().contains(expected_message),
            "prefix {prefix:?} returned unexpected error: {error}"
        );
    }
}

#[test]
fn given_valid_trailing_slash_prefix_when_successor_is_built_then_slash_is_incremented() {
    assert_eq!(
        validate_prefix_end(b"aux-ducklake/receipt/").unwrap(),
        b"aux-ducklake/receipt0"
    );
}

#[test]
fn given_prefix_successor_overflow_when_validating_then_mutation_is_rejected() {
    let error = validate_prefix_end(&[u8::MAX; 10_000]).unwrap_err();
    assert!(error.to_string().contains("successor overflow"));
}
