#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn request_round_trips_with_payload() {
        let request = RuntimeRequest::new(
            "req-1",
            RuntimeCatalogBackend::FoundationDb,
            "GetSnapshot",
            b"catalog=1".to_vec(),
        )
        .unwrap()
        .with_catalog_id(CatalogId(42))
        .unwrap();

        assert_eq!(
            RuntimeRequest::decode(&request.encode().unwrap()).unwrap(),
            request
        );
    }

    #[test]
    fn request_decode_defaults_legacy_frames_to_catalog_one() {
        let bytes = b"aux-ducklake-runtime/1\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=0\n\n";

        let request = RuntimeRequest::decode(bytes).unwrap();

        assert_eq!(request.catalog_id, CatalogId(1));
    }

    #[test]
    fn response_round_trips_with_status() {
        let response = RuntimeResponse::ok("req-1", b"snapshot=7".to_vec()).unwrap();

        assert_eq!(
            RuntimeResponse::decode(&response.encode().unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn request_rejects_oversized_payload() {
        let error = RuntimeRequest::new(
            "req-1",
            RuntimeCatalogBackend::FoundationDb,
            "CommitDataMutation",
            vec![b'x'; MAX_RUNTIME_PAYLOAD_BYTES + 1],
        )
        .unwrap_err();

        assert!(error.to_string().contains("runtime request payload"));
    }

    #[test]
    fn decode_rejects_payload_length_mismatch() {
        let bytes = b"aux-ducklake-runtime/1\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=10\n\nabc";

        let error = RuntimeRequest::decode(bytes).unwrap_err();

        assert!(error.to_string().contains("payload length mismatch"));
    }

    #[test]
    fn decode_rejects_unknown_header() {
        let bytes = b"aux-ducklake-runtime/1\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=0\nsurprise=true\n\n";

        let error = RuntimeRequest::decode(bytes).unwrap_err();

        assert!(error.to_string().contains("unknown runtime request header"));
    }

    #[test]
    fn request_rejects_shell_like_operation_tokens() {
        let error = RuntimeRequest::new(
            "req-1",
            RuntimeCatalogBackend::FoundationDb,
            "GetSnapshot;rm",
            Vec::new(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("unsupported characters"));
    }
}
