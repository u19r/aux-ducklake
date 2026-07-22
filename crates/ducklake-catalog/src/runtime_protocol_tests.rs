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
        let bytes = b"aux-ducklake-runtime/2\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=0\n\n";

        let request = RuntimeRequest::decode(bytes).unwrap();

        assert_eq!(request.catalog_id, CatalogId(1));
    }

    #[test]
    fn request_rejects_pre_pagination_protocol_to_prevent_truncated_reads() {
        let bytes = b"aux-ducklake-runtime/1\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=0\n\n";

        let error = RuntimeRequest::decode(bytes).unwrap_err();

        assert!(error.to_string().contains("unsupported runtime protocol version 1"));
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
    fn paged_response_round_trips_and_reassembles_payload_over_transport_limit() {
        let payload = vec![b'x'; MAX_RUNTIME_RESPONSE_BYTES + 700_000];
        let mut offset = 0;
        let mut etag = None;
        let mut reassembled = Vec::new();
        let mut page_count = 0;

        loop {
            let response = paged_runtime_response(
                "req-page".to_owned(),
                payload.clone(),
                offset,
                etag.as_deref(),
            )
            .unwrap();
            let response = RuntimeResponse::decode(&response.encode().unwrap()).unwrap();
            assert!(response.payload.len() <= MAX_RUNTIME_PAGE_PAYLOAD_BYTES);
            reassembled.extend_from_slice(&response.payload);
            page_count += 1;
            let Some(next_offset) = response.next_page_offset else {
                break;
            };
            offset = next_offset;
            etag = response.page_etag;
        }

        assert!(page_count > 1);
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn paged_response_rejects_changed_payload_on_continuation() {
        let original = vec![b'a'; MAX_RUNTIME_PAGE_PAYLOAD_BYTES + 1];
        let first = paged_runtime_response("req-page".to_owned(), original, 0, None).unwrap();
        let mut changed = vec![b'a'; MAX_RUNTIME_PAGE_PAYLOAD_BYTES + 1];
        changed[0] = b'b';

        let error = paged_runtime_response(
            "req-page".to_owned(),
            changed,
            first.next_page_offset.unwrap(),
            first.page_etag.as_deref(),
        )
        .unwrap_err();

        assert!(error.to_string().contains("conflict"));
    }

    #[test]
    fn page_continuation_request_round_trips() {
        let request = RuntimeRequest::new(
            "req-page",
            RuntimeCatalogBackend::FoundationDb,
            "ListDataFilesAt",
            b"snapshot_id=7\ntable_id=3\n".to_vec(),
        )
        .unwrap()
        .with_page(512, "a".repeat(64))
        .unwrap();

        assert_eq!(
            RuntimeRequest::decode(&request.encode().unwrap()).unwrap(),
            request
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
        let bytes = b"aux-ducklake-runtime/2\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=10\n\nabc";

        let error = RuntimeRequest::decode(bytes).unwrap_err();

        assert!(error.to_string().contains("payload length mismatch"));
    }

    #[test]
    fn decode_rejects_unknown_header() {
        let bytes = b"aux-ducklake-runtime/2\nrequest_id=req-1\nbackend=fdb\noperation=GetSnapshot\npayload_len=0\nsurprise=true\n\n";

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
