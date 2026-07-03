#[cfg(test)]
mod tests {
    use super::super::{
        old_files_cleanup_request_from_payload, parse_ducklake_utc_timestamp_micros,
    };

    #[test]
    fn given_ducklake_timestamp_filter_when_parsed_then_threshold_micros_are_extracted() {
        let request = old_files_cleanup_request_from_payload(
            b"cleanup_all=false\nfilter=WHERE schedule_start::TIMESTAMPTZ < '1970-01-01T00:00:01.000123+00'\n",
        )
        .unwrap();

        assert!(!request.cleanup_all);
        assert_eq!(request.schedule_before_micros, Some(1_000_123));
    }

    #[test]
    fn given_empty_cleanup_payload_when_parsed_then_all_cleanup_candidates_are_requested() {
        let request = old_files_cleanup_request_from_payload(b"").unwrap();

        assert!(request.cleanup_all);
        assert_eq!(request.schedule_before_micros, None);
    }

    #[test]
    fn given_ducklake_utc_timestamp_when_parsed_then_unix_epoch_micros_are_returned() {
        assert_eq!(
            parse_ducklake_utc_timestamp_micros("1970-01-02T03:04:05.006007+00").unwrap(),
            97_445_006_007
        );
    }
}
