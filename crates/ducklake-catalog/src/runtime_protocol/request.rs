use super::*;

pub(crate) struct RuntimeRequestRef<'a> {
    pub request_id: &'a str,
    pub backend: RuntimeCatalogBackend,
    pub catalog_id: CatalogId,
    pub operation: &'a str,
    pub payload: &'a [u8],
    pub read_context_id: Option<u64>,
    pub snapshot_fence: Option<crate::SnapshotFence>,
    pub page_offset: usize,
    pub page_etag: Option<&'a str>,
}

impl<'a> RuntimeRequestRef<'a> {
    pub(crate) fn decode(bytes: &'a [u8]) -> CatalogResult<Self> {
        validate_frame_size(bytes.len(), MAX_RUNTIME_REQUEST_BYTES, "runtime request")?;
        let (header, payload) = split_frame(bytes)?;
        let headers = RequestHeaders::parse(header)?;
        require_payload_len(headers.payload_len, payload.len())?;
        let mut request = Self {
            request_id: required_header(headers.request_id, "request_id")?,
            backend: required_header(headers.backend, "backend")?,
            catalog_id: headers.catalog_id.unwrap_or(CatalogId(1)),
            operation: required_header(headers.operation, "operation")?,
            payload,
            read_context_id: headers.read_context_id,
            snapshot_fence: headers.snapshot_fence,
            page_offset: 0,
            page_etag: None,
        };
        request.validate()?;
        match (headers.page_offset, headers.page_etag) {
            (None, None) => {}
            (Some(offset), Some(etag)) => {
                request.page_offset = offset;
                request.page_etag = Some(etag);
                request.validate()?;
            }
            _ => {
                return Err(CatalogError::Decode(
                    "runtime page continuation requires page_offset and page_etag".to_owned(),
                ));
            }
        }
        Ok(request)
    }

    pub(super) fn validate(&self) -> CatalogResult<()> {
        validate_token(
            &self.request_id,
            MAX_RUNTIME_REQUEST_ID_BYTES,
            "runtime request_id",
        )?;
        validate_token(
            &self.operation,
            MAX_RUNTIME_OPERATION_BYTES,
            "runtime operation",
        )?;
        validate_frame_size(
            self.payload.len(),
            MAX_RUNTIME_PAYLOAD_BYTES,
            "runtime request payload",
        )?;
        match (&self.page_etag, self.page_offset) {
            (None, 0) => Ok(()),
            (Some(etag), offset) if offset > 0 => validate_hex_token(etag, "runtime page_etag"),
            _ => Err(CatalogError::Decode(
                "runtime page continuation requires a positive offset and page_etag".to_owned(),
            )),
        }
    }
}

impl<'a> From<&'a RuntimeRequest> for RuntimeRequestRef<'a> {
    fn from(request: &'a RuntimeRequest) -> Self {
        Self {
            request_id: &request.request_id,
            backend: request.backend,
            catalog_id: request.catalog_id,
            operation: &request.operation,
            payload: &request.payload,
            read_context_id: request.read_context_id,
            snapshot_fence: request.snapshot_fence,
            page_offset: request.page_offset,
            page_etag: request.page_etag.as_deref(),
        }
    }
}

impl From<RuntimeRequestRef<'_>> for RuntimeRequest {
    fn from(request: RuntimeRequestRef<'_>) -> Self {
        Self {
            request_id: request.request_id.to_owned(),
            backend: request.backend,
            catalog_id: request.catalog_id,
            operation: request.operation.to_owned(),
            payload: request.payload.to_vec(),
            read_context_id: request.read_context_id,
            snapshot_fence: request.snapshot_fence,
            page_offset: request.page_offset,
            page_etag: request.page_etag.map(str::to_owned),
        }
    }
}

#[derive(Default)]
struct RequestHeaders<'a> {
    request_id: Option<&'a str>,
    backend: Option<RuntimeCatalogBackend>,
    catalog_id: Option<CatalogId>,
    operation: Option<&'a str>,
    payload_len: Option<usize>,
    page_offset: Option<usize>,
    page_etag: Option<&'a str>,
    read_context_id: Option<u64>,
    snapshot_fence: Option<crate::SnapshotFence>,
}

impl<'a> RequestHeaders<'a> {
    fn parse(header: &'a str) -> CatalogResult<Self> {
        let mut lines = header.lines();
        let version = lines.next().map(parse_magic).transpose()?;
        let mut headers = Self::default();
        for line in lines {
            let (key, value) = parse_header_line(line)?;
            headers.apply(key, value)?;
        }
        require_version(version)?;
        Ok(headers)
    }

    fn apply(&mut self, key: &str, value: &'a str) -> CatalogResult<()> {
        match key {
            "request_id" => self.request_id = Some(value),
            "backend" => self.backend = Some(RuntimeCatalogBackend::parse(value)?),
            "catalog_id" => self.catalog_id = Some(CatalogId(parse_u64(value, "catalog_id")?)),
            "operation" => self.operation = Some(value),
            "payload_len" => self.payload_len = Some(parse_usize(value, "payload_len")?),
            _ => self.apply_continuation(key, value)?,
        }
        Ok(())
    }

    fn apply_continuation(&mut self, key: &str, value: &'a str) -> CatalogResult<()> {
        match key {
            "page_offset" => self.page_offset = Some(parse_usize(value, "page_offset")?),
            "page_etag" => self.page_etag = Some(value),
            "read_context_id" => self.read_context_id = Some(parse_u64(value, "read_context_id")?),
            "snapshot_fence" => {
                if self
                    .snapshot_fence
                    .replace(crate::SnapshotFence::try_from(value)?)
                    .is_some()
                {
                    return Err(CatalogError::Decode("duplicate snapshot fence".to_owned()));
                }
            }
            _ => {
                return Err(CatalogError::Decode(format!(
                    "unknown runtime request header {key}"
                )));
            }
        }
        Ok(())
    }
}
