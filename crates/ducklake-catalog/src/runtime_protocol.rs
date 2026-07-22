use crate::{CatalogError, CatalogId, CatalogResult};
use sha2::{Digest, Sha256};

pub const RUNTIME_PROTOCOL_VERSION: u16 = 2;
pub const MAX_RUNTIME_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_RUNTIME_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_RUNTIME_PAGE_PAYLOAD_BYTES: usize = 512 * 1024;
pub const MAX_RUNTIME_PAYLOAD_BYTES: usize = 900 * 1024;
pub const MAX_RUNTIME_OPERATION_BYTES: usize = 64;
pub const MAX_RUNTIME_REQUEST_ID_BYTES: usize = 64;

const MAGIC: &str = "aux-ducklake-runtime";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCatalogBackend {
    FoundationDb,
}

impl RuntimeCatalogBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FoundationDb => "foundationdb",
        }
    }

    fn parse(value: &str) -> CatalogResult<Self> {
        match value {
            "foundationdb" | "fdb" => Ok(Self::FoundationDb),
            _ => Err(CatalogError::Decode(format!(
                "unsupported runtime backend {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRequest {
    pub request_id: String,
    pub backend: RuntimeCatalogBackend,
    pub catalog_id: CatalogId,
    pub operation: String,
    pub payload: Vec<u8>,
    pub page_offset: usize,
    pub page_etag: Option<String>,
}

impl RuntimeRequest {
    pub fn new(
        request_id: impl Into<String>,
        backend: RuntimeCatalogBackend,
        operation: impl Into<String>,
        payload: Vec<u8>,
    ) -> CatalogResult<Self> {
        let request = Self {
            request_id: request_id.into(),
            backend,
            catalog_id: CatalogId(1),
            operation: operation.into(),
            payload,
            page_offset: 0,
            page_etag: None,
        };
        request.validate()?;
        Ok(request)
    }

    pub fn with_catalog_id(mut self, catalog_id: CatalogId) -> CatalogResult<Self> {
        self.catalog_id = catalog_id;
        self.validate()?;
        Ok(self)
    }

    pub fn with_page(mut self, offset: usize, etag: impl Into<String>) -> CatalogResult<Self> {
        self.page_offset = offset;
        self.page_etag = Some(etag.into());
        self.validate()?;
        Ok(self)
    }

    pub fn encode(&self) -> CatalogResult<Vec<u8>> {
        self.validate()?;
        let mut header = format!(
            "{MAGIC}/{RUNTIME_PROTOCOL_VERSION}\nrequest_id={}\nbackend={}\ncatalog_id={}\noperation={}\npayload_len={}\n",
            self.request_id,
            self.backend.as_str(),
            self.catalog_id.0,
            self.operation,
            self.payload.len()
        );
        if let Some(etag) = &self.page_etag {
            header.push_str(&format!(
                "page_offset={}\npage_etag={}\n",
                self.page_offset, etag
            ));
        }
        header.push('\n');
        let mut out = Vec::with_capacity(header.len().saturating_add(self.payload.len()));
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&self.payload);
        validate_frame_size(out.len(), MAX_RUNTIME_REQUEST_BYTES, "runtime request")?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        validate_frame_size(bytes.len(), MAX_RUNTIME_REQUEST_BYTES, "runtime request")?;
        let (header, payload) = split_frame(bytes)?;
        let mut version = None;
        let mut request_id = None;
        let mut backend = None;
        let mut catalog_id = None;
        let mut operation = None;
        let mut payload_len = None;
        let mut page_offset = None;
        let mut page_etag = None;
        for (index, line) in header.lines().enumerate() {
            if index == 0 {
                version = Some(parse_magic(line)?);
                continue;
            }
            let (key, value) = parse_header_line(line)?;
            match key {
                "request_id" => request_id = Some(value.to_owned()),
                "backend" => backend = Some(RuntimeCatalogBackend::parse(value)?),
                "catalog_id" => catalog_id = Some(CatalogId(parse_u64(value, "catalog_id")?)),
                "operation" => operation = Some(value.to_owned()),
                "payload_len" => payload_len = Some(parse_usize(value, "payload_len")?),
                "page_offset" => page_offset = Some(parse_usize(value, "page_offset")?),
                "page_etag" => page_etag = Some(value.to_owned()),
                _ => {
                    return Err(CatalogError::Decode(format!(
                        "unknown runtime request header {key}"
                    )));
                }
            }
        }
        require_version(version)?;
        require_payload_len(payload_len, payload.len())?;
        let mut request = Self::new(
            required_header(request_id, "request_id")?,
            required_header(backend, "backend")?,
            required_header(operation, "operation")?,
            payload.to_vec(),
        )?
        .with_catalog_id(catalog_id.unwrap_or(CatalogId(1)))?;
        match (page_offset, page_etag) {
            (None, None) => Ok(request),
            (Some(offset), Some(etag)) => {
                request.page_offset = offset;
                request.page_etag = Some(etag);
                request.validate()?;
                Ok(request)
            }
            _ => Err(CatalogError::Decode(
                "runtime page continuation requires page_offset and page_etag".to_owned(),
            )),
        }
    }

    fn validate(&self) -> CatalogResult<()> {
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
            (Some(etag), offset) if offset > 0 => {
                validate_hex_token(etag, "runtime page_etag")
            }
            _ => Err(CatalogError::Decode(
                "runtime page continuation requires a positive offset and page_etag".to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeResponse {
    pub request_id: String,
    pub status: RuntimeResponseStatus,
    pub payload: Vec<u8>,
    pub next_page_offset: Option<usize>,
    pub page_etag: Option<String>,
}

impl RuntimeResponse {
    pub fn ok(request_id: impl Into<String>, payload: Vec<u8>) -> CatalogResult<Self> {
        let response = Self {
            request_id: request_id.into(),
            status: RuntimeResponseStatus::Ok,
            payload,
            next_page_offset: None,
            page_etag: None,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn error(request_id: impl Into<String>, payload: Vec<u8>) -> CatalogResult<Self> {
        let response = Self {
            request_id: request_id.into(),
            status: RuntimeResponseStatus::Error,
            payload,
            next_page_offset: None,
            page_etag: None,
        };
        response.validate()?;
        Ok(response)
    }

    pub fn encode(&self) -> CatalogResult<Vec<u8>> {
        self.validate()?;
        let mut header = format!(
            "{MAGIC}/{RUNTIME_PROTOCOL_VERSION}\nrequest_id={}\nstatus={}\npayload_len={}\n",
            self.request_id,
            self.status.as_str(),
            self.payload.len()
        );
        if let (Some(offset), Some(etag)) = (self.next_page_offset, &self.page_etag) {
            header.push_str(&format!(
                "next_page_offset={offset}\npage_etag={etag}\n"
            ));
        }
        header.push('\n');
        let mut out = Vec::with_capacity(header.len().saturating_add(self.payload.len()));
        out.extend_from_slice(header.as_bytes());
        out.extend_from_slice(&self.payload);
        validate_frame_size(out.len(), MAX_RUNTIME_RESPONSE_BYTES, "runtime response")?;
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        validate_frame_size(bytes.len(), MAX_RUNTIME_RESPONSE_BYTES, "runtime response")?;
        let (header, payload) = split_frame(bytes)?;
        let mut version = None;
        let mut request_id = None;
        let mut status = None;
        let mut payload_len = None;
        let mut next_page_offset = None;
        let mut page_etag = None;
        for (index, line) in header.lines().enumerate() {
            if index == 0 {
                version = Some(parse_magic(line)?);
                continue;
            }
            let (key, value) = parse_header_line(line)?;
            match key {
                "request_id" => request_id = Some(value.to_owned()),
                "status" => status = Some(RuntimeResponseStatus::parse(value)?),
                "payload_len" => payload_len = Some(parse_usize(value, "payload_len")?),
                "next_page_offset" => {
                    next_page_offset = Some(parse_usize(value, "next_page_offset")?)
                }
                "page_etag" => page_etag = Some(value.to_owned()),
                _ => {
                    return Err(CatalogError::Decode(format!(
                        "unknown runtime response header {key}"
                    )));
                }
            }
        }
        require_version(version)?;
        require_payload_len(payload_len, payload.len())?;
        let response = Self {
            request_id: required_header(request_id, "request_id")?,
            status: required_header(status, "status")?,
            payload: payload.to_vec(),
            next_page_offset,
            page_etag,
        };
        response.validate()?;
        Ok(response)
    }

    fn validate(&self) -> CatalogResult<()> {
        validate_token(
            &self.request_id,
            MAX_RUNTIME_REQUEST_ID_BYTES,
            "runtime request_id",
        )?;
        validate_frame_size(
            self.payload.len(),
            MAX_RUNTIME_RESPONSE_BYTES,
            "runtime response payload",
        )?;
        match (self.next_page_offset, &self.page_etag) {
            (None, None) => Ok(()),
            (Some(offset), Some(etag)) if offset > 0 => {
                validate_hex_token(etag, "runtime page_etag")
            }
            _ => Err(CatalogError::Decode(
                "runtime paged response requires next_page_offset and page_etag".to_owned(),
            )),
        }
    }
}

pub(crate) fn paged_runtime_response(
    request_id: String,
    payload: Vec<u8>,
    page_offset: usize,
    requested_etag: Option<&str>,
) -> CatalogResult<RuntimeResponse> {
    let page_etag = runtime_payload_etag(&payload);
    if requested_etag.is_some_and(|requested| requested != page_etag) {
        return Err(CatalogError::InvalidMutation(
            "conflict: runtime response changed during pagination".to_owned(),
        ));
    }
    if page_offset > payload.len() {
        return Err(CatalogError::Decode(format!(
            "runtime page offset {page_offset} exceeds payload length {}",
            payload.len()
        )));
    }
    let page_end = page_offset
        .saturating_add(MAX_RUNTIME_PAGE_PAYLOAD_BYTES)
        .min(payload.len());
    let mut response = RuntimeResponse::ok(request_id, payload[page_offset..page_end].to_vec())?;
    if page_end < payload.len() {
        response.next_page_offset = Some(page_end);
        response.page_etag = Some(page_etag);
        response.validate()?;
    }
    Ok(response)
}

fn runtime_payload_etag(payload: &[u8]) -> String {
    format!("{:x}", Sha256::digest(payload))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeResponseStatus {
    Ok,
    Error,
}

impl RuntimeResponseStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }

    fn parse(value: &str) -> CatalogResult<Self> {
        match value {
            "ok" => Ok(Self::Ok),
            "error" => Ok(Self::Error),
            _ => Err(CatalogError::Decode(format!(
                "unsupported runtime response status {value}"
            ))),
        }
    }
}

fn split_frame(bytes: &[u8]) -> CatalogResult<(&str, &[u8])> {
    let Some(separator) = bytes.windows(2).position(|window| window == b"\n\n") else {
        return Err(CatalogError::Decode(
            "runtime frame missing header separator".to_owned(),
        ));
    };
    let header = std::str::from_utf8(&bytes[..separator])
        .map_err(|error| CatalogError::Decode(format!("runtime header is not utf-8: {error}")))?;
    Ok((header, &bytes[separator + 2..]))
}

fn parse_magic(line: &str) -> CatalogResult<u16> {
    let Some(version) = line.strip_prefix(&format!("{MAGIC}/")) else {
        return Err(CatalogError::Decode(
            "runtime frame has invalid magic".to_owned(),
        ));
    };
    version
        .parse::<u16>()
        .map_err(|error| CatalogError::Decode(format!("invalid runtime version: {error}")))
}

fn parse_header_line(line: &str) -> CatalogResult<(&str, &str)> {
    let Some((key, value)) = line.split_once('=') else {
        return Err(CatalogError::Decode(format!(
            "runtime header line missing '=': {line}"
        )));
    };
    validate_header_key(key)?;
    Ok((key, value))
}

fn parse_usize(value: &str, field: &str) -> CatalogResult<usize> {
    value
        .parse::<usize>()
        .map_err(|error| CatalogError::Decode(format!("invalid runtime {field}: {error}")))
}

fn parse_u64(value: &str, field: &str) -> CatalogResult<u64> {
    value
        .parse::<u64>()
        .map_err(|error| CatalogError::Decode(format!("invalid runtime {field}: {error}")))
}

fn require_version(version: Option<u16>) -> CatalogResult<()> {
    match version {
        Some(RUNTIME_PROTOCOL_VERSION) => Ok(()),
        Some(version) => Err(CatalogError::Decode(format!(
            "unsupported runtime protocol version {version}"
        ))),
        None => Err(CatalogError::Decode(
            "runtime frame missing version".to_owned(),
        )),
    }
}

fn require_payload_len(expected: Option<usize>, actual: usize) -> CatalogResult<()> {
    match expected {
        Some(expected) if expected == actual => Ok(()),
        Some(expected) => Err(CatalogError::Decode(format!(
            "runtime payload length mismatch: header={expected} actual={actual}"
        ))),
        None => Err(CatalogError::Decode(
            "runtime frame missing payload_len".to_owned(),
        )),
    }
}

fn required_header<T>(value: Option<T>, key: &str) -> CatalogResult<T> {
    value.ok_or_else(|| CatalogError::Decode(format!("runtime frame missing {key}")))
}

fn validate_frame_size(actual: usize, limit: usize, label: &str) -> CatalogResult<()> {
    if actual <= limit {
        return Ok(());
    }
    Err(CatalogError::InvalidMutation(format!(
        "{label} is {actual} bytes, over {limit} byte limit"
    )))
}

fn validate_header_key(key: &str) -> CatalogResult<()> {
    if key.is_empty()
        || !key
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
    {
        return Err(CatalogError::Decode(format!(
            "invalid runtime header key {key}"
        )));
    }
    Ok(())
}

fn validate_token(value: &str, limit: usize, label: &str) -> CatalogResult<()> {
    if value.is_empty() || value.len() > limit {
        return Err(CatalogError::InvalidMutation(format!(
            "{label} length must be 1..={limit} bytes"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(CatalogError::InvalidMutation(format!(
            "{label} contains unsupported characters"
        )));
    }
    Ok(())
}

fn validate_hex_token(value: &str, label: &str) -> CatalogResult<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CatalogError::Decode(format!(
            "{label} must be a 64-character hexadecimal digest"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "runtime_protocol_tests.rs"]
mod runtime_protocol_tests;
