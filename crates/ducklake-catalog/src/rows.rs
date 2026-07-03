use crate::{
    CatalogError, CatalogResult,
    ids::{
        CatalogOrderId, CatalogOrderKind, DataFileId, DeleteFileId, RawSnapshotSequence, TableId,
    },
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRow {
    pub order: CatalogOrderId,
    pub sequence: RawSnapshotSequence,
    pub created_at_micros: i64,
    pub created_by: String,
    pub commit_message: Option<String>,
    pub commit_extra_info: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotCommitMetadata {
    pub author: Option<String>,
    pub commit_message: Option<String>,
    pub commit_extra_info: Option<String>,
}

impl SnapshotRow {
    const VERSION: u8 = 3;

    #[must_use]
    pub fn initial(order: CatalogOrderId) -> Self {
        Self::new(order, RawSnapshotSequence::initial())
    }

    #[must_use]
    pub fn new(order: CatalogOrderId, sequence: RawSnapshotSequence) -> Self {
        Self::with_created_at_micros(order, sequence, current_timestamp_micros())
    }

    #[must_use]
    pub fn with_created_at_micros(
        order: CatalogOrderId,
        sequence: RawSnapshotSequence,
        created_at_micros: i64,
    ) -> Self {
        Self {
            order,
            sequence,
            created_at_micros,
            created_by: "aux-ducklake".to_owned(),
            commit_message: None,
            commit_extra_info: None,
        }
    }

    #[must_use]
    pub fn with_commit_metadata(
        mut self,
        author: impl Into<String>,
        commit_message: Option<String>,
        commit_extra_info: Option<String>,
    ) -> Self {
        self.created_by = author.into();
        self.commit_message = commit_message;
        self.commit_extra_info = commit_extra_info;
        self
    }

    #[must_use]
    pub fn with_optional_commit_metadata(
        mut self,
        metadata: Option<&SnapshotCommitMetadata>,
    ) -> Self {
        let Some(metadata) = metadata else {
            return self;
        };
        if let Some(author) = metadata.author.as_ref() {
            self.created_by = author.clone();
        }
        self.commit_message = metadata.commit_message.clone();
        self.commit_extra_info = metadata.commit_extra_info.clone();
        self
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(Self::VERSION);
        out.extend_from_slice(&self.order.as_bytes());
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.created_at_micros.to_be_bytes());
        encode_optional_snapshot_string(&mut out, Some(self.created_by.as_str()));
        encode_optional_snapshot_string(&mut out, self.commit_message.as_deref());
        encode_optional_snapshot_string(&mut out, self.commit_extra_info.as_deref());
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let minimum_len = 1 + CatalogOrderId::LEN + 8;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "snapshot row is too short: {} bytes",
                bytes.len()
            )));
        }
        if !matches!(bytes[0], 2 | Self::VERSION) {
            return Err(CatalogError::Decode(format!(
                "unsupported snapshot row version {}",
                bytes[0]
            )));
        }
        let order_start = 1;
        let sequence_start = order_start + CatalogOrderId::LEN;
        let timestamp_start = sequence_start + 8;
        let created_by_start = timestamp_start + 8;
        if bytes.len() < created_by_start {
            return Err(CatalogError::Decode(format!(
                "snapshot row is too short: {} bytes",
                bytes.len(),
            )));
        }
        let order = CatalogOrderId::uuid_v7(u128::from_be_bytes(
            bytes[order_start..sequence_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("snapshot order is truncated".to_owned()))?,
        ));
        let sequence = RawSnapshotSequence(u64::from_be_bytes(
            bytes[sequence_start..timestamp_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("snapshot sequence is truncated".to_owned()))?,
        ));
        let created_at_micros = i64::from_be_bytes(
            bytes[timestamp_start..created_by_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("snapshot timestamp is truncated".to_owned()))?,
        );
        let (created_by, commit_message, commit_extra_info) = if bytes[0] == 2 {
            (
                std::str::from_utf8(&bytes[created_by_start..])
                    .map_err(|err| CatalogError::Decode(format!("created_by is not utf8: {err}")))?
                    .to_owned(),
                None,
                None,
            )
        } else {
            let (created_by, offset) =
                decode_optional_snapshot_string(bytes, created_by_start, "created_by")?;
            let (commit_message, offset) =
                decode_optional_snapshot_string(bytes, offset, "commit_message")?;
            let (commit_extra_info, offset) =
                decode_optional_snapshot_string(bytes, offset, "commit_extra_info")?;
            if offset != bytes.len() {
                return Err(CatalogError::Decode(format!(
                    "snapshot row has {} trailing bytes",
                    bytes.len() - offset
                )));
            }
            (
                created_by.unwrap_or_else(|| "aux-ducklake".to_owned()),
                commit_message,
                commit_extra_info,
            )
        };
        Ok(Self {
            order,
            sequence,
            created_at_micros,
            created_by,
            commit_message,
            commit_extra_info,
        })
    }
}

fn encode_optional_snapshot_string(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            out.push(1);
            let bytes = value.as_bytes();
            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0_u32.to_be_bytes());
        }
    }
}

fn decode_optional_snapshot_string(
    bytes: &[u8],
    offset: usize,
    field: &str,
) -> CatalogResult<(Option<String>, usize)> {
    if bytes.len() < offset + 5 {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let present = bytes[offset];
    let len_start = offset + 1;
    let value_start = len_start + 4;
    let len = u32::from_be_bytes(
        bytes[len_start..value_start]
            .try_into()
            .map_err(|_| CatalogError::Decode(format!("{field} length is truncated")))?,
    ) as usize;
    let value_end = value_start + len;
    if bytes.len() < value_end {
        return Err(CatalogError::Decode(format!("{field} bytes are truncated")));
    }
    let value = match present {
        0 if len == 0 => None,
        0 => {
            return Err(CatalogError::Decode(format!(
                "{field} absent marker has nonzero length"
            )));
        }
        1 => Some(
            std::str::from_utf8(&bytes[value_start..value_end])
                .map_err(|err| CatalogError::Decode(format!("{field} is not utf8: {err}")))?
                .to_owned(),
        ),
        other => {
            return Err(CatalogError::Decode(format!(
                "{field} has unsupported marker {other}"
            )));
        }
    };
    Ok((value, value_end))
}

pub(crate) fn current_timestamp_micros() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => clamp_i128_to_i64(
            i128::from(duration.as_secs())
                .saturating_mul(1_000_000)
                .saturating_add(i128::from(duration.subsec_micros())),
        ),
        Err(error) => {
            let duration = error.duration();
            -clamp_i128_to_i64(
                i128::from(duration.as_secs())
                    .saturating_mul(1_000_000)
                    .saturating_add(i128::from(duration.subsec_micros())),
            )
        }
    }
}

fn clamp_i128_to_i64(value: i128) -> i64 {
    value.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidityWindow {
    pub begin_order: CatalogOrderId,
    pub end_order: Option<CatalogOrderId>,
}

impl ValidityWindow {
    #[must_use]
    pub const fn new(begin_order: CatalogOrderId, end_order: Option<CatalogOrderId>) -> Self {
        Self {
            begin_order,
            end_order,
        }
    }

    #[must_use]
    pub fn is_visible_at(self, snapshot_order: CatalogOrderId) -> bool {
        self.begin_order <= snapshot_order
            && self
                .end_order
                .is_none_or(|end_order| snapshot_order < end_order)
    }
}

pub(crate) const STORED_ORDER_LEN: usize = 1 + CatalogOrderId::LEN;

pub(crate) fn encode_stored_order(out: &mut Vec<u8>, order: CatalogOrderId) {
    out.push(match order.kind() {
        CatalogOrderKind::FdbVersionstamp => 1,
        CatalogOrderKind::UuidV7 => 2,
    });
    out.extend_from_slice(&order.as_bytes());
}

pub(crate) fn decode_stored_order(bytes: &[u8], field: &str) -> CatalogResult<CatalogOrderId> {
    if bytes.len() != STORED_ORDER_LEN {
        return Err(CatalogError::Decode(format!("{field} is truncated")));
    }
    let kind = match bytes[0] {
        1 => CatalogOrderKind::FdbVersionstamp,
        2 => CatalogOrderKind::UuidV7,
        other => {
            return Err(CatalogError::Decode(format!(
                "unsupported {field} order kind {other}"
            )));
        }
    };
    let order_bytes = bytes[1..]
        .try_into()
        .map_err(|_| CatalogError::Decode(format!("{field} is truncated")))?;
    Ok(CatalogOrderId::from_bytes(kind, order_bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFileRow {
    pub data_file_id: DataFileId,
    pub table_id: TableId,
    pub path: String,
    pub record_count: u64,
    pub file_size_bytes: u64,
    pub footer_size: Option<u64>,
    pub row_id_start: u64,
    pub row_id_start_known: bool,
    pub mapping_id: Option<u64>,
    pub max_partial_order: Option<CatalogOrderId>,
    pub validity: ValidityWindow,
}

impl DataFileRow {
    const VERSION: u8 = 7;
    #[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 8 + 8 + 8 + 1 + 1 + 8 + 1;
    #[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
    pub(crate) const END_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(
        data_file_id: DataFileId,
        table_id: TableId,
        path: impl Into<String>,
        record_count: u64,
        file_size_bytes: u64,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            data_file_id,
            table_id,
            path: path.into(),
            record_count,
            file_size_bytes,
            footer_size: None,
            row_id_start: 0,
            row_id_start_known: false,
            mapping_id: None,
            max_partial_order: None,
            validity: ValidityWindow::new(begin_order, None),
        }
    }

    #[must_use]
    pub fn with_row_id_start(mut self, row_id_start: u64) -> Self {
        self.row_id_start = row_id_start;
        self.row_id_start_known = true;
        self
    }

    #[must_use]
    pub fn with_mapping_id(mut self, mapping_id: Option<u64>) -> Self {
        self.mapping_id = mapping_id;
        self
    }

    #[must_use]
    pub fn with_footer_size(mut self, footer_size: Option<u64>) -> Self {
        self.footer_size = footer_size;
        self
    }

    #[must_use]
    pub fn with_begin_order(mut self, begin_order: CatalogOrderId) -> Self {
        self.validity.begin_order = begin_order;
        self
    }

    #[must_use]
    pub fn with_max_partial_order(mut self, max_partial_order: Option<CatalogOrderId>) -> Self {
        self.max_partial_order = max_partial_order;
        self
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            1 + 8
                + 8
                + 8
                + 8
                + 8
                + 1
                + 1
                + 8
                + STORED_ORDER_LEN * 2
                + 1
                + 1
                + STORED_ORDER_LEN
                + 1
                + 8
                + self.path.len(),
        );
        out.push(Self::VERSION);
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.table_id.0.to_be_bytes());
        out.extend_from_slice(&self.record_count.to_be_bytes());
        out.extend_from_slice(&self.file_size_bytes.to_be_bytes());
        out.extend_from_slice(&self.row_id_start.to_be_bytes());
        out.push(u8::from(self.row_id_start_known));
        match self.mapping_id {
            Some(mapping_id) => {
                out.push(1);
                out.extend_from_slice(&mapping_id.to_be_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0_u64.to_be_bytes());
            }
        }
        encode_stored_order(&mut out, self.validity.begin_order);
        match self.validity.end_order {
            Some(end_order) => {
                out.push(1);
                encode_stored_order(&mut out, end_order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        match self.max_partial_order {
            Some(order) => {
                out.push(1);
                encode_stored_order(&mut out, order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        match self.footer_size {
            Some(footer_size) => {
                out.push(1);
                out.extend_from_slice(&footer_size.to_be_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0_u64.to_be_bytes());
            }
        }
        out.extend_from_slice(self.path.as_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let version = match bytes.first().copied() {
            Some(3..=Self::VERSION) => bytes[0],
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported data file row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "data file row is too short: 0 bytes".to_owned(),
                ));
            }
        };
        let row_id_known_len = if version >= Self::VERSION { 1 } else { 0 };
        let mapping_len = if version >= 4 { 1 + 8 } else { 0 };
        let max_partial_len = if version >= 5 {
            1 + STORED_ORDER_LEN
        } else {
            0
        };
        let footer_len = if version >= 6 { 1 + 8 } else { 0 };
        let minimum_len = 1
            + 8
            + 8
            + 8
            + 8
            + 8
            + row_id_known_len
            + mapping_len
            + STORED_ORDER_LEN
            + 1
            + STORED_ORDER_LEN
            + max_partial_len
            + footer_len;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "data file row is too short: {} bytes",
                bytes.len()
            )));
        }
        let data_file_start = 1;
        let table_start = data_file_start + 8;
        let record_count_start = table_start + 8;
        let file_size_start = record_count_start + 8;
        let row_id_start = file_size_start + 8;
        let row_id_known_start = row_id_start + 8;
        let mapping_start = row_id_known_start + row_id_known_len;
        let begin_start = mapping_start + mapping_len;
        let end_present_index = begin_start + STORED_ORDER_LEN;
        let end_start = end_present_index + 1;
        let max_partial_present_index = end_start + STORED_ORDER_LEN;
        let max_partial_start = max_partial_present_index + 1;
        let footer_present_index = max_partial_present_index + max_partial_len;
        let footer_start = footer_present_index + 1;
        let path_start = footer_present_index + footer_len;
        let data_file_id = DataFileId(u64::from_be_bytes(
            bytes[data_file_start..table_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("data file id is truncated".to_owned()))?,
        ));
        let table_id = TableId(u64::from_be_bytes(
            bytes[table_start..record_count_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("table id is truncated".to_owned()))?,
        ));
        let record_count = u64::from_be_bytes(
            bytes[record_count_start..file_size_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("record count is truncated".to_owned()))?,
        );
        let file_size_bytes = u64::from_be_bytes(
            bytes[file_size_start..row_id_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("file size is truncated".to_owned()))?,
        );
        let row_id_start = u64::from_be_bytes(
            bytes[row_id_start..row_id_known_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("row id start is truncated".to_owned()))?,
        );
        let row_id_start_known = if version >= Self::VERSION {
            match bytes[row_id_known_start] {
                0 => false,
                1 => true,
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid data file row-id-known marker {other}"
                    )));
                }
            }
        } else {
            false
        };
        let mapping_id = if version >= 4 {
            let marker = bytes[mapping_start];
            let value_start = mapping_start + 1;
            let value_end = value_start + 8;
            let value = u64::from_be_bytes(
                bytes[value_start..value_end]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("mapping id is truncated".to_owned()))?,
            );
            match marker {
                0 => None,
                1 => Some(value),
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid data file mapping marker {other}"
                    )));
                }
            }
        } else {
            None
        };
        let begin_order =
            decode_stored_order(&bytes[begin_start..end_present_index], "begin_order")?;
        let end_order = match bytes[end_present_index] {
            0 => None,
            1 => Some(decode_stored_order(
                &bytes[end_start..max_partial_present_index],
                "end_order",
            )?),
            other => {
                return Err(CatalogError::Decode(format!(
                    "invalid data file end-order marker {other}"
                )));
            }
        };
        let max_partial_order = if version >= 5 {
            match bytes[max_partial_present_index] {
                0 => None,
                1 => Some(decode_stored_order(
                    &bytes[max_partial_start..footer_present_index],
                    "max_partial_order",
                )?),
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid data file max-partial marker {other}"
                    )));
                }
            }
        } else {
            None
        };
        let footer_size = if version >= 6 {
            let value = u64::from_be_bytes(
                bytes[footer_start..path_start]
                    .try_into()
                    .map_err(|_| CatalogError::Decode("footer size is truncated".to_owned()))?,
            );
            match bytes[footer_present_index] {
                0 => None,
                1 => Some(value),
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid data file footer-size marker {other}"
                    )));
                }
            }
        } else {
            None
        };
        let path = std::str::from_utf8(&bytes[path_start..])
            .map_err(|err| CatalogError::Decode(format!("data file path is not utf8: {err}")))?
            .to_owned();
        Ok(Self {
            data_file_id,
            table_id,
            path,
            record_count,
            file_size_bytes,
            footer_size,
            row_id_start,
            row_id_start_known,
            mapping_id,
            max_partial_order,
            validity: ValidityWindow::new(begin_order, end_order),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFileRow {
    pub delete_file_id: DeleteFileId,
    pub data_file_id: DataFileId,
    pub path: String,
    pub record_count: u64,
    pub file_size_bytes: u64,
    pub validity: ValidityWindow,
    pub max_partial_order: Option<CatalogOrderId>,
}

impl DeleteFileRow {
    const VERSION: u8 = 3;
    const VERSION_WITHOUT_MAX_PARTIAL_ORDER: u8 = 2;
    #[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
    pub(crate) const BEGIN_ORDER_BYTES_OFFSET: usize = 1 + 8 + 8 + 8 + 8 + 1;
    #[cfg_attr(not(feature = "foundationdb"), allow(dead_code))]
    pub(crate) const MAX_PARTIAL_ORDER_BYTES_OFFSET: usize =
        Self::BEGIN_ORDER_BYTES_OFFSET + STORED_ORDER_LEN + 1;

    #[must_use]
    pub fn new(
        delete_file_id: DeleteFileId,
        data_file_id: DataFileId,
        path: impl Into<String>,
        record_count: u64,
        file_size_bytes: u64,
        begin_order: CatalogOrderId,
    ) -> Self {
        Self {
            delete_file_id,
            data_file_id,
            path: path.into(),
            record_count,
            file_size_bytes,
            validity: ValidityWindow::new(begin_order, None),
            max_partial_order: None,
        }
    }

    #[must_use]
    pub fn with_max_partial_order(mut self, max_partial_order: Option<CatalogOrderId>) -> Self {
        self.max_partial_order = max_partial_order;
        self
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(1 + 8 + 8 + 8 + 8 + STORED_ORDER_LEN * 3 + 2 + self.path.len());
        out.push(Self::VERSION);
        out.extend_from_slice(&self.delete_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.data_file_id.0.to_be_bytes());
        out.extend_from_slice(&self.record_count.to_be_bytes());
        out.extend_from_slice(&self.file_size_bytes.to_be_bytes());
        encode_stored_order(&mut out, self.validity.begin_order);
        match self.validity.end_order {
            Some(end_order) => {
                out.push(1);
                encode_stored_order(&mut out, end_order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        match self.max_partial_order {
            Some(max_partial_order) => {
                out.push(1);
                encode_stored_order(&mut out, max_partial_order);
            }
            None => {
                out.push(0);
                encode_stored_order(&mut out, CatalogOrderId::uuid_v7(0));
            }
        }
        out.extend_from_slice(self.path.as_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> CatalogResult<Self> {
        let version = match bytes.first().copied() {
            Some(Self::VERSION) => Self::VERSION,
            Some(Self::VERSION_WITHOUT_MAX_PARTIAL_ORDER) => {
                Self::VERSION_WITHOUT_MAX_PARTIAL_ORDER
            }
            Some(other) => {
                return Err(CatalogError::Decode(format!(
                    "unsupported delete file row version {other}"
                )));
            }
            None => {
                return Err(CatalogError::Decode(
                    "delete file row is too short: 0 bytes".to_owned(),
                ));
            }
        };
        let minimum_len = 1 + 8 + 8 + 8 + 8 + STORED_ORDER_LEN + 1 + STORED_ORDER_LEN;
        if bytes.len() < minimum_len {
            return Err(CatalogError::Decode(format!(
                "delete file row is too short: {} bytes",
                bytes.len()
            )));
        }
        let delete_file_start = 1;
        let data_file_start = delete_file_start + 8;
        let record_count_start = data_file_start + 8;
        let file_size_start = record_count_start + 8;
        let begin_start = file_size_start + 8;
        let end_present_index = begin_start + STORED_ORDER_LEN;
        let end_start = end_present_index + 1;
        let legacy_path_start = end_start + STORED_ORDER_LEN;
        let max_partial_present_index = legacy_path_start;
        let max_partial_start = max_partial_present_index + 1;
        let path_start = if version == Self::VERSION {
            max_partial_start + STORED_ORDER_LEN
        } else {
            legacy_path_start
        };
        if bytes.len() < path_start {
            return Err(CatalogError::Decode(format!(
                "delete file row version {version} is too short: {} bytes",
                bytes.len()
            )));
        }
        let delete_file_id = DeleteFileId(u64::from_be_bytes(
            bytes[delete_file_start..data_file_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("delete file id is truncated".to_owned()))?,
        ));
        let data_file_id = DataFileId(u64::from_be_bytes(
            bytes[data_file_start..record_count_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("data file id is truncated".to_owned()))?,
        ));
        let record_count = u64::from_be_bytes(
            bytes[record_count_start..file_size_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("record count is truncated".to_owned()))?,
        );
        let file_size_bytes = u64::from_be_bytes(
            bytes[file_size_start..begin_start]
                .try_into()
                .map_err(|_| CatalogError::Decode("file size is truncated".to_owned()))?,
        );
        let begin_order =
            decode_stored_order(&bytes[begin_start..end_present_index], "begin_order")?;
        let end_order = match bytes[end_present_index] {
            0 => None,
            1 => Some(decode_stored_order(
                &bytes[end_start..legacy_path_start],
                "end_order",
            )?),
            other => {
                return Err(CatalogError::Decode(format!(
                    "invalid delete file end-order marker {other}"
                )));
            }
        };
        let max_partial_order = if version == Self::VERSION {
            match bytes[max_partial_present_index] {
                0 => None,
                1 => Some(decode_stored_order(
                    &bytes[max_partial_start..max_partial_start + STORED_ORDER_LEN],
                    "max_partial_order",
                )?),
                other => {
                    return Err(CatalogError::Decode(format!(
                        "invalid delete file max-partial marker {other}"
                    )));
                }
            }
        } else {
            None
        };
        let path = std::str::from_utf8(&bytes[path_start..])
            .map_err(|err| CatalogError::Decode(format!("delete file path is not utf8: {err}")))?
            .to_owned();
        Ok(Self {
            delete_file_id,
            data_file_id,
            path,
            record_count,
            file_size_bytes,
            validity: ValidityWindow::new(begin_order, end_order),
            max_partial_order,
        })
    }
}
