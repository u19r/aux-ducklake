use crate::{CatalogError, CatalogResult};

const INLINE_FIELD_CAPACITY: usize = 16;

pub(crate) struct TabularPayload<'a> {
    operation: &'static str,
    lines: std::str::Lines<'a>,
}

impl<'a> TabularPayload<'a> {
    pub(crate) fn new(operation: &'static str, payload: &'a [u8]) -> CatalogResult<Self> {
        let text = std::str::from_utf8(payload).map_err(|error| {
            CatalogError::Decode(format!("{operation} payload is not valid utf-8: {error}"))
        })?;
        Ok(Self {
            operation,
            lines: text.lines(),
        })
    }
}

impl<'a> Iterator for TabularPayload<'a> {
    type Item = CatalogResult<TabularRow<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        for line in self.lines.by_ref() {
            if line.is_empty() {
                continue;
            }
            return Some(Ok(TabularRow {
                operation: self.operation,
                line,
            }));
        }
        None
    }
}

pub(crate) struct TabularRow<'a> {
    operation: &'static str,
    line: &'a str,
}

impl<'a> TabularRow<'a> {
    pub(crate) fn line(&self) -> &'a str {
        self.line
    }

    pub(crate) fn fields(&self) -> TabularFields<'a> {
        TabularFields::new(self.line)
    }

    pub(crate) fn has_fields(&self, first: &str, second_present: bool) -> bool {
        let mut fields = self.line.split('\t');
        if fields.next() != Some(first) {
            return false;
        }
        if second_present && fields.next().is_none() {
            return false;
        }
        fields.next().is_none()
    }

    pub(crate) fn invalid(&self) -> CatalogError {
        CatalogError::Decode(format!(
            "{} payload has invalid row: {}",
            self.operation, self.line
        ))
    }
}

pub(crate) struct TabularFields<'a> {
    inline: [&'a str; INLINE_FIELD_CAPACITY],
    len: usize,
    overflow: Option<Vec<&'a str>>,
}

impl<'a> TabularFields<'a> {
    fn new(line: &'a str) -> Self {
        let mut inline = [""; INLINE_FIELD_CAPACITY];
        let mut len = 0;
        let mut fields = line.split('\t');
        while len < INLINE_FIELD_CAPACITY {
            let Some(field) = fields.next() else {
                return Self {
                    inline,
                    len,
                    overflow: None,
                };
            };
            inline[len] = field;
            len += 1;
        }
        let mut overflow = inline.to_vec();
        overflow.extend(fields);
        Self {
            inline,
            len,
            overflow: Some(overflow),
        }
    }

    pub(crate) fn as_slice(&self) -> &[&'a str] {
        match &self.overflow {
            Some(fields) => fields,
            None => &self.inline[..self.len],
        }
    }

    pub(crate) fn to_vec(&self) -> Vec<&'a str> {
        self.as_slice().to_vec()
    }

    pub(crate) fn join(&self, separator: &str) -> String {
        self.as_slice().join(separator)
    }
}

pub(crate) fn parse_u64_field(
    operation: &'static str,
    value: &str,
    field: &str,
) -> CatalogResult<u64> {
    value.parse::<u64>().map_err(|error| {
        CatalogError::Decode(format!(
            "{operation} payload has invalid {field} {value}: {error}"
        ))
    })
}

pub(crate) fn parse_u32_field(
    operation: &'static str,
    value: &str,
    field: &str,
) -> CatalogResult<u32> {
    value.parse::<u32>().map_err(|error| {
        CatalogError::Decode(format!(
            "{operation} payload has invalid {field} {value}: {error}"
        ))
    })
}

pub(crate) fn parse_bool_field(
    operation: &'static str,
    value: &str,
    field: &str,
) -> CatalogResult<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(CatalogError::Decode(format!(
            "{operation} payload has invalid {field} {value}"
        ))),
    }
}

pub(crate) fn empty_to_none(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

pub(crate) fn default_value_to_option(value: &str, default_value_type: &str) -> Option<String> {
    if default_value_type == "literal" && value == "NULL" {
        None
    } else {
        Some(value.to_owned())
    }
}

#[cfg(test)]
#[path = "runtime_tabular_payload_tests.rs"]
mod runtime_tabular_payload_tests;
