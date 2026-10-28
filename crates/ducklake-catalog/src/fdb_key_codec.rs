use std::borrow::Cow;

use foundationdb::tuple::{self, Bytes, PackResult, TupleDepth, TupleUnpack};

use crate::{
    CatalogError, CatalogResult, FdbOrderedCatalogKv, constants::FDB_LOGICAL_FAMILY_PREFIX_BYTES,
    keys::KeyFamily,
};

impl FdbOrderedCatalogKv {
    pub(super) fn stage_transaction_value(
        &self,
        transaction: &foundationdb::Transaction,
        key: &[u8],
        value: &[u8],
    ) -> CatalogResult<()> {
        let encoded = encode_value(key, value)?;
        transaction.set(&self.namespaced_key(key), &encoded);
        Ok(())
    }
}

struct PhysicalKey<'a> {
    namespace: Bytes<'a>,
    family: Bytes<'a>,
    tail: &'a [u8],
}

impl<'a> TupleUnpack<'a> for PhysicalKey<'a> {
    fn unpack(input: &'a [u8], depth: TupleDepth) -> PackResult<(&'a [u8], Self)> {
        let (input, namespace) = Bytes::unpack(input, depth)?;
        let (tail, family) = Bytes::unpack(input, depth)?;
        Ok((
            &[],
            Self {
                namespace,
                family,
                tail,
            },
        ))
    }
}

pub(super) fn encode_key(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let split = key.len().min(FDB_LOGICAL_FAMILY_PREFIX_BYTES);
    let mut physical = tuple::pack(&(namespace, &key[..split]));
    if is_primary_file_key(key) {
        tuple::pack_into(&(&key[split..],), &mut physical);
    } else {
        physical.extend_from_slice(&key[split..]);
    }
    physical
}

pub(super) fn encode_range_bound(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let split = key.len().min(FDB_LOGICAL_FAMILY_PREFIX_BYTES);
    let mut physical = tuple::pack(&(namespace, &key[..split]));
    if key.len() < FDB_LOGICAL_FAMILY_PREFIX_BYTES {
        physical.pop();
    } else if is_primary_file_key(key) && key.len() > split {
        let mut tail = tuple::pack(&(&key[split..],));
        tail.pop();
        physical.extend_from_slice(&tail);
    } else {
        physical.extend_from_slice(&key[split..]);
    }
    physical
}

pub(super) fn decode_key(namespace: &[u8], physical: &[u8]) -> CatalogResult<Vec<u8>> {
    let key = tuple::unpack::<PhysicalKey<'_>>(physical).map_err(map_tuple_error)?;
    if key.namespace.as_ref() != namespace {
        return Err(CatalogError::InvalidKey(
            "FoundationDB key has another namespace".to_owned(),
        ));
    }
    let mut logical = key.family.into_owned();
    if is_primary_file_key(&logical) {
        let (tail,) = tuple::unpack::<(Bytes<'_>,)>(key.tail).map_err(map_tuple_error)?;
        logical.extend_from_slice(&tail);
    } else {
        logical.extend_from_slice(key.tail);
    }
    Ok(logical)
}

pub(super) fn encode_value<'a>(key: &[u8], value: &'a [u8]) -> CatalogResult<Cow<'a, [u8]>> {
    if !is_cleanup_key(key) {
        return Ok(Cow::Borrowed(value));
    }
    let (family, id) = cleanup_reference(key)?;
    Ok(Cow::Owned(tuple::pack(&(family.as_slice(), id, value))))
}

pub(super) fn decode_value(key: &[u8], value: &[u8]) -> CatalogResult<Vec<u8>> {
    if !is_cleanup_key(key) {
        return Ok(value.to_vec());
    }
    let (family, id, payload) =
        tuple::unpack::<(Bytes<'_>, Bytes<'_>, Bytes<'_>)>(value).map_err(map_tuple_error)?;
    let (expected_family, expected_id) = cleanup_reference(key)?;
    if family.as_ref() != expected_family || id.as_ref() != expected_id {
        return Err(CatalogError::Decode(
            "cleanup reference does not match its index key".to_owned(),
        ));
    }
    Ok(payload.into_owned())
}

pub(super) fn decode_owned_value(key: &[u8], value: Vec<u8>) -> CatalogResult<Vec<u8>> {
    if is_cleanup_key(key) {
        decode_value(key, &value)
    } else {
        Ok(value)
    }
}

pub(super) fn namespace_range_start(namespace: &[u8]) -> Vec<u8> {
    let mut prefix = tuple::pack(&(namespace,));
    prefix.pop();
    prefix
}

fn is_primary_file_key(key: &[u8]) -> bool {
    key.get(8) == Some(&b'/')
        && key.get(10) == Some(&b'/')
        && matches!(key.get(9), Some(code) if *code == KeyFamily::DataFile.code() || *code == KeyFamily::DeleteFile.code())
}

fn is_cleanup_key(key: &[u8]) -> bool {
    key.get(8) == Some(&b'/')
        && key.get(9) == Some(&KeyFamily::ScheduledCleanup.code())
        && key.get(10) == Some(&b'/')
}

fn cleanup_reference(key: &[u8]) -> CatalogResult<(Vec<u8>, &[u8])> {
    if key.len() != FDB_LOGICAL_FAMILY_PREFIX_BYTES + 10 || key.get(12) != Some(&b'/') {
        return Err(CatalogError::InvalidKey(
            "cleanup index key has an invalid shape".to_owned(),
        ));
    }
    let family = match key[11] {
        b'd' => KeyFamily::DataFile,
        b'x' => KeyFamily::DeleteFile,
        _ => {
            return Err(CatalogError::InvalidKey(
                "cleanup index has an unknown object kind".to_owned(),
            ));
        }
    };
    let mut prefix = key[..FDB_LOGICAL_FAMILY_PREFIX_BYTES].to_vec();
    prefix[9] = family.code();
    Ok((prefix, &key[13..]))
}

fn map_tuple_error(error: tuple::PackError) -> CatalogError {
    CatalogError::Decode(format!("invalid FoundationDB physical tuple: {error}"))
}
