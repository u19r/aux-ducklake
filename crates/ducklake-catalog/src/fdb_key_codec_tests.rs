use foundationdb::tuple::{self, Bytes};

use crate::{
    CatalogId, DataFileId, DeleteFileId,
    fdb_key_codec::{
        decode_key, decode_value, encode_key, encode_range_bound, encode_value,
        namespace_range_start,
    },
    keys::{self, KeyFamily},
};

#[test]
fn given_binary_keys_when_encoding_ranges_then_order_and_prefix_membership_are_preserved() {
    let namespace = b"codec/test/\0binary/";
    let catalog = CatalogId(u64::MAX - 1);
    let mut keys = vec![
        Vec::new(),
        b"a".to_vec(),
        b"a\0".to_vec(),
        b"a\0z".to_vec(),
        b"a/b".to_vec(),
    ];
    for id in [0, 1, 255, 256, 1 << 63, u64::MAX] {
        keys.push(keys::data_file_key(catalog, DataFileId(id)));
        keys.push(keys::delete_file_key(catalog, DeleteFileId(id)));
        keys.push(keys::current_data_file_key(
            catalog,
            crate::TableId(7),
            DataFileId(id),
        ));
    }
    keys.sort();
    let mut physical = keys
        .iter()
        .map(|key| encode_key(namespace, key))
        .collect::<Vec<_>>();
    physical.sort();
    let decoded = physical
        .iter()
        .map(|key| decode_key(namespace, key).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(decoded, keys);
    for prefix in [
        b"a".to_vec(),
        b"a\0".to_vec(),
        keys::catalog_prefix(catalog),
        keys::family_prefix(catalog, KeyFamily::DataFile),
        keys::data_file_key(catalog, DataFileId(0))[..15].to_vec(),
    ] {
        let start = encode_range_bound(namespace, &prefix);
        let end = encode_range_bound(namespace, &keys::prefix_end(&prefix));
        let actual = physical
            .iter()
            .filter(|key| **key >= start && **key < end)
            .map(|key| decode_key(namespace, key).unwrap())
            .collect::<Vec<_>>();
        let expected = keys
            .iter()
            .filter(|key| key.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "prefix {prefix:?}");
    }
}

#[test]
fn given_versionstamped_history_key_when_commit_bytes_include_zero_then_tail_remains_unescaped() {
    let namespace = b"codec/versionstamp/";
    let catalog = CatalogId(7);
    let mut logical = keys::snapshot_key(catalog, crate::fdb_versionstamp::incomplete_order());
    let offset = crate::fdb_versionstamp::snapshot_key_order_offset(catalog);
    let physical_offset = encode_key(namespace, &logical[..offset]).len();
    let mut physical = encode_key(namespace, &logical);
    assert_eq!(&physical[physical_offset..physical_offset + 10], &[0; 10]);
    let committed = [0, 0, 0, 0, 0, 17, 0, 29, 0, 1];
    logical[offset..offset + 10].copy_from_slice(&committed);
    physical[physical_offset..physical_offset + 10].copy_from_slice(&committed);
    assert_eq!(decode_key(namespace, &physical).unwrap(), logical);
}

#[test]
fn given_cleanup_pointer_when_mapper_packs_target_then_primary_key_matches_exactly() {
    let namespace = b"codec/mapped/";
    for id in [0, 255, 1 << 63, u64::MAX] {
        let index = keys::scheduled_data_file_cleanup_key(CatalogId(9), DataFileId(id));
        let payload = b"scheduled metadata";
        let encoded = encode_value(&index, payload).unwrap();
        let (family, identity, metadata) =
            tuple::unpack::<(Bytes<'_>, Bytes<'_>, Bytes<'_>)>(&encoded).unwrap();
        let mapped = tuple::pack(&(namespace.as_slice(), family.as_ref(), identity.as_ref()));
        assert_eq!(
            mapped,
            encode_key(
                namespace,
                &keys::data_file_key(CatalogId(9), DataFileId(id))
            )
        );
        assert_eq!(metadata.as_ref(), payload);
        assert_eq!(decode_value(&index, &encoded).unwrap(), payload);
        let other = keys::scheduled_data_file_cleanup_key(CatalogId(10), DataFileId(id));
        assert!(decode_value(&other, &encoded).is_err());
    }
}

#[test]
fn given_parent_namespace_when_selecting_cleanup_bounds_then_children_match_and_neighbors_do_not() {
    let (start, end) = crate::foundationdb_prefix_bounds(b"codec/parent/").unwrap();
    for namespace in [
        b"codec/parent/".as_slice(),
        b"codec/parent/child/".as_slice(),
    ] {
        let key = encode_key(namespace, b"test-key");
        assert!(key >= start && key < end);
    }
    for namespace in [b"codec/parent0/".as_slice(), b"codec/sibling/".as_slice()] {
        let key = encode_key(namespace, b"test-key");
        assert!(!(key >= start && key < end));
    }
    assert_eq!(start, namespace_range_start(b"codec/parent/"));
}
