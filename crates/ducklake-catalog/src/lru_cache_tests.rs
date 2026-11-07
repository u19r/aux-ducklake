use crate::lru_cache::LruCache;

#[test]
fn given_capacity_is_exceeded_when_recent_entry_is_touched_then_oldest_entry_is_evicted() {
    let mut cache = LruCache::new(2);

    cache.insert(1, "first");
    cache.insert(2, "second");
    assert_eq!(cache.get(&1), Some("first"));
    cache.insert(3, "third");

    assert_eq!(cache.get(&1), Some("first"));
    assert_eq!(cache.get(&2), None);
    assert_eq!(cache.get(&3), Some("third"));
}

#[test]
fn given_hot_and_replaced_entries_when_reused_then_bookkeeping_stays_within_capacity() {
    let mut cache = LruCache::new(2);
    cache.insert(1, "first");
    for _ in 0..10_000 {
        assert_eq!(cache.get(&1), Some("first"));
    }
    cache.assert_bounded();
    for _ in 0..100 {
        cache.insert(1, "replacement");
        cache.assert_bounded();
    }
    cache.insert(2, "second");
    cache.retain(|key, _| *key == 2);
    cache.assert_bounded();
    cache.remove(&2);
    cache.assert_bounded();
    for _ in 0..100 {
        cache.insert(3, "temporary");
        cache.remove(&3);
        cache.assert_bounded();
    }
    let mut zero = LruCache::new(0);
    zero.insert(1, "never retained");
    assert_eq!(zero.get(&1), None);
    zero.assert_bounded();
}
