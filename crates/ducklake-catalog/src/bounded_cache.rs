#![cfg_attr(test, allow(dead_code))]

use std::sync::{Mutex, OnceLock};

use crate::lru_cache::LruCache;

pub(crate) struct BoundedCache<K, V> {
    entries: Mutex<LruCache<K, V>>,
}

impl<K, V> BoundedCache<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(LruCache::new(capacity)),
        }
    }

    pub(crate) fn get(&self, key: K) -> Option<V> {
        self.get_ref(&key)
    }

    pub(crate) fn get_ref(&self, key: &K) -> Option<V> {
        self.entries
            .lock()
            .ok()
            .and_then(|mut entries| entries.get(key))
    }

    pub(crate) fn insert(&self, key: K, value: V) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(key, value);
        }
    }

    pub(crate) fn retain(&self, mut keep: impl FnMut(&K, &V) -> bool) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        entries.retain(|key, value| keep(key, value));
    }
}

pub(crate) fn static_bounded_cache<K, V>(
    cache: &'static OnceLock<BoundedCache<K, V>>,
    capacity: usize,
) -> &'static BoundedCache<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    cache.get_or_init(|| BoundedCache::new(capacity))
}

#[cfg(test)]
mod tests {
    use super::BoundedCache;

    #[test]
    fn given_capacity_is_exceeded_when_entry_was_read_then_lru_entry_is_evicted() {
        let cache = BoundedCache::new(2);
        cache.insert(1, "first");
        cache.insert(2, "second");

        assert_eq!(cache.get(1), Some("first"));
        cache.insert(3, "third");

        assert_eq!(cache.get(1), Some("first"));
        assert_eq!(cache.get(2), None);
        assert_eq!(cache.get(3), Some("third"));
    }

    #[test]
    fn given_entries_are_retained_when_capacity_is_exceeded_then_removed_entries_do_not_return() {
        let cache = BoundedCache::new(2);
        cache.insert(1, "first");
        cache.insert(2, "second");
        cache.retain(|key, _| *key != 1);

        cache.insert(3, "third");

        assert_eq!(cache.get(1), None);
        assert_eq!(cache.get(2), Some("second"));
        assert_eq!(cache.get(3), Some("third"));
    }

    #[test]
    fn given_entry_exists_when_borrowed_key_is_used_then_entry_is_returned() {
        let cache = BoundedCache::new(2);
        cache.insert("first".to_owned(), 1);

        assert_eq!(cache.get_ref(&"first".to_owned()), Some(1));
    }

    #[test]
    fn given_capacity_is_zero_when_entry_is_inserted_then_entry_is_not_cached() {
        let cache = BoundedCache::new(0);

        cache.insert(1, "first");

        assert_eq!(cache.get(1), None);
    }
}
