//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Once, RwLock, Weak,
    },
    time::Duration,
};

use zenoh_core::{zlock, zread, zwrite};

// An entry is released when it was neither looked up nor referenced for one whole interval
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

static CACHES: Mutex<Vec<Weak<dyn Sweep>>> = Mutex::new(Vec::new());

pub(crate) trait Sweep: Send + Sync {
    fn sweep(&self);
}

struct CachedSegment<V: ?Sized> {
    segment: Arc<V>,
    used: AtomicBool,
}

/// A cache of mapped segments that releases the ones nobody uses anymore.
///
/// Every reference to a cached segment comes from this cache, so once the cache holds the last
/// references (its own plus `holders` known long-lived ones) under its write lock, no buffer
/// points into the segment and none can appear: dropping it is safe.
pub(crate) struct SegmentCache<K, V: ?Sized> {
    entries: RwLock<BTreeMap<K, CachedSegment<V>>>,
    holders: usize,
}

impl<K, V: ?Sized> std::fmt::Debug for SegmentCache<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("SegmentCache");
        match self.entries.try_read() {
            Ok(entries) => debug.field("len", &entries.len()),
            Err(_) => debug.field("len", &"<locked>"),
        };
        debug.finish()
    }
}

impl<K, V> SegmentCache<K, V>
where
    K: Ord + Send + Sync + 'static,
    V: ?Sized + Send + Sync + 'static,
{
    pub(crate) fn new(holders: usize) -> Arc<Self> {
        let cache = Arc::new(Self {
            entries: RwLock::default(),
            holders,
        });
        register(Arc::downgrade(&cache) as Weak<dyn Sweep>);
        cache
    }

    pub(crate) fn get_or_try_insert<E>(
        &self,
        key: K,
        insert: impl FnOnce() -> Result<Arc<V>, E>,
    ) -> Result<Arc<V>, E> {
        // fastest path: try to utilize already existing segment
        if let Some(cached) = zread!(self.entries).get(&key) {
            cached.used.store(true, Ordering::Relaxed);
            return Ok(cached.segment.clone());
        }

        // many concurrent threads may be racing for this segment, so we must check again under write lock
        match zwrite!(self.entries).entry(key) {
            Entry::Occupied(occupied) => {
                occupied.get().used.store(true, Ordering::Relaxed);
                Ok(occupied.get().segment.clone())
            }
            Entry::Vacant(vacant) => {
                let cached = vacant.insert(CachedSegment {
                    segment: insert()?,
                    used: AtomicBool::new(true),
                });
                Ok(cached.segment.clone())
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &K) -> bool {
        zread!(self.entries).contains_key(key)
    }
}

impl<K, V> Sweep for SegmentCache<K, V>
where
    K: Ord + Send + Sync,
    V: ?Sized + Send + Sync,
{
    fn sweep(&self) {
        let mut released = vec![];
        zwrite!(self.entries).retain(|_, cached| {
            let keep = cached.used.swap(false, Ordering::Relaxed)
                || Arc::strong_count(&cached.segment) > 1 + self.holders;
            if !keep {
                released.push(cached.segment.clone());
            }
            keep
        });
        // segments are unmapped here, outside of the lock
        drop(released);
    }
}

fn register(cache: Weak<dyn Sweep>) {
    static SWEEPER: Once = Once::new();
    SWEEPER.call_once(|| {
        let spawned = std::thread::Builder::new()
            .name("SHM segment sweeper".to_owned())
            .spawn(|| loop {
                std::thread::sleep(SWEEP_INTERVAL);
                sweep_all();
            });
        if let Err(e) = spawned {
            tracing::warn!("Unable to start SHM segment sweeper, unused segments stay mapped: {e}");
        }
    });
    zlock!(CACHES).push(cache);
}

/// Release the cached segments that were not used since the previous call.
pub(crate) fn sweep_all() {
    let caches: Vec<Arc<dyn Sweep>> = {
        let mut guard = zlock!(CACHES);
        guard.retain(|cache| cache.strong_count() > 0);
        guard.iter().filter_map(Weak::upgrade).collect()
    };
    for cache in caches {
        cache.sweep();
    }
}
