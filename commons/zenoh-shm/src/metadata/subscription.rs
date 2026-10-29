//
// Copyright (c) 2025 ZettaScale Technology
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
use std::sync::Arc;

use static_init::dynamic;
use zenoh_result::ZResult;

use super::{
    descriptor::{MetadataDescriptor, MetadataSegmentID, OwnedMetadataDescriptor},
    segment::MetadataSegment,
};
use crate::segment_cache::SegmentCache;

#[dynamic(lazy, drop)]
pub static mut GLOBAL_METADATA_SUBSCRIPTION: Subscription = Subscription::new();

pub struct Subscription {
    pub(crate) linked_table: Arc<SegmentCache<MetadataSegmentID, MetadataSegment>>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("linked_table", &self.linked_table)
            .finish()
    }
}

impl Subscription {
    fn new() -> Self {
        Self {
            linked_table: SegmentCache::new(0),
        }
    }

    pub fn link(&self, descriptor: &MetadataDescriptor) -> ZResult<OwnedMetadataDescriptor> {
        let segment = self.linked_table.get_or_try_insert(descriptor.id, || {
            MetadataSegment::open(descriptor.id).map(Arc::new)
        })?;

        // construct owned descriptor
        // SAFETY: MetadataDescriptor source guarantees that descriptor.index is valid for segment
        let (header, watchdog) = unsafe { segment.data.fast_elem_compute(descriptor.index) };

        let owned_descriptor = OwnedMetadataDescriptor::new(segment, header, watchdog);
        Ok(owned_descriptor)
    }
}
