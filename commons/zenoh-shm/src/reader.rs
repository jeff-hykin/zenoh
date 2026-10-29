//
// Copyright (c) 2023 ZettaScale Technology
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

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use zenoh_core::{bail, zerror};
use zenoh_result::ZResult;

use crate::{
    api::{
        client::shm_segment::ShmSegment,
        client_storage::ShmClientStorage,
        common::types::{ProtocolID, PtrInSegment, SegmentID},
        provider::chunk::ChunkDescriptor,
    },
    metadata::subscription::GLOBAL_METADATA_SUBSCRIPTION,
    watchdog::confirmator::GLOBAL_CONFIRMATOR,
    ShmBufInfo, ShmBufInner,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ShmReader {
    client_storage: Arc<ShmClientStorage>,
}

impl Deref for ShmReader {
    type Target = ShmClientStorage;

    fn deref(&self) -> &Self::Target {
        &self.client_storage
    }
}

impl ShmReader {
    pub fn new(client_storage: Arc<ShmClientStorage>) -> Self {
        Self { client_storage }
    }

    pub fn read_shmbuf(&self, info: ShmBufInfo) -> ZResult<Arc<ShmBufInner>> {
        // Read does not increment the reference count as it is assumed
        // that the sender of this buffer has incremented it for us.

        let metadata = GLOBAL_METADATA_SUBSCRIPTION.read().link(&info.metadata)?;
        // attach to the watchdog before doing other things
        let confirmed_metadata = GLOBAL_CONFIRMATOR.read().add(metadata);

        // retrieve data descriptor from metadata
        let data_descriptor = confirmed_metadata.owned.header().data_descriptor();

        let segment = self.ensure_data_segment(
            confirmed_metadata
                .owned
                .header()
                .protocol
                .load(std::sync::atomic::Ordering::Relaxed),
            &data_descriptor,
        )?;
        let buf = segment.map(data_descriptor.chunk)?;
        let shmb = Arc::new(ShmBufInner {
            metadata: confirmed_metadata,
            buf: PtrInSegment::new(buf, Arc::new(segment)),
            info: info.clone(),
        });

        // Validate buffer
        match shmb.is_valid() {
            true => Ok(shmb),
            false => bail!("Buffer is invalidated"),
        }
    }

    fn ensure_data_segment(
        &self,
        protocol_id: ProtocolID,
        descriptor: &ChunkDescriptor,
    ) -> ZResult<Arc<dyn ShmSegment>> {
        let id = get_global_id(protocol_id, descriptor.segment);
        self.segments.get_or_try_insert(id, || {
            // find appropriate client and mount a new segment
            let client = self
                .clients
                .get_clients()
                .get(&protocol_id)
                .ok_or_else(|| zerror!("Unsupported SHM protocol: {}", protocol_id))?;
            client.attach(descriptor.segment)
        })
    }
}

#[derive(Debug)]
pub(crate) struct ClientStorage<Inner>
where
    Inner: Sized,
{
    clients: BTreeMap<ProtocolID, Inner>,
}

impl<Inner: Sized> ClientStorage<Inner> {
    pub(crate) fn new(clients: BTreeMap<ProtocolID, Inner>) -> Self {
        Self { clients }
    }

    pub(crate) fn get_clients(&self) -> &BTreeMap<ProtocolID, Inner> {
        &self.clients
    }
}

/// # Safety
/// Only immutable access to internal container is allowed,
/// so we are Send if the contained type is Send
unsafe impl<Inner: Send> Send for ClientStorage<Inner> {}

/// # Safety
/// Only immutable access to internal container is allowed,
/// so we are Sync if the contained type is Sync
unsafe impl<Inner: Sync> Sync for ClientStorage<Inner> {}

pub(crate) type GlobalDataSegmentId = u64;
fn get_global_id(protocol: ProtocolID, segment: SegmentID) -> GlobalDataSegmentId {
    static_assertions::assert_eq_size!(GlobalDataSegmentId, (ProtocolID, SegmentID));
    ((protocol as GlobalDataSegmentId) << 32) | (segment as GlobalDataSegmentId)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use zenoh_core::Wait;

    use super::{get_global_id, ShmReader};
    use crate::{
        api::{client_storage::ShmClientStorage, provider::shm_provider::ShmProviderBuilder},
        metadata::subscription::GLOBAL_METADATA_SUBSCRIPTION,
        segment_cache::sweep_all,
    };

    #[test]
    fn segments_are_released_after_last_buffer_drops() {
        let provider = ShmProviderBuilder::default_backend(65536).wait().unwrap();
        let mut sent = provider.alloc(64).wait().unwrap();
        sent.as_mut().fill(42);
        // the receiver's buffer releases a reference that a real sender would have added for it
        unsafe { sent.inner.inc_ref_count() };

        let reader = ShmReader::new(Arc::new(
            ShmClientStorage::builder()
                .with_default_client_set()
                .build(),
        ));
        let received = reader.read_shmbuf(sent.inner.info.clone()).unwrap();
        let data_id = get_global_id(
            received.protocol(),
            received.metadata.owned.header().data_descriptor().segment,
        );
        let metadata_id = sent.inner.info.metadata.id;
        let is_mapped = || {
            (
                reader.segments.contains(&data_id),
                GLOBAL_METADATA_SUBSCRIPTION
                    .read()
                    .linked_table
                    .contains(&metadata_id),
            )
        };

        // a live buffer keeps both mappings, however many sweeps pass
        for _ in 0..3 {
            sweep_all();
        }
        assert_eq!(is_mapped(), (true, true));
        assert!(received.as_slice().iter().all(|byte| *byte == 42));

        drop(received);
        // the first sweep only clears the "used" marks, the second releases the data segment
        sweep_all();
        sweep_all();
        assert!(!is_mapped().0);

        // the metadata mapping goes once the confirmator task has let go of it
        std::thread::sleep(Duration::from_millis(200));
        sweep_all();
        assert_eq!(is_mapped(), (false, false));
    }
}
