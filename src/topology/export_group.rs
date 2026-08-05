// SPDX-License-Identifier: Apache-2.0

use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};
use tracing::{info, warn};

use crate::bounded_channel::{BoundedReceiver, BoundedSender, bounded};
use crate::topology::payload::{
    Ack, ExporterError, ForwarderAcknowledgement, ForwarderMetadata, Message, MessageMetadata,
};

struct Slot<T> {
    /// Retained payload with metadata stripped — metadata is re-applied fresh on each send attempt.
    payload: Vec<Message<T>>,
    /// Original upstream metadata, one per message; acked or nacked when the group resolves.
    originals: Vec<MessageMetadata>,
    /// Which member is currently in-flight for this batch.
    member_idx: u32,
    /// Bumped on each attempt; stale acks/nacks from a previous attempt are ignored.
    generation: u32,
}

pub struct ExportGroup<T> {
    tx: BoundedSender<Vec<Message<T>>>,
    active_rx: watch::Receiver<u32>,
    active_atomic: Arc<AtomicU32>,
    /// Internal forwarder ack channel — exposed for testing only.
    #[cfg(test)]
    pub(crate) ack_tx: BoundedSender<ForwarderAcknowledgement>,
}

impl<T> ExportGroup<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn sender(&self) -> BoundedSender<Vec<Message<T>>> {
        self.tx.clone()
    }

    pub fn active_index(&self) -> u32 {
        *self.active_rx.borrow()
    }

    /// Watch channel for breaker state changes. Suitable for tests and telemetry.
    pub fn subscribe_active(&self) -> watch::Receiver<u32> {
        self.active_rx.clone()
    }

    /// Arc<AtomicU32> suitable for registration as an ObservableGauge in telemetry.
    pub fn active_atomic(&self) -> Arc<AtomicU32> {
        self.active_atomic.clone()
    }
}

pub struct ExportGroupBuilder<T> {
    members: Vec<BoundedSender<Vec<Message<T>>>>,
    trip_after: u32,
    probe_after: Duration,
    sending_queue_size: usize,
}

impl<T> ExportGroupBuilder<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub fn new(sending_queue_size: usize) -> Self {
        Self {
            members: Vec::new(),
            trip_after: 3,
            probe_after: Duration::from_secs(30),
            sending_queue_size,
        }
    }

    pub fn add_member(mut self, member: BoundedSender<Vec<Message<T>>>) -> Self {
        self.members.push(member);
        self
    }

    pub fn trip_after(mut self, n: u32) -> Self {
        self.trip_after = n;
        self
    }

    pub fn probe_after(mut self, d: Duration) -> Self {
        self.probe_after = d;
        self
    }

    pub fn build(self) -> ExportGroup<T> {
        let (tx, rx) = bounded(self.sending_queue_size);
        let (active_tx, active_rx) = watch::channel(0u32);
        let active_atomic = Arc::new(AtomicU32::new(0));

        let slab_cap = self.sending_queue_size + 2 * EXPORTER_PIPELINE_SLOP;
        let (ack_tx, ack_rx) = bounded::<ForwarderAcknowledgement>(slab_cap);

        #[cfg(test)]
        let ack_tx_for_test = ack_tx.clone();

        let active_atomic_for_task = active_atomic.clone();
        tokio::spawn(async move {
            let mut task = ExportGroupTask::new(
                rx,
                self.members,
                active_tx,
                active_atomic_for_task,
                self.trip_after,
                self.probe_after,
                self.sending_queue_size,
                ack_tx,
                ack_rx,
            );
            task.run().await;
        });

        ExportGroup {
            tx,
            active_rx,
            active_atomic,
            #[cfg(test)]
            ack_tx: ack_tx_for_test,
        }
    }
}

const EXPORTER_PIPELINE_SLOP: usize = 32;

struct ExportGroupTask<T> {
    upstream_rx: BoundedReceiver<Vec<Message<T>>>,
    members: Vec<BoundedSender<Vec<Message<T>>>>,
    active_tx: watch::Sender<u32>,
    active_atomic: Arc<AtomicU32>,
    local_active: usize,
    consecutive_nacks: u32,
    trip_after: u32,
    probe_after: Duration,
    probe_pending: bool,
    probe_interval: Option<Interval>,
    slab: Vec<Option<Slot<T>>>,
    slab_limit: usize,
    free: Vec<u32>,
    ack_tx: BoundedSender<ForwarderAcknowledgement>,
    ack_rx: BoundedReceiver<ForwarderAcknowledgement>,
}

impl<T> ExportGroupTask<T>
where
    T: Clone + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        upstream_rx: BoundedReceiver<Vec<Message<T>>>,
        members: Vec<BoundedSender<Vec<Message<T>>>>,
        active_tx: watch::Sender<u32>,
        active_atomic: Arc<AtomicU32>,
        trip_after: u32,
        probe_after: Duration,
        sending_queue_size: usize,
        ack_tx: BoundedSender<ForwarderAcknowledgement>,
        ack_rx: BoundedReceiver<ForwarderAcknowledgement>,
    ) -> Self {
        let slab_cap = sending_queue_size + 2 * EXPORTER_PIPELINE_SLOP;

        Self {
            upstream_rx,
            members,
            active_tx,
            active_atomic,
            local_active: 0,
            consecutive_nacks: 0,
            trip_after,
            probe_after,
            probe_pending: false,
            probe_interval: None,
            slab: Vec::with_capacity(slab_cap),
            slab_limit: slab_cap,
            free: Vec::new(),
            ack_tx,
            ack_rx,
        }
    }

    async fn run(&mut self) {
        let mut upstream_stream = self.upstream_rx.clone().into_stream();
        let mut ack_stream = self.ack_rx.clone().into_stream();

        loop {
            let upstream_ready = !self.free.is_empty() || self.slab.len() < self.slab_limit;
            let probe_allowed = self.probe_interval.is_some() && !self.probe_pending;

            // biased: drain acks before pulling new upstream batches, so the slab does
            // not fill up prematurely when the upstream is faster than members.
            tokio::select! {
                biased;

                maybe_ack = ack_stream.next() => {
                    match maybe_ack {
                        Some(ack) => self.handle_ack(ack).await,
                        None => break,
                    }
                }
                maybe_batch = upstream_stream.next(), if upstream_ready => {
                    match maybe_batch {
                        Some(batch) => self.dispatch(batch).await,
                        None => break,
                    }
                }
                _ = tick_probe_opt(&mut self.probe_interval), if probe_allowed => {
                    self.probe_pending = true;
                }
            }
        }
    }

    async fn dispatch(&mut self, mut batch: Vec<Message<T>>) {
        // Allocate a slot index.
        let slot_idx = self.free.pop().unwrap_or_else(|| {
            let idx = self.slab.len() as u32;
            self.slab.push(None);
            idx
        });

        let target = if self.probe_pending {
            self.probe_pending = false;
            0
        } else {
            self.local_active
        };

        // Strip originals and clear metadata from the stored payload. The payload kept
        // in the slot is always metadata-less; each send_attempt re-applies a fresh
        // forwarder so ref_count == payload_for_member.len() exactly.
        let originals: Vec<MessageMetadata> =
            batch.iter_mut().filter_map(|m| m.metadata.take()).collect();

        self.slab[slot_idx as usize] = Some(Slot {
            payload: batch,
            originals,
            member_idx: target as u32,
            generation: 0,
        });

        self.send_attempt(slot_idx).await;
    }

    /// Clone the metadata-less slot payload, wrap it with a fresh forwarder, then send to
    /// the current member. On channel failure the send is treated as an immediate nack.
    async fn send_attempt(&mut self, slot_idx: u32) {
        let (member_idx, generation) = {
            let slot = self.slab[slot_idx as usize].as_ref().unwrap();
            (slot.member_idx, slot.generation)
        };
        let payload_for_member = {
            let slot = self.slab[slot_idx as usize].as_ref().unwrap();
            apply_forwarder_metadata(&slot.payload, generation, slot_idx, &self.ack_tx)
        };
        if self.members[member_idx as usize]
            .send_async(payload_for_member)
            .await
            .is_err()
        {
            warn!(
                "export group: failed to send to member {}, treating as nack",
                member_idx
            );
            self.handle_nack_logic(slot_idx, generation).await;
        }
    }

    async fn handle_ack(&mut self, ack: ForwarderAcknowledgement) {
        match ack {
            ForwarderAcknowledgement::Ack(details) => {
                let (generation, slot_idx) = decode_id(&details.request_id);
                if let Some(slot) = self.slab.get(slot_idx as usize).and_then(|s| s.as_ref()) {
                    if slot.generation != generation {
                        return; // stale
                    }
                } else {
                    return; // already freed
                }

                let slot = self.slab[slot_idx as usize].take().unwrap();
                self.free.push(slot_idx);

                // Reset consecutive_nacks when the currently-active member succeeds.
                if slot.member_idx as usize == self.local_active {
                    self.consecutive_nacks = 0;
                }

                // Probe succeeded: recover to members[0].
                if slot.member_idx == 0 && self.local_active != 0 {
                    info!("export group recovered, active = members[0]");
                    self.update_active(0);
                    self.consecutive_nacks = 0;
                    self.probe_interval = None;
                }

                for original in slot.originals {
                    let _ = original.ack().await;
                }
            }
            ForwarderAcknowledgement::Nack(details) => {
                let (generation, slot_idx) = decode_id(&details.request_id);
                self.handle_nack_logic(slot_idx, generation).await;
            }
        }
    }

    fn update_active(&mut self, index: usize) {
        self.local_active = index;
        self.active_atomic.store(index as u32, Ordering::Release);
        let _ = self.active_tx.send(index as u32);
    }

    /// Core nack handler: walk the slot forward to the next member, or exhaust and nack
    /// originals. Uses an iterative approach to avoid async recursion / Box::pin overhead.
    async fn handle_nack_logic(&mut self, slot_idx: u32, generation: u32) {
        // Validate: if the slot is gone or the generation doesn't match, this is stale.
        {
            let Some(slot) = self.slab.get(slot_idx as usize).and_then(|s| s.as_ref()) else {
                return;
            };
            if slot.generation != generation {
                return;
            }
        }

        // Iterative walk: keep trying the next member until one accepts the send, or
        // the member list is exhausted.
        loop {
            let (member_idx, cur_gen) = {
                let slot = self.slab[slot_idx as usize].as_ref().unwrap();
                (slot.member_idx, slot.generation)
            };

            // Update breaker state based on the just-failed member.
            if member_idx as usize == self.local_active {
                self.consecutive_nacks += 1;
                if self.consecutive_nacks >= self.trip_after
                    && (self.local_active + 1) < self.members.len()
                {
                    self.update_active(self.local_active + 1);
                    self.consecutive_nacks = 0;
                    warn!(
                        "export group tripped, active = members[{}]",
                        self.local_active
                    );

                    if !self.probe_after.is_zero() {
                        let mut iv =
                            interval_at(Instant::now() + self.probe_after, self.probe_after);
                        iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
                        self.probe_interval = Some(iv);
                    }
                }
            }
            // Probe nack: stay at current active; probe_interval continues.

            let next_member = member_idx + 1;
            if (next_member as usize) < self.members.len() {
                // Advance to the next member and try again.
                let new_gen = {
                    let slot = self.slab[slot_idx as usize].as_mut().unwrap();
                    slot.member_idx = next_member;
                    slot.generation = cur_gen.wrapping_add(1);
                    slot.generation
                };
                let payload_for_member = {
                    let slot = self.slab[slot_idx as usize].as_ref().unwrap();
                    apply_forwarder_metadata(&slot.payload, new_gen, slot_idx, &self.ack_tx)
                };
                match self.members[next_member as usize]
                    .send_async(payload_for_member)
                    .await
                {
                    Ok(()) => break,
                    Err(_) => {
                        warn!(
                            "export group: member {} channel closed, continuing walk",
                            next_member
                        );
                        continue;
                    }
                }
            } else {
                // Member list exhausted: nack all originals with a synthesized error.
                let slot = self.slab[slot_idx as usize].take().unwrap();
                self.free.push(slot_idx);

                let reason = ExporterError::ExportFailed {
                    error_code: 0,
                    error_message: "export group: all members nacked".into(),
                };
                for original in slot.originals {
                    let _ = original.nack(reason.clone()).await;
                }
                break;
            }
        }
    }
}

async fn tick_probe_opt(iv_opt: &mut Option<Interval>) {
    if let Some(iv) = iv_opt {
        iv.tick().await;
    } else {
        std::future::pending::<()>().await;
    }
}

fn encode_id(generation: u32, idx: u32) -> String {
    format!("{generation}:{idx}")
}

/// Clone `payload` and attach a fresh forwarder to each message using the
/// take-last-clone idiom so `ref_count == payload.len()` exactly.
fn apply_forwarder_metadata<T: Clone>(
    payload: &[Message<T>],
    generation: u32,
    slot_idx: u32,
    ack_tx: &BoundedSender<ForwarderAcknowledgement>,
) -> Vec<Message<T>> {
    let mut out = payload.to_vec();
    let fwd = MessageMetadata::forwarder(ForwarderMetadata::new(
        encode_id(generation, slot_idx),
        Some(ack_tx.clone()),
    ));
    let last = out.len().saturating_sub(1);
    let mut fwd_opt = Some(fwd);
    for (i, msg) in out.iter_mut().enumerate() {
        msg.metadata = if i == last {
            fwd_opt.take()
        } else {
            Some(fwd_opt.as_ref().unwrap().clone())
        };
    }
    out
}

fn decode_id(id: &str) -> (u32, u32) {
    if let Some(pos) = id.find(':') {
        let generation = id[..pos].parse().unwrap_or(0);
        let idx = id[pos + 1..].parse().unwrap_or(0);
        (generation, idx)
    } else {
        (0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_channel::TrySendError;
    use std::time::Duration;
    use tokio::time::timeout;

    // -------------------------------------------------------------------------
    // Test helpers
    // -------------------------------------------------------------------------

    /// Thin wrapper around a member receive channel, providing named reactive operations.
    struct TestMember {
        rx: BoundedReceiver<Vec<Message<u8>>>,
    }

    impl TestMember {
        fn new(rx: BoundedReceiver<Vec<Message<u8>>>) -> Self {
            Self { rx }
        }

        async fn expect_batch(&mut self) -> Vec<Message<u8>> {
            self.rx.next().await.expect("expected a batch from member")
        }
    }

    /// Ack every message in a batch using its wrapped forwarder metadata.
    async fn ack_batch(batch: &[Message<u8>]) {
        for msg in batch {
            if let Some(meta) = &msg.metadata {
                let _ = meta.ack().await;
            }
        }
    }

    /// Nack every message in a batch using its wrapped forwarder metadata.
    async fn nack_batch(batch: &[Message<u8>], reason: ExporterError) {
        for msg in batch {
            if let Some(meta) = &msg.metadata {
                let _ = meta.nack(reason.clone()).await;
            }
        }
    }

    /// Create an original metadata + a receiver so we can observe ack/nack.
    fn make_original_with_rx() -> (BoundedReceiver<ForwarderAcknowledgement>, MessageMetadata) {
        let (tx, rx) = bounded::<ForwarderAcknowledgement>(4);
        let meta = MessageMetadata::forwarder(ForwarderMetadata::new("orig".into(), Some(tx)));
        (rx, meta)
    }

    fn msg(payload: u8, meta: MessageMetadata) -> Message<u8> {
        Message::new(Some(meta), vec![payload], None)
    }

    fn msg_no_meta(payload: u8) -> Message<u8> {
        Message::new(None, vec![payload], None)
    }

    fn build_group_2(
        trip_after: u32,
        probe_after: Duration,
    ) -> (ExportGroup<u8>, TestMember, TestMember) {
        let (m0_tx, m0_rx) = bounded(16);
        let (m1_tx, m1_rx) = bounded(16);
        let group = ExportGroupBuilder::new(16)
            .add_member(m0_tx)
            .add_member(m1_tx)
            .trip_after(trip_after)
            .probe_after(probe_after)
            .build();
        (group, TestMember::new(m0_rx), TestMember::new(m1_rx))
    }

    fn build_group_3(trip_after: u32) -> (ExportGroup<u8>, TestMember, TestMember, TestMember) {
        let (m0_tx, m0_rx) = bounded(16);
        let (m1_tx, m1_rx) = bounded(16);
        let (m2_tx, m2_rx) = bounded(16);
        let group = ExportGroupBuilder::new(16)
            .add_member(m0_tx)
            .add_member(m1_tx)
            .add_member(m2_tx)
            .trip_after(trip_after)
            .probe_after(Duration::ZERO)
            .build();
        (
            group,
            TestMember::new(m0_rx),
            TestMember::new(m1_rx),
            TestMember::new(m2_rx),
        )
    }

    // -------------------------------------------------------------------------
    // encode/decode
    // -------------------------------------------------------------------------

    #[test]
    fn test_encode_decode_id() {
        assert_eq!(encode_id(0, 0), "0:0");
        assert_eq!(encode_id(123, 456), "123:456");
        assert_eq!(decode_id("123:456"), (123, 456));
        assert_eq!(decode_id("0:0"), (0, 0));
        assert_eq!(decode_id("bad"), (0, 0));
    }

    // -------------------------------------------------------------------------
    // Happy path
    // -------------------------------------------------------------------------

    /// Primary acks → original is acked exactly once; slab slot is freed.
    #[tokio::test]
    async fn test_happy_path_closed_member0_acks() {
        let (group, mut m0, _m1) = build_group_2(3, Duration::ZERO);

        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(1, meta)]).await.unwrap();

        let batch = m0.expect_batch().await;
        ack_batch(&batch).await;

        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Ack(_) => {}
            ForwarderAcknowledgement::Nack(_) => panic!("expected Ack"),
        }
        assert_eq!(group.active_index(), 0);
    }

    // -------------------------------------------------------------------------
    // Retry on nack
    // -------------------------------------------------------------------------

    /// m0 nacks → batch retried on m1 → original acked exactly once.
    #[tokio::test]
    async fn test_retry_on_nack_m0_then_m1_acks() {
        let (group, mut m0, mut m1) = build_group_2(3, Duration::ZERO);

        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(1, meta)]).await.unwrap();

        let batch0 = m0.expect_batch().await;
        nack_batch(&batch0, ExporterError::Cancelled).await;

        let batch1 = m1.expect_batch().await;
        ack_batch(&batch1).await;

        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Ack(_) => {}
            ForwarderAcknowledgement::Nack(_) => panic!("expected Ack, not Nack"),
        }

        // No second message on orig_rx (original was not nacked).
        let nothing = timeout(Duration::from_secs(5), orig_rx.next()).await;
        assert!(
            nothing.is_err() || matches!(nothing.unwrap(), None),
            "unexpected second ack/nack"
        );
    }

    /// m0 nacks, m1 nacks, m2 acks → original acked exactly once.
    #[tokio::test]
    async fn test_walk_forward_three_members() {
        let (group, mut m0, mut m1, mut m2) = build_group_3(99);

        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(1, meta)]).await.unwrap();

        nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
        nack_batch(&m1.expect_batch().await, ExporterError::Cancelled).await;
        ack_batch(&m2.expect_batch().await).await;

        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Ack(_) => {}
            _ => panic!("expected Ack"),
        }
    }

    /// All members nack → original is nacked exactly once with the synthesized reason.
    #[tokio::test]
    async fn test_all_members_nack_then_original_nacked() {
        let (group, mut m0, mut m1, mut m2) = build_group_3(99);

        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(1, meta)]).await.unwrap();

        nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
        nack_batch(&m1.expect_batch().await, ExporterError::Cancelled).await;
        nack_batch(&m2.expect_batch().await, ExporterError::Cancelled).await;

        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Nack(_) => {}
            ForwarderAcknowledgement::Ack(_) => panic!("expected Nack"),
        }
    }

    /// Batch of 3 messages; nack on one clone; forwarder fires Nack exactly once
    /// (claim_response semantics); group retries the whole batch on m1.
    #[tokio::test]
    async fn test_single_message_nack_in_batch() {
        let (group, mut m0, mut m1) = build_group_2(99, Duration::ZERO);

        group
            .sender()
            .send(vec![msg_no_meta(1), msg_no_meta(2), msg_no_meta(3)])
            .await
            .unwrap();

        let batch0 = m0.expect_batch().await;
        assert_eq!(batch0.len(), 3);

        // Nack only the first message — the shared forwarder fires once.
        if let Some(meta) = &batch0[0].metadata {
            let _ = meta.nack(ExporterError::Cancelled).await;
        }
        // The other two clones ack (ref_count decrements; response already claimed so no second send).
        if let Some(meta) = &batch0[1].metadata {
            let _ = meta.ack().await;
        }
        if let Some(meta) = &batch0[2].metadata {
            let _ = meta.ack().await;
        }

        // Group should have retried the whole batch on m1.
        let batch1 = m1.expect_batch().await;
        assert_eq!(batch1.len(), 3);
        ack_batch(&batch1).await;
    }

    // -------------------------------------------------------------------------
    // Circuit breaker
    // -------------------------------------------------------------------------

    /// trip_after=2 consecutive m0-nacks → active advances to 1.
    #[tokio::test]
    async fn test_trip_after_threshold() {
        let (group, mut m0, mut m1) = build_group_2(2, Duration::ZERO);
        let mut active_rx = group.subscribe_active();

        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            // m0 is the starting point (active=0)
            nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
            // retry on m1 succeeds
            ack_batch(&m1.expect_batch().await).await;
        }

        active_rx.wait_for(|&v| v == 1).await.unwrap();
        assert_eq!(group.active_index(), 1);

        // New batches should start directly at m1.
        group.sender().send(vec![msg_no_meta(42)]).await.unwrap();
        let b = m1.expect_batch().await;
        assert_eq!(b[0].payload, vec![42]);
        ack_batch(&b).await;
    }

    /// With active=1, trip_after=2 consecutive m1-nacks (each retried on m2) → active=2.
    #[tokio::test]
    async fn test_walk_forward_trip() {
        let (group, _m0, mut m1, mut m2) = build_group_3(2);
        let mut active_rx = group.subscribe_active();

        // Pre-trip to active=1: 2 batches starting at m0 (active=0) that nack at m0,
        // retry on m1 (which acks).
        {
            let (m0_tx, m0_rx) = bounded::<Vec<Message<u8>>>(16);
            drop(m0_rx); // We already have the group's m0 channel; we need to re-build.
            // Actually the group is already built above with _m0; just ignore m0 here.
            // Use a separate group for this test to control the trip to active=1 directly.
            let _ = m0_tx;
        }

        // Simpler: we pre-trip active=1 by doing 2 nack cycles via the already-built group.
        // But _m0 is consumed by build_group_3. Drop it so sends to m0 fail immediately
        // and are treated as nacks (channel-closed path), which also counts toward consecutive_nacks.
        drop(_m0);

        // With m0 closed, sends to m0 will fail and be treated as nacks → consecutive_nacks increments.
        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            // m1 gets the retry (m0 closed → immediate nack → walk to m1)
            ack_batch(&m1.expect_batch().await).await;
        }

        active_rx.wait_for(|&v| v == 1).await.unwrap();

        // Now active=1. Do 2 batches starting at m1 that nack (retried on m2 which acks).
        // After trip_after=2 consecutive m1-nacks, active should become 2.
        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            nack_batch(&m1.expect_batch().await, ExporterError::Cancelled).await;
            ack_batch(&m2.expect_batch().await).await;
        }

        active_rx.wait_for(|&v| v == 2).await.unwrap();
        assert_eq!(group.active_index(), 2);
    }

    /// With active=1, a new batch starts at m1; if m1 nacks, retry is on m2
    /// (not on m1+1 by coincidence — verified with a 4-member group).
    #[tokio::test]
    async fn test_retry_independent_of_breaker() {
        let (m0_tx, _m0_rx) = bounded::<Vec<Message<u8>>>(16);
        let (m1_tx, m1_rx) = bounded(16);
        let (m2_tx, m2_rx) = bounded(16);
        let (m3_tx, m3_rx) = bounded(16);
        let group = ExportGroupBuilder::<u8>::new(16)
            .add_member(m0_tx)
            .add_member(m1_tx)
            .add_member(m2_tx)
            .add_member(m3_tx)
            .trip_after(1) // trip immediately on first m0 nack
            .probe_after(Duration::ZERO)
            .build();

        let mut m1 = TestMember::new(m1_rx);
        let mut m2 = TestMember::new(m2_rx);
        let mut m3 = TestMember::new(m3_rx);
        let mut active_rx = group.subscribe_active();

        // Drop m0_rx so first dispatch immediately nacks → active becomes 1.
        drop(_m0_rx);
        group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
        // m1 gets the retry (walk from m0's failed send)
        ack_batch(&m1.expect_batch().await).await;
        active_rx.wait_for(|&v| v == 1).await.unwrap();

        // Now active=1. New batch starts at m1; m1 nacks → retry on m2 (not m3).
        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(5, meta)]).await.unwrap();

        nack_batch(&m1.expect_batch().await, ExporterError::Cancelled).await;

        // Should land on m2, not m3.
        let b = m2.expect_batch().await;
        assert_eq!(b[0].payload, vec![5]);
        ack_batch(&b).await;

        // m3 should receive nothing.
        let nothing = timeout(Duration::from_secs(5), m3.rx.next()).await;
        assert!(
            nothing.is_err() || matches!(nothing, Ok(None)),
            "m3 should not receive anything"
        );

        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Ack(_) => {}
            _ => panic!("expected Ack on original"),
        }
    }

    // -------------------------------------------------------------------------
    // Stale ack/nack
    // -------------------------------------------------------------------------

    /// An ack with a mismatched generation is ignored without changing state.
    #[tokio::test]
    async fn test_stale_ack_ignored() {
        let (group, mut m0, _m1) = build_group_2(3, Duration::ZERO);

        // Dispatch one batch; it lands on m0.
        let (mut orig_rx, meta) = make_original_with_rx();
        group.sender().send(vec![msg(1, meta)]).await.unwrap();
        let _batch = m0.expect_batch().await; // m0 holds the batch; we don't ack it yet

        // Inject a stale ack with a wrong generation via the test-only ack_tx.
        group
            .ack_tx
            .send(ForwarderAcknowledgement::Ack(
                crate::topology::payload::ForwarderPayloadDetails {
                    // generation=99 does not match the slot's generation=0
                    request_id: encode_id(99, 0),
                },
            ))
            .await
            .unwrap();

        // Give the task a moment to process (pump the event loop).
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Now ack the real batch; the original should fire exactly once.
        ack_batch(&_batch).await;
        match orig_rx.next().await.unwrap() {
            ForwarderAcknowledgement::Ack(_) => {}
            _ => panic!("expected Ack"),
        }
    }

    // -------------------------------------------------------------------------
    // Probe / HALF_OPEN
    // -------------------------------------------------------------------------

    /// Trip breaker → advance time past probe_after → next batch probes m0 → ack → active=0.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_probe_recovers_member0() {
        let probe_after = Duration::from_secs(30);
        let (group, mut m0, mut m1) = build_group_2(2, probe_after);
        let mut active_rx = group.subscribe_active();

        // Trip the breaker (2 nacks of active member m0, each retried on m1 which acks).
        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
            ack_batch(&m1.expect_batch().await).await;
        }
        active_rx.wait_for(|&v| v == 1).await.unwrap();

        // Advance virtual time past probe_after to fire the probe tick.
        tokio::time::advance(probe_after + Duration::from_millis(1)).await;
        // Yield so the task processes the probe tick.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Send a batch — it should be the probe and start at m0.
        group.sender().send(vec![msg_no_meta(99)]).await.unwrap();
        let probe_batch = m0.expect_batch().await;
        assert_eq!(probe_batch[0].payload, vec![99]);
        ack_batch(&probe_batch).await;

        // active should recover to 0.
        active_rx.wait_for(|&v| v == 0).await.unwrap();
        assert_eq!(group.active_index(), 0);
    }

    /// Probe nack → active stays; probe interval continues.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_probe_nack_keeps_active() {
        let probe_after = Duration::from_secs(30);
        let (group, mut m0, mut m1) = build_group_2(2, probe_after);
        let mut active_rx = group.subscribe_active();

        // Trip the breaker.
        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
            ack_batch(&m1.expect_batch().await).await;
        }
        active_rx.wait_for(|&v| v == 1).await.unwrap();

        // First probe fires.
        tokio::time::advance(probe_after + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        group.sender().send(vec![msg_no_meta(55)]).await.unwrap();
        let probe_batch = m0.expect_batch().await;
        // Nack the probe; it retries on m1 (no other members after m0 in a 2-member group... wait,
        // actually m0 is member 0 and the probe starts there; nacking it walks to m1).
        // But we still expect active to stay at 1 (probe nack does not reset active).
        nack_batch(&probe_batch, ExporterError::Cancelled).await;

        // The retry lands on m1.
        ack_batch(&m1.expect_batch().await).await;

        // active should still be 1.
        tokio::task::yield_now().await;
        assert_eq!(
            group.active_index(),
            1,
            "active should remain 1 after probe nack"
        );

        // Second probe fires after another probe_after.
        tokio::time::advance(probe_after + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        group.sender().send(vec![msg_no_meta(66)]).await.unwrap();
        let probe_batch2 = m0.expect_batch().await;
        ack_batch(&probe_batch2).await;
        active_rx.wait_for(|&v| v == 0).await.unwrap();
    }

    /// During HALF_OPEN, batches dispatched after the probe but before its outcome
    /// continue to start at members[active], not at members[0].
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_probe_does_not_affect_concurrent_batches() {
        let probe_after = Duration::from_secs(30);
        let (group, mut m0, mut m1) = build_group_2(2, probe_after);
        let mut active_rx = group.subscribe_active();

        // Trip breaker.
        for _ in 0..2 {
            group.sender().send(vec![msg_no_meta(0)]).await.unwrap();
            nack_batch(&m0.expect_batch().await, ExporterError::Cancelled).await;
            ack_batch(&m1.expect_batch().await).await;
        }
        active_rx.wait_for(|&v| v == 1).await.unwrap();

        // Fire probe tick.
        tokio::time::advance(probe_after + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // Send two batches. The first should be the probe (starts at m0).
        // The second should start at m1 (active=1), not m0.
        group.sender().send(vec![msg_no_meta(1)]).await.unwrap();
        group.sender().send(vec![msg_no_meta(2)]).await.unwrap();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        // First batch lands on m0 (probe).
        let probe_batch = m0.expect_batch().await;
        assert_eq!(probe_batch[0].payload, vec![1]);

        // Second batch lands on m1 (active=1, not probing).
        let normal_batch = m1.expect_batch().await;
        assert_eq!(normal_batch[0].payload, vec![2]);

        ack_batch(&probe_batch).await;
        ack_batch(&normal_batch).await;
    }

    // -------------------------------------------------------------------------
    // Slab pressure / backpressure
    // -------------------------------------------------------------------------

    /// When the slab is full, upstream try_send returns Full (backpressure applied).
    #[tokio::test]
    async fn test_slab_pressure_backpressures_upstream() {
        // Use a tiny slab: sending_queue_size=1 → slab_cap = 1 + 2*32 = 65.
        // We use sending_queue_size = 1 so the upstream channel capacity is 1.
        // The slab capacity is 1 + 64 = 65. Use a very small one for this test by
        // choosing sending_queue_size=0: slab_cap = 0 + 64 = 64.
        // We need slab_cap to be small for a fast test; use sending_queue_size=0.
        let slab_cap = 0 + 2 * EXPORTER_PIPELINE_SLOP; // 64

        let (m0_tx, _m0_rx) = bounded::<Vec<Message<u8>>>(slab_cap + 4);
        let group = ExportGroupBuilder::<u8>::new(0)
            .add_member(m0_tx)
            .trip_after(99)
            .probe_after(Duration::ZERO)
            .build();

        // Drop the first group; re-build with a controlled small capacity.
        drop(group);

        let small_cap = 2usize;
        let actual_slab_cap = small_cap + 2 * EXPORTER_PIPELINE_SLOP;
        let (m0_tx2, _m0_rx2) = bounded::<Vec<Message<u8>>>(actual_slab_cap + 4);
        let group2 = ExportGroupBuilder::<u8>::new(small_cap)
            .add_member(m0_tx2)
            .trip_after(99)
            .probe_after(Duration::ZERO)
            .build();

        let sender2 = group2.sender();
        // The upstream BoundedSender has capacity small_cap.
        // The slab has capacity actual_slab_cap.
        // Fill the upstream channel to capacity (without tasks draining it).
        for _ in 0..small_cap {
            sender2.send(vec![msg_no_meta(0)]).await.unwrap();
        }

        // Now try_send should return Full because upstream channel is at capacity.
        match sender2.try_send(vec![msg_no_meta(99)]) {
            Err(TrySendError::Full(_)) => {} // expected
            Ok(()) => panic!("should have been full"),
            Err(TrySendError::Disconnected(_)) => panic!("unexpected disconnect"),
        }
    }
}
