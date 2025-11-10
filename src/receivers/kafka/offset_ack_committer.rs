use crate::bounded_channel::BoundedReceiver;
use crate::receivers::kafka::offset_tracker::TopicTrackers;
use crate::receivers::kafka::receiver::{AssignedPartitions, KafkaConsumerContext};
use crate::topology::payload;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub struct KafkaOffsetCommitter {
    tick_interval: tokio::time::Interval,
    ack_receiver: BoundedReceiver<payload::KafkaAcknowledgement>,
    topic_trackers: Arc<TopicTrackers>,
    topic_names_to_id: HashMap<String, u8>,
    assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
    consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
}

impl KafkaOffsetCommitter {
    pub fn new(
        tick_every: Duration,
        ack_receiver: BoundedReceiver<payload::KafkaAcknowledgement>,
        topic_trackers: Arc<TopicTrackers>,
        topic_names_to_id: HashMap<String, u8>,
        assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
        consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
    ) -> Self {
        let tick_interval = tokio::time::interval(tick_every);

        KafkaOffsetCommitter {
            tick_interval,
            ack_receiver,
            topic_trackers,
            topic_names_to_id,
            assigned_partitions,
            consumer,
        }
    }

    pub async fn run(
        &mut self,
        cancel_token: CancellationToken,
    ) -> crate::receivers::kafka::error::Result<()> {
        loop {
            select! {
                biased;
                _ = self.tick_interval.tick() => {
                    commit_offset(self.assigned_partitions.clone(),
                        &self.topic_names_to_id,
                        &self.topic_trackers,
                        |tpl, mode| self.consumer.commit(tpl, mode));
                }
                 ack_result = self.ack_receiver.next() => {
                    match ack_result {
                        Some(ack) => {
                            match ack {
                                payload::KafkaAcknowledgement::Ack(kafka_ack) => {
                                    debug!("Current tracker pending is {}", self.topic_trackers.pending_count(kafka_ack.topic_id, kafka_ack.partition));
                                    debug!("Received ack for topic {} partition {} offset {}", kafka_ack.topic_id, kafka_ack.partition, kafka_ack.offset);
                                    self.topic_trackers.acknowledge(kafka_ack.topic_id, kafka_ack.partition, kafka_ack.offset);
                                }
                                payload::KafkaAcknowledgement::Nack(kafka_nack) => {
                                    warn!(
                                        "Message export failed - topic {} partition {} offset {}: {:?}",
                                        kafka_nack.topic_id, kafka_nack.partition, kafka_nack.offset, kafka_nack.reason
                                    );
                                    debug!("Current tracker pending is {}", self.topic_trackers.pending_count(kafka_nack.topic_id, kafka_nack.partition));
                                    debug!("Received nack for topic {} partition {} offset {}", kafka_nack.topic_id, kafka_nack.partition, kafka_nack.offset);
                                    // Treat nack like ack when disable_exporter_indefinite_retry is enabled
                                    // This acknowledges the offset to prevent blocking further processing
                                    self.topic_trackers.nack(kafka_nack.topic_id, kafka_nack.partition, kafka_nack.offset);
                                }
                            }
                        }
                        None => {
                            // Channel closed - exit the main loop
                            debug!("Ack channel closed, exiting offset committer run loop");
                            break;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    debug!("KafkaOffsetCommitter cancelled, draining pending acknowledgements");
                    break;
                }
            }
        }
        self.drain().await;
        Ok(())
    }

    pub async fn drain(&mut self) {
        let drain_start = tokio::time::Instant::now();
        let drain_deadline = drain_start + Duration::from_secs(2);
        let mut ack_count = 0;

        // Simple drain loop with deadline check
        loop {
            if tokio::time::Instant::now() >= drain_deadline {
                debug!(
                    "Drain deadline reached after processing {} acknowledgements",
                    ack_count
                );
                break;
            }
            let poll_res = timeout(Duration::from_millis(500), self.ack_receiver.next()).await;
            match poll_res {
                Ok(Some(ack)) => {
                    match ack {
                        payload::KafkaAcknowledgement::Ack(kafka_ack) => {
                            debug!(
                                "Draining ack for topic {} partition {} offset {}",
                                kafka_ack.topic_id, kafka_ack.partition, kafka_ack.offset
                            );
                            self.topic_trackers.acknowledge(
                                kafka_ack.topic_id,
                                kafka_ack.partition,
                                kafka_ack.offset,
                            );
                            ack_count += 1;
                        }
                        payload::KafkaAcknowledgement::Nack(kafka_nack) => {
                            warn!(
                                "Message export failed - topic {} partition {} offset {}: {:?}",
                                kafka_nack.topic_id,
                                kafka_nack.partition,
                                kafka_nack.offset,
                                kafka_nack.reason
                            );
                            // Treat nack like ack when disable_exporter_indefinite_retry is enabled
                            // This acknowledges the offset to prevent blocking further processing
                            self.topic_trackers.nack(
                                kafka_nack.topic_id,
                                kafka_nack.partition,
                                kafka_nack.offset,
                            );
                            ack_count += 1;
                        }
                    }
                }
                Ok(None) => {
                    debug!("Ack channel closed during drain after {} acks", ack_count);
                    break;
                }
                Err(_) => {
                    debug!(
                        "Error polling ack channel with time out during drain after {} acks",
                        ack_count
                    );
                }
            }
        }

        if ack_count > 0 {
            debug!("Drained {} pending acknowledgements", ack_count);
        }

        // Perform the final commit using the existing commit_offset function
        debug!("Performing final commit after draining");
        commit_offset(
            self.assigned_partitions.clone(),
            &self.topic_names_to_id,
            &self.topic_trackers,
            |tpl, mode| self.consumer.commit(tpl, mode),
        );
    }
}

pub fn commit_offset<F>(
    assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
    topic_name_to_id: &HashMap<String, u8>,
    topic_trackers: &Arc<TopicTrackers>,
    commit_fn: F,
) where
    F: FnOnce(
        &rdkafka::topic_partition_list::TopicPartitionList,
        CommitMode,
    ) -> rdkafka::error::KafkaResult<()>,
{
    let mut commits = Vec::new();

    {
        // Lock and clone assigned partitions
        let assigned = assigned_partitions.lock().unwrap();

        // Build list of topic/partition/offset to commit
        for (topic_name, partitions) in assigned.topics.iter() {
            let topic_id = match topic_name_to_id.get(topic_name) {
                Some(id) => *id,
                None => {
                    debug!("Unknown topic name in assigned partitions: {}", topic_name);
                    continue;
                }
            };

            // For each assigned partition, determine the offset to commit
            for &partition in partitions.iter() {
                // Try to get lowest pending offset first
                if let Some(offset) = topic_trackers.lowest_pending_offset(topic_id, partition) {
                    debug!(
                        "Topic {} (id {}) partition {} has pending offset: {}",
                        topic_name, topic_id, partition, offset
                    );
                    commits.push((topic_name.clone(), partition, offset));
                } else if let Some(hwm) = topic_trackers.high_water_mark(topic_id, partition) {
                    debug!(
                        "Topic {} (id {}) partition {} has no pending offsets, using high water mark: {}",
                        topic_name, topic_id, partition, hwm
                    );
                    commits.push((topic_name.clone(), partition, hwm + 1));
                } else {
                    debug!(
                        "Topic {} (id {}) partition {} has no pending offsets and no high water mark",
                        topic_name, topic_id, partition
                    );
                }
            }
        }
    }

    if !commits.is_empty() {
        debug!("Built commit list with {} entries", commits.len());
        // Build TopicPartitionList for commit
        let mut topic_partition_list = rdkafka::topic_partition_list::TopicPartitionList::new();
        for (topic_name, partition, offset) in commits.iter() {
            debug!(
                "Committing: topic {} partition {} offset {}",
                topic_name, partition, offset
            );
            topic_partition_list
                .add_partition_offset(
                    topic_name,
                    *partition,
                    rdkafka::topic_partition_list::Offset::Offset(*offset),
                )
                .map_err(|e| {
                    warn!("Failed to add partition to commit list: {:?}", e);
                    e
                })
                .ok();
        }

        // Commit the offsets to Kafka using the provided commit function
        match commit_fn(&topic_partition_list, CommitMode::Sync) {
            Ok(_) => {
                debug!(
                    "Successfully committed offsets for {} partitions",
                    commits.len()
                );
            }
            Err(e) => {
                warn!("Failed to commit offsets to Kafka: {:?}", e);
            }
        }
    }
}
