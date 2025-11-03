use crate::bounded_channel::BoundedReceiver;
use crate::receivers::kafka::offset_tracker::TopicTrackers;
use crate::receivers::kafka::receiver::{AssignedPartitions, KafkaConsumerContext};
use crate::topology::payload;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

pub struct KafkaOffsetCommitter {
    tick_interval: tokio::time::Interval,
    ack_receiver: BoundedReceiver<payload::KafkaAcknowledgement>,
    topic_trackers: Arc<TopicTrackers>,
    topic_name_to_id: HashMap<String, u8>,
    assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
    consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
}

impl KafkaOffsetCommitter {
    pub fn new(
        tick_every: std::time::Duration,
        ack_receiver: BoundedReceiver<payload::KafkaAcknowledgement>,
        topic_trackers: Arc<TopicTrackers>,
        topic_names: HashMap<u8, String>,
        assigned_partitions: Arc<std::sync::Mutex<AssignedPartitions>>,
        consumer: Arc<StreamConsumer<KafkaConsumerContext>>,
    ) -> Self {
        let tick_interval = tokio::time::interval(tick_every);

        // Build reverse map: topic name -> topic ID
        let topic_name_to_id: HashMap<String, u8> = topic_names
            .iter()
            .map(|(id, name)| (name.clone(), *id))
            .collect();

        KafkaOffsetCommitter {
            tick_interval,
            ack_receiver,
            topic_trackers,
            topic_name_to_id,
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
                // Can't collapse these two into one case because select! is matching on futures completing, not boolean conditions.
                // We could do something like this:
                // _ = self.tick_interval.tick() => self.handle_commit().await,
                // _ = self.rebalance_rx.next() => self.handle_commit().await,
                // But prefer to avoid another await and just be explicit about the two cases
                _ = self.tick_interval.tick() => {
                    commit_offset(self.assigned_partitions.clone(),
                        &self.topic_name_to_id,
                        &self.topic_trackers,
                        |tpl, mode| self.consumer.commit(tpl, mode));
                }
                ack = self.ack_receiver.next() => {
                    if let Some(ack) = ack {
                        match ack {
                            payload::KafkaAcknowledgement::Ack(kafka_ack) => {
                                debug!("Current tracker pending is {}", self.topic_trackers.pending_count(kafka_ack.topic_id, kafka_ack.partition));
                                debug!("Received ack for topic {} partition {} offset {}", kafka_ack.topic_id, kafka_ack.partition, kafka_ack.offset);
                                self.topic_trackers.acknowledge(kafka_ack.topic_id, kafka_ack.partition, kafka_ack.offset);
                            }
                            payload::KafkaAcknowledgement::Nack(_kafka_nack) => {
                                // TODO: Implement nack
                                //self.topic_trackers.nack(kafka_nack.topic_id, kafka_nack.partition, kafka_nack.offset);
                            }
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    debug!("KafkaOffsetCommitter cancelled, performing final commit before shutdown");

                    // Perform final commit before shutting down
                    let assigned = self.assigned_partitions.lock().unwrap().clone();
                    let mut final_commits = Vec::new();

                    for (topic_name, partitions) in assigned.topics.iter() {
                        let topic_id = match self.topic_name_to_id.get(topic_name) {
                            Some(id) => *id,
                            None => continue,
                        };

                        for &partition in partitions.iter() {
                            if let Some(offset) = self.topic_trackers.lowest_pending_offset(topic_id, partition) {
                                final_commits.push((topic_name.clone(), partition, offset));
                            } else if let Some(hwm) = self.topic_trackers.high_water_mark(topic_id, partition) {
                                final_commits.push((topic_name.clone(), partition, hwm + 1));
                            }
                        }
                    }

                    if !final_commits.is_empty() {
                        let mut topic_partition_list = rdkafka::topic_partition_list::TopicPartitionList::new();
                        for (topic_name, partition, offset) in final_commits.iter() {
                            topic_partition_list
                                .add_partition_offset(topic_name, *partition, rdkafka::topic_partition_list::Offset::Offset(*offset))
                                .ok();
                        }
                        debug!("About to commit TPL of {:?} entries:", topic_partition_list);

                        match self.consumer.commit(&topic_partition_list, rdkafka::consumer::CommitMode::Sync) {
                            Ok(_) => debug!("Final commit successful for {} partitions", final_commits.len()),
                            Err(e) => warn!("Failed to perform final commit: {:?}", e),
                        }
                    }

                    break;
                }
            }
        }
        Ok(())
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
    // Lock and clone assigned partitions
    let assigned = assigned_partitions.lock().unwrap().clone();

    // Build list of topic/partition/offset to commit
    let mut commits = Vec::new();
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
                commits.push((topic_name, partition, offset));
            } else if let Some(hwm) = topic_trackers.high_water_mark(topic_id, partition) {
                debug!(
                    "Topic {} (id {}) partition {} has no pending offsets, using high water mark: {}",
                    topic_name, topic_id, partition, hwm
                );
                commits.push((topic_name, partition, hwm + 1));
            } else {
                debug!(
                    "Topic {} (id {}) partition {} has no pending offsets and no high water mark",
                    topic_name, topic_id, partition
                );
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
