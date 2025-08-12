use serde_json::{Value, json};
use tracing::warn;

pub struct Event {
    pub(crate) timestamp_ms: i64,
    pub(crate) message: String,
}

const PER_EVENT_HEADER_BYTES: usize = 26;

const MAX_BATCH_EVENTS: usize = 10_000;
const MAX_BATCH_SIZE: usize = 1024 * 1024;
const MAX_BATCH_TIMERANGE_MS: i64 = 24 * 3600 * 1_000; // batch can't span more than 24 hours

impl Event {
    pub(crate) fn new(timestamp: i64, message: String) -> Self {
        Event {
            timestamp_ms: timestamp,
            message,
        }
    }

    fn size(&self) -> usize {
        self.message.len() + PER_EVENT_HEADER_BYTES
    }
}

pub struct EventBatch {
    events: Vec<Event>,
    bytes_total: usize,
    min_timestamp_ms: i64,
    max_timestamp_ms: i64,
}

impl EventBatch {
    pub(crate) fn new() -> Self {
        EventBatch {
            events: Vec::new(),
            bytes_total: 0,
            min_timestamp_ms: 0,
            max_timestamp_ms: 0,
        }
    }

    fn out_of_range(&self, timestamp_ms: i64) -> bool {
        if self.min_timestamp_ms == 0 || self.max_timestamp_ms == 0 {
            return false;
        }

        if timestamp_ms - self.min_timestamp_ms > MAX_BATCH_TIMERANGE_MS {
            return true;
        }
        if self.max_timestamp_ms - timestamp_ms > MAX_BATCH_TIMERANGE_MS {
            return true;
        }

        false
    }

    fn exceeds_limit(&self, next_evt_bytes: usize) -> bool {
        if self.events.len() >= MAX_BATCH_EVENTS {
            return true;
        }

        self.bytes_total + next_evt_bytes > MAX_BATCH_SIZE
    }

    pub(crate) fn add_event(&mut self, event: Event) -> Option<Event> {
        let mut sz = event.size();
        let mut event = event;

        // If the size of a single event exceeds the maximum, then we must
        // truncate it. The alternative would be to drop it entirely.
        if sz >= MAX_BATCH_SIZE {
            warn!(
                log_size = sz,
                "Truncating long log line to meet Cloudwatch limits"
            );
            // event.size() adds the event header size, so substract it here
            event
                .message
                .truncate(MAX_BATCH_SIZE - PER_EVENT_HEADER_BYTES);
            sz = event.size();
        }

        if self.out_of_range(event.timestamp_ms) || self.exceeds_limit(sz) {
            return Some(event);
        }

        if self.min_timestamp_ms == 0 || event.timestamp_ms < self.min_timestamp_ms {
            self.min_timestamp_ms = event.timestamp_ms;
        }
        if self.max_timestamp_ms == 0 || event.timestamp_ms > self.max_timestamp_ms {
            self.max_timestamp_ms = event.timestamp_ms;
        }
        self.bytes_total += sz;

        self.events.push(event);

        None
    }

    pub(crate) fn get_events(self) -> Vec<Value> {
        let mut log_events = Vec::with_capacity(self.events.len());
        for emf_log in self.events {
            log_events.push(json!({
                "timestamp": emf_log.timestamp_ms,
                "message": emf_log.message,
            }));
        }

        log_events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_batch_new() {
        let batch = EventBatch::new();
        assert_eq!(batch.events.len(), 0);
        assert_eq!(batch.bytes_total, 0);
        assert_eq!(batch.min_timestamp_ms, 0);
        assert_eq!(batch.max_timestamp_ms, 0);
    }

    #[test]
    fn test_event_batch_add_single_event() {
        let mut batch = EventBatch::new();
        let event = Event::new(1000, "test".to_string());

        let rejected = batch.add_event(event);
        assert!(rejected.is_none());
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.min_timestamp_ms, 1000);
        assert_eq!(batch.max_timestamp_ms, 1000);
        assert_eq!(batch.bytes_total, 30); // "test" (4) + header (26) = 30
    }

    #[test]
    fn test_event_batch_multiple_events_update_stats() {
        let mut batch = EventBatch::new();

        // Add event with timestamp 2000
        let event1 = Event::new(2000, "test1".to_string());
        batch.add_event(event1);
        assert_eq!(batch.min_timestamp_ms, 2000);
        assert_eq!(batch.max_timestamp_ms, 2000);
        assert_eq!(batch.bytes_total, 5 + 26);

        // Add event with earlier timestamp
        let event2 = Event::new(1000, "test2_".to_string());
        batch.add_event(event2);
        assert_eq!(batch.min_timestamp_ms, 1000);
        assert_eq!(batch.max_timestamp_ms, 2000);
        assert_eq!(batch.bytes_total, (5 + 26) + (6 + 26));

        // Add event with later timestamp
        let event3 = Event::new(3000, "test3__".to_string());
        batch.add_event(event3);
        assert_eq!(batch.min_timestamp_ms, 1000);
        assert_eq!(batch.max_timestamp_ms, 3000);
        assert_eq!(batch.bytes_total, (5 + 26) + (6 + 26) + (7 + 26));
    }

    #[test]
    fn test_event_batch_exceeds_max_events() {
        let mut batch = EventBatch::new();

        // Add MAX_BATCH_EVENTS events
        for i in 0..MAX_BATCH_EVENTS {
            let event = Event::new(1000, format!("msg{}", i));
            let rejected = batch.add_event(event);
            assert!(rejected.is_none());
        }

        assert_eq!(batch.events.len(), MAX_BATCH_EVENTS);

        // Try to add one more - should be rejected
        let extra_event = Event::new(1000, "extra".to_string());
        let rejected = batch.add_event(extra_event);
        assert!(rejected.is_some());
        assert_eq!(batch.events.len(), MAX_BATCH_EVENTS);
    }

    #[test]
    fn test_event_batch_exceeds_max_size() {
        let mut batch = EventBatch::new();

        // Create an event that would push us over MAX_BATCH_SIZE
        let large_message = "x".repeat(MAX_BATCH_SIZE - PER_EVENT_HEADER_BYTES - 10);
        let event1 = Event::new(1000, large_message);
        batch.add_event(event1);

        // Try to add another small event - should be rejected due to size
        let event2 = Event::new(1000, "small".to_string());
        let rejected = batch.add_event(event2);
        assert!(rejected.is_some());
        assert_eq!(batch.events.len(), 1);
    }

    #[test]
    fn test_event_batch_single_event_exceeds_max_size() {
        let mut batch = EventBatch::new();

        // Create an event larger than MAX_BATCH_SIZE
        let huge_message = "x".repeat(MAX_BATCH_SIZE + 1000);
        let event = Event::new(1000, huge_message);

        let rejected = batch.add_event(event);
        assert!(rejected.is_none()); // Should be accepted but truncated
        assert_eq!(batch.events.len(), 1);

        // Check that the message was truncated
        let expected_size = MAX_BATCH_SIZE - PER_EVENT_HEADER_BYTES;
        assert_eq!(batch.events[0].message.len(), expected_size);
    }

    #[test]
    fn test_event_batch_time_range_exceeded() {
        let mut batch = EventBatch::new();

        // Add first event
        let base_time = 1000;
        let event1 = Event::new(base_time, "first".to_string());
        batch.add_event(event1);

        // Try to add event that's more than 24 hours later
        let far_future_time = base_time + MAX_BATCH_TIMERANGE_MS + 1000;
        let event2 = Event::new(far_future_time, "future".to_string());
        let rejected = batch.add_event(event2);
        assert!(rejected.is_some());

        // Try to add event that's more than 24 hours earlier
        let far_past_time = base_time - MAX_BATCH_TIMERANGE_MS - 1000;
        let event3 = Event::new(far_past_time, "past".to_string());
        let rejected = batch.add_event(event3);
        assert!(rejected.is_some());

        assert_eq!(batch.events.len(), 1);
    }

    #[test]
    fn test_event_batch_time_range_within_limits() {
        let mut batch = EventBatch::new();

        // Add first event
        let base_time = 1000;
        let event1 = Event::new(base_time, "first".to_string());
        batch.add_event(event1);

        // Add event exactly at the 24-hour limit (should be accepted)
        let limit_time = base_time + MAX_BATCH_TIMERANGE_MS;
        let event2 = Event::new(limit_time, "limit".to_string());
        let rejected = batch.add_event(event2);
        assert!(rejected.is_none());

        assert_eq!(batch.events.len(), 2);
    }

    #[test]
    fn test_event_batch_get_events_with_data() {
        let mut batch = EventBatch::new();

        let event1 = Event::new(1000, "message1".to_string());
        let event2 = Event::new(2000, "message2".to_string());

        batch.add_event(event1);
        batch.add_event(event2);

        let events = batch.get_events();
        assert_eq!(events.len(), 2);

        let expected1 = json!({
            "timestamp": 1000,
            "message": "message1"
        });
        let expected2 = json!({
            "timestamp": 2000,
            "message": "message2"
        });

        assert_eq!(events[0], expected1);
        assert_eq!(events[1], expected2);
    }
}
