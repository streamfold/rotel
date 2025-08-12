
use serde_json::{json, Value};
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
        let sz = event.size();
        
        let mut event = event;
        
        // If the size of a single event exceeds the maximum, then we must
        // truncate it. The alternative would be to drop it entirely.
        if sz >= MAX_BATCH_SIZE {
            warn!(log_size = sz, "Truncating long log line to meet Cloudwatch limits");
            // event.size() adds the event header size, so substract it here
            event.message.truncate(MAX_BATCH_SIZE - PER_EVENT_HEADER_BYTES);
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
