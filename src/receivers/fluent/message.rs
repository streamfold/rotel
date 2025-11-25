use chrono::serde::ts_seconds;
use chrono::{DateTime, Utc};
use serde::de;
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum Message {
    Message(EventTag, EventTimestamp, EventRecord),
    MessageWithOptions(EventTag, EventTimestamp, EventRecord, EventOptions),

    Forward(EventTag, Vec<EventEntry>),
    ForwardWithOption(EventTag, Vec<EventEntry>, EventOptions),
}

impl Message {
    /// Extract tag, entries for conversion to OTLP
    pub(crate) fn entries(&self) -> (&str, Vec<(&EventTimestamp, &EventRecord)>) {
        match self {
            Message::Message(tag, ts, record) => (tag.as_str(), vec![(ts, record)]),
            Message::MessageWithOptions(tag, ts, record, _) => (tag.as_str(), vec![(ts, record)]),
            Message::Forward(tag, entries) => {
                let items = entries.iter().map(|e| e.as_tuple()).collect();
                (tag.as_str(), items)
            }
            Message::ForwardWithOption(tag, entries, _) => {
                let items = entries.iter().map(|e| e.as_tuple()).collect();
                (tag.as_str(), items)
            }
        }
    }
}

pub(crate) type EventTag = String;

impl EventTime {
    pub(crate) fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct EventTime(DateTime<Utc>);

// Custom deserializer for EventTime that handles rmpv::Value::Ext
impl<'de> Deserialize<'de> for EventTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize as rmpv::Value to handle the Ext type
        let value: rmpv::Value = Deserialize::deserialize(deserializer)?;

        match value {
            rmpv::Value::Ext(tag, bytes) => {
                if tag != 0 {
                    return Err(de::Error::custom(format!(
                        "expected EventTime extension type 0, got {}",
                        tag
                    )));
                }

                if bytes.len() != 8 {
                    return Err(de::Error::custom(format!(
                        "EventTime ext format must be exactly 8 bytes, got {}",
                        bytes.len()
                    )));
                }

                // Parse 4 bytes for seconds (big-endian)
                let seconds = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64;

                // Parse 4 bytes for nanoseconds (big-endian)
                let nanos = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

                // Create DateTime from seconds and nanoseconds
                let dt = DateTime::from_timestamp(seconds, nanos)
                    .ok_or_else(|| de::Error::custom("invalid timestamp"))?;

                Ok(EventTime(dt))
            }
            _ => Err(de::Error::custom("expected msgpack Ext type for EventTime")),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub(crate) enum EventTimestamp {
    #[serde(with = "ts_seconds")]
    Unix(DateTime<Utc>),
    Ext(EventTime),
}

impl EventTimestamp {
    pub(crate) fn as_datetime(&self) -> &DateTime<Utc> {
        match self {
            EventTimestamp::Unix(dt) => dt,
            EventTimestamp::Ext(et) => et.as_datetime(),
        }
    }
}

pub(crate) type EventRecord = BTreeMap<String, EventValue>;

#[derive(Debug, Deserialize)]
pub(crate) struct EventEntry(EventTimestamp, EventRecord);

impl EventEntry {
    pub(crate) fn as_tuple(&self) -> (&EventTimestamp, &EventRecord) {
        (&self.0, &self.1)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct EventValue(rmpv::Value);

impl EventValue {
    pub(crate) fn as_value(&self) -> &rmpv::Value {
        &self.0
    }
}

impl From<rmpv::Value> for EventValue {
    fn from(value: rmpv::Value) -> Self {
        EventValue(value)
    }
}

#[derive(Default, Debug, Deserialize, PartialEq)]
#[serde(default)]
pub(crate) struct EventOptions {
    pub(crate) size: u64,
    pub(crate) compressed: Option<String>,
    pub(crate) chunk: Option<String>,
}
