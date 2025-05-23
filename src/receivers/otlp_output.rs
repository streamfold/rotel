// SPDX-License-Identifier: Apache-2.0

use crate::bounded_channel::{BoundedSender, SendError};

// #[derive(Clone)]
// pub enum OTLPPayload {
//     Traces(Vec<ResourceSpans>),
//     Metrics(Vec<ResourceMetrics>),
// }

#[derive(Clone)]
pub struct OTLPOutput<T> {
    tx: BoundedSender<T>,
}

impl<T> OTLPOutput<T> {
    pub fn new(tx: BoundedSender<T>) -> Self {
        Self { tx }
    }

    pub async fn send(&self, events: T) -> Result<(), SendError> {
        self.tx.send(events).await
    }
}
