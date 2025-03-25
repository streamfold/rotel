// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
pub struct BatchConfig {
    pub max_size: usize,
    pub timeout: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: 8192,
            timeout: Duration::from_millis(200),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TooManyItemsError;

impl fmt::Display for TooManyItemsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "too many items to batch")
    }
}

pub(crate) struct NestedBatch<T: BatchSizer + BatchSplittable> {
    items: Vec<T>,
    item_count: usize, // We need this because there are nested items in items.
    max_size: usize,
    last_flush: Instant,
    batch_timeout: Duration,
}

impl<T: BatchSizer + BatchSplittable> NestedBatch<T>
// where
//     OTLPPayload: From<Vec<T>>,
{
    /// Creates a new NestedBatch. NestedBatches are vectors of type T but contain
    /// additional items per instance of T in the vector. We batch based on the total number
    /// of nested items in Vec<T>, rather than Vec<T>.len().
    ///
    /// # Arguments
    /// - `max_size`: The maximum size that will be sent to the batch handler per flush cycle
    /// - `batch_timeout`: How long to wait before flushing if we've not reached max_size
    pub(crate) fn new(max_size: usize, batch_timeout: Duration) -> NestedBatch<T> {
        Self {
            items: Vec::with_capacity(max_size),
            item_count: 0,
            max_size,
            last_flush: Instant::now(),
            batch_timeout,
        }
    }

    pub(crate) fn get_timeout(&self) -> Duration {
        self.batch_timeout
    }

    pub(crate) fn take_batch(&mut self) -> Vec<T> {
        let items = std::mem::take(&mut self.items);
        self.item_count = 0;
        self.last_flush = Instant::now();
        items
    }

    pub(crate) fn should_flush(&self, now: Instant) -> bool {
        self.item_count > 0 && (self.last_flush + self.batch_timeout) < now
    }

    pub(crate) fn offer(
        &mut self,
        mut new_items: Vec<T>,
    ) -> Result<Option<Vec<T>>, TooManyItemsError> {
        let new_items_count = new_items.iter().map(|n| n.size_of()).sum::<usize>();
        if self.item_count + new_items_count < self.max_size {
            self.items.append(&mut new_items);
            self.item_count += new_items_count;
            return Ok(None);
        }

        // We can't just use all the new_items, as we'd overflow the batch. So we need to carefully
        // take enough from new_items to fill our current batch and push the remaining self.items.
        while !new_items.is_empty() && self.item_count < self.max_size {
            let s = new_items[0].size_of();
            if s < self.max_size - self.item_count {
                self.items.push(new_items.remove(0));
                self.item_count += s;
            } else {
                // We'll need to split this T
                let res = new_items[0].split(self.max_size - self.item_count);
                let split_size = res.size_of();
                self.items.push(res);
                self.item_count += split_size;
            }
        }
        let harvested = Some(self.take_batch());
        // Final error condition we need to make sure remaining isn't larger than max_size;
        let final_new_item_count = new_items.iter().map(|n| n.size_of()).sum::<usize>();
        if final_new_item_count > self.max_size {
            return Err(TooManyItemsError);
        }
        self.item_count += final_new_item_count;
        self.items.append(&mut new_items);
        Ok(harvested)
    }
}

pub trait BatchSizer {
    fn size_of(&self) -> usize;
}

pub trait BatchSplittable {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized;
}
