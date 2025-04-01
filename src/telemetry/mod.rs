pub mod internal_exporter;

use opentelemetry::KeyValue;

pub trait Counter<T> {
    fn add(&self, value: T, attributes: &[KeyValue]);
    fn clone_box(&self) -> Box<dyn Counter<T> + Send + Sync + 'static>;
}

impl Clone for Box<dyn Counter<u64> + Send + Sync + 'static> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Clone)]
pub struct RotelCounter<T> {
    inner: opentelemetry::metrics::Counter<T>,
}

impl<T: Clone + 'static> RotelCounter<T> {
    pub(crate) fn new(inner: opentelemetry::metrics::Counter<T>) -> Self {
        Self { inner }
    }
}

impl<T: Clone + 'static> Counter<T> for RotelCounter<T> {
    fn add(&self, value: T, attributes: &[KeyValue]) {
        self.inner.add(value, attributes);
    }

    fn clone_box(&self) -> Box<dyn Counter<T> + Send + Sync + 'static> {
        Box::new(RotelCounter {
            inner: self.inner.clone(),
        })
    }
}

#[derive(Clone)]
pub struct NoOpCounter {}

impl NoOpCounter {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl<T: Clone + 'static> Counter<T> for NoOpCounter {
    fn add(&self, _value: T, _attributes: &[KeyValue]) {
        //noop
    }

    fn clone_box(&self) -> Box<dyn Counter<T> + Send + Sync + 'static> {
        Box::new(NoOpCounter {})
    }
}
