pub mod internal_exporter;

use opentelemetry::KeyValue;

pub trait Counter<T> {
    fn add(&self, value: T, attributes: &[KeyValue]);
}

#[derive(Clone)]
pub enum RotelCounter<T> {
    OTELCounter(opentelemetry::metrics::Counter<T>),
    NoOpCounter,
}

impl<T> Counter<T> for RotelCounter<T> {
    fn add(&self, value: T, attributes: &[KeyValue]) {
        match self {
            RotelCounter::OTELCounter(c) => c.add(value, attributes),
            RotelCounter::NoOpCounter => {}
        }
    }
}

#[derive(Clone)]
pub struct NoOpCounter {}
