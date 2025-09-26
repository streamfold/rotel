use opentelemetry_proto::tonic::{
    logs::v1::{ResourceLogs, ScopeLogs},
    metrics::v1::{ResourceMetrics, ScopeMetrics, metric::Data},
    trace::v1::{ResourceSpans, ScopeSpans},
};

use crate::topology::batch::{BatchSizer, BatchSplittable};

impl BatchSizer for ResourceSpans {
    fn size_of(&self) -> usize {
        self.scope_spans.iter().map(|sc| sc.spans.len()).sum()
    }
}

impl BatchSizer for [ResourceSpans] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceSpans {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_spans = vec![];
        while !self.scope_spans.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let span_len = self.scope_spans[0].spans.len();
            if span_len + count_moved <= split_n {
                let v = self.scope_spans.remove(0);
                split_scope_spans.push(v);
                count_moved += span_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut spans = vec![];
                while !self.scope_spans[0].spans.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_spans[0].spans.remove(0);
                    spans.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let ss = ScopeSpans {
                    scope: self.scope_spans[0].scope.clone(),
                    schema_url: self.scope_spans[0].schema_url.clone(),
                    spans,
                };
                split_scope_spans.push(ss)
            }
        }
        ResourceSpans {
            scope_spans: split_scope_spans,
            schema_url: self.schema_url.clone(),
            resource: self.resource.clone(),
        }
    }
}

impl BatchSizer for ResourceMetrics {
    fn size_of(&self) -> usize {
        self.scope_metrics
            .iter()
            .flat_map(|sc| &sc.metrics)
            .map(|m| match &m.data {
                None => 0,
                Some(d) => match d {
                    Data::Gauge(g) => g.data_points.len(),
                    Data::Sum(s) => s.data_points.len(),
                    Data::Histogram(h) => h.data_points.len(),
                    Data::ExponentialHistogram(e) => e.data_points.len(),
                    Data::Summary(s) => s.data_points.len(),
                },
            })
            .sum()
    }
}

impl BatchSizer for [ResourceMetrics] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceMetrics {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_metrics = vec![];
        while !self.scope_metrics.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let metric_len = self.scope_metrics[0].metrics.len();
            if metric_len + count_moved <= split_n {
                let v = self.scope_metrics.remove(0);
                split_scope_metrics.push(v);
                count_moved += metric_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut metrics = vec![];
                while !self.scope_metrics[0].metrics.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_metrics[0].metrics.remove(0);
                    metrics.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let sm = ScopeMetrics {
                    scope: self.scope_metrics[0].scope.clone(),
                    schema_url: self.scope_metrics[0].schema_url.clone(),
                    metrics,
                };
                split_scope_metrics.push(sm)
            }
        }
        ResourceMetrics {
            scope_metrics: split_scope_metrics,
            schema_url: self.schema_url.clone(),
            resource: self.resource.clone(),
        }
    }
}

impl BatchSizer for ResourceLogs {
    fn size_of(&self) -> usize {
        self.scope_logs.iter().map(|sc| sc.log_records.len()).sum()
    }
}

impl BatchSizer for [ResourceLogs] {
    fn size_of(&self) -> usize {
        self.iter().map(|n| n.size_of()).sum()
    }
}

impl BatchSplittable for ResourceLogs {
    fn split(&mut self, split_n: usize) -> Self
    where
        Self: Sized,
    {
        let mut count_moved = 0;
        let mut split_scope_logs = vec![];
        while !self.scope_logs.is_empty() && count_moved < split_n {
            // first just check and see if we can hoist all the spans in this scope span
            let span_len = self.scope_logs[0].log_records.len();
            if span_len + count_moved <= split_n {
                let v = self.scope_logs.remove(0);
                split_scope_logs.push(v);
                count_moved += span_len;
            } else {
                // We'll need to harvest a subset of the spans
                let mut log_records = vec![];
                while !self.scope_logs[0].log_records.is_empty() && count_moved < split_n {
                    // Remove the span and add to the new ss
                    let s = self.scope_logs[0].log_records.remove(0);
                    log_records.push(s);
                    count_moved += 1
                }
                // Now we need to clone data from this ScopeSpan for our new ScopeSpans
                let sl = ScopeLogs {
                    scope: self.scope_logs[0].scope.clone(),
                    schema_url: self.scope_logs[0].schema_url.clone(),
                    log_records,
                };
                split_scope_logs.push(sl)
            }
        }
        ResourceLogs {
            scope_logs: split_scope_logs,
            schema_url: self.schema_url.clone(),
            resource: self.resource.clone(),
        }
    }
}
