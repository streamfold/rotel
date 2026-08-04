// SPDX-License-Identifier: Apache-2.0

//! Textfile collector and Prometheus exposition-format parsing.
//!
//! Behaviour and the `node_textfile_*` semantics are derived from Prometheus
//! node_exporter — see the notice in this module's `mod.rs`.

use crate::receivers::node_metrics::collector::{CollectedMetric, MetricType, SystemCollector};
use crate::receivers::node_metrics::config::NodeMetricsConfig;
use std::collections::HashMap;
use std::fs;
use tracing::{debug, warn};

impl SystemCollector {
    /// Collect custom metrics from Prometheus-format textfiles
    ///
    /// The textfile path can be either a directory (all `*.prom` files are read in
    /// sorted filename order) or a single `.prom` file.
    pub(super) fn collect_textfile(&self, config: &NodeMetricsConfig) -> Vec<CollectedMetric> {
        let mut metrics = Vec::new();

        let textfile_path = match &config.textfile_directory {
            Some(dir) => dir,
            None => {
                debug!("Textfile collector enabled but no directory configured");
                return metrics;
            }
        };

        let path = std::path::Path::new(textfile_path);

        // Support both single-file and directory paths
        let prom_files: Vec<std::path::PathBuf> = if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            match fs::read_dir(path) {
                Ok(entries) => {
                    let mut files: Vec<std::path::PathBuf> = entries
                        .flatten()
                        .map(|e| e.path())
                        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("prom"))
                        // Regular files only: a FIFO or socket named `foo.prom` would
                        // otherwise reach `read_to_string`, where `open(2)` blocks
                        // indefinitely with no writer and would wedge the scrape thread.
                        // `unwrap_or(true)` keeps an entry whose metadata cannot be read —
                        // a dangling symlink, say — so it is still reported through
                        // `node_textfile_scrape_error` rather than disappearing.
                        // `fs::metadata` follows symlinks, unlike `DirEntry::file_type`,
                        // so a valid symlinked `.prom` is collected.
                        .filter(|p| fs::metadata(p).map(|m| m.is_file()).unwrap_or(true))
                        .collect();
                    // Read in a deterministic order: the OTLP conversion takes a metric's
                    // description and unit from the first sample it sees, so traversal
                    // order would otherwise decide which file's HELP text wins, and would
                    // reorder data points between scrapes.
                    files.sort();
                    files
                }
                Err(e) => {
                    // A mistyped or unreadable directory must not fail silently, otherwise
                    // the receiver just emits nothing forever.
                    warn!(
                        "Failed to read textfile path {} (set via --node-metrics-receiver-textfile-directory): {}",
                        textfile_path, e
                    );
                    if config.should_include_metric("node_textfile_scrape_error") {
                        metrics.push(textfile_scrape_error(textfile_path.clone(), 1.0));
                    }
                    return metrics;
                }
            }
        };

        let mut total_samples = 0usize;
        let mut over_sample_limit = false;

        for path in &prom_files {
            let filename = path
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_default();

            // Expose the file's modification time so a stalled writer script is
            // detectable even while its (stale) metrics keep being exported.
            let metadata = fs::metadata(path).ok();
            if let Some(mtime) = metadata
                .as_ref()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                && config.should_include_metric("node_textfile_mtime_seconds")
            {
                metrics.push(CollectedMetric {
                    name: "node_textfile_mtime_seconds".to_string(),
                    value: mtime.as_secs_f64(),
                    labels: vec![("filename".to_string(), filename.clone())],
                    metric_type: MetricType::Gauge,
                    unit: Some("s".to_string()),
                    description: Some("Modification time of the textfile in seconds".to_string()),
                });
            }

            // Guard against a runaway writer script: an unbounded file would be read
            // into memory in full on every scrape interval.
            if let Some(size) = metadata.as_ref().map(|m| m.len())
                && size > MAX_TEXTFILE_SIZE_BYTES
            {
                // Logged at debug, not warn: this repeats every scrape for as long as the
                // file is oversized. `node_textfile_scrape_error` is the durable signal.
                debug!(
                    "Skipping textfile {:?}: {} bytes exceeds the {} byte limit",
                    path, size, MAX_TEXTFILE_SIZE_BYTES
                );
                if config.should_include_metric("node_textfile_scrape_error") {
                    metrics.push(textfile_scrape_error(filename, 1.0));
                }
                continue;
            }

            let content = match read_capped(path) {
                Ok(c) => c,
                Err(e) => {
                    // Debug, like the sibling failure paths: this repeats every scrape for
                    // as long as the file is unreadable, and `node_textfile_scrape_error`
                    // is the durable signal.
                    debug!("Failed to read textfile {:?}: {}", path, e);
                    if config.should_include_metric("node_textfile_scrape_error") {
                        metrics.push(textfile_scrape_error(filename, 1.0));
                    }
                    continue;
                }
            };

            // Track metric types and descriptions from comments, scoped per file.
            // TYPE annotations determine counter vs gauge; conflicting types for
            // the same metric across files coexist and the OTLP conversion keeps
            // them as separate metrics (see `convert.rs`).
            let mut described: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut metric_types: HashMap<String, MetricType> = HashMap::new();
            let mut metric_descriptions: HashMap<String, String> = HashMap::new();
            let mut parsed = Vec::new();
            let mut parse_failures = 0usize;

            // Parse the file
            for line in content.lines() {
                let line = line.trim();

                // Skip empty lines
                if line.is_empty() {
                    continue;
                }

                // Parse HELP comments — preserve descriptions for OTLP.
                // The exposition format allows any run of whitespace after the keyword.
                if let Some(rest) = strip_comment_keyword(line, "HELP") {
                    if let Some((name, desc)) = rest.trim_start().split_once(char::is_whitespace)
                        && !name.is_empty()
                    {
                        metric_descriptions
                            .insert(name.to_string(), unescape_exposition(desc.trim()));
                    }
                    continue;
                }

                // Parse TYPE comments
                if let Some(rest) = strip_comment_keyword(line, "TYPE") {
                    if let Some((name, type_str)) =
                        rest.trim_start().split_once(char::is_whitespace)
                        && !name.is_empty()
                    {
                        let type_str = type_str.trim();
                        let metric_type = match type_str {
                            "counter" => MetricType::Counter,
                            "gauge" => MetricType::Gauge,
                            // histogram/summary/untyped are not modelled; their component
                            // series are exported as gauges.
                            other => {
                                debug!(
                                    "Textfile {}: unsupported metric type {:?} for {}, treating as gauge",
                                    filename, other, name
                                );
                                MetricType::Gauge
                            }
                        };
                        metric_types.insert(name.to_string(), metric_type);
                    }
                    continue;
                }

                // Skip other comments
                if line.starts_with('#') {
                    continue;
                }

                // Parse metric line
                match parse_prometheus_line(line, &metric_types) {
                    Some(mut metric) => {
                        if !config.should_include_metric(&metric.name) {
                            continue;
                        }
                        // Attach HELP description if available
                        // Attach the HELP text to the first sample of each metric only: the
                        // OTLP conversion takes the description from the first member of a
                        // group, so cloning it onto every sample would hold hundreds of
                        // thousands of copies of the same string for a large file.
                        if let Some(desc) = metric_descriptions.get(&metric.name)
                            && described.insert(metric.name.clone())
                        {
                            metric.description = Some(desc.clone());
                        }
                        parsed.push(metric);
                    }
                    None => {
                        // A malformed line must not disappear without a trace, otherwise a
                        // broken writer script is indistinguishable from an empty file.
                        parse_failures += 1;
                        debug!("Textfile {}: skipping malformed line {:?}", filename, line);
                    }
                }
            }

            if parse_failures > 0 {
                // Debug rather than warn: a static file with a bad line would otherwise log
                // on every scrape forever. `node_textfile_scrape_error` carries the signal.
                debug!(
                    "Textfile {}: skipped {} malformed line(s)",
                    filename, parse_failures
                );
            }

            if config.should_include_metric("node_textfile_scrape_error") {
                let value = if parse_failures > 0 { 1.0 } else { 0.0 };
                metrics.push(textfile_scrape_error(filename.clone(), value));
            }

            if parsed.len() + total_samples > MAX_TEXTFILE_SAMPLES {
                let room = MAX_TEXTFILE_SAMPLES.saturating_sub(total_samples);
                warn!(
                    "Textfile {}: keeping {} of {} samples; the {} sample per-scrape limit \
                     was reached",
                    filename,
                    room,
                    parsed.len(),
                    MAX_TEXTFILE_SAMPLES
                );
                parsed.truncate(room);
                over_sample_limit = true;
            }
            total_samples += parsed.len();
            metrics.append(&mut parsed);

            if over_sample_limit {
                break;
            }
        }

        metrics
    }
}

/// Maximum number of samples taken from the textfile directory in one scrape
const MAX_TEXTFILE_SAMPLES: usize = 100_000;

/// Read a textfile, enforcing the size cap during the read
///
/// The pre-read `stat` is only advisory: a file can grow between the stat and the read, and
/// a non-regular file reports a length of zero. Reading through a bounded reader means the
/// cap holds regardless.
fn read_capped(path: &std::path::Path) -> std::io::Result<String> {
    use std::io::Read;

    let mut buf = String::new();
    fs::File::open(path)?
        // One byte over the cap, so an oversized file is detectable rather than silently
        // truncated mid-sample.
        .take(MAX_TEXTFILE_SIZE_BYTES + 1)
        .read_to_string(&mut buf)?;

    if buf.len() as u64 > MAX_TEXTFILE_SIZE_BYTES {
        return Err(std::io::Error::other(format!(
            "textfile exceeds the {} byte limit",
            MAX_TEXTFILE_SIZE_BYTES
        )));
    }
    Ok(buf)
}

/// Maximum size of a single `.prom` textfile that will be read
const MAX_TEXTFILE_SIZE_BYTES: u64 = 10 * 1024 * 1024;

/// Build the `node_textfile_scrape_error` metric for a given file
fn textfile_scrape_error(filename: String, value: f64) -> CollectedMetric {
    CollectedMetric {
        name: "node_textfile_scrape_error".to_string(),
        value,
        labels: vec![("filename".to_string(), filename)],
        metric_type: MetricType::Gauge,
        unit: None,
        description: Some("1 if there was an error scraping the textfile, 0 otherwise".to_string()),
    }
}

/// Strip a `# <KEYWORD>` comment prefix, returning the remainder.
///
/// The exposition format allows any run of whitespace after `#` and after the keyword,
/// so `#HELP`, `# HELP` and `#  HELP` are all accepted. Returns `None` when the line is
/// not this kind of comment, so that plain comments fall through untouched.
fn strip_comment_keyword<'a>(line: &'a str, keyword: &str) -> Option<&'a str> {
    let rest = line.strip_prefix('#')?.trim_start();
    let rest = rest.strip_prefix(keyword)?;
    // The keyword must be followed by whitespace, so `# HELPfoo` is not a HELP line
    if rest.starts_with(char::is_whitespace) {
        Some(rest)
    } else {
        None
    }
}

/// Decode the escape sequences the Prometheus exposition format allows in HELP text
fn unescape_exposition(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('\\') => out.push('\\'),
            // Any other escape is passed through verbatim, as the format leaves it undefined
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

/// Parse a single Prometheus exposition format line
/// Format: metric_name[{label="value",...}] value [timestamp]
fn parse_prometheus_line(
    line: &str,
    metric_types: &HashMap<String, MetricType>,
) -> Option<CollectedMetric> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    // Find the metric name and optional labels
    let (name, labels_str, value_str) = if let Some(brace_start) = line.find('{') {
        // Has labels — find closing '}' while respecting quoted strings
        let brace_end = find_closing_brace(&line[brace_start + 1..])?;
        let brace_end = brace_start + 1 + brace_end;
        // Whitespace between the name and `{` is tolerated by the exposition format
        let name = line[..brace_start].trim_end();
        let labels_str = Some(&line[brace_start + 1..brace_end]);
        let rest = line[brace_end + 1..].trim();
        (name, labels_str, rest)
    } else {
        // No labels - split on whitespace
        let mut parts = line.splitn(2, char::is_whitespace);
        let name = parts.next()?;
        let rest = parts.next().unwrap_or("").trim();
        (name, None, rest)
    };

    // Reject names that are not valid metric identifiers — `.prom` content is
    // user-supplied and flows straight into the OTLP metric name.
    if !is_valid_metric_name(name) {
        return None;
    }

    // Parse the value, ignoring the optional trailing timestamp: as in Prometheus
    // `node_exporter`, textfile samples are stamped at scrape time.
    // `f64::from_str` already accepts the format's `+Inf`, `-Inf` and `NaN` spellings.
    let value_str = value_str.split_whitespace().next()?;
    let value: f64 = value_str.parse().ok()?;

    // Parse labels if present
    let mut labels: Vec<(String, String)> = Vec::new();
    if let Some(labels_str) = labels_str {
        for part in split_labels(labels_str) {
            // Any malformed label fails the whole line rather than being dropped: losing a
            // label changes the series identity, which would collapse distinct samples onto
            // one attribute set with no diagnostic. The caller counts this as a parse
            // failure and raises `node_textfile_scrape_error`.
            let (key, val) = parse_label(part)?;

            // `.prom` content is user-supplied and flows straight into OTLP attribute keys,
            // so apply the same grammar check as the metric name.
            if !is_valid_label_name(&key) {
                return None;
            }
            // Keep the first occurrence of a repeated key — two attributes sharing a key
            // would make the data point invalid, and first-wins preserves identity.
            if labels.iter().any(|(k, _)| *k == key) {
                continue;
            }
            labels.push((key, val));
        }
    }

    // Determine metric type (default to gauge)
    let metric_type = metric_types.get(name).copied().unwrap_or(MetricType::Gauge);

    Some(CollectedMetric {
        name: name.to_string(),
        value,
        labels,
        metric_type,
        unit: None,
        description: None,
    })
}

/// Check that a name matches the Prometheus label name grammar `[a-zA-Z_][a-zA-Z0-9_]*`
fn is_valid_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check that a name matches the Prometheus metric name grammar `[a-zA-Z_:][a-zA-Z0-9_:]*`
fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Find the closing `}` in a label block, skipping over quoted strings and escapes.
/// Input starts right after the opening `{`. Returns the byte offset of `}` within `s`.
fn find_closing_brace(s: &str) -> Option<usize> {
    let mut in_quotes = false;
    let mut prev_backslash = false;
    for (i, c) in s.char_indices() {
        match c {
            '\\' if in_quotes => {
                prev_backslash = !prev_backslash;
                continue;
            }
            '"' if !prev_backslash => in_quotes = !in_quotes,
            '}' if !in_quotes => return Some(i),
            _ => {}
        }
        prev_backslash = false;
    }
    None
}

/// Split label string by commas, respecting quoted values and escape sequences
fn split_labels(labels_str: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut prev_backslash = false;

    for (i, c) in labels_str.char_indices() {
        match c {
            '\\' if in_quotes => {
                prev_backslash = !prev_backslash;
                continue;
            }
            '"' if !prev_backslash => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                parts.push(&labels_str[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        prev_backslash = false;
    }

    // Add the last part
    if start < labels_str.len() {
        parts.push(&labels_str[start..]);
    }

    parts
}

/// Parse a single label: key="value"
fn parse_label(label: &str) -> Option<(String, String)> {
    let label = label.trim();
    let eq_pos = label.find('=')?;
    let key = label[..eq_pos].trim();
    let val = label[eq_pos + 1..].trim();

    // An empty key would become an empty OTLP attribute key
    if key.is_empty() {
        return None;
    }

    // Remove exactly one surrounding quote from each end
    let val = if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
        &val[1..val.len() - 1]
    } else {
        val
    };

    // Handle escape sequences in a single pass (avoids NUL-byte placeholder issues)
    let mut result = String::with_capacity(val.len());
    let mut chars = val.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => result.push('\\'),
                Some('"') => result.push('"'),
                Some('n') => result.push('\n'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }

    Some((key.to_string(), result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::node_metrics::collector::test_support::*;
    use crate::receivers::node_metrics::config::Collector;
    use std::fs::File;
    use std::os::unix::fs::symlink;

    // ---------------------------------------------------------------------
    // Textfile collector
    // ---------------------------------------------------------------------

    #[test]
    fn test_collect_textfile_directory_contents() {
        let tmp = tempfile::tempdir().unwrap();
        // Created in the exact reverse of sorted filename order, so a collector that
        // dropped the sort could only pass by accident: the OTLP conversion takes a
        // metric's description and unit from the first sample it sees, and reorders
        // data points between scrapes if traversal order decides which file wins.
        std::fs::write(tmp.path().join("z.prom"), "shared_metric{job=\"z\"} 40\n").unwrap();
        std::fs::write(
            tmp.path().join("m.prom"),
            "\
# HELP shared_metric Documented in m.prom
shared_metric{job=\"m\"} 30
",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("b.prom"),
            "\
# HELP other_metric Documented in b.prom
shared_metric{job=\"b\"} 20
other_metric 3.5
",
        )
        .unwrap();
        std::fs::write(
            tmp.path().join("a.prom"),
            "\
# HELP shared_metric Documented in a.prom
# TYPE shared_metric counter
shared_metric{job=\"a\"} 10
this line is not valid exposition format
",
        )
        .unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Textfile],
            textfile_directory: Some(tmp.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let metrics = collector.collect_textfile(&config);

        // Values, labels, types and HELP text all come from the file.
        let from_a = find_one(&metrics, "shared_metric", &[("job", "a")]);
        assert_eq!(from_a.value, 10.0);
        assert_eq!(from_a.metric_type, MetricType::Counter);
        assert_eq!(from_a.description.as_deref(), Some("Documented in a.prom"));

        // TYPE and HELP are scoped to the file that declares them.
        let from_b = find_one(&metrics, "shared_metric", &[("job", "b")]);
        assert_eq!(from_b.value, 20.0);
        assert_eq!(from_b.metric_type, MetricType::Gauge);
        assert_eq!(from_b.description, None);

        let other = find_one(&metrics, "other_metric", &[]);
        assert_eq!(other.value, 3.5);
        assert_eq!(other.description.as_deref(), Some("Documented in b.prom"));

        let from_m = find_one(&metrics, "shared_metric", &[("job", "m")]);
        assert_eq!(from_m.value, 30.0);
        assert_eq!(from_m.description.as_deref(), Some("Documented in m.prom"));

        let from_z = find_one(&metrics, "shared_metric", &[("job", "z")]);
        assert_eq!(from_z.value, 40.0);
        assert_eq!(from_z.description, None);

        // The malformed line marks only its own file as failed.
        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "a.prom")]
            )
            .value,
            1.0
        );
        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "b.prom")]
            )
            .value,
            0.0
        );

        // Modification times are exported so a stalled writer is detectable, and
        // the files are visited in sorted filename order, not creation order.
        let mtimes: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == "node_textfile_mtime_seconds")
            .map(|m| label(m, "filename"))
            .collect();
        assert_eq!(mtimes, ["a.prom", "b.prom", "m.prom", "z.prom"]);

        // The samples themselves are emitted in that same order, so the description
        // and unit the OTLP conversion keeps for `shared_metric` are always a.prom's.
        let shared_order: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == "shared_metric")
            .map(|m| label(m, "job"))
            .collect();
        assert_eq!(shared_order, ["a", "b", "m", "z"]);

        assert!(
            find_one(
                &metrics,
                "node_textfile_mtime_seconds",
                &[("filename", "a.prom")]
            )
            .value
                > 0.0
        );
    }

    #[test]
    fn test_textfile_help_descriptions() {
        let tmp = tempfile::tempdir().unwrap();
        let prom = tmp.path().join("test.prom");
        std::fs::write(
            &prom,
            "\
# HELP my_gauge A helpful description for the gauge
# TYPE my_gauge gauge
my_gauge 42
# HELP my_counter Total number of things
# TYPE my_counter counter
my_counter 100
no_help_metric 7
",
        )
        .unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Textfile],
            textfile_directory: Some(prom.to_str().unwrap().to_string()),
            ..Default::default()
        };

        let metrics = collector.collect_textfile(&config);

        let find = |name: &str| metrics.iter().find(|m| m.name == name);

        // Metrics with HELP comments should have descriptions
        let gauge = find("my_gauge").expect("my_gauge not found");
        assert_eq!(
            gauge.description.as_deref(),
            Some("A helpful description for the gauge")
        );
        assert_eq!(gauge.metric_type, MetricType::Gauge);

        let counter = find("my_counter").expect("my_counter not found");
        assert_eq!(
            counter.description.as_deref(),
            Some("Total number of things")
        );
        assert_eq!(counter.metric_type, MetricType::Counter);

        // Metric without HELP should have no description
        let no_help = find("no_help_metric").expect("no_help_metric not found");
        assert_eq!(no_help.description, None);
    }

    #[test]
    fn test_textfile_single_file_path() {
        let tmp = tempfile::tempdir().unwrap();
        let prom = tmp.path().join("single.prom");
        std::fs::write(&prom, "single_metric 99\n").unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Textfile],
            textfile_directory: Some(prom.to_str().unwrap().to_string()),
            ..Default::default()
        };

        let metrics = collector.collect_textfile(&config);
        let found = metrics.iter().find(|m| m.name == "single_metric");
        assert!(
            found.is_some(),
            "single_metric not found in {:?}",
            metrics.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
        assert_eq!(found.unwrap().value, 99.0);
    }

    #[test]
    fn test_textfile_directory_path() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("a.prom"), "metric_a 1\n").unwrap();
        std::fs::write(tmp.path().join("b.prom"), "metric_b 2\n").unwrap();
        std::fs::write(tmp.path().join("ignored.txt"), "not_a_metric 3\n").unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let config = NodeMetricsConfig {
            collectors: vec![Collector::Textfile],
            textfile_directory: Some(tmp.path().to_str().unwrap().to_string()),
            ..Default::default()
        };

        let metrics = collector.collect_textfile(&config);
        let names: Vec<&str> = metrics.iter().map(|m| m.name.as_str()).collect();

        assert!(
            names.contains(&"metric_a"),
            "metric_a not found in {:?}",
            names
        );
        assert!(
            names.contains(&"metric_b"),
            "metric_b not found in {:?}",
            names
        );
        assert!(
            !names.contains(&"not_a_metric"),
            ".txt file should be ignored"
        );
    }

    /// The textfile config for a `.prom` file or a directory of them.
    fn textfile_config(path: &str) -> NodeMetricsConfig {
        NodeMetricsConfig {
            collectors: vec![Collector::Textfile],
            textfile_directory: Some(path.to_string()),
            ..Default::default()
        }
    }

    /// A `.prom` that cannot be read — a dangling symlink is the realistic case — must be
    /// reported, not silently skipped, or a deployment whose symlink target was garbage
    /// collected loses metrics with no signal at all.
    #[test]
    fn test_textfile_dangling_symlink_is_reported_as_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(
            tmp.path().join("no-such-target.prom"),
            tmp.path().join("dangling.prom"),
        )
        .unwrap();
        write(tmp.path(), "good.prom", "good_metric 1\n");

        let collector = collector_with_empty_roots();
        let metrics = collector
            .1
            .collect_textfile(&textfile_config(tmp.path().to_str().unwrap()));

        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "dangling.prom")]
            )
            .value,
            1.0
        );
        // A dangling link has no mtime to report, and the healthy file is unaffected.
        assert!(
            matching(
                &metrics,
                "node_textfile_mtime_seconds",
                &[("filename", "dangling.prom")]
            )
            .is_empty()
        );
        assert_eq!(find_one(&metrics, "good_metric", &[]).value, 1.0);
    }

    /// A FIFO named `*.prom` must never be opened: `open(2)` on one with no writer blocks
    /// forever, which would wedge the scrape thread and stop all node metrics.
    #[test]
    fn test_textfile_fifo_is_skipped_without_reading_it() {
        let tmp = tempfile::tempdir().unwrap();
        let fifo = tmp.path().join("pipe.prom");
        let cpath = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
        // SAFETY: mkfifo on a path inside our own temp directory
        let rc = unsafe { libc::mkfifo(cpath.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "could not create fifo for the test");
        write(tmp.path(), "plain.prom", "plain_metric 7\n");

        let collector = collector_with_empty_roots();
        // If the FIFO were opened this call would never return.
        let metrics = collector
            .1
            .collect_textfile(&textfile_config(tmp.path().to_str().unwrap()));

        assert_eq!(find_one(&metrics, "plain_metric", &[]).value, 7.0);
        // Skipped silently: it is not a textfile at all, so it is not an error either.
        assert!(
            matching(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "pipe.prom")]
            )
            .is_empty()
        );
    }

    /// The exposition format allows any whitespace run after `#`, but the keyword itself
    /// must be followed by whitespace — `# HELPfoo` is an ordinary comment.
    #[test]
    fn test_comment_keyword_whitespace_handling() {
        let mut types = HashMap::new();
        types.insert("m".to_string(), MetricType::Counter);

        assert_eq!(
            strip_comment_keyword("#HELP m desc", "HELP"),
            Some(" m desc")
        );
        assert_eq!(
            strip_comment_keyword("#   TYPE m counter", "TYPE"),
            Some(" m counter")
        );
        assert_eq!(strip_comment_keyword("# HELPfoo m desc", "HELP"), None);
        assert_eq!(strip_comment_keyword("# TYPEfoo m counter", "TYPE"), None);
        assert_eq!(strip_comment_keyword("# just a comment", "HELP"), None);
        assert_eq!(strip_comment_keyword("not a comment", "HELP"), None);
        assert_eq!(strip_comment_keyword("#", "HELP"), None);
    }

    /// `.prom` content is user-supplied, so label keys get the same grammar check as metric
    /// names. An invalid key fails the line — dropping just the label would change the
    /// series identity and silently collapse distinct samples onto one attribute set.
    #[test]
    fn test_invalid_label_key_rejects_the_line_and_duplicates_are_deduped() {
        let types = HashMap::new();

        assert!(parse_prometheus_line(r#"m{bad-key="1"} 5"#, &types).is_none());
        assert!(parse_prometheus_line(r#"m{="1"} 5"#, &types).is_none());
        assert!(parse_prometheus_line(r#"m{1abc="1"} 5"#, &types).is_none());

        // A colon is legal in a metric name but not in a label name
        assert!(parse_prometheus_line(r#"m{a:b="1"} 5"#, &types).is_none());

        // First occurrence of a repeated key wins, and the line still parses
        let metric = parse_prometheus_line(r#"m{ok="first",ok="second"} 5"#, &types).unwrap();
        assert_eq!(metric.labels, vec![("ok".to_string(), "first".to_string())]);
    }

    #[test]
    fn test_textfile_oversized_file_is_skipped_and_flagged() {
        let tmp = tempfile::tempdir().unwrap();

        // A runaway writer script: reading this on every scrape interval would pull
        // the whole file into memory.  Created sparsely — only the first line is
        // written, then the file is extended one byte past the cap.
        let big = tmp.path().join("big.prom");
        std::fs::write(&big, "big_metric 1\n").unwrap();
        File::options()
            .write(true)
            .open(&big)
            .unwrap()
            .set_len(MAX_TEXTFILE_SIZE_BYTES + 1)
            .unwrap();
        std::fs::write(tmp.path().join("small.prom"), "small_metric 2\n").unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let metrics = collector.collect_textfile(&textfile_config(tmp.path().to_str().unwrap()));

        // The oversized file is not parsed at all, and says so.
        assert!(matching(&metrics, "big_metric", &[]).is_empty());
        let error = find_one(
            &metrics,
            "node_textfile_scrape_error",
            &[("filename", "big.prom")],
        );
        assert_eq!(error.value, 1.0);
        assert_eq!(error.metric_type, MetricType::Gauge);
        assert_eq!(error.unit, None);
        assert_eq!(error.labels.len(), 1);

        // Its modification time is still reported, so a writer stuck emitting an
        // oversized file stays observable.
        find_one(
            &metrics,
            "node_textfile_mtime_seconds",
            &[("filename", "big.prom")],
        );

        // Skipping one file does not abandon the rest of the directory.
        assert_eq!(find_one(&metrics, "small_metric", &[]).value, 2.0);
        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "small.prom")]
            )
            .value,
            0.0
        );
    }

    #[test]
    fn test_textfile_missing_directory_reports_error_against_configured_path() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-directory");
        let configured = missing.to_str().unwrap().to_string();

        let (_roots, collector) = collector_with_empty_roots();
        let metrics = collector.collect_textfile(&textfile_config(&configured));

        // A mistyped directory must not fail silently, or the receiver just emits
        // nothing forever.
        assert_eq!(metrics.len(), 1);
        let error = &metrics[0];
        assert_eq!(error.name, "node_textfile_scrape_error");
        assert_eq!(error.value, 1.0);
        assert_eq!(error.metric_type, MetricType::Gauge);
        assert_eq!(error.unit, None);

        // Deliberately asymmetric with the per-file errors, which carry a bare
        // filename: there is no file here, so the configured path is the only thing
        // that identifies what went wrong.
        assert_eq!(error.labels, vec![("filename".to_string(), configured)]);
    }

    #[test]
    fn test_textfile_unsupported_type_falls_back_to_gauge() {
        let tmp = tempfile::tempdir().unwrap();
        let prom = tmp.path().join("types.prom");
        std::fs::write(
            &prom,
            "\
# TYPE my_histogram histogram
my_histogram 5
# TYPE my_summary summary
my_summary 6
# TYPE my_untyped untyped
my_untyped 7
# TYPE my_counter counter
my_counter 8
",
        )
        .unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let metrics = collector.collect_textfile(&textfile_config(prom.to_str().unwrap()));

        // histogram, summary and untyped are not modelled; their series are exported
        // as gauges rather than being dropped or promoted to counters.
        for (name, value) in [
            ("my_histogram", 5.0),
            ("my_summary", 6.0),
            ("my_untyped", 7.0),
        ] {
            let metric = find_one(&metrics, name, &[]);
            assert_eq!(metric.metric_type, MetricType::Gauge, "{}", name);
            assert_eq!(metric.value, value, "{}", name);
        }

        // A recognised type in the same file is still honoured.
        let counter = find_one(&metrics, "my_counter", &[]);
        assert_eq!(counter.metric_type, MetricType::Counter);
        assert_eq!(counter.value, 8.0);

        // An unsupported TYPE is not a parse failure.
        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "types.prom")]
            )
            .value,
            0.0
        );
    }

    #[test]
    fn test_textfile_symlinked_prom_file_is_collected() {
        // The real file lives outside the scanned directory, as it does when a
        // deployment symlinks generated metrics into place.
        let elsewhere = tempfile::tempdir().unwrap();
        let real = elsewhere.path().join("generated.prom");
        std::fs::write(&real, "# TYPE linked_metric counter\nlinked_metric 5\n").unwrap();

        let tmp = tempfile::tempdir().unwrap();
        symlink(&real, tmp.path().join("linked.prom")).unwrap();
        std::fs::write(tmp.path().join("plain.prom"), "plain_metric 1\n").unwrap();
        // A *directory* named like a textfile is still not a textfile.
        std::fs::create_dir(tmp.path().join("adirectory.prom")).unwrap();

        let (_roots, collector) = collector_with_empty_roots();
        let metrics = collector.collect_textfile(&textfile_config(tmp.path().to_str().unwrap()));

        // The scan resolves symlinks, so the link is read like any other file.
        let linked = find_one(&metrics, "linked_metric", &[]);
        assert_eq!(linked.value, 5.0);
        assert_eq!(linked.metric_type, MetricType::Counter);
        assert_eq!(
            find_one(
                &metrics,
                "node_textfile_scrape_error",
                &[("filename", "linked.prom")]
            )
            .value,
            0.0
        );
        // The link's own name is what identifies it, not the target's.
        find_one(
            &metrics,
            "node_textfile_mtime_seconds",
            &[("filename", "linked.prom")],
        );
        assert!(
            matching(
                &metrics,
                "node_textfile_mtime_seconds",
                &[("filename", "generated.prom")]
            )
            .is_empty()
        );

        assert_eq!(find_one(&metrics, "plain_metric", &[]).value, 1.0);

        // Exactly two files were scanned: the directory named `adirectory.prom` is
        // not reported as an unreadable textfile.
        let mut scanned: Vec<&str> = metrics
            .iter()
            .filter(|m| m.name == "node_textfile_scrape_error")
            .map(|m| label(m, "filename"))
            .collect();
        scanned.sort_unstable();
        assert_eq!(scanned, ["linked.prom", "plain.prom"]);
    }

    // ---------------------------------------------------------------------
    // Prometheus exposition-format parsing
    // ---------------------------------------------------------------------

    #[test]
    fn test_parse_prometheus_line_simple() {
        let types = HashMap::new();
        let metric = parse_prometheus_line("my_metric 42.5", &types).unwrap();
        assert_eq!(metric.name, "my_metric");
        assert_eq!(metric.value, 42.5);
        assert!(metric.labels.is_empty());
        assert_eq!(metric.metric_type, MetricType::Gauge);
    }

    #[test]
    fn test_parse_prometheus_line_with_labels() {
        let types = HashMap::new();
        let metric = parse_prometheus_line(
            "http_requests_total{method=\"GET\",path=\"/api\"} 1234",
            &types,
        )
        .unwrap();
        assert_eq!(metric.name, "http_requests_total");
        assert_eq!(metric.value, 1234.0);
        let find_label = |key: &str| {
            metric
                .labels
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(find_label("method"), Some("GET".to_string()));
        assert_eq!(find_label("path"), Some("/api".to_string()));
    }

    #[test]
    fn test_parse_prometheus_line_with_type() {
        let mut types = HashMap::new();
        types.insert("my_counter".to_string(), MetricType::Counter);

        let metric = parse_prometheus_line("my_counter 100", &types).unwrap();
        assert_eq!(metric.metric_type, MetricType::Counter);
    }

    #[test]
    fn test_parse_prometheus_line_special_values() {
        let types = HashMap::new();

        let inf = parse_prometheus_line("metric_inf +Inf", &types).unwrap();
        assert!(inf.value.is_infinite() && inf.value.is_sign_positive());

        let neg_inf = parse_prometheus_line("metric_neginf -Inf", &types).unwrap();
        assert!(neg_inf.value.is_infinite() && neg_inf.value.is_sign_negative());

        let nan = parse_prometheus_line("metric_nan NaN", &types).unwrap();
        assert!(nan.value.is_nan());
    }

    #[test]
    fn test_parse_prometheus_line_with_timestamp() {
        let types = HashMap::new();
        // Timestamp should be ignored
        let metric = parse_prometheus_line("my_metric 42.5 1395066363000", &types).unwrap();
        assert_eq!(metric.name, "my_metric");
        assert_eq!(metric.value, 42.5);
    }

    #[test]
    fn test_parse_prometheus_line_escaped_label() {
        let types = HashMap::new();
        let metric =
            parse_prometheus_line("metric{label=\"value with \\\"quotes\\\"\"} 1", &types).unwrap();
        let label_val = metric
            .labels
            .iter()
            .find(|(k, _)| k == "label")
            .map(|(_, v)| v.clone());
        assert_eq!(label_val, Some("value with \"quotes\"".to_string()));
    }

    #[test]
    fn test_parse_prometheus_line_brace_in_label_value() {
        let types = HashMap::new();
        let metric = parse_prometheus_line(r#"metric{label="a}b"} 42"#, &types).unwrap();
        assert_eq!(metric.name, "metric");
        assert_eq!(metric.value, 42.0);
        let label_val = metric
            .labels
            .iter()
            .find(|(k, _)| k == "label")
            .map(|(_, v)| v.clone());
        assert_eq!(label_val, Some("a}b".to_string()));
    }

    #[test]
    fn test_parse_prometheus_line_empty() {
        let types = HashMap::new();
        assert!(parse_prometheus_line("", &types).is_none());
        assert!(parse_prometheus_line("# comment", &types).is_none());
    }

    #[test]
    fn test_split_labels() {
        let labels = split_labels("method=\"GET\",path=\"/api\"");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], "method=\"GET\"");
        assert_eq!(labels[1], "path=\"/api\"");
    }

    #[test]
    fn test_split_labels_with_comma_in_value() {
        let labels = split_labels("label=\"value,with,commas\",other=\"ok\"");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], "label=\"value,with,commas\"");
        assert_eq!(labels[1], "other=\"ok\"");
    }

    #[test]
    fn test_split_labels_with_escaped_quotes() {
        let labels = split_labels(r#"method="GET",path="/foo\"bar""#);
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], r#"method="GET""#);
        assert_eq!(labels[1], r#"path="/foo\"bar""#);
    }

    #[test]
    fn test_split_labels_with_escaped_backslash_before_quote() {
        // \\\" in the source: escaped backslash followed by escaped quote
        let labels = split_labels(r#"a="x\\\"y",b="ok""#);
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0], r#"a="x\\\"y""#);
        assert_eq!(labels[1], r#"b="ok""#);
    }

    #[test]
    fn test_parse_label_escaped_backslash_before_n() {
        // \\n in exposition format = literal backslash followed by 'n'
        let result = parse_label(r#"key="a\\nb""#).unwrap();
        assert_eq!(result.0, "key");
        assert_eq!(result.1, "a\\nb");
    }

    #[test]
    fn test_parse_label_unknown_escape() {
        // Unknown escape sequences should preserve the backslash
        let result = parse_label(r#"key="a\tb""#).unwrap();
        assert_eq!(result.1, "a\\tb");
    }
}
