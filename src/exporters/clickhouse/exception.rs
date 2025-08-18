// From Clickhouse Rust lib

use std::sync::LazyLock;

use regex::bytes::{Regex, RegexBuilder};

// Format:
// ```
//   <data>Code: <code>. DB::Exception: <desc> (version <version> (official build))\n
// ```
pub fn extract_exception(chunk: &[u8]) -> Option<(i32, String, String)> {
    // `))\n` is very rare in real data, so it's fast dirty check.
    // In random data, it occurs with a probability of ~6*10^-8 only.
    if chunk.ends_with(b"))\n") {
        extract_exception_slow(chunk)
    } else {
        None
    }
}

pub static DB_EXCEPTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(r"Code: (\d+)\. DB::Exception: ([^.]*).*\(version ([^ ]+)")
        .dot_matches_new_line(true)
        .build()
        .unwrap()
});

fn extract_exception_slow(chunk: &[u8]) -> Option<(i32, String, String)> {
    DB_EXCEPTION_RE
        .captures(chunk)
        .map(|caps| match (caps.get(1), caps.get(2), caps.get(3)) {
            (Some(code), Some(desc), Some(version)) => Some((
                String::from_utf8_lossy(code.as_bytes()).to_string(),
                String::from_utf8_lossy(desc.as_bytes()).to_string(),
                String::from_utf8_lossy(version.as_bytes()).to_string(),
            )),
            _ => None,
        })
        .flatten()
        .map(|(code, desc, version)| match code.parse::<i32>() {
            Ok(code) => Some((code, desc, version)),
            Err(_) => None,
        })
        .flatten()
}
