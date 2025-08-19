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
    RegexBuilder::new(r"Code: (\d+)\. DB::Exception: ([^()]*).*\(version ([^ ]+)")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_exception_with_valid_format() {
        let chunk = b"Code: 62. DB::Exception: Syntax error: failed at position 1: SELECT. Expected one of: SHOW, CREATE, DROP, RENAME, ALTER (version 23.8.1.94854 (official build))\n";

        let result = extract_exception(chunk);
        assert!(result.is_some());

        let (code, desc, version) = result.unwrap();
        assert_eq!(code, 62);
        assert_eq!(
            desc,
            "Syntax error: failed at position 1: SELECT. Expected one of: SHOW, CREATE, DROP, RENAME, ALTER "
        );
        assert_eq!(version, "23.8.1.94854");
    }

    #[test]
    fn test_extract_exception_with_different_code() {
        let chunk = b"Code: 404. DB::Exception: Table default.test_table doesn't exist (version 22.3.15.33 (official build))\n";

        let result = extract_exception(chunk);
        assert!(result.is_some());

        let (code, desc, version) = result.unwrap();
        assert_eq!(code, 404);
        assert_eq!(desc, "Table default.test_table doesn't exist ");
        assert_eq!(version, "22.3.15.33");
    }

    #[test]
    fn test_extract_exception_without_ending_pattern() {
        let chunk = b"Code: 62. DB::Exception: Some error (version 23.8.1.94854 (official build))";

        let result = extract_exception(chunk);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_exception_with_invalid_format() {
        let chunk = b"Some random error message))\n";

        let result = extract_exception(chunk);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_exception_with_malformed_code() {
        let chunk =
            b"Code: abc. DB::Exception: Invalid code (version 23.8.1.94854 (official build))\n";

        let result = extract_exception(chunk);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_exception_with_empty_chunk() {
        let chunk = b"";

        let result = extract_exception(chunk);
        assert!(result.is_none());
    }
}
