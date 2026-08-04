// SPDX-License-Identifier: Apache-2.0

//! Path, device-name and `utsname` field helpers shared by the collectors.
//!
//! The virtual-device and partition exclusion heuristics are derived from Prometheus
//! node_exporter — see the notice in this module's `mod.rs`.

/// Check if `path` matches a prefix with path-boundary awareness.
///
/// Returns `true` when `path` equals `prefix` exactly or starts with `prefix`
/// followed by `/`.  This prevents `/dev` from matching `/developer`.
///
/// A trailing `/` on the prefix is ignored, so `/dev` and `/dev/` behave alike.
pub(crate) fn matches_path_prefix(path: &str, prefix: &str) -> bool {
    // "/" trims to "", and the root prefix matches every absolute path
    let trimmed = prefix.trim_end_matches('/');
    if trimmed.is_empty() {
        return prefix.starts_with('/') && path.starts_with('/');
    }
    path == trimmed
        || path.starts_with(trimmed) && path.as_bytes().get(trimmed.len()) == Some(&b'/')
}

/// Check if a device name is a virtual block device that should not be reported
///
/// Covers the `z?ram`, `loop` and `fd` families of node_exporter's default device
/// exclusion (its full default also excludes partitions, which `is_partition` handles).
pub(super) fn is_virtual_disk(device: &str) -> bool {
    for prefix in &["zram", "ram", "loop", "fd"] {
        if let Some(rest) = device.strip_prefix(prefix)
            && !rest.is_empty()
            && rest.chars().all(|c| c.is_ascii_digit())
        {
            return true;
        }
    }
    false
}

/// Check if a device name looks like a partition
///
/// This is a name heuristic. Partitions are excluded from disk I/O metrics so their
/// counters are not double-counted alongside the whole disk that contains them.
///
/// Deliberately stricter than node_exporter, whose default `p<digits>` rule applies only
/// to NVMe: this also treats `mmcblk0p1`, `md127p1`, `nbd0p1`, `zd16p1`, `pmem0p1` and
/// `dasda1` as partitions, so those per-partition series are not reported.
pub(super) fn is_partition(device: &str) -> bool {
    // Devices whose whole-disk name ends in a digit delimit the partition with `p`:
    // nvme0n1p1, mmcblk0p1, md127p1, nbd0p1, zd16p1, pmem0p1, loop0p1.
    if let Some((disk, part)) = device.rsplit_once('p')
        && !part.is_empty()
        && part.chars().all(|c| c.is_ascii_digit())
        && disk.ends_with(|c: char| c.is_ascii_digit())
    {
        return true;
    }

    // SCSI/SATA/virtio/s390 partitions: sda1, vda1, xvda1, hda1, sdaa1, dasda1.
    // After the prefix, one or more lowercase letters identify the disk,
    // and any trailing digits indicate a partition number.
    for prefix in &["sd", "vd", "xvd", "hd", "dasd"] {
        if let Some(rest) = device.strip_prefix(prefix) {
            let letter_count = rest.chars().take_while(|c| c.is_ascii_lowercase()).count();
            if letter_count == 0 {
                continue;
            }
            let after_letters = &rest[letter_count..];
            if !after_letters.is_empty() && after_letters.chars().all(|c| c.is_ascii_digit()) {
                return true;
            }
        }
    }

    false
}

/// Decode octal escape sequences from /proc/mounts paths.
///
/// The kernel encodes space as `\040`, tab as `\011`, newline as `\012`,
/// and backslash as `\134` in device and mount-point fields.
pub(super) fn decode_mount_path(s: &str) -> String {
    // Fast path: no escapes present (common case)
    if !s.contains('\\') {
        return s.to_string();
    }
    let bytes = s.as_bytes();
    let mut result = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\'
            && i + 3 < bytes.len()
            && (b'0'..=b'7').contains(&bytes[i + 1])
            && (b'0'..=b'7').contains(&bytes[i + 2])
            && (b'0'..=b'7').contains(&bytes[i + 3])
        {
            let val = (bytes[i + 1] - b'0') as u16 * 64
                + (bytes[i + 2] - b'0') as u16 * 8
                + (bytes[i + 3] - b'0') as u16;
            // Three octal digits can represent up to \777 = 511, so
            // guard against values that don't fit in a single byte.
            // The kernel only emits valid byte values, but be safe.
            if val <= 255 {
                result.push(val as u8);
                i += 4;
            } else {
                result.push(bytes[i]);
                i += 1;
            }
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(result).unwrap_or_else(|_| s.to_string())
}

/// Convert a fixed-size C character array from `utsname` into a Rust string
///
/// `uname(2)` NUL-terminates each field, but the scan is bounded by the array length so
/// that a field which is not terminated cannot be read out of bounds.
pub(super) fn utsname_field(field: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = field
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    String::from_utf8_lossy(&bytes).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---------------------------------------------------------------------
    // Device-name classification
    // ---------------------------------------------------------------------

    #[test]
    fn test_is_virtual_disk() {
        // Virtual devices: an accepted prefix followed by nothing but digits,
        // equivalent to node_exporter's `^(z?ram|loop|fd)\d+$`.
        assert!(is_virtual_disk("ram0"));
        assert!(is_virtual_disk("ram15"));
        assert!(is_virtual_disk("zram0"));
        assert!(is_virtual_disk("loop0"));
        assert!(is_virtual_disk("loop10"));
        assert!(is_virtual_disk("fd0"));

        // Plain prefix matching would wrongly exclude these: the suffix must be
        // digits only, and a bare prefix with no index is not a device instance.
        assert!(!is_virtual_disk("ramdisk0"));
        assert!(!is_virtual_disk("loopback"));
        assert!(!is_virtual_disk("ram"));
        assert!(!is_virtual_disk("zram"));
        assert!(!is_virtual_disk("loop"));
        assert!(!is_virtual_disk("fd"));
        assert!(!is_virtual_disk("loop0p1"));

        // Real disks are never excluded.
        assert!(!is_virtual_disk("sda"));
        assert!(!is_virtual_disk("nvme0n1"));
        assert!(!is_virtual_disk("mmcblk0"));
    }

    #[test]
    fn test_is_partition() {
        // Whole disks
        assert!(!is_partition("sda"));
        assert!(!is_partition("sdb"));
        assert!(!is_partition("vda"));
        assert!(!is_partition("xvda"));
        assert!(!is_partition("nvme0n1"));
        assert!(!is_partition("hda"));

        assert!(!is_partition("mmcblk0"));
        assert!(!is_partition("mmcblk1"));

        // Multi-letter disk identifiers (e.g. 27th+ disk)
        assert!(!is_partition("sdaa"));
        assert!(!is_partition("sdab"));

        // Whole disks whose names end in a digit but delimit no partition
        assert!(!is_partition("dm-0"));
        assert!(!is_partition("md0"));
        assert!(!is_partition("md127"));
        assert!(!is_partition("sr0"));
        assert!(!is_partition("loop0"));

        // Partitions
        assert!(is_partition("sda1"));
        assert!(is_partition("sda12"));
        assert!(is_partition("sdaa1"));
        assert!(is_partition("sdaa12"));
        assert!(is_partition("vda1"));
        assert!(is_partition("nvme0n1p1"));
        assert!(is_partition("nvme0n1p12"));
        assert!(is_partition("xvda1"));
        assert!(is_partition("mmcblk0p1"));
        assert!(is_partition("mmcblk0p12"));
        assert!(is_partition("md127p1"));
        assert!(is_partition("dasda1"));
    }

    // ---------------------------------------------------------------------
    // Path decoding and prefix matching
    // ---------------------------------------------------------------------

    #[test]
    fn test_decode_mount_path_no_escapes() {
        assert_eq!(decode_mount_path("/mnt/data"), "/mnt/data");
    }

    #[test]
    fn test_decode_mount_path_space() {
        // \040 is space
        assert_eq!(decode_mount_path("/mnt/my\\040drive"), "/mnt/my drive");
    }

    #[test]
    fn test_decode_mount_path_multiple_escapes() {
        // \040 space, \011 tab, \012 newline, \134 backslash
        assert_eq!(decode_mount_path("/mnt/a\\040b\\134c"), "/mnt/a b\\c");
    }

    #[test]
    fn test_decode_mount_path_tab_and_newline() {
        assert_eq!(decode_mount_path("a\\011b\\012c"), "a\tb\nc");
    }

    #[test]
    fn test_decode_mount_path_trailing_backslash() {
        // Incomplete escape at end should be preserved as-is
        assert_eq!(decode_mount_path("/mnt/foo\\04"), "/mnt/foo\\04");
    }

    #[test]
    fn test_decode_mount_path_non_octal_after_backslash() {
        // \9 is not a valid octal digit, should be preserved
        assert_eq!(decode_mount_path("/mnt/foo\\9bar"), "/mnt/foo\\9bar");
    }

    #[test]
    fn test_matches_path_prefix_exact() {
        assert!(matches_path_prefix("/dev", "/dev"));
        assert!(matches_path_prefix("/proc", "/proc"));
    }

    #[test]
    fn test_matches_path_prefix_subpath() {
        assert!(matches_path_prefix("/dev/shm", "/dev"));
        assert!(matches_path_prefix("/proc/sys/fs", "/proc"));
        assert!(matches_path_prefix(
            "/var/lib/docker/overlay2",
            "/var/lib/docker"
        ));
        assert!(matches_path_prefix(
            "/var/lib/containers/storage/overlay",
            "/var/lib/containers"
        ));
    }

    #[test]
    fn test_matches_path_prefix_no_false_positives() {
        // Must not match when the prefix is only a substring, not a path boundary
        assert!(!matches_path_prefix("/developer", "/dev"));
        assert!(!matches_path_prefix("/devops", "/dev"));
        assert!(!matches_path_prefix("/processing", "/proc"));
        assert!(!matches_path_prefix(
            "/var/lib/dockerfiles",
            "/var/lib/docker"
        ));
        assert!(!matches_path_prefix(
            "/var/lib/containers2",
            "/var/lib/containers"
        ));
    }

    #[test]
    fn test_matches_path_prefix_no_match() {
        assert!(!matches_path_prefix("/mnt/data", "/dev"));
        assert!(!matches_path_prefix("/home/user", "/proc"));
    }

    #[test]
    fn test_matches_path_prefix_root_and_empty_prefix() {
        // The root prefix trims to the empty string, so it cannot be compared as a
        // path component: it has to match every absolute path instead.
        assert!(matches_path_prefix("/", "/"));
        assert!(matches_path_prefix("/x", "/"));
        assert!(matches_path_prefix("/var/lib/docker", "/"));

        // An empty prefix is not the root and must match nothing, otherwise a blank
        // configured exclusion would silently drop every filesystem.
        assert!(!matches_path_prefix("/x", ""));
        assert!(!matches_path_prefix("", ""));

        // A relative path is not below the root prefix.
        assert!(!matches_path_prefix("x", "/"));
    }

    #[test]
    fn test_matches_path_prefix_ignores_trailing_slash_on_prefix() {
        // A trailing slash must behave exactly like no trailing slash, so a
        // configured "/dev/" is not silently inert.
        for (path, expected) in [
            ("/dev", true),
            ("/dev/shm", true),
            ("/developer", false),
            ("/mnt/data", false),
        ] {
            assert_eq!(
                matches_path_prefix(path, "/dev/"),
                expected,
                "prefix /dev/ against {}",
                path
            );
            assert_eq!(
                matches_path_prefix(path, "/dev/"),
                matches_path_prefix(path, "/dev"),
                "trailing slash changed the result for {}",
                path
            );
        }
    }

    // ---------------------------------------------------------------------
    // utsname fields
    // ---------------------------------------------------------------------

    /// Build a `utsname`-style field from raw bytes.
    ///
    /// `libc::c_char` is signed on most targets but unsigned on others (aarch64 Linux),
    /// so the cast rather than a literal type keeps this portable.
    fn utsname_bytes(bytes: &[u8]) -> Vec<libc::c_char> {
        bytes.iter().map(|&b| b as libc::c_char).collect()
    }

    #[test]
    fn test_utsname_field_stops_at_nul() {
        // uname(2) NUL-terminates each field and leaves the remainder of the array
        // undefined; everything past the terminator must be ignored.
        let field = utsname_bytes(b"Linux\0-leftover-garbage\0");
        assert_eq!(utsname_field(&field), "Linux");

        // An empty field is an empty string, not a stray NUL character.
        assert_eq!(utsname_field(&utsname_bytes(b"\0\0\0")), "");
        assert_eq!(utsname_field(&[]), "");
    }

    #[test]
    fn test_utsname_field_without_terminator_truncates_at_array_length() {
        // A field that fills the array with no room for the terminator must stop at
        // the end of the slice rather than reading past it.
        let field = utsname_bytes(b"6.1.0-no-nul-here");
        assert_eq!(utsname_field(&field), "6.1.0-no-nul-here");
        assert_eq!(utsname_field(&field).len(), field.len());
    }

    #[test]
    fn test_utsname_field_converts_non_utf8_lossily() {
        // The kernel does not promise UTF-8 (a hostname can hold arbitrary bytes), and
        // a lossy conversion is preferable to dropping the whole label.
        let field = utsname_bytes(&[b'a', 0xFF, b'b', 0]);
        assert_eq!(utsname_field(&field), "a\u{FFFD}b");

        // A truncated multi-byte sequence is replaced, not silently dropped.
        assert_eq!(utsname_field(&utsname_bytes(&[0xC3, 0x28, 0])), "\u{FFFD}(");
    }
}
