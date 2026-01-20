// SPDX-License-Identifier: Apache-2.0

//! Platform-independent file identity based on inode (Unix) or file index (Windows).
//!
//! This allows tracking files across renames/rotations, since the inode/file index
//! remains stable even when the file is renamed.

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::path::Path;

/// A platform-independent unique identifier for a file.
///
/// On Unix systems, this is the device ID + inode number.
/// On Windows, this is the volume serial number + file index.
///
/// This identifier remains stable across file renames, making it ideal
/// for tracking log files through rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId {
    /// Device ID (Unix) or volume serial number (Windows)
    dev: u64,
    /// Inode number (Unix) or file index (Windows)
    ino: u64,
}

impl FileId {
    /// Create a FileId from raw device and inode values.
    /// Used for loading persisted state.
    pub fn new(dev: u64, ino: u64) -> Self {
        Self { dev, ino }
    }

    /// Create a FileId from an open file handle.
    #[cfg(unix)]
    pub fn from_file(file: &File) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        let metadata = file.metadata()?;
        Ok(Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        })
    }

    /// Create a FileId from an open file handle.
    #[cfg(windows)]
    pub fn from_file(file: &File) -> io::Result<Self> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::HANDLE;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };

        let handle = file.as_raw_handle() as HANDLE;
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };

        let result = unsafe { GetFileInformationByHandle(handle, &mut info) };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }

        // Combine high and low parts of file index
        let file_index = ((info.nFileIndexHigh as u64) << 32) | (info.nFileIndexLow as u64);

        Ok(Self {
            dev: info.dwVolumeSerialNumber as u64,
            ino: file_index,
        })
    }

    /// Create a FileId from a path by opening the file.
    pub fn from_path(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        Self::from_file(&file)
    }

    /// Get the device ID (Unix) or volume serial number (Windows).
    pub fn dev(&self) -> u64 {
        self.dev
    }

    /// Get the inode number (Unix) or file index (Windows).
    pub fn ino(&self) -> u64 {
        self.ino
    }
}

impl std::fmt::Display for FileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.dev, self.ino)
    }
}

/// Get the current path of an open file handle.
///
/// This is useful for detecting if a file has been renamed/rotated.
/// If the file was deleted but the handle is still open, this returns an error.
#[cfg(target_os = "linux")]
pub fn get_path_from_file(file: &File) -> io::Result<std::path::PathBuf> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();
    let link_path = format!("/proc/self/fd/{}", fd);
    std::fs::read_link(&link_path)
}

/// Get the current path of an open file handle.
#[cfg(target_os = "macos")]
pub fn get_path_from_file(file: &File) -> io::Result<std::path::PathBuf> {
    use std::os::unix::io::AsRawFd;

    let fd = file.as_raw_fd();

    // F_GETPATH returns the path in a buffer
    let mut buf = vec![0u8; libc::PATH_MAX as usize];
    let result = unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) };

    if result == -1 {
        return Err(io::Error::last_os_error());
    }

    // Find the null terminator
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let path_str = std::str::from_utf8(&buf[..len])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok(std::path::PathBuf::from(path_str))
}

/// Get the current path of an open file handle.
#[cfg(windows)]
pub fn get_path_from_file(file: &File) -> io::Result<std::path::PathBuf> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_NAME_NORMALIZED, GetFinalPathNameByHandleW,
    };

    let handle = file.as_raw_handle() as HANDLE;

    // First call to get the required buffer size
    let size =
        unsafe { GetFinalPathNameByHandleW(handle, std::ptr::null_mut(), 0, FILE_NAME_NORMALIZED) };

    if size == 0 {
        return Err(io::Error::last_os_error());
    }

    // Allocate buffer and get the path
    let mut buf: Vec<u16> = vec![0; size as usize];
    let result = unsafe {
        GetFinalPathNameByHandleW(
            handle,
            buf.as_mut_ptr(),
            buf.len() as u32,
            FILE_NAME_NORMALIZED,
        )
    };

    if result == 0 {
        return Err(io::Error::last_os_error());
    }

    // Convert to PathBuf (remove \\?\ prefix if present)
    let path = String::from_utf16_lossy(&buf[..result as usize]);
    let path = path.strip_prefix(r"\\?\").unwrap_or(&path);
    Ok(std::path::PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_id_from_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let f = file.reopen().unwrap();
        let id = FileId::from_file(&f).unwrap();

        // IDs should be non-zero
        assert!(id.dev() > 0 || id.ino() > 0);
    }

    #[test]
    fn test_file_id_from_path() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let id = FileId::from_path(file.path()).unwrap();

        // IDs should be non-zero
        assert!(id.dev() > 0 || id.ino() > 0);
    }

    #[test]
    fn test_file_id_same_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let id1 = FileId::from_path(file.path()).unwrap();
        let id2 = FileId::from_path(file.path()).unwrap();

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_file_id_different_files() {
        let mut file1 = NamedTempFile::new().unwrap();
        let mut file2 = NamedTempFile::new().unwrap();

        file1.write_all(b"content 1").unwrap();
        file2.write_all(b"content 2").unwrap();
        file1.flush().unwrap();
        file2.flush().unwrap();

        let id1 = FileId::from_path(file1.path()).unwrap();
        let id2 = FileId::from_path(file2.path()).unwrap();

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_file_id_serde() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let id = FileId::from_path(file.path()).unwrap();
        let json = serde_json::to_string(&id).unwrap();
        let id2: FileId = serde_json::from_str(&json).unwrap();

        assert_eq!(id, id2);
    }

    #[test]
    fn test_file_id_stable_across_reopen() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let path = file.path().to_path_buf();

        // Get ID from first open
        let id1 = FileId::from_path(&path).unwrap();

        // Append content and reopen
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(b" more content").unwrap();
            f.flush().unwrap();
        }

        // Get ID again
        let id2 = FileId::from_path(&path).unwrap();

        // Should be the same inode
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_get_path_from_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();
        file.flush().unwrap();

        let original_path = file.path().to_path_buf();
        let f = file.reopen().unwrap();

        let retrieved_path = get_path_from_file(&f).unwrap();

        // On macOS/Linux, the paths should match (canonicalized)
        // Note: tempfile paths may be symlinked, so we compare canonical paths
        let original_canonical = original_path.canonicalize().unwrap();
        let retrieved_canonical = retrieved_path.canonicalize().unwrap_or(retrieved_path);

        assert_eq!(original_canonical, retrieved_canonical);
    }

    #[test]
    fn test_file_id_display() {
        let id = FileId { dev: 123, ino: 456 };
        assert_eq!(format!("{}", id), "123:456");
    }
}
