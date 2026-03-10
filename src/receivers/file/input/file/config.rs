use serde::Deserialize;

/// Where to start reading from when a file is first discovered
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartAt {
    /// Start reading from the beginning of the file
    Beginning,
    /// Start reading from the end of the file (only new content)
    #[default]
    End,
}
