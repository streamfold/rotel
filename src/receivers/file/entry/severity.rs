use serde::{Deserialize, Serialize};
use std::fmt;

/// Severity indicates the seriousness of a log entry.
/// Values are aligned with OpenTelemetry severity numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "SeverityRepr", into = "SeverityRepr")]
#[repr(u8)]
pub enum Severity {
    /// Unknown severity
    Default = 0,
    /// Detailed debugging
    Trace = 10,
    Trace2 = 12,
    Trace3 = 13,
    Trace4 = 14,
    /// Debugging purposes
    Debug = 20,
    Debug2 = 22,
    Debug3 = 23,
    Debug4 = 24,
    /// High level application details
    Info = 30,
    Info2 = 32,
    Info3 = 33,
    Info4 = 34,
    /// Should be noticed
    Notice = 40,
    /// Someone should look into this
    Warning = 50,
    Warning2 = 52,
    Warning3 = 53,
    Warning4 = 54,
    /// Something undesirable happened
    Error = 60,
    Error2 = 62,
    Error3 = 63,
    Error4 = 64,
    /// Requires immediate attention
    Critical = 70,
    /// Action must be taken immediately
    Alert = 80,
    /// Application is unusable
    Emergency = 90,
    Emergency2 = 92,
    Emergency3 = 93,
    Emergency4 = 94,
    /// Too late
    Catastrophe = 100,
}

impl Default for Severity {
    fn default() -> Self {
        Self::Default
    }
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Default => "default",
            Self::Trace => "trace",
            Self::Trace2 => "trace2",
            Self::Trace3 => "trace3",
            Self::Trace4 => "trace4",
            Self::Debug => "debug",
            Self::Debug2 => "debug2",
            Self::Debug3 => "debug3",
            Self::Debug4 => "debug4",
            Self::Info => "info",
            Self::Info2 => "info2",
            Self::Info3 => "info3",
            Self::Info4 => "info4",
            Self::Notice => "notice",
            Self::Warning => "warning",
            Self::Warning2 => "warning2",
            Self::Warning3 => "warning3",
            Self::Warning4 => "warning4",
            Self::Error => "error",
            Self::Error2 => "error2",
            Self::Error3 => "error3",
            Self::Error4 => "error4",
            Self::Critical => "critical",
            Self::Alert => "alert",
            Self::Emergency => "emergency",
            Self::Emergency2 => "emergency2",
            Self::Emergency3 => "emergency3",
            Self::Emergency4 => "emergency4",
            Self::Catastrophe => "catastrophe",
        };
        write!(f, "{}", name)
    }
}

impl TryFrom<u8> for Severity {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match value {
            0 => Ok(Self::Default),
            10 => Ok(Self::Trace),
            12 => Ok(Self::Trace2),
            13 => Ok(Self::Trace3),
            14 => Ok(Self::Trace4),
            20 => Ok(Self::Debug),
            22 => Ok(Self::Debug2),
            23 => Ok(Self::Debug3),
            24 => Ok(Self::Debug4),
            30 => Ok(Self::Info),
            32 => Ok(Self::Info2),
            33 => Ok(Self::Info3),
            34 => Ok(Self::Info4),
            40 => Ok(Self::Notice),
            50 => Ok(Self::Warning),
            52 => Ok(Self::Warning2),
            53 => Ok(Self::Warning3),
            54 => Ok(Self::Warning4),
            60 => Ok(Self::Error),
            62 => Ok(Self::Error2),
            63 => Ok(Self::Error3),
            64 => Ok(Self::Error4),
            70 => Ok(Self::Critical),
            80 => Ok(Self::Alert),
            90 => Ok(Self::Emergency),
            92 => Ok(Self::Emergency2),
            93 => Ok(Self::Emergency3),
            94 => Ok(Self::Emergency4),
            100 => Ok(Self::Catastrophe),
            _ => Err(format!("invalid severity value: {}", value)),
        }
    }
}

impl std::str::FromStr for Severity {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "default" => Ok(Self::Default),
            "trace" => Ok(Self::Trace),
            "trace2" => Ok(Self::Trace2),
            "trace3" => Ok(Self::Trace3),
            "trace4" => Ok(Self::Trace4),
            "debug" => Ok(Self::Debug),
            "debug2" => Ok(Self::Debug2),
            "debug3" => Ok(Self::Debug3),
            "debug4" => Ok(Self::Debug4),
            "info" => Ok(Self::Info),
            "info2" => Ok(Self::Info2),
            "info3" => Ok(Self::Info3),
            "info4" => Ok(Self::Info4),
            "notice" => Ok(Self::Notice),
            "warning" | "warn" => Ok(Self::Warning),
            "warning2" => Ok(Self::Warning2),
            "warning3" => Ok(Self::Warning3),
            "warning4" => Ok(Self::Warning4),
            "error" | "err" => Ok(Self::Error),
            "error2" => Ok(Self::Error2),
            "error3" => Ok(Self::Error3),
            "error4" => Ok(Self::Error4),
            "critical" | "crit" => Ok(Self::Critical),
            "alert" => Ok(Self::Alert),
            "emergency" | "emerg" => Ok(Self::Emergency),
            "emergency2" => Ok(Self::Emergency2),
            "emergency3" => Ok(Self::Emergency3),
            "emergency4" => Ok(Self::Emergency4),
            "catastrophe" | "fatal" => Ok(Self::Catastrophe),
            _ => Err(format!("unknown severity: {}", s)),
        }
    }
}

// Helper type for serde serialization
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
enum SeverityRepr {
    Number(u8),
    String(String),
}

impl TryFrom<SeverityRepr> for Severity {
    type Error = String;

    fn try_from(repr: SeverityRepr) -> Result<Self, <Self as TryFrom<SeverityRepr>>::Error> {
        match repr {
            SeverityRepr::Number(n) => Self::try_from(n),
            SeverityRepr::String(s) => s.parse(),
        }
    }
}

impl From<Severity> for SeverityRepr {
    fn from(s: Severity) -> Self {
        SeverityRepr::Number(s as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Debug < Severity::Info);
        assert!(Severity::Info < Severity::Warning);
        assert!(Severity::Warning < Severity::Error);
    }

    #[test]
    fn test_severity_from_str() {
        assert_eq!("info".parse::<Severity>().unwrap(), Severity::Info);
        assert_eq!("warn".parse::<Severity>().unwrap(), Severity::Warning);
        assert_eq!("error".parse::<Severity>().unwrap(), Severity::Error);
    }

    #[test]
    fn test_severity_display() {
        assert_eq!(format!("{}", Severity::Info), "info");
        assert_eq!(format!("{}", Severity::Warning), "warning");
    }
}
