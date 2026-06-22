// Error types for the application

use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network error: {0}")]
    Network(#[from] std::net::AddrParseError),

    #[error("Parse error: {0}")]
    Parse(#[from] anyhow::Error),

    #[error("Transfer cancelled")]
    Cancelled,

    #[error("File not found: {0}")]
    FileNotFoundError(String),

    #[error("Permission denied")]
    PermissionDenied,

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("Hash mismatch: expected {expected}, got {actual}")]
    HashMismatch { expected: String, actual: String },
}

pub type AppResult<T> = Result<T, AppError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = AppError::FileNotFoundError("test.txt".to_string());
        assert_eq!(format!("{}", err), "File not found: test.txt");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let app_err: AppError = io_err.into();
        assert!(matches!(app_err, AppError::Io(_)));
    }
}
