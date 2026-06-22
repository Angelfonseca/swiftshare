// Resume support for interrupted transfers

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    pub session_id: String,
    pub file_id: String,
    pub file_name: String,
    pub total_size: u64,
    pub bytes_transferred: u64,
    pub sha256: String,
}

impl ResumeState {
    pub fn new(session_id: String, file_id: String, file_name: String, total_size: u64, sha256: String) -> Self {
        Self {
            session_id,
            file_id,
            file_name,
            total_size,
            bytes_transferred: 0,
            sha256,
        }
    }

    pub async fn save(&self, dir: &PathBuf) -> std::io::Result<()> {
        let path = dir.join(format!("{}.resume.json", self.session_id));
        let data = serde_json::to_string_pretty(self)?;
        fs::write(path, data).await
    }

    pub async fn load(dir: &PathBuf, session_id: &str) -> Result<Option<Self>, std::io::Error> {
        let path = dir.join(format!("{}.resume.json", session_id));
        if path.exists() {
            let data = fs::read_to_string(path).await?;
            Ok(Some(serde_json::from_str(&data)?))
        } else {
            Ok(None)
        }
    }

    pub async fn delete(&self, dir: &PathBuf) -> std::io::Result<()> {
        let path = dir.join(format!("{}.resume.json", self.session_id));
        fs::remove_file(path).await
    }

    pub fn progress(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        (self.bytes_transferred as f64 / self.total_size as f64) * 100.0
    }

    pub fn is_complete(&self) -> bool {
        self.bytes_transferred >= self.total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_resume_state_creation() {
        let state = ResumeState::new(
            "sess1".to_string(),
            "file1".to_string(),
            "test.txt".to_string(),
            1000,
            "abc123".to_string(),
        );

        assert_eq!(state.session_id, "sess1");
        assert_eq!(state.bytes_transferred, 0);
        assert_eq!(state.progress(), 0.0);
        assert!(!state.is_complete());
    }

    #[test]
    fn test_resume_state_progress() {
        let mut state = ResumeState::new(
            "sess1".to_string(),
            "file1".to_string(),
            "test.txt".to_string(),
            1000,
            "abc".to_string(),
        );

        state.bytes_transferred = 500;
        assert_eq!(state.progress(), 50.0);

        state.bytes_transferred = 1000;
        assert_eq!(state.progress(), 100.0);
        assert!(state.is_complete());
    }

    #[tokio::test]
    async fn test_resume_state_save_and_load() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let state = ResumeState::new(
            "sess1".to_string(),
            "file1".to_string(),
            "test.txt".to_string(),
            1000,
            "abc123".to_string(),
        );

        state.save(&path).await.unwrap();
        let loaded = ResumeState::load(&path, "sess1").await.unwrap();

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.session_id, "sess1");
        assert_eq!(loaded.file_name, "test.txt");
        assert_eq!(loaded.total_size, 1000);
    }

    #[tokio::test]
    async fn test_resume_state_not_found() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let loaded = ResumeState::load(&path, "nonexistent").await.unwrap();
        assert!(loaded.is_none());
    }

    #[tokio::test]
    async fn test_resume_state_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        let state = ResumeState::new(
            "sess1".to_string(),
            "file1".to_string(),
            "test.txt".to_string(),
            1000,
            "abc".to_string(),
        );

        state.save(&path).await.unwrap();
        state.delete(&path).await.unwrap();

        let loaded = ResumeState::load(&path, "sess1").await.unwrap();
        assert!(loaded.is_none());
    }
}
