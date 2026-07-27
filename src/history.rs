// Persistent transfer history (SQLite). Separate from AppState's in-memory
// `transfers` map, which only tracks what's active or recently finished
// (pruned after HISTORY_TTL_SECS) — this survives restarts, so "what did I
// send last week" still has an answer.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::state::{Direction, TransferState, TransferStatus};

pub struct History {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HistoryEntry {
    pub session_id: String,
    pub peer: String,
    pub direction: String, // "send" | "recv"
    pub status: String,    // "completed" | "rejected" | "cancelled" | "failed"
    pub error: Option<String>,
    pub file_count: i64,
    pub total_size: i64,
    pub started_at: i64,
    pub finished_at: i64,
}

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS transfers (
        session_id  TEXT PRIMARY KEY,
        peer        TEXT NOT NULL,
        direction   TEXT NOT NULL,
        status      TEXT NOT NULL,
        error       TEXT,
        file_count  INTEGER NOT NULL,
        total_size  INTEGER NOT NULL,
        started_at  INTEGER NOT NULL,
        finished_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_transfers_finished_at ON transfers(finished_at DESC);
";

impl History {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    /// Only terminal states are worth a row; Pending/Active mean "still
    /// going" and get a row once they actually finish.
    pub fn record(&self, t: &TransferState) {
        let direction = match t.direction {
            Direction::Send => "send",
            Direction::Recv => "recv",
        };
        let (status, error) = match &t.status {
            TransferStatus::Completed => ("completed", None),
            TransferStatus::Rejected => ("rejected", None),
            TransferStatus::Cancelled => ("cancelled", None),
            TransferStatus::Failed { error } => ("failed", Some(error.as_str())),
            TransferStatus::Pending | TransferStatus::Active => return,
        };
        let total_size: u64 = t.files.iter().map(|f| f.size).sum();
        let finished_at = t.finished_at.unwrap_or_else(crate::state::now_secs);

        // A write failure here means history is missing one entry, not that
        // the transfer itself failed — log and move on.
        let conn = self.conn.lock().unwrap();
        let result = conn.execute(
            "INSERT OR REPLACE INTO transfers
             (session_id, peer, direction, status, error, file_count, total_size, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                t.session_id,
                t.peer,
                direction,
                status,
                error,
                t.files.len() as i64,
                total_size as i64,
                t.started_at as i64,
                finished_at as i64,
            ],
        );
        if let Err(e) = result {
            tracing::warn!("Failed to record transfer history: {}", e);
        }
    }

    pub fn recent(&self, limit: u32) -> Vec<HistoryEntry> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT session_id, peer, direction, status, error, file_count, total_size, started_at, finished_at
             FROM transfers ORDER BY finished_at DESC LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("History query failed: {}", e);
                return Vec::new();
            }
        };

        let rows = stmt.query_map(params![limit], |row| {
            Ok(HistoryEntry {
                session_id: row.get(0)?,
                peer: row.get(1)?,
                direction: row.get(2)?,
                status: row.get(3)?,
                error: row.get(4)?,
                file_count: row.get(5)?,
                total_size: row.get(6)?,
                started_at: row.get(7)?,
                finished_at: row.get(8)?,
            })
        });

        match rows {
            Ok(iter) => iter.filter_map(Result::ok).collect(),
            Err(e) => {
                tracing::warn!("History query failed: {}", e);
                Vec::new()
            }
        }
    }

    pub fn clear(&self) {
        if let Err(e) = self.conn.lock().unwrap().execute("DELETE FROM transfers", []) {
            tracing::warn!("Failed to clear history: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{now_secs, FileProgress};

    fn transfer(status: TransferStatus) -> TransferState {
        TransferState {
            session_id: "sess1".into(),
            peer: "PeerPC".into(),
            direction: Direction::Recv,
            files: vec![FileProgress {
                file_id: "f1".into(),
                name: "report.pdf".into(),
                size: 1000,
                bytes: 1000,
                done: true,
                error: None,
            }],
            status,
            started_at: now_secs(),
            finished_at: Some(now_secs()),
        }
    }

    #[test]
    fn test_record_and_recent() {
        let history = History::open_in_memory().unwrap();
        history.record(&transfer(TransferStatus::Completed));

        let entries = history.recent(10);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].peer, "PeerPC");
        assert_eq!(entries[0].status, "completed");
        assert_eq!(entries[0].total_size, 1000);
        assert_eq!(entries[0].file_count, 1);
    }

    #[test]
    fn test_pending_and_active_are_not_recorded() {
        let history = History::open_in_memory().unwrap();
        history.record(&transfer(TransferStatus::Pending));
        history.record(&transfer(TransferStatus::Active));
        assert_eq!(history.recent(10).len(), 0);
    }

    #[test]
    fn test_failed_keeps_error_message() {
        let history = History::open_in_memory().unwrap();
        history.record(&transfer(TransferStatus::Failed { error: "disco lleno".into() }));

        let entries = history.recent(10);
        assert_eq!(entries[0].status, "failed");
        assert_eq!(entries[0].error.as_deref(), Some("disco lleno"));
    }

    #[test]
    fn test_recent_orders_newest_first_and_respects_limit() {
        let history = History::open_in_memory().unwrap();
        for i in 0..5 {
            let mut t = transfer(TransferStatus::Completed);
            t.session_id = format!("sess{}", i);
            t.finished_at = Some(now_secs() + i);
            history.record(&t);
        }

        let entries = history.recent(3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].session_id, "sess4");
        assert_eq!(entries[1].session_id, "sess3");
        assert_eq!(entries[2].session_id, "sess2");
    }

    #[test]
    fn test_clear_empties_history() {
        let history = History::open_in_memory().unwrap();
        history.record(&transfer(TransferStatus::Completed));
        assert_eq!(history.recent(10).len(), 1);
        history.clear();
        assert_eq!(history.recent(10).len(), 0);
    }

    #[test]
    fn test_open_creates_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/history.db");
        let history = History::open(&path).unwrap();
        history.record(&transfer(TransferStatus::Completed));
        assert!(path.exists());
        assert_eq!(history.recent(10).len(), 1);
    }
}
