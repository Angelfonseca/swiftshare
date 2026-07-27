// TCP transfer codec: [u8 kind][u32 be len][payload]
//
// The kind byte matters: the old codec guessed by trying to JSON-parse every
// frame, which both cost a parse attempt over each megabyte of file data and
// could misread binary that happened to look like a command.

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{TransferCommand, TransferFrame};

pub struct TransferCodec;

const KIND_MESSAGE: u8 = 0;
const KIND_DATA: u8 = 1;
const HEADER_LEN: usize = 5;

/// Must stay above CHUNK_SIZE in transfer.rs.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

impl Decoder for TransferCodec {
    type Item = TransferFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        let kind = src[0];
        let length = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large: {} bytes", length),
            ));
        }

        if src.len() < HEADER_LEN + length {
            src.reserve(HEADER_LEN + length - src.len());
            return Ok(None);
        }

        src.advance(HEADER_LEN);
        let payload = src.split_to(length);

        match kind {
            KIND_MESSAGE => {
                let cmd: TransferCommand = serde_json::from_slice(&payload).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("Bad command: {}", e))
                })?;
                Ok(Some(TransferFrame::Message(cmd)))
            }
            KIND_DATA => Ok(Some(TransferFrame::Data(payload.freeze()))),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown frame kind: {}", other),
            )),
        }
    }
}

impl Encoder<TransferFrame> for TransferCodec {
    type Error = io::Error;

    fn encode(&mut self, item: TransferFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (kind, payload) = match item {
            TransferFrame::Message(cmd) => (KIND_MESSAGE, bytes::Bytes::from(serde_json::to_vec(&cmd)?)),
            TransferFrame::Data(data) => (KIND_DATA, data),
        };

        if payload.len() > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large to send: {} bytes", payload.len()),
            ));
        }

        dst.reserve(HEADER_LEN + payload.len());
        dst.put_u8(kind);
        dst.put_u32(payload.len() as u32);
        dst.extend_from_slice(&payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{FileMetadata, TransferCommand};

    fn roundtrip(frame: TransferFrame) -> TransferFrame {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        codec.decode(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn test_encode_decode_message() {
        let cmd = TransferCommand::PrepareTransfer {
            session_id: "test1".to_string(),
            peer_alias: "TestPC".to_string(),
            files: vec![FileMetadata {
                id: "f1".to_string(),
                name: "test.txt".to_string(),
                size: 100,
                mime_type: "text/plain".to_string(),
                relative_path: None,
            }],
        };

        match roundtrip(TransferFrame::Message(cmd)) {
            TransferFrame::Message(TransferCommand::PrepareTransfer {
                session_id, files, ..
            }) => {
                assert_eq!(session_id, "test1");
                assert_eq!(files[0].name, "test.txt");
            }
            _ => panic!("Expected PrepareTransfer"),
        }
    }

    #[test]
    fn test_encode_decode_data() {
        let data = bytes::Bytes::from_static(&[1u8, 2, 3, 4, 5]);
        match roundtrip(TransferFrame::Data(data.clone())) {
            TransferFrame::Data(decoded) => assert_eq!(decoded, data),
            _ => panic!("Expected Data"),
        }
    }

    /// Binary data that happens to be valid JSON must still decode as Data.
    #[test]
    fn test_json_shaped_data_stays_data() {
        let payload = bytes::Bytes::from(br#"{"SessionComplete":{"session_id":"x"}}"#.to_vec());
        match roundtrip(TransferFrame::Data(payload.clone())) {
            TransferFrame::Data(decoded) => assert_eq!(decoded, payload),
            _ => panic!("JSON-shaped file bytes were misread as a command"),
        }
    }

    #[test]
    fn test_partial_frame_returns_none() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();
        codec
            .encode(TransferFrame::Data(bytes::Bytes::from(vec![7u8; 64])), &mut buf)
            .unwrap();

        let mut partial = buf.split_to(20);
        assert!(codec.decode(&mut partial).unwrap().is_none());

        partial.extend_from_slice(&buf);
        assert!(codec.decode(&mut partial).unwrap().is_some());
    }

    #[test]
    fn test_oversized_frame_rejected() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();
        buf.put_u8(KIND_DATA);
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);
        assert!(codec.decode(&mut buf).is_err());
    }
}
