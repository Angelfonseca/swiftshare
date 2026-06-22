// TCP transfer codec for length-prefixed binary framing

use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{TransferCommand, TransferFrame};

pub struct TransferCodec;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024; // 16MB max message

impl Decoder for TransferCodec {
    type Item = TransferFrame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Frame too large: {} bytes", length),
            ));
        }

        if src.len() < 4 + length {
            src.reserve(4 + length - src.len());
            return Ok(None);
        }

        src.advance(4);

        let frame_data = src.split_to(length);

        if let Ok(cmd) = serde_json::from_slice::<TransferCommand>(&frame_data) {
            Ok(Some(TransferFrame::Message(cmd)))
        } else {
            Ok(Some(TransferFrame::Data(frame_data.to_vec())))
        }
    }
}

impl Encoder<TransferFrame> for TransferCodec {
    type Error = io::Error;

    fn encode(&mut self, item: TransferFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            TransferFrame::Message(cmd) => {
                let json = serde_json::to_vec(&cmd)?;
                let len = json.len() as u32;
                dst.reserve(4 + json.len());
                dst.put_u32(len);
                dst.extend_from_slice(&json);
            }
            TransferFrame::Data(data) => {
                let len = data.len() as u32;
                dst.reserve(4 + data.len());
                dst.put_u32(len);
                dst.extend_from_slice(&data);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{FileMetadata, FileToken, TransferCommand};
    use std::collections::HashMap;

    #[test]
    fn test_encode_decode_message() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();

        let cmd = TransferCommand::PrepareTransfer {
            session_id: "test1".to_string(),
            files: vec![FileMetadata {
                id: "f1".to_string(),
                name: "test.txt".to_string(),
                size: 100,
                mime_type: "text/plain".to_string(),
                sha256: "abc123".to_string(),
            }],
        };

        codec
            .encode(TransferFrame::Message(cmd), &mut buf)
            .unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            TransferFrame::Message(TransferCommand::PrepareTransfer { session_id, files }) => {
                assert_eq!(session_id, "test1");
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].name, "test.txt");
                assert_eq!(files[0].size, 100);
            }
            _ => panic!("Expected PrepareTransfer"),
        }
    }

    #[test]
    fn test_encode_decode_data() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();

        let data = vec![1u8, 2, 3, 4, 5];
        codec
            .encode(TransferFrame::Data(data.clone()), &mut buf)
            .unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();

        match decoded {
            TransferFrame::Data(decoded_data) => {
                assert_eq!(decoded_data, data);
            }
            _ => panic!("Expected Data"),
        }
    }

    #[test]
    fn test_large_frame_rejection() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();

        let large_data = vec![0u8; MAX_FRAME_SIZE + 1];
        codec
            .encode(TransferFrame::Data(large_data), &mut buf)
            .unwrap();

        // The encode should succeed, but decode should fail
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_incremental_decode() {
        let mut codec = TransferCodec;
        let mut buf = BytesMut::new();

        let cmd = TransferCommand::PrepareTransfer {
            session_id: "test2".to_string(),
            files: vec![],
        };

        codec
            .encode(TransferFrame::Message(cmd), &mut buf)
            .unwrap();

        // Test: feed full buffer - should succeed
        let mut full_buf = buf.clone();
        let result = codec.decode(&mut full_buf);
        assert!(result.unwrap().is_some());
    }
}
