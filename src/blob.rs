//! Blob metadata and index structures for Walrus storage.
//!
//! This module defines the metadata structures returned by the Walrus archival
//! service and the internal blob index format.

use serde::{Deserialize, Serialize};

/// Walrus blob metadata from archival service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    /// The blob ID (base64url encoded)
    #[serde(rename = "blob_id")]
    pub blob_id: String,

    /// First checkpoint number in this blob
    #[serde(rename = "start_checkpoint")]
    pub start_checkpoint: u64,

    /// Last checkpoint number in this blob
    #[serde(rename = "end_checkpoint")]
    pub end_checkpoint: u64,

    /// Number of checkpoints in this blob
    #[serde(rename = "entries_count")]
    pub entries_count: u64,

    /// Total size of the blob in bytes
    pub total_size: u64,

    /// Whether this blob ends at an epoch boundary
    #[serde(default)]
    pub end_of_epoch: bool,

    /// Epoch when this blob expires
    #[serde(default)]
    pub expiry_epoch: u64,
}

/// Response from the blobs list endpoint
#[derive(Debug, Deserialize)]
pub struct BlobsResponse {
    pub blobs: Vec<BlobMetadata>,
    pub next_cursor: Option<u64>,
}

/// Checkpoint location within a blob
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointLocation {
    pub checkpoint_number: u64,
    pub blob_id: String,
    pub offset: u64,
    pub length: u64,
}

/// Blob footer format (24 bytes)
///
/// ```text
/// +--------+--------+------------------+-------+
/// | Magic  | Version| Index Start      | Count |
/// | (4B)   | (4B)   | (8B)             | (4B)  |
/// +--------+--------+------------------+-------+
/// ```
///
/// Magic: 0x574c4244 ("DBLW" in little-endian)
#[derive(Debug, Clone)]
pub struct BlobFooter {
    pub magic: u32,
    pub version: u32,
    pub index_start_offset: u64,
    pub entry_count: u32,
}

impl BlobFooter {
    /// The magic number for valid blob footers
    pub const MAGIC: u32 = 0x574c4244; // "DBLW"

    /// Parse a footer from 24 bytes
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 24 {
            return None;
        }

        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if magic != Self::MAGIC {
            return None;
        }

        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let index_start_offset = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);
        let entry_count = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

        Some(Self {
            magic,
            version,
            index_start_offset,
            entry_count,
        })
    }
}

/// Index entry format (variable length)
///
/// ```text
/// +----------+------+--------+--------+-----+
/// | Name Len | Name | Offset | Length | CRC |
/// | (4B)     | (var)| (8B)   | (8B)   | (4B)|
/// +----------+------+--------+--------+-----+
/// ```
#[derive(Debug, Clone)]
pub struct BlobIndexEntry {
    pub name: String,
    pub offset: u64,
    pub length: u64,
    pub crc: u32,
}

impl BlobIndexEntry {
    /// Parse an entry from a cursor
    pub fn from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        use byteorder::{LittleEndian, ReadBytesExt};

        let name_len = reader.read_u32::<LittleEndian>()? as usize;
        let mut name_bytes = vec![0u8; name_len];
        reader.read_exact(&mut name_bytes)?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let offset = reader.read_u64::<LittleEndian>()?;
        let length = reader.read_u64::<LittleEndian>()?;
        let crc = reader.read_u32::<LittleEndian>()?;

        Ok(Self {
            name,
            offset,
            length,
            crc,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_footer_parsing() {
        let mut bytes = [0u8; 24];
        // Magic "DBLW" in little-endian
        bytes[0..4].copy_from_slice(&0x574c4244u32.to_le_bytes());
        // Version 1
        bytes[4..8].copy_from_slice(&1u32.to_le_bytes());
        // Index start at offset 1000
        bytes[8..16].copy_from_slice(&1000u64.to_le_bytes());
        // 500 entries
        bytes[16..20].copy_from_slice(&500u32.to_le_bytes());

        let footer = BlobFooter::from_bytes(&bytes).unwrap();
        assert_eq!(footer.magic, BlobFooter::MAGIC);
        assert_eq!(footer.version, 1);
        assert_eq!(footer.index_start_offset, 1000);
        assert_eq!(footer.entry_count, 500);
    }

    #[test]
    fn test_invalid_magic() {
        let bytes = [0u8; 24]; // All zeros - invalid magic
        assert!(BlobFooter::from_bytes(&bytes).is_none());
    }
}
