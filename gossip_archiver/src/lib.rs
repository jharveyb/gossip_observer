use chrono::{DateTime, Utc};
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

#[cfg(test)]
mod test_constants;

pub static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";

// Format: format_args!("{now},{recv_peer},{msg_type},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
// ldk-node/src/logger.rs#L272, LdkLogger.export()
#[derive(Debug, Clone)]
pub struct ExportedGossip {
    // stamp set by collector before sending a message over NATS
    pub recv_timestamp: DateTime<Utc>,
    // stamp that's part of a node_ann or chan_update msg
    pub orig_timestamp: Option<DateTime<Utc>>,
    // node pubkeys
    pub collector: String,
    pub recv_peer: String,
    pub recv_peer_hash: u64,
    // source node in a node_ann msg
    pub orig_node: Option<String>,
    // metadata
    pub msg_type: u8,
    pub msg_size: u16,
    // part of a chan_ann or chan_update msg
    pub scid: Option<u64>,
    // full unsigned message
    pub msg: String,
    pub msg_hash: u64,
}

pub struct RawMessage {
    pub msg_hash: u64,
    pub msg: String,
}

impl RawMessage {
    pub fn unroll(m: RawMessage) -> (Vec<u8>, String) {
        (m.msg_hash.to_le_bytes().to_vec(), m.msg)
    }
}

pub struct MessageNodeTimings {
    pub msg_hash: u64,
    pub collector: String,
    pub recv_peer: String,
    pub recv_peer_hash: u64,
    pub recv_timestamp: DateTime<Utc>,
    pub orig_timestamp: Option<DateTime<Utc>>,
}

impl MessageNodeTimings {
    #[allow(clippy::type_complexity)]
    pub fn unroll(
        m: MessageNodeTimings,
    ) -> (
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    ) {
        (
            m.msg_hash.to_le_bytes().to_vec(),
            m.collector,
            m.recv_peer,
            m.recv_peer_hash.to_le_bytes().to_vec(),
            m.recv_timestamp,
            m.orig_timestamp,
        )
    }
}

pub struct MessageMetadata {
    pub msg_hash: u64,
    pub msg_type: u8,
    pub msg_size: u16,
    pub orig_node: Option<String>,
    pub scid: Option<u64>,
}

impl MessageMetadata {
    pub fn unroll(m: MessageMetadata) -> (Vec<u8>, i16, i32, Option<String>, Option<Vec<u8>>) {
        (
            m.msg_hash.to_le_bytes().to_vec(),
            m.msg_type.into(),
            m.msg_size.into(),
            m.orig_node,
            m.scid.map(|s| s.to_le_bytes().to_vec()),
        )
    }
}

pub fn split_exported_gossip(
    msg: ExportedGossip,
) -> (RawMessage, MessageNodeTimings, MessageMetadata) {
    let raw = RawMessage {
        msg_hash: msg.msg_hash,
        msg: msg.msg,
    };
    let timings = MessageNodeTimings {
        msg_hash: msg.msg_hash,
        collector: msg.collector,
        recv_peer: msg.recv_peer,
        recv_peer_hash: msg.recv_peer_hash,
        recv_timestamp: msg.recv_timestamp,
        orig_timestamp: msg.orig_timestamp,
    };
    let metadata = MessageMetadata {
        msg_hash: msg.msg_hash,
        msg_type: msg.msg_type,
        msg_size: msg.msg_size,
        orig_node: msg.orig_node,
        scid: msg.scid,
    };
    (raw, timings, metadata)
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum MessageType {
    Unknown,
    ChannelAnnouncement,
    NodeAnnouncement,
    ChannelUpdate,
}

impl std::str::FromStr for MessageType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ca" => Ok(Self::ChannelAnnouncement),
            "na" => Ok(Self::NodeAnnouncement),
            "cu" => Ok(Self::ChannelUpdate),
            _ => Err(anyhow::Error::msg("Invalid message type")),
        }
    }
}

impl From<MessageType> for u8 {
    fn from(msg_type: MessageType) -> Self {
        msg_type as u8
    }
}

// TODO: rkyv or smthn cool here? Jk this is fine on release builds
// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<ExportedGossip> {
    let parts = msg.split(INTRA_MSG_DELIM).collect::<Vec<&str>>();
    // None values will be "", but we'll always have 9 fields.
    if parts.len() != 9 {
        anyhow::bail!("Invalid message from collector: {msg}")
    }

    // Format: format_args!("{now},{recv_peer},{msg_type},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
    // ldk-node/src/logger.rs#L272, LdkLogger.export()
    let recv_timestamp = DateTime::from_timestamp_micros(parts[0].parse::<i64>()?).unwrap();
    let recv_peer = parts[1].to_owned();
    let recv_peer_hash = XX3Hasher::oneshot(recv_peer.as_bytes());
    let msg_type = parts[2].parse::<MessageType>()?.into();
    let msg_size = parts[3].parse::<u16>()?;
    let msg = parts[4].to_owned();
    let msg_hash = XX3Hasher::oneshot(msg.as_bytes());
    let orig_timestamp = (!parts[5].is_empty())
        .then(|| parts[5].parse::<i64>())
        .transpose()?
        .and_then(DateTime::from_timestamp_micros);
    let orig_node = (!parts[6].is_empty()).then(|| parts[6].to_owned());
    let scid = (!parts[7].is_empty())
        .then(|| parts[7].parse::<u64>())
        .transpose()?;
    let collector = parts[8].to_owned();

    Ok(ExportedGossip {
        recv_timestamp,
        orig_timestamp,
        collector,
        recv_peer,
        recv_peer_hash,
        orig_node,
        msg_type,
        msg_size,
        scid,
        msg,
        msg_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v0_msg() {
        let msg = test_constants::v0_all_fields_populated();
        assert!(decode_msg(&msg).is_ok());
    }

    #[test]
    fn v0_missing_collector_key() {
        let msg = test_constants::v0_missing_collector_key();
        assert!(decode_msg(&msg).is_err());
    }

    #[test]
    fn v0_minimal_msg() {
        let msg = test_constants::v0_minimal_fields();
        assert!(decode_msg(&msg).is_ok());
    }

    #[test]
    fn v0_invalid_msg_type() {
        let msg = test_constants::v0_invalid_msg_type();
        assert!(decode_msg(&msg).is_err());
    }
}
