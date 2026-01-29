use anyhow::anyhow;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use lightning::util::logger::ExportMessageDirection;
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

pub mod config;
pub mod nats;
#[cfg(test)]
mod test_constants;

pub static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
static V0_MSG_PARTS: usize = 11;

// Format: format_args!("{now},{peer},{msg_type},{msg_dir},{msg_size},{inner_hash},{msg},{send_ts},{node_id},{scid},{collector_id}")
// ldk-node/src/logger.rs#L395, LdkLogger.export()
#[derive(Debug, Clone)]
pub struct ExportedGossip {
    // stamp set by collector before sending a message over NATS
    pub net_timestamp: DateTime<Utc>,
    // stamp that's part of a node_ann or chan_update msg
    pub orig_timestamp: Option<DateTime<Utc>>,
    // node pubkeys
    pub collector: String,
    pub peer: String,
    pub peer_hash: u64,
    // source node in a node_ann msg
    pub orig_node: Option<String>,
    // metadata
    pub msg_type: u8,
    pub msg_dir: u8,
    pub msg_size: u16,
    // part of a chan_ann or chan_update msg
    pub scid: Option<u64>,
    // full unsigned message
    pub msg: String,
    // XXH3_64 hash of the encoded full message
    pub msg_hash: u64,
    // XXH3_64 hash of the encoded message with any timestamp and signature removed
    pub inner_hash: u64,
}

#[derive(Debug, Clone)]
pub struct RawMessage {
    pub msg_hash: u64,
    pub msg: String,
}

impl RawMessage {
    pub fn unroll(m: RawMessage) -> (Vec<u8>, String) {
        (m.msg_hash.to_le_bytes().to_vec(), m.msg)
    }
}

#[derive(Debug, Clone)]
pub struct MessageNodeTimings {
    pub msg_hash: u64,
    pub inner_hash: u64,
    pub collector: String,
    pub peer: String,
    pub peer_hash: u64,
    pub dir: u8,
    pub net_timestamp: DateTime<Utc>,
    pub orig_timestamp: Option<DateTime<Utc>>,
}

impl MessageNodeTimings {
    #[allow(clippy::type_complexity)]
    pub fn unroll(
        m: MessageNodeTimings,
    ) -> (
        Vec<u8>,
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        i16,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    ) {
        (
            m.msg_hash.to_le_bytes().to_vec(),
            m.inner_hash.to_le_bytes().to_vec(),
            m.collector,
            m.peer,
            m.peer_hash.to_le_bytes().to_vec(),
            m.dir.into(),
            m.net_timestamp,
            m.orig_timestamp,
        )
    }
}

#[derive(Debug, Clone)]
pub struct MessageMetadata {
    pub msg_hash: u64,
    pub inner_hash: u64,
    pub msg_type: u8,
    pub msg_size: u16,
    pub orig_node: Option<String>,
    pub scid: Option<u64>,
}

impl MessageMetadata {
    #[allow(clippy::type_complexity)]
    pub fn unroll(
        m: MessageMetadata,
    ) -> (Vec<u8>, Vec<u8>, i16, i32, Option<String>, Option<Vec<u8>>) {
        (
            m.msg_hash.to_le_bytes().to_vec(),
            m.inner_hash.to_le_bytes().to_vec(),
            m.msg_type.into(),
            m.msg_size.into(),
            m.orig_node,
            m.scid.map(|s| s.to_le_bytes().to_vec()),
        )
    }
}

#[derive(Debug, Clone)]
pub struct MessageHashMapping {
    pub outer_hash: u64,
    pub inner_hash: u64,
}

impl MessageHashMapping {
    #[allow(clippy::type_complexity)]
    pub fn unroll(m: MessageHashMapping) -> (Vec<u8>, Vec<u8>) {
        (
            m.outer_hash.to_le_bytes().to_vec(),
            m.inner_hash.to_le_bytes().to_vec(),
        )
    }
}

// Destructure our decoded message to match our DB schema; one struct per table.
pub fn split_exported_gossip(
    msg: ExportedGossip,
) -> (
    RawMessage,
    MessageNodeTimings,
    MessageMetadata,
    MessageHashMapping,
) {
    let raw = RawMessage {
        msg_hash: msg.msg_hash,
        msg: msg.msg,
    };
    let timings = MessageNodeTimings {
        msg_hash: msg.msg_hash,
        inner_hash: msg.inner_hash,
        collector: msg.collector,
        peer: msg.peer,
        peer_hash: msg.peer_hash,
        dir: msg.msg_dir,
        net_timestamp: msg.net_timestamp,
        orig_timestamp: msg.orig_timestamp,
    };
    let metadata = MessageMetadata {
        msg_hash: msg.msg_hash,
        inner_hash: msg.inner_hash,
        msg_type: msg.msg_type,
        msg_size: msg.msg_size,
        orig_node: msg.orig_node,
        scid: msg.scid,
    };
    let hash_mapping = MessageHashMapping {
        outer_hash: msg.msg_hash,
        inner_hash: msg.inner_hash,
    };
    (raw, timings, metadata, hash_mapping)
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum MessageType {
    Unknown,
    ChannelAnnouncement,
    NodeAnnouncement,
    ChannelUpdate,
    Ping,
    Pong,
}

impl std::str::FromStr for MessageType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ca" => Ok(Self::ChannelAnnouncement),
            "na" => Ok(Self::NodeAnnouncement),
            "cu" => Ok(Self::ChannelUpdate),
            "ping" => Ok(Self::Ping),
            "pong" => Ok(Self::Pong),
            _ => Err(anyhow::Error::msg("Invalid message type")),
        }
    }
}

impl From<MessageType> for u8 {
    fn from(msg_type: MessageType) -> Self {
        msg_type as u8
    }
}

// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<ExportedGossip> {
    // None values will be "", but we'll always have 9 fields.
    let parts: [&str; V0_MSG_PARTS] = msg
        .split(INTRA_MSG_DELIM)
        .collect_array()
        .ok_or(anyhow::anyhow!("Invalid message from collector: {msg}"))?;
    let mut parts = parts.into_iter();

    // Format: format_args!("{now},{peer},{msg_type},{msg_dir},{msg_size},{inner_hash},{msg},{send_ts},{node_id},{scid},{collector_id}")
    // ldk-node/src/logger.rs#L395, LdkLogger.export()
    let mut next_part = || -> anyhow::Result<&str> {
        parts.next().ok_or(anyhow::anyhow!(
            "decode_msg: incorrect number of parts: {msg}"
        ))
    };

    let net_timestamp = DateTime::from_timestamp_micros(next_part()?.parse::<i64>()?)
        .ok_or_else(|| anyhow!("Invalid net_timestamp"))?;
    // TODO: add another hash for the message, but omitting optional fields?
    let peer = next_part()?;
    let peer_hash = XX3Hasher::oneshot(peer.as_bytes());
    let peer = peer.to_owned();
    let msg_type = next_part()?.parse::<MessageType>()?.into();
    let msg_dir: u8 = next_part()?.parse::<ExportMessageDirection>()?.into();

    let msg_size = next_part()?.parse::<u16>()?;
    let inner_hash = next_part()?.parse::<u64>()?;
    let msg = next_part()?;
    let msg_hash = XX3Hasher::oneshot(msg.as_bytes());
    let msg = msg.to_owned();
    let orig_timestamp = match next_part()? {
        "" => None,
        x => Some(
            DateTime::from_timestamp_micros(x.parse::<i64>()?)
                .ok_or_else(|| anyhow!("Invalid orig_timestamp"))?,
        ),
    };
    let orig_node = match next_part()? {
        "" => None,
        x => Some(x.to_owned()),
    };
    let scid = match next_part()? {
        "" => None,
        x => Some(x.parse::<u64>()?),
    };
    let collector = next_part()?.to_owned();

    Ok(ExportedGossip {
        net_timestamp,
        orig_timestamp,
        collector,
        peer,
        peer_hash,
        orig_node,
        msg_type,
        msg_dir,
        msg_size,
        scid,
        msg,
        msg_hash,
        inner_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as b64_engine};
    use lightning::bitcoin::secp256k1::Secp256k1;
    use lightning::ln::msgs;
    use lightning::util::ser::LengthReadable;

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
    fn v0_pong_msg() {
        let msg = test_constants::v0_pong_msg();
        assert!(decode_msg(&msg).is_ok());
    }

    #[test]
    fn v0_msg_with_direction() {
        let msg = test_constants::v0_outbound_pong_msg();
        assert!(decode_msg(&msg).is_ok());
    }

    #[test]
    fn v0_invalid_msg_type() {
        let msg = test_constants::v0_invalid_msg_type();
        assert!(decode_msg(&msg).is_err());
    }

    #[test]
    fn read_exported_msg() {
        let msg_bin = test_constants::RAW_NODE_ANNOUNCEMENT;
        let msg = b64_engine.decode(msg_bin).unwrap();

        // The gossip message types implement LengthReadable, not Readable.
        // A properly stored message should have a valid signature.
        let na =
            msgs::NodeAnnouncement::read_from_fixed_length_buffer(&mut msg.as_slice()).unwrap();

        let ctx = Secp256k1::verification_only();
        lightning::routing::gossip::verify_node_announcement(&na, &ctx).unwrap();
    }
}
