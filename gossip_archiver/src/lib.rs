use chrono::{DateTime, Utc};
use config::{Config, Environment, File};
use itertools::Itertools;
use lightning::util::logger::ExportMessageDirection;
use serde::Deserialize;
use std::env;
use twox_hash::xxhash3_64::Hasher as XX3Hasher;

#[cfg(test)]
mod test_constants;

pub static INTER_MSG_DELIM: &str = ";";
static INTRA_MSG_DELIM: &str = ",";
static V0_MSG_PARTS: usize = 10;

// Format: format_args!("{now},{peer},{msg_type},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
// ldk-node/src/logger.rs#L272, LdkLogger.export()
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
    pub msg_hash: u64,
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
        String,
        String,
        Vec<u8>,
        i16,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
    ) {
        (
            m.msg_hash.to_le_bytes().to_vec(),
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
    pub msg_type: u8,
    pub msg_size: u16,
    pub orig_node: Option<String>,
    pub scid: Option<u64>,
}

impl MessageMetadata {
    #[allow(clippy::type_complexity)]
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

// Destructure our decoded message to match our DB schema; one struct per table.
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
        peer: msg.peer,
        peer_hash: msg.peer_hash,
        dir: msg.msg_dir,
        net_timestamp: msg.net_timestamp,
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

// TODO: rkyv or smthn cool here? Jk this is fine on release builds
// This should mirror whatever we're exporting to NATS
pub fn decode_msg(msg: &str) -> anyhow::Result<ExportedGossip> {
    // None values will be "", but we'll always have 9 fields.
    let parts: [&str; V0_MSG_PARTS] = msg
        .split(INTRA_MSG_DELIM)
        .collect_array()
        .ok_or(anyhow::anyhow!("Invalid message from collector: {msg}"))?;
    let mut parts = parts.into_iter();

    // Format: format_args!("{now},{peer},{msg_type},{msg_dir},{msg_size},{msg},{send_ts},{node_id},{scid},{collector_id}")
    // ldk-node/src/logger.rs#L281, LdkLogger.export_record()
    let mut next_part = || -> anyhow::Result<&str> {
        parts.next().ok_or(anyhow::anyhow!(
            "decode_msg: incorrect number of parts: {msg}"
        ))
    };

    let net_timestamp = DateTime::from_timestamp_micros(next_part()?.parse::<i64>()?).unwrap();
    // TODO: remove peer_hash, unless indexing on BYTEA is way better than TEXT
    let peer = next_part()?;
    let peer_hash = XX3Hasher::oneshot(peer.as_bytes());
    let peer = peer.to_owned();
    let msg_type = next_part()?.parse::<MessageType>()?.into();
    let msg_dir: u8 = next_part()?.parse::<ExportMessageDirection>()?.into();

    let msg_size = next_part()?.parse::<u16>()?;
    let msg = next_part()?;
    let msg_hash = XX3Hasher::oneshot(msg.as_bytes());
    let msg = msg.to_owned();
    let orig_timestamp = match next_part()? {
        "" => None,
        x => Some(DateTime::from_timestamp_micros(x.parse::<i64>()?).unwrap()),
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
    })
}

// A stream can have multiple subjects, and a consumer can pull
// messages from a stream + filter by subject.
#[derive(Debug, Deserialize)]
pub struct Nats {
    pub server_addr: String,
    pub stream_name: String,
    pub consumer_name: String,
    pub subject_prefix: String,
}

#[derive(Debug, Deserialize)]
pub struct ArchiverConfig {
    pub nats: Nats,
    pub db_url: String,
    pub uuid: String,
}

impl ArchiverConfig {
    pub fn new() -> anyhow::Result<Self> {
        // Required, but we're not checking its a valid UUIDv7
        let id = env::var("ARCHIVER_UUID")?;
        let mode = env::var("ARCHIVER_MODE")?;

        // Config file is optional; env. vars can substitute and will override
        let cfg_path = match mode.as_str() {
            // Templated cfg created on deploy
            "production" => format!("/etc/gossip_archiver/{id}/config.toml"),
            // One-off, likely running from repo root
            "local" => "archiver_config.toml".to_string(),
            _ => anyhow::bail!("Unknown ARCHIVER_MODE: {mode}"),
        };

        let cfg = Config::builder()
            // All default config values
            .set_default("nats.server_addr", "localhost:4222")?
            .set_default("nats.stream_name", "main")?
            .set_default("nats.consumer_name", "gossip_recv")?
            .set_default("nats.subject_prefix", "observer.*")?
            .add_source(File::with_name(&cfg_path).required(false))
            .add_source(Environment::with_prefix("ARCHIVER"))
            .build()?;

        cfg.try_deserialize().map_err(anyhow::Error::new)
    }
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
