use anyhow::anyhow;
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::SocketAddress;
use observer_common::types::PeerConnectionInfo;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::str::FromStr;
use tracing::info;

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct NodeInfoRecord {
    pub pubkey: String,
    pub net_type: String,
    pub sockets: String,
    pub alias: String,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct NodeCommunitiesRecord {
    pub pubkey: String,
    pub level_0: u32,
    pub level_1: u32,
    pub level_2: u32,
}

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, Default)]
pub struct NodeAnnotatedRecord {
    pub pubkey: String,
    pub net_type: Option<String>,
    pub sockets: Option<String>,
    pub alias: Option<String>,
}

impl NodeAnnotatedRecord {
    // Somehow we had records with Some(""), or similar. Filter those out.
    pub fn has_sockets(&self) -> bool {
        self.sockets.as_ref().is_some_and(|s| !s.is_empty())
    }
}

// Convert straight to the validated domain type; the wire conversion happens
// once, inside CollectorClient::send_eligible_peers. A record with a bad
// pubkey or any unparseable address (e.g. OnionV2) errors as a whole, so
// permissive callers skip the peer entirely.
impl TryFrom<&NodeAnnotatedRecord> for PeerConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(record: &NodeAnnotatedRecord) -> Result<Self, Self::Error> {
        let pubkey = PublicKey::from_str(&record.pubkey)
            .map_err(|e| anyhow!("Bad pubkey {}: {e}", record.pubkey))?;
        let sockets = match &record.sockets {
            // Remove any quotes left from CSV ingestion. They would be present if
            // we have multiple socket addresses.
            Some(addrs) => addrs.trim_matches('"'),
            None => anyhow::bail!("No sockets for {}", record.pubkey),
        };
        let addrs = sockets
            .split(',')
            .map(SocketAddress::from_str)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow!("Bad socket address for {}: {e:?}", record.pubkey))?;
        Ok(PeerConnectionInfo { pubkey, addrs })
    }
}

pub fn load_csv<T>(path: &str) -> anyhow::Result<Vec<T>>
where
    T: DeserializeOwned,
{
    info!(path = %path, "Loading CSV file");

    let file = File::open(path)?;
    let reader = csv::Reader::from_reader(file);
    let records = reader.into_deserialize().collect::<Result<Vec<T>, _>>()?;

    info!(record_count = records.len(), "CSV file loaded successfully");
    Ok(records)
}

pub fn write_csv<T>(path: &str, records: &[T]) -> anyhow::Result<()>
where
    T: serde::Serialize,
{
    info!(path = %path, record_count = records.len(), "Writing CSV file");
    let mut writer = csv::Writer::from_path(path)?;
    for record in records {
        writer.serialize(record)?;
    }
    writer.flush()?;
    Ok(())
}
