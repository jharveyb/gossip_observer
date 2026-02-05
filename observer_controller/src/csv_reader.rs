use serde::de::DeserializeOwned;
use std::fs::File;
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

impl TryFrom<NodeAnnotatedRecord> for observer_common::common::PeerConnectionInfo {
    type Error = anyhow::Error;

    fn try_from(record: NodeAnnotatedRecord) -> Result<Self, Self::Error> {
        let sockets = match record.sockets {
            // Remove any quotes left from CSV ingestion. They would be present if
            // we have multiple socket addresses.
            Some(addrs) => addrs.trim_matches('"').to_owned(),
            None => anyhow::bail!("No sockets for {}", record.pubkey),
        };
        let sockets = sockets
            .split(',')
            .map(|s| observer_common::common::SocketAddress {
                address: s.to_owned(),
            })
            .collect::<Vec<_>>();
        let info = observer_common::common::PeerConnectionInfo {
            pubkey: Some(observer_common::common::Pubkey {
                pubkey: record.pubkey,
            }),
            socket_addrs: sockets,
        };
        Ok(info)
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
