#![allow(dead_code)]

// Base64 URL-SAFE encoded unsigned gossip message.
static B64_MSG_NO_SIG: &str = "b-KMCrbxs3LBpqJGrmP3T5Meg2XhWgicaNYZAAAAAAANC0gABOMAAGjXqasBAABQAAAAAAAAA-gAAAAAAAAAAAAAAAE2L5uo";
// XXH3_64 hash of an unsigned gossip message.
static MSG_HASH: u64 = 9246706498963405000;
// XXH3_64 hash of a node key.
static NODE_KEY_HASH: u64 = 14513808960632510027;
// Real short channel ID.
static SCID: u64 = 977510586604410100;
// Real, randomly-chosen node keys.
static NODE_KEY: &str = "02abee897f5ab347fb917b30bf184c4b3ee33bd81d568958193f0cf3b7f7b41b44";
static NODE_2_KEY: &str = "032eafb65801a6cfb8da47c25138aa686640aec7eff486c04e8db777cf14725864";
static NODE_3_KEY: &str = "0310889bc71cf90822894487d1a6b52a56f47e5b011c1420f32db62810ef66d734";

// UNIX timestamp of seconds, converted to microseconds.
static TIMESTAMP_UNIX_USEC: u64 = 1758918330 * 1000000;
// UNIX timestamp with microseond precision.
static TIMESTAMP_2_UNIX_USEC: u64 = 1758918670989620;
static MSG_SIZE: u16 = 430;
static MSG_TYPE_VALID: &str = "ca";

// All 9 fields are populated; in practice this shouldn't occur, but the decoder
// isn't checking the gossip message type against the populated fields.
pub fn v0_all_fields_populated() -> String {
    format!(
        "{TIMESTAMP_UNIX_USEC},{NODE_KEY},{MSG_TYPE_VALID},{MSG_SIZE},{B64_MSG_NO_SIG},{TIMESTAMP_2_UNIX_USEC},{NODE_2_KEY},{SCID},{NODE_3_KEY}"
    )
}

// Decode should fail if our node did not add its own pubkey to the message.
pub fn v0_missing_collector_key() -> String {
    format!(
        "{TIMESTAMP_UNIX_USEC},{NODE_KEY},{MSG_TYPE_VALID},{MSG_SIZE},{B64_MSG_NO_SIG},{TIMESTAMP_2_UNIX_USEC},{NODE_2_KEY},{SCID}"
    )
}

// Only 6 required fields populated.
pub fn v0_minimal_fields() -> String {
    format!(
        "{TIMESTAMP_UNIX_USEC},{NODE_KEY},{MSG_TYPE_VALID},{MSG_SIZE},{B64_MSG_NO_SIG},,,,{NODE_3_KEY}"
    )
}

pub fn v0_invalid_msg_type() -> String {
    let msg_type_invalid = "nope";
    format!(
        "{TIMESTAMP_UNIX_USEC},{NODE_KEY},{msg_type_invalid},{MSG_SIZE},{B64_MSG_NO_SIG},,,,{NODE_3_KEY}"
    )
}
