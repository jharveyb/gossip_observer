use crate::common;
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::SocketAddress;
use tonic::Status;
use tracing::warn;

/// Map error results into `tonic::Status` without per-site `map_err` noise.
/// Pick the variant matching who is at fault: `or_invalid_argument` for bad
/// client payloads, `or_unavailable_ctx` for unreachable backends,
/// `or_internal` for our own failures.
pub trait StatusExt<T> {
    fn or_internal(self) -> Result<T, Status>;
    fn or_invalid_argument(self) -> Result<T, Status>;
    fn or_unavailable_ctx(self, ctx: impl FnOnce() -> String) -> Result<T, Status>;
}

impl<T, E: std::fmt::Display> StatusExt<T> for Result<T, E> {
    fn or_internal(self) -> Result<T, Status> {
        self.map_err(|e| Status::internal(e.to_string()))
    }

    fn or_invalid_argument(self) -> Result<T, Status> {
        self.map_err(|e| Status::invalid_argument(e.to_string()))
    }

    fn or_unavailable_ctx(self, ctx: impl FnOnce() -> String) -> Result<T, Status> {
        self.map_err(|e| Status::unavailable(format!("{}: {}", ctx(), e)))
    }
}

/// Macro to implement bidirectional conversions between proto string wrapper types
/// and Rust types that implement Display + FromStr.
///
/// # Requirements
/// The Rust type must implement:
/// - `Display` for converting to proto (From direction)
/// - `FromStr` for converting from proto (TryFrom direction)
///
/// # Arguments
/// * `$proto_type` - The generated protobuf message type (e.g., collector::Pubkey)
/// * `$field` - The string field name in the proto type (e.g., pubkey)
/// * `$rust_type` - The Rust type to convert to/from (e.g., PublicKey)
///
/// # Example
/// ```ignore
/// impl_string_wrapper_conversions!(collector::Pubkey, pubkey, PublicKey);
/// ```
macro_rules! impl_string_wrapper_conversions {
    ($proto_type:ty, $field:ident, $rust_type:ty) => {
        impl From<$rust_type> for $proto_type {
            fn from(value: $rust_type) -> Self {
                Self {
                    $field: value.to_string(),
                }
            }
        }

        impl TryFrom<$proto_type> for $rust_type {
            type Error = anyhow::Error;

            fn try_from(proto: $proto_type) -> Result<Self, Self::Error> {
                proto.$field.parse().map_err(anyhow::Error::new)
            }
        }
    };
}

// Apply the macro to our wrapper types
impl_string_wrapper_conversions!(common::SocketAddress, address, SocketAddress);

// Pubkeys travel as 33 raw compressed-point bytes, not hex strings — half the
// wire size and no hex round-trip on the (large) gossip-graph paths.
impl From<PublicKey> for common::Pubkey {
    fn from(value: PublicKey) -> Self {
        Self {
            pubkey: value.serialize().to_vec().into(),
        }
    }
}

impl TryFrom<common::Pubkey> for PublicKey {
    type Error = anyhow::Error;

    fn try_from(proto: common::Pubkey) -> Result<Self, Self::Error> {
        PublicKey::from_slice(&proto.pubkey).map_err(anyhow::Error::new)
    }
}

/// Bidirectional `From` impls between two structs whose listed fields are
/// identically named and typed (pure field-by-field restatements).
macro_rules! impl_mirror_conversion {
    ($a:ty, $b:ty, { $($field:ident),+ $(,)? }) => {
        impl From<$a> for $b {
            fn from(value: $a) -> Self {
                Self { $($field: value.$field),+ }
            }
        }

        impl From<$b> for $a {
            fn from(value: $b) -> Self {
                Self { $($field: value.$field),+ }
            }
        }
    };
}
pub(crate) use impl_mirror_conversion;

// Generic helper functions that work with any convertible type
pub fn try_convert_vec<T, U, E>(items: Vec<T>) -> Result<Vec<U>, E>
where
    U: TryFrom<T, Error = E>,
{
    items.into_iter().map(U::try_from).collect()
}

// Skip failed conversions instead of propagating errors
pub fn try_convert_vec_permissive<T, U, E>(items: Vec<T>) -> Vec<U>
where
    U: TryFrom<T, Error = E>,
    E: std::fmt::Display,
{
    let mut converted = Vec::with_capacity(items.len());
    for item in items {
        match U::try_from(item) {
            Ok(u) => converted.push(u),
            // This may end up spamming logs
            Err(e) => {
                warn!(error = %e, "Skipping unconvertible element");
            }
        }
    }

    converted
}

pub fn convert_vec<T, U>(items: Vec<T>) -> Vec<U>
where
    U: From<T>,
{
    items.into_iter().map(U::from).collect()
}

pub fn convert_option<T, U, E>(opt: Option<T>) -> Result<Option<U>, E>
where
    U: TryFrom<T, Error = E>,
{
    opt.map(U::try_from).transpose()
}

pub fn convert_required_field<T, U, E>(
    field: Option<T>,
    field_name: &str,
) -> Result<U, anyhow::Error>
where
    U: TryFrom<T, Error = E>,
    E: Into<anyhow::Error>,
{
    U::try_from(field.ok_or_else(|| anyhow::anyhow!("{} is required", field_name))?)
        .map_err(Into::into)
}
