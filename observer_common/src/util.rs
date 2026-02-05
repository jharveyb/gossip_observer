use crate::common;
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::SocketAddress;
use tracing::warn;

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
impl_string_wrapper_conversions!(common::Pubkey, pubkey, PublicKey);
impl_string_wrapper_conversions!(common::SocketAddress, address, SocketAddress);

// Generic helper functions that work with any convertible type
pub fn try_convert_vec<T, U, E>(items: Vec<T>) -> Result<Vec<U>, E>
where
    U: TryFrom<T, Error = E>,
{
    items.into_iter().map(U::try_from).collect()
}

// Skip failed conversions instead of propating errors
pub fn try_convert_vec_permissive<T, U, E>(items: Vec<T>) -> Vec<U>
where
    T: std::fmt::Debug,
    U: TryFrom<T, Error = E>,
    E: std::fmt::Debug,
{
    let mut converted = Vec::with_capacity(items.len());
    let mut errstr: String;
    for item in items {
        errstr = format!("{:?}", item);
        match U::try_from(item) {
            Ok(u) => converted.push(u),
            // This may end up spamming logs
            Err(e) => {
                warn!(error = ?e, item = errstr, "Failed to convert");
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
