use std::collections::HashSet;

use hyperborealib::crypto::asymmetric::PublicKey;

mod authorities_file;

pub use authorities_file::*;

#[async_trait::async_trait]
/// Trait implementing this struct should hold information
/// about the blockchain's authorities (blocks validators).
pub trait AuthoritiesIndex {
    type Error: std::error::Error + Send + Sync;

    /// Get public keys of authorities.
    async fn get_authorities(&self) -> Result<HashSet<PublicKey>, Self::Error>;

    /// Add new authority.
    async fn insert_authority(&self, validator: PublicKey) -> Result<bool, Self::Error>;

    /// Delete authority.
    async fn delete_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error>;

    /// Verify that the given validator's public key
    /// belongs to an authority.
    async fn is_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        Ok(self.get_authorities().await?.contains(validator))
    }
}
