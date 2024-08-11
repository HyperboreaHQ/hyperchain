use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Information about the network shard.
pub struct ShardInfo {
    pub(crate) name: String,
    pub(crate) owner: ShardMember,
    pub(crate) members: HashSet<ShardMember>,
}

impl ShardInfo {
    #[inline]
    /// Get shard's name.
    ///
    /// It is used in hyperborea inbox's channel
    /// to distinguish different shards.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    /// Get shard's owner.
    pub fn owner(&self) -> &ShardMember {
        &self.owner
    }

    #[inline]
    /// Get shard's members.
    pub fn members(&self) -> &HashSet<ShardMember> {
        &self.members
    }
}
