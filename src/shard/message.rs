use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::prelude::*;

use super::ShardMember;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ShardMessage {
    /// Ask shard owner to start sending you status updates.
    Subscribe,

    /// Ask shard owner to stop sending you status updates.
    Unsubscribe,

    /// Note shard owner that you're still alive.
    ///
    /// Should be sent every once and a while to keep the connection.
    Heartbeat,

    /// Shard status update.
    Update(ShardUpdate)
}

impl AsJson for ShardMessage {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        match self {
            Self::Subscribe => Ok(json!({
                "format": 1,
                "type": "subscribe"
            })),

            Self::Unsubscribe => Ok(json!({
                "format": 1,
                "type": "unsubscribe"
            })),

            Self::Heartbeat => Ok(json!({
                "format": 1,
                "type": "heartbeat"
            })),

            Self::Update(update) => Ok(json!({
                "format": 1,
                "type": "update",
                "body": update.to_json()?
            }))
        }
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(message_type) = json.get("type").and_then(Json::as_str) else {
                    return Err(AsJsonError::FieldNotFound("type"));
                };

                match message_type {
                    "subscribe"   => Ok(Self::Subscribe),
                    "unsubscribe" => Ok(Self::Unsubscribe),
                    "heartbeat"   => Ok(Self::Heartbeat),

                    "update" => {
                        let Some(body) = json.get("body") else {
                            return Err(AsJsonError::FieldNotFound("body"));
                        };

                        Ok(Self::Update(ShardUpdate::from_json(body)?))
                    }

                    _ => Err(AsJsonError::FieldValueInvalid("type"))
                }
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ShardUpdate {
    /// Information about the shard's status.
    ///
    /// This update message should be sent once in a while
    /// to synchronize the shard's state between its subscribers.
    Status {
        /// Head block of the blockchain.
        head_block: Option<Block>,

        /// Tail block of the blockchain.
        tail_block: Option<Block>,

        /// List of known staged transactions' hashes.
        staged_transactions: Vec<Hash>
    },

    /// Announce members subscribed to some shard.
    ///
    /// This message is used to allow new members to subscribe
    /// to the shards mesh network even when your own subscribers
    /// list got to a limit.
    AnnounceMembers {
        members: Vec<ShardMember>
    },

    /// Announce blockchain's blocks.
    ///
    /// This is not necessary a new blocks.
    AnnounceBlocks {
        blocks: Vec<Block>
    },

    /// Announce blockchain's transactions.
    ///
    /// This is not necessary a new transactions.
    AnnounceTransactions {
        transactions: Vec<Transaction>
    }
}

impl From<ShardUpdate> for ShardMessage {
    #[inline]
    fn from(value: ShardUpdate) -> Self {
        ShardMessage::Update(value)
    }
}

impl AsJson for ShardUpdate {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        match self {
            Self::Status {
                head_block,
                tail_block,
                staged_transactions
            } => {
                Ok(json!({
                    "format": 1,
                    "type": "status",
                    "body": {
                        "blocks": {
                            "head": head_block.as_ref()
                                .map(Block::to_json)
                                .transpose()?,

                            "tail": tail_block.as_ref()
                                .map(Block::to_json)
                                .transpose()?
                        },

                        "transactions": staged_transactions.iter()
                            .map(Hash::to_base64)
                            .collect::<Vec<_>>()
                    }
                }))
            }

            Self::AnnounceMembers { members } => Ok(json!({
                "format": 1,
                "type": "announce_members",
                "members": members.iter()
                    .map(ShardMember::to_json)
                    .collect::<Result<Vec<_>, _>>()?
            })),

            Self::AnnounceBlocks { blocks } => Ok(json!({
                "format": 1,
                "type": "announce_blocks",
                "blocks": blocks.iter()
                    .map(Block::to_json)
                    .collect::<Result<Vec<_>, _>>()?
            })),

            Self::AnnounceTransactions { transactions } => Ok(json!({
                "format": 1,
                "type": "announce_transactions",
                "transactions": transactions.iter()
                    .map(Transaction::to_json)
                    .collect::<Result<Vec<_>, _>>()?
            }))
        }
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(update_type) = json.get("type").and_then(Json::as_str) else {
                    return Err(AsJsonError::FieldNotFound("type"));
                };

                match update_type {
                    "status" => {
                        let Some(body) = json.get("body") else {
                            return Err(AsJsonError::FieldNotFound("body"));
                        };

                        let Some(blocks) = body.get("blocks") else {
                            return Err(AsJsonError::FieldNotFound("body.blocks"));
                        };

                        Ok(Self::Status {
                            head_block: blocks.get("head")
                                .map(|block| {
                                    if block.is_null() {
                                        None
                                    } else {
                                        Some(Block::from_json(block))
                                    }
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.blocks.head"))?
                                .transpose()?,

                            tail_block: blocks.get("tail")
                                .map(|block| {
                                    if block.is_null() {
                                        None
                                    } else {
                                        Some(Block::from_json(block))
                                    }
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.blocks.tail"))?
                                .transpose()?,

                            staged_transactions: body.get("transactions")
                                .and_then(Json::as_array)
                                .map(|transactions| {
                                    transactions.iter()
                                        .flat_map(Json::as_str)
                                        .map(Hash::from_base64)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.transactions"))?
                                .map_err(|err| AsJsonError::Other(err.into()))?
                        })
                    }

                    "announce_members" => Ok(Self::AnnounceMembers {
                        members: json.get("members")
                            .and_then(Json::as_array)
                            .map(|members| {
                                members.iter()
                                    .map(ShardMember::from_json)
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .ok_or_else(|| AsJsonError::FieldNotFound("members"))??
                    }),

                    "announce_blocks" => Ok(Self::AnnounceBlocks {
                        blocks: json.get("blocks")
                            .and_then(Json::as_array)
                            .map(|blocks| {
                                blocks.iter()
                                    .map(Block::from_json)
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .ok_or_else(|| AsJsonError::FieldNotFound("block"))??
                    }),

                    "announce_transactions" => Ok(Self::AnnounceTransactions {
                        transactions: json.get("transactions")
                            .and_then(Json::as_array)
                            .map(|transactions| {
                                transactions.iter()
                                    .map(Transaction::from_json)
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .ok_or_else(|| AsJsonError::FieldNotFound("transactions"))??
                    }),

                    _ => Err(AsJsonError::FieldValueInvalid("type"))
                }
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::block::builder::tests::get_chained;
    use crate::shard::member::tests::get_member;

    use crate::block::transaction::builder::tests::{
        get_message,
        get_announcement
    };

    use super::*;

    pub fn get_updates() -> Vec<ShardUpdate> {
        let (root, tail, _) = get_chained();

        vec![
            ShardUpdate::Status {
                head_block: None,
                tail_block: None,
                staged_transactions: vec![]
            },

            ShardUpdate::Status {
                head_block: Some(root.clone()),
                tail_block: Some(tail.clone()),
                staged_transactions: vec![
                    get_message().0.get_hash(),
                    get_announcement().0.get_hash()
                ]
            },

            ShardUpdate::AnnounceMembers {
                members: vec![
                    get_member(),
                    get_member()
                ]
            },

            ShardUpdate::AnnounceBlocks {
                blocks: vec![
                    root,
                    tail
                ]
            },

            ShardUpdate::AnnounceTransactions {
                transactions: vec![
                    get_message().0,
                    get_announcement().0
                ]
            }
        ]
    }

    #[test]
    fn serialize_message() -> Result<(), AsJsonError> {
        assert_eq!(ShardMessage::from_json(&ShardMessage::Subscribe.to_json()?)?, ShardMessage::Subscribe);
        assert_eq!(ShardMessage::from_json(&ShardMessage::Unsubscribe.to_json()?)?, ShardMessage::Unsubscribe);

        Ok(())
    }

    #[test]
    fn serialize_update() -> Result<(), AsJsonError> {
        for update in get_updates() {
            let message = ShardMessage::Update(update);

            assert_eq!(ShardMessage::from_json(&message.to_json()?)?, message);
        }

        Ok(())
    }
}
