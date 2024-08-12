use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::{
    Block,
    Transaction,
    Hash
};

use super::ShardMember;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardMessage {
    /// Ask shard owner to start senging you status updates.
    Subscribe,

    /// Ask shard owner to stop sending you status updates.
    Unsubscribe,

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

            Self::Update(update) => Ok(json!({
                "format": 1,
                "type": "update",
                "content": update.to_json()?
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

                    "update" => {
                        let Some(content) = json.get("content") else {
                            return Err(AsJsonError::FieldNotFound("content"));
                        };

                        Ok(Self::Update(ShardUpdate::from_json(&content)?))
                    }

                    _ => Err(AsJsonError::FieldValueInvalid("type"))
                }
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardUpdate {
    /// Announce shard member that you're still online.
    ///
    /// This update message should be sent once in a while
    /// so the shard's owner can know you're still reading
    /// updates. Otherwise it can stop sending you them.
    Heartbeat,

    /// Information about the shard's status.
    ///
    /// This update message should be sent once in a while
    /// to synchronize the shard's state between its subscribers.
    Status {
        /// Root block of the blockchain.
        root_block: Option<Block>,

        /// Tail block of the blockchain.
        tail_block: Option<Block>,

        /// Announced list of clients
        /// subscribed to this shard.
        subscribers: Vec<ShardMember>,

        /// Announced list of clients to which
        /// the shard's owner is subscribed.
        subscribed: Vec<ShardMember>
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
    },

    /// Ask client to send you blockchain's blocks.
    AskBlocks {
        /// Starting block's number that you want to receive.
        from_number: u64,

        /// Maximum amount of blocks you want to receive.
        ///
        /// If `None`, then this value is chosen by the shard owner.
        /// `Some` value is only limiting the upper value. Actual amount
        /// of sent blocks is determined by the shard's owner.
        max_amount: Option<u64>
    },

    /// Ask client to send you staged transactions.
    AskTransactions {
        /// List of known transactions' hashes.
        known_transactions: Vec<Hash>
    }
}

impl AsJson for ShardUpdate {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        match self {
            Self::Heartbeat => Ok(json!({
                "format": 1,
                "type": "heartbeat"
            })),

            Self::Status {
                root_block,
                tail_block,
                subscribers,
                subscribed
            } => {
                Ok(json!({
                    "format": 1,
                    "type": "status",
                    "body": {
                        "blocks": {
                            "root": root_block.as_ref()
                                .map(Block::to_json)
                                .transpose()?,

                            "tail": tail_block.as_ref()
                                .map(Block::to_json)
                                .transpose()?
                        },

                        "subscribers": subscribers.iter()
                            .map(ShardMember::to_json)
                            .collect::<Result<Vec<_>, _>>()?,

                        "subscribed": subscribed.iter()
                            .map(ShardMember::to_json)
                            .collect::<Result<Vec<_>, _>>()?
                    }
                }))
            }

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
            })),

            Self::AskBlocks { from_number, max_amount } => Ok(json!({
                "format": 1,
                "type": "ask_blocks",
                "body": {
                    "from_number": from_number,
                    "max_amount": max_amount
                }
            })),

            Self::AskTransactions { known_transactions } => Ok(json!({
                "format": 1,
                "type": "ask_transactions",
                "known": known_transactions.iter()
                    .map(Hash::to_base64)
                    .collect::<Vec<_>>()
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
                    "heartbeat" => Ok(Self::Heartbeat),

                    "status" => {
                        let Some(body) = json.get("body") else {
                            return Err(AsJsonError::FieldNotFound("body"));
                        };

                        let Some(blocks) = body.get("blocks") else {
                            return Err(AsJsonError::FieldNotFound("body.blocks"));
                        };

                        Ok(Self::Status {
                            root_block: blocks.get("root")
                                .map(|block| {
                                    if block.is_null() {
                                        None
                                    } else {
                                        Some(Block::from_json(block))
                                    }
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.blocks.root"))?
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

                            subscribers: body.get("subscribers")
                                .and_then(Json::as_array)
                                .map(|subscribers| {
                                    subscribers.iter()
                                        .map(ShardMember::from_json)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.subscribers"))??,

                            subscribed: body.get("subscribed")
                                .and_then(Json::as_array)
                                .map(|subscribers| {
                                    subscribers.iter()
                                        .map(ShardMember::from_json)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.subscribed"))??
                        })
                    }

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

                    "ask_blocks" => {
                        let Some(body) = json.get("body") else {
                            return Err(AsJsonError::FieldNotFound("body"));
                        };

                        Ok(Self::AskBlocks {
                            from_number: body.get("from_number")
                                .and_then(Json::as_u64)
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.from_number"))?,

                            max_amount: body.get("max_amount")
                                .and_then(|max_amount| {
                                    if max_amount.is_null() {
                                        Some(None)
                                    } else {
                                        max_amount.as_u64()
                                            .map(Some)
                                    }
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.max_amount"))?
                        })
                    }

                    "ask_transactions" => Ok(Self::AskTransactions {
                        known_transactions: json.get("known")
                            .and_then(Json::as_array)
                            .map(|known| {
                                known.iter()
                                    .flat_map(Json::as_str)
                                    .map(Hash::from_base64)
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .ok_or_else(|| AsJsonError::FieldNotFound("known"))?
                            .map_err(|err| AsJsonError::Other(err.into()))?
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
            ShardUpdate::Heartbeat,

            ShardUpdate::Status {
                root_block: None,
                tail_block: None,
                subscribers: vec![],
                subscribed: vec![]
            },

            ShardUpdate::Status {
                root_block: Some(root.clone()),
                tail_block: Some(tail.clone()),
                subscribers: vec![
                    get_member(),
                    get_member()
                ],
                subscribed: vec![
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
            },

            ShardUpdate::AskBlocks {
                from_number: 10,
                max_amount: None
            },

            ShardUpdate::AskBlocks {
                from_number: 10,
                max_amount: Some(10)
            },

            ShardUpdate::AskTransactions {
                known_transactions: vec![
                    Hash::MIN,
                    Hash::MAX
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
