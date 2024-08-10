use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::shard::ShardMember;

use crate::block::{
    Block,
    Transaction
};

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Request connection to the shard.
/// 
/// Channel: `hyperchain/<name>/v1/request/connect`.
pub struct ConnectRequest;

impl AsJson for ConnectRequest {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
/// Response on connection to the shard.
/// 
/// Channel: `hyperchain/<name>/v1/response/connect`.
pub enum ConnectResponse {
    /// Connection allowed.
    Connected {
        /// List of members connected to this shard.
        members: Vec<ShardMember>,

        /// Blockchain's root block.
        root_block: Block,

        /// Blockchain's tail block.
        tail_block: Block,

        /// List of staged transactions (which are not
        /// yet included to the blockchain).
        /// 
        /// This list may not be full. You can request
        /// it separately later.
        transactions: Vec<Transaction>
    },

    /// Connection aborted.
    Aborted
}

impl AsJson for ConnectResponse {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        match self {
            Self::Connected { members, root_block, tail_block, transactions } => {
                Ok(json!({
                    "format": 1,
                    "status": "connected",
                    "body": {
                        "members": members.iter()
                            .map(ShardMember::to_json)
                            .collect::<Result<Vec<_>, _>>()?,

                        "blocks": {
                            "root": root_block.to_json()?,
                            "tail": tail_block.to_json()?
                        },

                        "transactions": transactions.iter()
                            .map(Transaction::to_json)
                            .collect::<Result<Vec<_>, _>>()?
                    }
                }))
            }

            Self::Aborted => {
                Ok(json!({
                    "format": 1,
                    "status": "aborted"
                }))
            }
        }
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(status) = json.get("status").and_then(Json::as_str) else {
                    return Err(AsJsonError::FieldNotFound("status"));
                };

                match status {
                    "connected" => {
                        let Some(body) = json.get("body") else {
                            return Err(AsJsonError::FieldNotFound("body"));
                        };

                        let Some(blocks) = body.get("blocks") else {
                            return Err(AsJsonError::FieldNotFound("body.blocks"));
                        };

                        Ok(Self::Connected {
                            members: body.get("members")
                                .and_then(Json::as_array)
                                .map(|members| {
                                    members.iter()
                                        .map(ShardMember::from_json)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.members"))??,
    
                            root_block: blocks.get("root")
                                .map(Block::from_json)
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.blocks.root"))??,

                            tail_block: blocks.get("tail")
                                .map(Block::from_json)
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.blocks.tail"))??,

                            transactions: body.get("transactions")
                                .and_then(Json::as_array)
                                .map(|transactions| {
                                    transactions.iter()
                                        .map(Transaction::from_json)
                                        .collect::<Result<Vec<_>, _>>()
                                })
                                .ok_or_else(|| AsJsonError::FieldNotFound("body.transactions"))??,
                        })
                    }

                    "aborted" => Ok(Self::Aborted),

                    _ => Err(AsJsonError::FieldValueInvalid("status"))
                }
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::shard::member::tests::get_member;

    use crate::block::builder::tests::{
        get_root,
        get_chained
    };

    use crate::block::transaction::builder::tests::{
        get_message,
        get_announcement
    };

    use super::*;

    #[test]
    fn serialize_request() -> Result<(), AsJsonError> {
        let request = ConnectRequest;

        assert_eq!(ConnectRequest::from_json(&request.to_json()?)?, request);

        Ok(())
    }

    #[test]
    fn serialize_response() -> Result<(), AsJsonError> {
        let responses = [
            ConnectResponse::Connected {
                members: vec![
                    get_member(),
                    get_member(),
                    get_member()
                ],
                root_block: get_root().0,
                tail_block: get_chained().1,
                transactions: vec![
                    get_announcement().0,
                    get_message().0,
                    get_message().0
                ]
            },
            ConnectResponse::Aborted
        ];

        for response in responses {
            assert_eq!(ConnectResponse::from_json(&response.to_json()?)?, response);
        }

        Ok(())
    }
}
