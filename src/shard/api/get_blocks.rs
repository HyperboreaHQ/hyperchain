use std::collections::HashSet;

use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::Block;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Request blocks slice.
///
/// Channel: `hyperchain/<name>/v1/request/get_blocks`.
pub struct GetBlocksRequest {
    /// Request blocks starting (and including) from this one.
    pub from_number: u64,

    /// Maximum amount of blocks to return.
    ///
    /// If `None`, then the upper value is chosen by the shard owner.
    /// Returned amount of blocks can be smaller than requested one.
    pub max_amount: Option<u64>
}

impl AsJson for GetBlocksRequest {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "blocks": {
                "from": self.from_number,
                "amount": self.max_amount
            }
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(blocks) = json.get("blocks") else {
                    return Err(AsJsonError::FieldNotFound("blocks"));
                };

                Ok(Self {
                    from_number: blocks.get("from")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldNotFound("blocks.from"))?,

                    max_amount: blocks.get("amount")
                        .and_then(|amount| {
                            if amount.is_null() {
                                Some(None)
                            } else {
                                amount.as_u64().map(Some)
                            }
                        })
                        .ok_or_else(|| AsJsonError::FieldNotFound("blocks.amount"))?
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Response blocks slice.
///
/// Channel: `hyperchain/<name>/v1/response/get_blocks`.
pub struct GetBlocksResponse {
    /// Root block of the blockchain.
    ///
    /// It's needed to verify that the shard's blockchain
    /// is valid and the one you need.
    pub root_block: Block,

    /// Tail block of the blockchain.
    ///
    /// It's needed to verify amount of remained
    /// blocks that are needed to be requested.
    pub tail_block: Block,

    /// Requested blocks (or at least some of them).
    pub requested_blocks: HashSet<Block>
}

impl AsJson for GetBlocksResponse {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "blocks": {
                "root": self.root_block.to_json()?,
                "tail": self.tail_block.to_json()?,

                "requested": self.requested_blocks.iter()
                    .map(Block::to_json)
                    .collect::<Result<HashSet<_>, _>>()?,
            }
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(blocks) = json.get("blocks") else {
                    return Err(AsJsonError::FieldNotFound("blocks"));
                };

                Ok(Self {
                    root_block: blocks.get("root")
                        .map(Block::from_json)
                        .ok_or_else(|| AsJsonError::FieldNotFound("blocks.root"))??,

                    tail_block: blocks.get("tail")
                        .map(Block::from_json)
                        .ok_or_else(|| AsJsonError::FieldNotFound("blocks.tail"))??,

                    requested_blocks: blocks.get("requested")
                        .and_then(Json::as_array)
                        .map(|members| {
                            members.iter()
                                .map(Block::from_json)
                                .collect::<Result<HashSet<_>, _>>()
                        })
                        .ok_or_else(|| AsJsonError::FieldNotFound("blocks.requested"))??
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::builder::tests::{
        get_root,
        get_chained
    };

    use super::*;

    #[test]
    fn serialize_request() -> Result<(), AsJsonError> {
        let requests = [
            GetBlocksRequest {
                from_number: 0,
                max_amount: Some(100)
            },

            GetBlocksRequest {
                from_number: 0,
                max_amount: None
            }
        ];

        for request in requests {
            assert_eq!(GetBlocksRequest::from_json(&request.to_json()?)?, request);
        }

        Ok(())
    }

    #[test]
    fn serialize_response() -> Result<(), AsJsonError> {
        let response = GetBlocksResponse {
            root_block: get_root().0,
            tail_block: get_chained().1,
            requested_blocks: HashSet::from([
                get_root().0,
                get_chained().1,
                get_chained().1
            ])
        };

        assert_eq!(GetBlocksResponse::from_json(&response.to_json()?)?, response);

        Ok(())
    }
}
