use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::Block;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Announce block.
/// 
/// Channel: `hyperchain/<name>/v1/announce/block`.
pub struct AnnounceBlock {
    pub block: Block
}

impl AsJson for AnnounceBlock {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "block": self.block.to_json()?
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self {
                block: json.get("block")
                    .map(Block::from_json)
                    .ok_or_else(|| AsJsonError::FieldNotFound("block"))??
            }),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::builder::tests::get_root;

    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let announcement = AnnounceBlock {
            block: get_root().0
        };

        assert_eq!(AnnounceBlock::from_json(&announcement.to_json()?)?, announcement);

        Ok(())
    }
}
