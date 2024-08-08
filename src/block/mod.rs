use std::hash::Hasher;

use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::crypto::encoding::base64;
use hyperborealib::crypto::Error as CryptographyError;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

mod builder;

pub use builder::BlockBuilder;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Block {
    pub(crate) prev_hash: Option<u64>,
    pub(crate) created_at: u64,
    pub(crate) random_seed: u64,
    pub(crate) data: Vec<u8>,
    pub(crate) validator: PublicKey,
    pub(crate) sign: Vec<u8>
}

impl Block {
    #[inline]
    /// Hash of the previous block.
    pub fn previous(&self) -> Option<u64> {
        self.prev_hash
    }

    #[inline]
    /// UTC timestamp (amount of seconds) when
    /// this block was made.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    #[inline]
    /// Sustainably random number to ensure
    /// that the block will have unique hash.
    pub fn random_seed(&self) -> u64 {
        self.random_seed
    }

    #[inline]
    /// Content of the block.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    /// Public key of the block's sign author.
    pub fn validator(&self) -> &PublicKey {
        &self.validator
    }

    #[inline]
    /// Digital signature of the block's hash.
    pub fn sign(&self) -> &[u8] {
        &self.sign
    }

    /// Get hash of the current block.
    /// 
    /// This is a relatively heavy function
    /// and should not be called frequently.
    pub fn hash(&self) -> u64 {
        let mut hasher = seahash::SeaHasher::new();

        if let Some(hash) = &self.prev_hash {
            hasher.write(&hash.to_be_bytes());
        }

        hasher.write(&self.created_at.to_be_bytes());
        hasher.write(&self.random_seed.to_be_bytes());
        hasher.write(&self.data);
        hasher.write(&self.validator.to_bytes());

        hasher.finish()
    }

    /// Verify block's signature using
    /// `hash()` method and stored `validator` value.
    pub fn validate(&self) -> Result<bool, CryptographyError> {
        let hash = self.hash();

        self.validator.verify_signature(hash.to_be_bytes(), &self.sign)
    }
}

impl AsJson for Block {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "block": {
                "previous": self.prev_hash,
                "metadata": {
                    "created_at": self.created_at,
                    "random_seed": self.random_seed
                },
                "content": {
                    "data": base64::encode(&self.data),
                    "validator": self.validator.to_base64(),
                    "sign": base64::encode(&self.sign)
                }
            }
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(block) = json.get("block") else {
                    return Err(AsJsonError::FieldNotFound("block"));
                };

                let Some(metadata) = block.get("metadata") else {
                    return Err(AsJsonError::FieldNotFound("block.metadata"));
                };

                let Some(content) = block.get("content") else {
                    return Err(AsJsonError::FieldNotFound("block.content"));
                };

                Ok(Self {
                    prev_hash: block.get("previous")
                        .and_then(|value| {
                            if value.is_null() {
                                Some(None)
                            } else {
                                value.as_u64()
                                    .map(Some)
                            }
                        })
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.previous"))?,

                    created_at: metadata.get("created_at")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.metadata.created_at"))?,

                    random_seed: metadata.get("random_seed")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.metadata.random_seed"))?,

                    data: content.get("data")
                        .and_then(Json::as_str)
                        .map(base64::decode)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.data"))??,

                    validator: content.get("validator")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.validator"))??,

                    sign: content.get("sign")
                        .and_then(Json::as_str)
                        .map(base64::decode)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.sign"))??,
                })
            }

            _ => Err(AsJsonError::FieldValueInvalid("format"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let block = builder::tests::get_block().0;

        assert_eq!(Block::from_json(&block.to_json()?)?, block);

        Ok(())
    }
}
