use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::crypto::encoding::base64;
use hyperborealib::crypto::Error as CryptographyError;

use hyperborealib::time::timestamp;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

pub(crate) mod hash;
pub(crate) mod transaction;
pub(crate) mod minter;
pub(crate) mod builder;

pub use hash::*;
pub use transaction::*;
pub use minter::*;
pub use builder::*;

#[derive(Debug, thiserror::Error)]
pub enum BlockValidationError {
    #[error("Failed to verify signature: {0}")]
    SignVerificationError(#[from] CryptographyError),

    #[error("Failed to validate transaction: {0}")]
    TransactionValidationError(#[from] TransactionValidationError)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlockValidationResult {
    /// Invalid creation timestamp.
    InvalidCreationTime {
        created_at: u64
    },

    /// Invalid hash.
    InvalidHash {
        stored: Hash,
        calculated: Hash
    },

    /// Invalid hash signature.
    InvalidSign {
        hash: Hash,
        sign: Vec<u8>
    },

    /// Invalid transaction.
    InvalidTransaction {
        transaction: Box<Transaction>,
        error: TransactionValidationResult,
    },

    Valid
}

impl BlockValidationResult {
    #[inline]
    pub fn is_valid(&self) -> bool {
        self == &Self::Valid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Block {
    // Header
    pub(crate) previous_block: Option<Hash>,
    pub(crate) hash: Hash,
    pub(crate) number: u64,

    // Metadata
    pub(crate) random_seed: u64,
    pub(crate) created_at: u64,

    // Body
    pub(crate) transactions: Vec<Transaction>,
    pub(crate) minters: Vec<BlockMinter>,
    pub(crate) validator: PublicKey,
    pub(crate) sign: Vec<u8>
}

impl Block {
    #[inline]
    /// Hash of the previous block.
    pub fn previous_block(&self) -> Option<Hash> {
        self.previous_block
    }

    #[inline]
    /// Number of the block in the blockchain.
    pub fn number(&self) -> u64 {
        self.number
    }

    #[inline]
    /// UTC timestamp (amount of seconds) when
    /// this block was made.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    #[inline]
    /// List of transactions signed in this block.
    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    #[inline]
    /// List of minters participated in this block's creation.
    pub fn minters(&self) -> &[BlockMinter] {
        &self.minters
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

    #[inline]
    /// Get hash stored in the block.
    ///
    /// This method will not validate this hash so
    /// you should treat its value as insecure.
    pub fn get_hash(&self) -> Hash {
        self.hash
    }

    /// Calculate hash of the block.
    ///
    /// This is a relatively heavy function and
    /// it should not be called often.
    pub fn calculate_hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();

        // Header
        if let Some(hash) = &self.previous_block {
            hasher.update(&hash.as_bytes());
        }

        hasher.update(&self.number.to_be_bytes());

        // Metadata
        hasher.update(&self.random_seed.to_be_bytes());
        hasher.update(&self.created_at.to_be_bytes());

        // Body
        for transaction in &self.transactions {
            hasher.update(&transaction.calculate_hash().as_bytes());
        }

        for minter in &self.minters {
            hasher.update(&minter.hash().as_bytes());
        }

        hasher.finalize().into()
    }

    /// Validate block.
    ///
    /// This method will:
    ///
    /// 1. Verify that the block's creation time
    ///    is not higher than the current UTC time.
    ///
    /// 2. Calculate block hash and compare it
    ///    with stored value.
    ///
    /// 3. Verify block's signature.
    ///
    /// 4. Verify each stored transaction.
    ///
    /// This is not recommended to call this method often.
    pub fn validate(&self) -> Result<BlockValidationResult, BlockValidationError> {
        // Validate block's creation time (+24h just in case)
        if self.created_at > timestamp() + 24 * 60 * 60 {
            return Ok(BlockValidationResult::InvalidCreationTime {
                created_at: self.created_at
            });
        }

        // Validate block's hash
        let hash = self.calculate_hash();

        if self.hash != hash {
            return Ok(BlockValidationResult::InvalidHash {
                stored: self.hash,
                calculated: hash
            });
        }

        // Validate block hash's signature
        if !self.validator.verify_signature(self.hash.as_bytes(), &self.sign)? {
            return Ok(BlockValidationResult::InvalidSign {
                hash: self.hash,
                sign: self.sign.clone()
            });
        }

        // Validate block's stored transactions
        for transaction in &self.transactions {
            let result = transaction.validate()?;

            if !result.is_valid() {
                return Ok(BlockValidationResult::InvalidTransaction {
                    transaction: Box::new(transaction.clone()),
                    error: result
                });
            }
        }

        Ok(BlockValidationResult::Valid)
    }
}

impl AsJson for Block {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "block": {
                "previous": self.previous_block.map(|hash| hash.to_base64()),
                "current": self.hash.to_base64(),
                "number": self.number,
                "metadata": {
                    "random_seed": self.random_seed,
                    "created_at": self.created_at
                },
                "content": {
                    "transactions": self.transactions.iter()
                        .map(|transaction| transaction.to_json())
                        .collect::<Result<Vec<_>, _>>()?,

                    "minters": self.minters.iter()
                        .map(|minter| minter.to_json())
                        .collect::<Result<Vec<_>, _>>()?,

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
                    previous_block: block.get("previous")
                        .and_then(|value| {
                            if value.is_null() {
                                Some(None)
                            } else if let Some(hash) = value.as_str() {
                                match Hash::from_base64(hash) {
                                    Ok(hash) => Some(Some(hash)),

                                    // FIXME: return this error
                                    Err(_) => None
                                }
                            } else {
                                None
                            }
                        })
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.previous"))?,

                    hash: block.get("current")
                        .and_then(Json::as_str)
                        .map(Hash::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.current"))?
                        .map_err(|err| AsJsonError::Other(err.into()))?,

                    number: block.get("number")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.number"))?,

                    random_seed: metadata.get("random_seed")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.metadata.random_seed"))?,

                    created_at: metadata.get("created_at")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.metadata.created_at"))?,

                    transactions: content.get("transactions")
                        .and_then(Json::as_array)
                        .map(|transactions| {
                            transactions.iter()
                                .map(Transaction::from_json)
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.transactions"))??,

                    minters: content.get("minters")
                        .and_then(Json::as_array)
                        .map(|minters| {
                            minters.iter()
                                .map(BlockMinter::from_json)
                                .collect::<Result<Vec<_>, _>>()
                        })
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.minters"))??,

                    validator: content.get("validator")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.validator"))??,

                    sign: content.get("sign")
                        .and_then(Json::as_str)
                        .map(base64::decode)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("block.content.sign"))??
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let block = builder::tests::get_chained().1;

        assert_eq!(Block::from_json(&block.to_json()?)?, block);

        Ok(())
    }
}
