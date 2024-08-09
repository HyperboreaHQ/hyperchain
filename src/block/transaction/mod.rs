use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{AsJson, AsJsonError};
use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::crypto::encoding::base64;
use hyperborealib::crypto::Error as CryptographyError;

use hyperborealib::time::timestamp;

use crate::block::Hash;

mod transaction_type;
mod transaction_body;
mod builder;

pub use transaction_type::*;
pub use transaction_body::*;
pub use builder::*;

#[derive(Debug, thiserror::Error)]
pub enum TransactionValidationError {
    #[error("Failed to verify signature: {0}")]
    SignVerificationError(#[from] CryptographyError),

    #[error("Failed to calculate hash: {0}")]
    HashCalculationError(#[from] std::io::Error)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TransactionValidationResult {
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

    Valid
}

impl TransactionValidationResult {
    #[inline]
    pub fn is_valid(&self) -> bool {
        self == &Self::Valid
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Transaction {
    // Header
    pub(crate) hash: Hash,

    // Metadata
    pub(crate) random_seed: u64,
    pub(crate) created_at: u64,

    // Body
    pub(crate) author: PublicKey,
    pub(crate) body: TransactionBody,
    pub(crate) sign: Vec<u8>
}

impl Transaction {
    #[inline]
    /// Get transaction's UTC creation time.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    #[inline]
    /// Get transaction's author.
    pub fn author(&self) -> &PublicKey {
        &self.author
    }

    #[inline]
    /// Get transaction's body.
    pub fn body(&self) -> &TransactionBody {
        &self.body
    }

    #[inline]
    /// Get transaction's sign.
    pub fn sign(&self) -> &[u8] {
        &self.sign
    }

    #[inline]
    /// Get hash stored in the transaction.
    /// 
    /// This method will not validate this hash so
    /// you should treat its value as insecure.
    pub fn get_hash(&self) -> Hash {
        self.hash
    }

    /// Calculate hash of the transaction.
    /// 
    /// This is a relatively heavy function and
    /// it should not be called often.
    pub fn calculate_hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();

        hasher.update(&self.random_seed.to_be_bytes());
        hasher.update(&self.author.to_bytes());
        hasher.update(&self.body.hash().as_bytes());

        hasher.finalize().into()
    }

    /// Validate transaction.
    /// 
    /// This method will:
    /// 
    /// 1. Verify that the transaction's creation time
    ///    is not higher than the current UTC time.
    /// 
    /// 2. Calculate transaction hash and compare it
    ///    with stored value.
    /// 
    /// 3. Verify transaction's signature.
    /// 
    /// This is not recommended to call this method often.
    pub fn validate(&self) -> Result<TransactionValidationResult, TransactionValidationError> {
        // Validate transaction's creation time (+24h just in case)
        if self.created_at > timestamp() + 24 * 60 * 60 {
            return Ok(TransactionValidationResult::InvalidCreationTime {
                created_at: self.created_at
            });
        }

        // Validate transaction's hash
        let hash = self.calculate_hash();

        if self.hash != hash {
            return Ok(TransactionValidationResult::InvalidHash {
                stored: self.hash,
                calculated: hash
            });
        }

        // Validate transaction hash's signature
        if !self.author.verify_signature(self.hash.as_bytes(), &self.sign)? {
            return Ok(TransactionValidationResult::InvalidSign {
                hash: self.hash,
                sign: self.sign.clone()
            });
        }

        Ok(TransactionValidationResult::Valid)
    }
}

impl AsJson for Transaction {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "transaction": {
                "hash": self.hash.to_base64(),
                "metadata": {
                    "random_seed": self.random_seed,
                    "created_at": self.created_at
                },
                "content": {
                    "author": self.author.to_base64(),
                    "body": self.body.to_json()?,
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
                let Some(transaction) = json.get("transaction") else {
                    return Err(AsJsonError::FieldNotFound("transaction"));
                };

                let Some(metadata) = transaction.get("metadata") else {
                    return Err(AsJsonError::FieldNotFound("transaction.metadata"));
                };

                let Some(content) = transaction.get("content") else {
                    return Err(AsJsonError::FieldNotFound("transaction.content"));
                };

                Ok(Self {
                    hash: transaction.get("hash")
                        .and_then(Json::as_str)
                        .map(Hash::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.hash"))?
                        .map_err(|err| AsJsonError::Other(err.into()))?,

                    random_seed: metadata.get("random_seed")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.metadata.random_seed"))?,

                    created_at: metadata.get("created_at")
                        .and_then(Json::as_u64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.metadata.created_at"))?,

                    author: content.get("author")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.content.author"))??,

                    body: content.get("body")
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.content.body"))
                        .and_then(TransactionBody::from_json)?,

                    sign: content.get("sign")
                        .and_then(Json::as_str)
                        .map(base64::decode)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("transaction.content.sign"))??
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::block::transaction::builder::tests::{
        get_message,
        get_announcement
    };

    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let transactions = [
            get_message().0,
            get_announcement().0
        ];

        for transaction in transactions {
            assert_eq!(Transaction::from_json(&transaction.to_json()?)?, transaction);
        }

        Ok(())
    }
}
