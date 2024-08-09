use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::crypto::asymmetric::PublicKey;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use super::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockMinter {
    pub(crate) public_key: PublicKey,
    pub(crate) balance_mask: Hash
}

impl BlockMinter {
    #[inline]
    pub fn new(public_key: PublicKey, balance_mask: Hash) -> Self {
        Self {
            public_key,
            balance_mask
        }
    }

    #[inline]
    /// Public key of the block's minter.
    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }

    #[inline]
    /// XOR mask of the block minter's balance.
    /// 
    /// This value is calculated as `previous_mask ^ minted_block`.
    pub fn balance_mask(&self) -> Hash {
        self.balance_mask
    }

    /// Calculate hash of the minter.
    pub fn hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();

        hasher.update(&self.public_key.to_bytes());
        hasher.update(&self.balance_mask.as_bytes());

        hasher.finalize().into()
    }
}

impl AsJson for BlockMinter {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "minter": {
                "public_key": self.public_key.to_base64(),
                "balance_mask": self.balance_mask.to_base64()
            }
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(minter) = json.get("minter") else {
                    return Err(AsJsonError::FieldNotFound("minter"));
                };

                Ok(Self {
                    public_key: minter.get("public_key")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("minter.public_key"))??,

                    balance_mask: minter.get("balance_mask")
                        .and_then(Json::as_str)
                        .map(Hash::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("minter.balance_mask"))?
                        .map_err(|err| AsJsonError::Other(err.into()))?
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use hyperborealib::crypto::asymmetric::SecretKey;

    use super::*;

    pub fn get_minter() -> (BlockMinter, SecretKey) {
        let secret = SecretKey::random();

        let minter = BlockMinter::new(secret.public_key(), Hash::MAX);

        (minter, secret)
    }

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let minter = get_minter().0;

        assert_eq!(BlockMinter::from_json(&minter.to_json()?)?, minter);

        Ok(())
    }
}
