use serde::{Serialize, Deserialize};

use hyperborealib::crypto::encoding::base64;
use hyperborealib::exports::base64::DecodeError;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HashError {
    #[error(transparent)]
    Base64Decode(#[from] DecodeError),

    #[error("Invalid hash length. 32 bytes expected, got {0}")]
    InvalidHashLength(usize)
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Minimal possible hash value.
    pub const MIN: Hash = Hash([0; 32]);

    /// Maximal possible hash value.
    pub const MAX: Hash = Hash([255; 32]);

    #[inline]
    pub fn as_bytes(&self) -> [u8; 32] {
        self.0
    }

    #[inline]
    pub fn from_slice(slice: impl AsRef<[u8]>) -> Self {
        blake3::hash(slice.as_ref()).into()
    }

    #[inline]
    pub fn to_base64(&self) -> String {
        base64::encode(self.0)
    }

    pub fn from_base64(hash: impl AsRef<str>) -> Result<Self, HashError> {
        let mut hash_slice = [0; 32];

        let hash = base64::decode(hash)?;

        if hash.len() != 32 {
            return Err(HashError::InvalidHashLength(hash.len()));
        }

        hash_slice.copy_from_slice(&hash);

        Ok(Self(hash_slice))
    }
}

impl From<blake3::Hash> for Hash {
    #[inline]
    fn from(value: blake3::Hash) -> Self {
        Self(*value.as_bytes())
    }
}
