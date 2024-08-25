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

#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Default, Debug, Clone, Copy, Eq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Minimal possible hash value.
    pub const MIN: Hash = Hash([0; 32]);

    /// Maximal possible hash value.
    pub const MAX: Hash = Hash([255; 32]);

    /// Bits length of hash.
    pub const BITS: usize = 256;

    /// Bytes length of hash.
    pub const BYTES: usize = 32;

    #[inline]
    /// Convert hash struct into the bytes representation.
    pub fn as_bytes(&self) -> [u8; 32] {
        self.0
    }

    #[inline]
    /// Convert bytes representation into the hash struct.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[inline]
    /// Create hash from the given bytes slice.
    ///
    /// Given bytes slice will be hashed with the `blake3` algorithm.
    pub fn hash_slice(slice: impl AsRef<[u8]>) -> Self {
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

// Function is taken from the blake3's crate.
fn cmp_slices(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut x = 0;

    for i in 0..a.len() {
        x |= a[i] ^ b[i];
    }

    x == 0
}

impl PartialEq for Hash {
    #[inline]
    fn eq(&self, other: &Hash) -> bool {
        cmp_slices(&self.0, &other.0)
    }
}

impl PartialEq<&Hash> for Hash {
    #[inline]
    fn eq(&self, other: &&Hash) -> bool {
        cmp_slices(&self.0, &other.0)
    }
}

impl PartialEq<[u8; Hash::BYTES]> for Hash {
    #[inline]
    fn eq(&self, other: &[u8; Hash::BYTES]) -> bool {
        cmp_slices(&self.0, other)
    }
}

impl PartialEq<[u8; Hash::BYTES]> for &Hash {
    #[inline]
    fn eq(&self, other: &[u8; Hash::BYTES]) -> bool {
        cmp_slices(&self.0, other)
    }
}

impl PartialEq<Hash> for [u8; Hash::BYTES] {
    #[inline]
    fn eq(&self, other: &Hash) -> bool {
        cmp_slices(self, &other.0)
    }
}

impl PartialEq<&Hash> for [u8; Hash::BYTES] {
    #[inline]
    fn eq(&self, other: &&Hash) -> bool {
        cmp_slices(self, &other.0)
    }
}

impl PartialEq<[u8]> for Hash {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        cmp_slices(&self.0, other)
    }
}

impl PartialEq<[u8]> for &Hash {
    #[inline]
    fn eq(&self, other: &[u8]) -> bool {
        cmp_slices(&self.0, other)
    }
}
