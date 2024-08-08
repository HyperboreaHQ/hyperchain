use hyperborealib::crypto::asymmetric::SecretKey;

use hyperborealib::time::timestamp;
use hyperborealib::crypto::utils::safe_random_u64;

use super::Block;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct BlockBuilder {
    prev_hash: Option<u64>,
    created_at: u64,
    random_seed: u64,
    data: Vec<u8>
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self {
            prev_hash: None,
            created_at: timestamp(),
            random_seed: safe_random_u64(),
            data: vec![]
        }
    }

    /// Set reference to the previous block.
    pub fn with_previous(self, prev_hash: u64) -> Self {
        Self {
            prev_hash: Some(prev_hash),
            ..self
        }
    }

    /// Change creation timestamp of the block.
    pub fn with_created_at(self, created_at: u64) -> Self {
        Self {
            created_at,
            ..self
        }
    }

    /// Change random seed of the block.
    /// 
    /// It's not recommended to do this because
    /// it's automatically chosen with sustainable randomness.
    pub fn with_random_seed(self, random_seed: u64) -> Self {
        Self {
            random_seed,
            ..self
        }
    }

    /// Change block's data.
    pub fn with_data(self, data: impl Into<Vec<u8>>) -> Self {
        Self {
            data: data.into(),
            ..self
        }
    }

    /// Build block by signing stored content's hash.
    pub fn sign(self, validator: &SecretKey) -> Block {
        let mut block = Block {
            prev_hash: self.prev_hash,
            created_at: self.created_at,
            random_seed: self.random_seed,
            data: self.data,
            validator: validator.public_key(),
            sign: vec![]
        };

        let block_hash = block.hash().to_be_bytes();

        block.sign = validator.create_signature(block_hash);

        block
    }

    /// Build new root block with default values.
    /// 
    /// ```
    /// use hyperborealib::crypto::asymmetric::SecretKey;
    /// use hyperchain::block::BlockBuilder;
    /// 
    /// let secret = SecretKey::random();
    /// 
    /// let block = BlockBuilder::build_root(b"Hello, World!", &secret);
    /// 
    /// assert_eq!(block.data(), b"Hello, World!");
    /// ```
    pub fn build_root(data: impl Into<Vec<u8>>, validator: &SecretKey) -> Block {
        Self::new()
            .with_data(data)
            .sign(validator)
    }

    /// Build new chained block with default values.
    /// 
    /// ```
    /// use hyperborealib::crypto::asymmetric::SecretKey;
    /// use hyperchain::block::BlockBuilder;
    /// 
    /// let secret = SecretKey::random();
    /// 
    /// let root = BlockBuilder::build_root(b"Root block", &secret);
    /// let block = BlockBuilder::build_chained(root.hash(), b"Chained block", &secret);
    /// 
    /// assert!(block.validate().unwrap());
    /// assert_eq!(block.data(), b"Chained block");
    /// ```
    pub fn build_chained(previos: u64, data: impl Into<Vec<u8>>, validator: &SecretKey) -> Block {
        Self::new()
            .with_previous(previos)
            .with_data(data)
            .sign(validator)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn get_block() -> (Block, SecretKey) {
        let secret = SecretKey::random();

        let block = BlockBuilder::new()
            .with_created_at(123456)
            .with_random_seed(987654)
            .with_previous(1239867)
            .with_data(b"Hello, World!")
            .sign(&secret);

        (block, secret)
    }

    #[test]
    fn build() {
        let (block, secret) = get_block();

        assert_eq!(block.validator(), &secret.public_key());

        assert!(block.validate().unwrap());

        assert_eq!(block.created_at(), 123456);
        assert_eq!(block.random_seed(), 987654);
        assert_eq!(block.previous(), Some(1239867));
        assert_eq!(block.data(), b"Hello, World!");
    }
}
