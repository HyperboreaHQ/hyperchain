use hyperborealib::crypto::asymmetric::SecretKey;

use hyperborealib::time::timestamp;
use hyperborealib::crypto::utils::safe_random_u64;

use super::prelude::*;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct BlockBuilder {
    prebious_block: Option<Hash>,
    number: u64,

    random_seed: u64,
    created_at: u64,

    transactions: Vec<Transaction>,
    minters: Vec<BlockMinter>
}

impl BlockBuilder {
    /// Create new block builder with default values.
    ///
    /// It's recommended to use `chained()` method instead.
    ///
    /// ```
    /// use hyperborealib::prelude::*;
    /// use hyperchain::prelude::*;
    ///
    /// let secret = SecretKey::random();
    ///
    /// // Create empty root block
    /// let block = BlockBuilder::new()
    ///     .sign(&secret);
    /// ```
    pub fn new() -> Self {
        Self {
            prebious_block: None,
            number: 0,

            random_seed: safe_random_u64(),
            created_at: timestamp(),

            transactions: Vec::new(),
            minters: Vec::new()
        }
    }

    /// Create new block builder with a reference
    /// to the previous block and a proper number.
    pub fn chained(previous: &Block) -> Self {
        Self::new()
            .with_previous(previous.hash)
            .with_number(previous.number + 1)
    }

    #[inline]
    /// Set reference to the previous block.
    pub fn with_previous(mut self, hash: impl Into<Hash>) -> Self {
        self.prebious_block = Some(hash.into());

        self
    }

    #[inline]
    /// Set block's number.
    pub fn with_number(mut self, number: impl Into<u64>) -> Self {
        self.number = number.into();

        self
    }

    #[inline]
    /// Add transaction to the block.
    pub fn add_transaction(mut self, transaction: Transaction) -> Self {
        self.transactions.push(transaction);

        self
    }

    #[inline]
    /// Add minter info to the block.
    pub fn add_minter(mut self, minter: BlockMinter) -> Self {
        self.minters.push(minter);

        self
    }

    /// Build block by signing stored content's hash.
    pub fn sign(self, validator: &SecretKey) -> Block {
        let mut block = Block {
            previous_block: self.prebious_block,
            hash: Hash::default(),
            number: self.number,

            random_seed: self.random_seed,
            created_at: self.created_at,

            transactions: self.transactions,
            minters: self.minters,
            validator: validator.public_key(),
            sign: vec![]
        };

        let hash = block.calculate_hash();
        let sign = validator.create_signature(hash.as_bytes());

        block.hash = hash;
        block.sign = sign;

        block
    }

    /// Build new root block with default values.
    ///
    /// ```
    /// use hyperborealib::prelude::*;
    /// use hyperchain::prelude::*;
    ///
    /// let secret = SecretKey::random();
    ///
    /// let block = BlockBuilder::build_root(&secret);
    /// ```
    pub fn build_root(validator: &SecretKey) -> Block {
        Self::new().sign(validator)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::block::transaction::builder::tests::{
        get_message,
        get_announcement
    };

    use crate::block::minter::tests::get_minter;

    use super::*;

    pub fn get_root() -> (Block, SecretKey) {
        let secret = SecretKey::random();

        let block = BlockBuilder::build_root(&secret);

        (block, secret)
    }

    pub fn get_chained() -> (Block, Block, SecretKey) {
        let (root, secret) = get_root();

        let block = BlockBuilder::chained(&root)
            .add_transaction(get_message().0)
            .add_transaction(get_announcement().0)
            .add_minter(get_minter().0)
            .sign(&secret);

        (root, block, secret)
    }

    #[test]
    fn validate() -> Result<(), BlockValidationError> {
        let (root, chained, secret) = get_chained();

        assert_eq!(root.validator, secret.public_key());
        assert_eq!(chained.validator, secret.public_key());

        assert_eq!(chained.previous_block, Some(root.hash));

        assert!(root.validate()?.is_valid());
        assert!(chained.validate()?.is_valid());

        Ok(())
    }
}
