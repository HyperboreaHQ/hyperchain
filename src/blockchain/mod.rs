use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::crypto::Error as CryptographyError;

use hyperborealib::time::timestamp;

use crate::block::Block;

mod disk_blockchain;

#[derive(Debug)]
pub enum BlockchainValidationResult {
    /// Unknown block hash.
    UnknownBlockHash(u64),

    /// Cryptography error.
    CryptographyError(CryptographyError),

    /// Invalid previous block hash.
    InvalidPreviosBlockReference {
        block_hash: u64,
        expected_previous: Option<u64>,
        got_previous: Option<u64>
    },

    /// Invalid block creation time.
    InvalidCreationTime {
        block_hash: u64,
        created_at: u64
    },

    /// Invalid block validator.
    InvalidValidator {
        block_hash: u64,
        validator: PublicKey
    },

    /// Invalid block's sign.
    InvalidSign {
        block_hash: u64,
        validator: PublicKey,
        sign: Vec<u8>
    },

    /// Blockchain is valid.
    Valid
}

#[async_trait::async_trait]
pub trait Blockchain {
    type Error: std::error::Error + Send + Sync;

    /// Get public keys of blockchain's authorities.
    async fn get_authorities(&self) -> Result<Vec<PublicKey>, Self::Error>;

    /// Verify that given validator's public key belongs
    /// to the blockchain's authority.
    async fn is_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        Ok(self.get_authorities().await?.contains(validator))
    }

    /// Add new blockchain authority.
    async fn add_authority(&self, validator: PublicKey) -> Result<bool, Self::Error>;

    /// Delete blockchain authority.
    async fn delete_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error>;

    /// Get root block.
    async fn get_root(&self) -> Result<Option<Block>, Self::Error>;

    /// Get blockchain's tail (last) block.
    async fn get_tail(&self) -> Result<Option<Block>, Self::Error>;

    /// Get block by its hash.
    async fn get_block(&self, hash: u64) -> Result<Option<Block>, Self::Error>;

    /// Get block next to the given one.
    async fn get_next_block(&self, hash: u64) -> Result<Option<Block>, Self::Error>;

    /// Try to push block to the blockchain.
    /// 
    /// It must reference the current blockchain's tail
    /// block and have correct signature.
    async fn push_block(&self, block: Block) -> Result<(), Self::Error>;

    /// Check if the blockchain is empty
    /// (doesn't have a root node).
    async fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.get_root().await?.is_none())
    }

    /// Check if the blockchain is truncated.
    /// 
    /// Truncated blockchain's root block reference some
    /// another block but it was dropped to save space.
    /// 
    /// Truncated blockchains can't be fully validated.
    async fn is_truncated(&self) -> Result<bool, Self::Error> {
        match self.get_root().await? {
            Some(block) => Ok(block.previous().is_some()),

            // Assume by default blockchain is not truncated
            None => Ok(false)
        }
    }

    /// Validate blockchain structure.
    /// 
    /// This method will:
    /// 
    /// 1. Iterate over the blockchain, calculate
    ///    blocks hashes and validate their consistency.
    /// 
    /// 2. Verify that each block is signed by the blockchain's
    ///    authority.
    /// 
    /// 3. Verify that each block's creation timestamp is increasing
    ///    in ascending order.
    /// 
    /// Since this method is resource heavy it's recommended
    /// to run it with `since_block` property and cache
    /// results for future validations.
    async fn validate(&self, since_block: Option<u64>) -> Result<BlockchainValidationResult, Self::Error> {
        // Get initial block
        let mut block = match since_block {
            Some(hash) => match self.get_block(hash).await? {
                Some(block) => Some(block),

                None => return Ok(BlockchainValidationResult::UnknownBlockHash(hash))
            }

            None => match self.get_root().await? {
                Some(root) => Some(root),

                // No need in validating empty blockchain
                None => return Ok(BlockchainValidationResult::Valid)
            }
        };

        // Maximum allowed timestamp (+24h just in case)
        let max_timestamp = timestamp() + 24 * 60 * 60;

        // Previous block's hash
        let mut prev_block_hash = block.as_ref()
            .and_then(|block| block.prev_hash);

        // Previous block's creation timestamp
        let mut prev_created_at = 0;

        // Validate all the other blocks
        while let Some(curr_block) = block.take() {
            let block_hash = curr_block.hash();

            // Validate block's timestamp
            if curr_block.created_at < prev_created_at || curr_block.created_at > max_timestamp {
                return Ok(BlockchainValidationResult::InvalidCreationTime {
                    block_hash,
                    created_at: curr_block.created_at
                });
            }

            // Validate block's previous hash reference
            if prev_block_hash != curr_block.prev_hash {
                return Ok(BlockchainValidationResult::InvalidPreviosBlockReference {
                    block_hash,
                    expected_previous: prev_block_hash,
                    got_previous: curr_block.prev_hash
                });
            }

            // Validate block's signer
            if self.is_authority(curr_block.validator()).await? {
                return Ok(BlockchainValidationResult::InvalidValidator {
                    block_hash,
                    validator: curr_block.validator
                })
            }

            // Validate block's sign
            match curr_block.validate() {
                Ok(false) => return Ok(BlockchainValidationResult::InvalidSign {
                    block_hash,
                    validator: curr_block.validator,
                    sign: curr_block.sign
                }),

                Err(err) => return Ok(BlockchainValidationResult::CryptographyError(err)),

                _ => ()
            }

            prev_created_at = curr_block.created_at;
            prev_block_hash = Some(block_hash);

            block = self.get_next_block(block_hash).await?;
        }

        Ok(BlockchainValidationResult::Valid)
    }
}
