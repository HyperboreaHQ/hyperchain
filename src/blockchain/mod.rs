use std::sync::Arc;

use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::time::timestamp;

use crate::block::prelude::*;

pub mod authorities;
pub mod blocks;
pub mod basic_blockchain;

pub mod prelude {
    pub use super::{
        BlockchainValidationError,
        BlockchainValidationResult,
        Blockchain
    };

    pub use super::authorities::*;
    pub use super::blocks::*;
    pub use super::basic_blockchain::*;
}

use prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum BlockchainValidationError<A, B> {
    #[error("Authorities index error: {0}")]
    AuthoritiesIndex(A),

    #[error("Blocks index error: {0}")]
    BlocksIndex(B),

    #[error("Failed to validate block: {0}")]
    BlockValidation(#[from] BlockValidationError)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockchainValidationResult {
    /// Unknown block hash.
    UnknownBlockHash(Hash),

    /// Unknown block number.
    UnknownBlockNumber(u64),

    /// Invalid block creation time.
    InvalidCreationTime {
        block_number: u64,
        created_at: u64
    },

    /// Invalid block number.
    InvalidNumber {
        block_number: u64,
        previous_number: u64
    },

    /// Invalid previous block hash.
    InvalidPreviosBlockReference {
        block_number: u64,
        expected_previous: Option<Hash>,
        got_previous: Option<Hash>
    },

    /// Invalid block validator.
    InvalidValidator {
        block_number: u64,
        validator: PublicKey
    },

    /// Invalid block's sign.
    InvalidSign {
        block_number: u64,
        validator: PublicKey,
        sign: Vec<u8>,
        reason: BlockValidationResult
    },

    /// Failed to verify block's sign.
    SignVerificationError {
        block_number: u64,
        validator: PublicKey,
        sign: Vec<u8>,

        // TODO: BlockValidationError
        reason: String
    },

    /// Blockchain is valid.
    Valid
}

#[async_trait::async_trait]
pub trait Blockchain {
    type AuthoritiesIndex: AuthoritiesIndex + Send + Sync;
    type BlocksIndex: BlocksIndex + Send + Sync;

    fn authorities_index(&self) -> Arc<Self::AuthoritiesIndex>;
    fn blocks_index(&self) -> Arc<Self::BlocksIndex>;

    fn authorities_index_ref(&self) -> &Self::AuthoritiesIndex;
    fn blocks_index_ref(&self) -> &Self::BlocksIndex;

    /// Validate blockchain structure.
    ///
    /// This method will:
    ///
    /// 1. Verify that each block's creation timestamp is increasing
    ///    in ascending order.
    ///
    /// 2. Verify that each block's number is increasing in ascending
    ///    order with a one step.
    ///
    /// 3. Verify that each block is signed by the blockchain's
    ///    authority.
    ///
    /// 4. Validate blocks consistency.
    ///
    /// Since this method is resource heavy it's recommended
    /// to run it with `since_block` property and cache
    /// results for future validations.
    async fn validate(&self) -> Result<
        BlockchainValidationResult,
        BlockchainValidationError<
            <Self::AuthoritiesIndex as AuthoritiesIndex>::Error,
            <Self::BlocksIndex as BlocksIndex>::Error
        >
    > {
        self.validate_since(0).await
    }

    /// Validate blockchain structure starting
    /// from the block with a given number.
    async fn validate_since(&self, start_block_number: u64) -> Result<
        BlockchainValidationResult,
        BlockchainValidationError<
            <Self::AuthoritiesIndex as AuthoritiesIndex>::Error,
            <Self::BlocksIndex as BlocksIndex>::Error
        >
    > {
        let authorities = self.authorities_index();
        let blocks = self.blocks_index();

        // Get initial block
        let mut block = if start_block_number > 0 {
            blocks.get_block(start_block_number).await
                .map_err(BlockchainValidationError::BlocksIndex)?
        } else {
            blocks.get_root_block().await
                .map_err(BlockchainValidationError::BlocksIndex)?
        };

        // Maximum allowed timestamp (+24h just in case)
        let max_timestamp = timestamp() + 24 * 60 * 60;

        // Previous block's hash
        let mut prev_block_hash = block.as_ref()
            .and_then(|block| block.previous_block);

        // Previous block's creation timestamp
        let mut prev_created_at = 0;

        let mut prev_number = if start_block_number > 0 {
            start_block_number - 1
        } else {
            0
        };

        // Validate all the blocks
        while let Some(curr_block) = block.take() {
            // Validate block's timestamp
            if curr_block.created_at < prev_created_at || curr_block.created_at > max_timestamp {
                return Ok(BlockchainValidationResult::InvalidCreationTime {
                    block_number: curr_block.number,
                    created_at: curr_block.created_at
                });
            }

            // Validate block's number
            if prev_number > 0 && prev_number + 1 != curr_block.number {
                return Ok(BlockchainValidationResult::InvalidNumber {
                    block_number: curr_block.number,
                    previous_number: prev_number
                });
            }

            // Validate block's previous hash reference
            if prev_block_hash != curr_block.previous_block {
                return Ok(BlockchainValidationResult::InvalidPreviosBlockReference {
                    block_number: curr_block.number,
                    expected_previous: prev_block_hash,
                    got_previous: curr_block.previous_block
                });
            }

            // Validate block's signer
            let is_authority = authorities.is_authority(&curr_block.validator).await
                .map_err(BlockchainValidationError::AuthoritiesIndex)?;

            if !is_authority {
                return Ok(BlockchainValidationResult::InvalidValidator {
                    block_number: curr_block.number,
                    validator: curr_block.validator
                });
            }

            // Validate block's sign
            match curr_block.validate() {
                Ok(reason) if !reason.is_valid() => return Ok(BlockchainValidationResult::InvalidSign {
                    block_number: curr_block.number,
                    validator: curr_block.validator,
                    sign: curr_block.sign,
                    reason
                }),

                Err(err) => return Ok(BlockchainValidationResult::SignVerificationError {
                    block_number: curr_block.number,
                    validator: curr_block.validator,
                    sign: curr_block.sign,
                    reason: err.to_string()
                }),

                _ => ()
            }

            prev_created_at = curr_block.created_at;
            prev_number = curr_block.number;

            prev_block_hash = Some(curr_block.get_hash());

            block = blocks.get_next_block(&curr_block).await
                .map_err(BlockchainValidationError::BlocksIndex)?;
        }

        Ok(BlockchainValidationResult::Valid)
    }
}
