use crate::block::{
    Block,
    Transaction,
    Hash
};

use super::*;

/// Connection to the remote shard.
pub struct RemoteShard<T: HttpClient> {
    pub(crate) middleware: ConnectedClient<T>,
    pub(crate) info: ShardInfo,
    pub(crate) root_block: Block,
    pub(crate) tail_block: Block,
    pub(crate) staged_transactions: HashSet<Hash>,

    // Data processers.
    pub(crate) transactions_handler: Option<Box<dyn Fn(Transaction) + 'static>>,
    pub(crate) block_handler: Option<Box<dyn Fn(Block) + 'static>>,

    // Pool of data stored before processing it.
    pub(crate) transactions_pool: HashSet<Transaction>,
    pub(crate) blocks_pool: HashSet<Block>
}

impl<C: HttpClient> RemoteShard<C> {
    #[inline]
    /// Get information about the shard.
    pub fn info(&self) -> &ShardInfo {
        &self.info
    }

    #[inline]
    /// Get root block of the shard's blockchain.
    pub fn root_block(&self) -> &Block {
        &self.root_block
    }

    #[inline]
    /// Get tail block of the shard's blockchain.
    pub fn tail_block(&self) -> &Block {
        &self.tail_block
    }

    #[inline]
    /// Get list of staged transactions hashes.
    pub fn staged_transactions(&self) -> &HashSet<Hash> {
        &self.staged_transactions
    }

    /// Set new transactions handler.
    pub fn subscribe_transaction(&mut self, handler: impl Fn(Transaction) + 'static) -> &mut Self {
        self.transactions_handler = Some(Box::new(handler));

        self
    }

    /// Set new block handler.
    pub fn subscribe_block(&mut self, handler: impl Fn(Block) + 'static) -> &mut Self {
        self.block_handler = Some(Box::new(handler));

        self
    }

    // TODO: use smart pointers to allow calling methods without mutable reference

    /// Listen incoming announcements.
    pub async fn listen(&mut self) -> Result<(), ShardError> {
        todo!()
    }

    /// Request shard members list.
    pub async fn get_members(&mut self) -> Result<HashSet<ShardMember>, ShardError> {
        send(
            &self.middleware,
            &self.info.owner,
            format!("hyperchain/{}/v1/request/get_members", &self.info.name),
            api::GetMembersRequest
        ).await?;

        let response = poll::<api::GetMembersResponse, _>(
            &self.middleware,
            format!("hyperchain/{}/v1/response/get_members", &self.info.name)
        ).await?;

        // Update local members list before returning it.
        for member in &response.members {
            self.info.members.insert(member.clone());
        }

        Ok(response.members)
    }

    /// Request shard blockchain's blocks.
    ///
    /// This method doesn't validate returned blocks.
    pub async fn get_blocks(&mut self, from_number: u64, max_amount: Option<u64>) -> Result<HashSet<Block>, ShardError> {
        send(
            &self.middleware,
            &self.info.owner,
            format!("hyperchain/{}/v1/request/get_blocks", &self.info.name),
            api::GetBlocksRequest {
                from_number,
                max_amount
            }
        ).await?;

        let response = poll::<api::GetBlocksResponse, _>(
            &self.middleware,
            format!("hyperchain/{}/v1/response/get_blocks", &self.info.name)
        ).await?;

        // Update root block if obtained one is older.
        if self.root_block.number > response.root_block.number {
            if !self.root_block.validate()?.is_valid() {
                return Err(ShardError::InvalidBlock);
            }

            self.root_block = response.root_block;
        }

        // Update tail block if obtained one is newer.
        if self.tail_block.number < response.tail_block.number {
            if !self.tail_block.validate()?.is_valid() {
                return Err(ShardError::InvalidBlock);
            }

            self.tail_block = response.tail_block;
        }

        // Return obtained blocks.
        // We're not validating them - this should be done
        // by the user.
        Ok(response.requested_blocks)
    }

    /// Request shard blockchain's staged transactions.
    ///
    /// This method doesn't validate returned transactions.
    pub async fn get_transactions(&mut self) -> Result<HashSet<Transaction>, ShardError> {
        send(
            &self.middleware,
            &self.info.owner,
            format!("hyperchain/{}/v1/request/get_transactions", &self.info.name),
            api::GetTransactionsRequest {
                known_transactions: self.staged_transactions.clone()
            }
        ).await?;

        let response = poll::<api::GetTransactionsResponse, _>(
            &self.middleware,
            format!("hyperchain/{}/v1/response/get_transactions", &self.info.name)
        ).await?;

        // Insert staged transactions hashes.
        for transaction in &response.transactions {
            self.staged_transactions.insert(transaction.hash);
        }

        // Return obtained transactions.
        // We're not validating them - this should be done
        // by the user.
        Ok(response.transactions)
    }
}
