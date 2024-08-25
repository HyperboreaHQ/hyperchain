use std::sync::Arc;

use super::*;

#[derive(Clone)]
/// Basic blockchain implementation.
pub struct BasicBlockchain<A, B, C> {
    authorities_index: Arc<A>,
    blocks_index: Arc<B>,
    transactions_index: Arc<C>
}

impl<A, B, C> BasicBlockchain<A, B, C> {
    #[inline]
    pub fn new(
        authorities_index: Arc<A>,
        blocks_index: Arc<B>,
        transactions_index: Arc<C>
    ) -> Self {
        Self {
            authorities_index,
            blocks_index,
            transactions_index
        }
    }
}

impl<A, B, C> Blockchain for BasicBlockchain<A, B, C>
where
    A: AuthoritiesIndex + Send + Sync,
    B: BlocksIndex + Send + Sync,
    C: TransactionsIndex<BlocksIndex = B> + Send + Sync
{
    type AuthoritiesIndex = A;
    type BlocksIndex = B;
    type TransactionsIndex = C;

    #[inline]
    fn authorities_index(&self) -> Arc<Self::AuthoritiesIndex> {
        self.authorities_index.clone()
    }

    #[inline]
    fn blocks_index(&self) -> Arc<Self::BlocksIndex> {
        self.blocks_index.clone()
    }

    #[inline]
    fn transactions_index(&self) -> Arc<Self::TransactionsIndex> {
        self.transactions_index.clone()
    }

    #[inline]
    fn authorities_index_ref(&self) ->  &Self::AuthoritiesIndex {
        &self.authorities_index
    }

    #[inline]
    fn blocks_index_ref(&self) ->  &Self::BlocksIndex {
        &self.blocks_index
    }

    #[inline]
    fn transactions_index_ref(&self) ->  &Self::TransactionsIndex {
        &self.transactions_index
    }
}
