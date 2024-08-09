use std::sync::Arc;

use super::*;

/// Basic blockchain implementation.
pub struct BasicBlockchain<A, B> {
    authorities_index: Arc<A>,
    blocks_index: Arc<B>
}

impl<A, B> Blockchain for BasicBlockchain<A, B>
where
    A: AuthoritiesIndex + Send + Sync,
    B: BlocksIndex + Send + Sync
{
    type AuthoritiesIndex = A;
    type BlocksIndex = B;

    #[inline]
    fn authorities_index(&self) -> Arc<Self::AuthoritiesIndex> {
        self.authorities_index.clone()
    }

    #[inline]
    fn blocks_index(&self) -> Arc<Self::BlocksIndex> {
        self.blocks_index.clone()
    }

    #[inline]
    fn authorities_index_ref(&self) ->  &Self::AuthoritiesIndex {
        &self.authorities_index
    }

    #[inline]
    fn blocks_index_ref(&self) ->  &Self::BlocksIndex {
        &self.blocks_index
    }
}
