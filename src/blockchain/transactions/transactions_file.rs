use std::path::PathBuf;
use std::sync::Arc;
use std::io::SeekFrom;

use hyperborealib::exports::tokio;

use tokio::fs::File;

use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    AsyncWriteExt,
    BufReader,
    BufWriter
};

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum TransactionsFileError<T> {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Failed to read block from the blocks index: {0}")]
    BlocksIndex(T)
}

/// Basic transactions index implementation.
///
/// This struct will store transactions info
/// in a separate file for fast lookups.
///
/// For large scale applications this solution
/// may not be good enough.
///
/// ## Index structure
///
/// ```text
/// [u64 last_block_entry_pos]<blocks>
/// ```
///
/// ## Blocks structure
///
/// ```text
/// [u64 prev_block_entry_pos][u64 block_number]
/// [u16 transactions_number]<transactions_hashes>
/// ```
pub struct TransactionsFile<T> {
    file: PathBuf,
    blocks_index: Arc<T>
}

impl<T> TransactionsFile<T>
where T: BlocksIndex + Send + Sync
{
    #[inline]
    pub async fn open(path: impl Into<PathBuf>, blocks_index: Arc<T>) -> std::io::Result<Self> {
        let file: PathBuf = path.into();

        if !file.exists() {
            tokio::fs::write(&file, &0u64.to_be_bytes()).await?;
        }

        Ok(Self {
            file,
            blocks_index
        })
    }

    /// Append block to the index file.
    async fn index_block(&self, block: Block) -> std::io::Result<()> {
        let file = File::options()
            .read(true)
            .write(true)
            .open(&self.file)
            .await?;

        let mut file = BufWriter::new(file);

        // Get reference to the last block.
        let last_block_pos = file.read_u64().await?;

        // Seek the end of the index file.
        let new_block_pos = file.seek(SeekFrom::End(0)).await?;

        // Get list of block transactions' hashes.
        let transactions = block.transactions()
            .iter()
            .map(|transaction| transaction.get_hash().as_bytes())
            .collect::<Vec<_>>();

        // Block buffer.
        //
        // We're saving all the data to this buffer
        // instead of writing it directly to make indexing atomic.
        //
        // Otherwise it would be really bad if some of the intermediate
        // file writes will fail, breaking its structure.
        let mut block_buffer = Vec::with_capacity(18 + transactions.len() * Hash::BYTES);

        // Write reference to the previous block.
        block_buffer.extend_from_slice(&last_block_pos.to_be_bytes());

        // Write number of the block.
        block_buffer.extend_from_slice(&block.number().to_be_bytes());

        // Write number of transactions in the block.
        block_buffer.extend_from_slice(&(transactions.len() as u16).to_be_bytes());

        // Write all the transactions.
        for transaction in transactions {
            block_buffer.extend_from_slice(&transaction);
        }

        // Write block's buffer to the file.
        file.write_all(&block_buffer).await?;

        // Update reference to the last block.
        file.seek(SeekFrom::Start(0)).await?;
        file.write_u64(new_block_pos).await?;

        file.flush().await?;

        dbg!(new_block_pos);

        Ok(())
    }

    /// Search for a block with given transaction hash.
    async fn lookup_block(&self, transaction: &Hash) -> std::io::Result<Option<u64>> {
        let mut file = BufReader::new(File::open(&self.file).await?);

        // Get reference to the last block.
        let mut block_entry_pos = file.read_u64().await?;

        while block_entry_pos > 0 {
            // Seek the entry position of the block.
            file.seek(SeekFrom::Start(block_entry_pos)).await?;

            // Read info about the block.
            block_entry_pos = file.read_u64().await?;

            let block_number = file.read_u64().await?;
            let transactions_num = file.read_u16().await?;

            // Read all the transactions stored in this block.
            for _ in 0..transactions_num {
                let mut block_transaction = [0; Hash::BYTES];

                // wtf is this warning??
                #[allow(clippy::needless_range_loop)]
                for j in 0..Hash::BYTES {
                    block_transaction[j] = file.read_u8().await?;
                }

                // If the block's transaction is what we search for
                // then return its block number.
                if block_transaction == transaction {
                    return Ok(Some(block_number));
                }
            }
        }

        Ok(None)
    }

    async fn index_if_needed(&self) -> Result<(), TransactionsFileError<T::Error>> {
        let mut file = BufReader::new(File::open(&self.file).await?);

        // Get reference to the last block.
        let last_entry_pos = file.read_u64().await?;

        let index = self.blocks_index();

        // Get the latest indexed block.
        let mut empty_index = false;

        let block = if last_entry_pos > 0 {
            // Seek to this block, skipping the prev block reference.
            file.seek(SeekFrom::Start(last_entry_pos + 8)).await?;

            // Read latest indexed block number.
            let block_number = file.read_u64().await?;

            index.get_block(block_number).await
                .map_err(TransactionsFileError::BlocksIndex)?
        } else {
            empty_index = true;

            index.get_head_block().await
                .map_err(TransactionsFileError::BlocksIndex)?
        };

        let Some(mut block) = block else {
            return Ok(());
        };

        // Index the root block if the index is empty.
        if empty_index {
            self.index_block(block.clone()).await?;
        }

        // Iterate over all the newer blocks.
        loop {
            let next_block = index.get_next_block(&block).await
                .map_err(TransactionsFileError::BlocksIndex)?;

            let Some(next_block) = next_block else {
                break;
            };

            // Index the newer block.
            self.index_block(next_block.clone()).await?;

            block = next_block;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<T> TransactionsIndex for TransactionsFile<T>
where T: BlocksIndex + Send + Sync
{
    type BlocksIndex = T;
    type Error = TransactionsFileError<T::Error>;

    fn blocks_index(&self) -> Arc<Self::BlocksIndex> {
        self.blocks_index.clone()
    }

    async fn get_transaction(&self, transaction: &Hash) -> Result<Option<(Transaction, Block)>, Self::Error> {
        self.index_if_needed().await?;

        match self.lookup_block(transaction).await? {
            Some(block_number) => {
                let index = self.blocks_index();

                let block = index.get_block(block_number).await
                    .map_err(TransactionsFileError::BlocksIndex)?;

                let Some(block) = block else {
                    return Ok(None);
                };

                for block_transaction in block.transactions() {
                    if block_transaction.get_hash() == transaction {
                        return Ok(Some((
                            block_transaction.to_owned(),
                            block
                        )));
                    }
                }

                Ok(None)
            }

            None => Ok(None)
        }
    }

    async fn has_transaction(&self, transaction: &Hash) -> Result<bool, Self::Error> {
        self.index_if_needed().await?;

        Ok(self.lookup_block(transaction).await?.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index() -> Result<(), TransactionsFileError<ChunkedBlocksIndexError>> {
        use hyperborealib::crypto::asymmetric::SecretKey;

        use crate::block::prelude::*;

        let path = std::env::temp_dir()
            .join(".hyperchain.transactions-file-test");

        if path.exists() {
            tokio::fs::remove_dir_all(&path).await?;
        }

        let validator = SecretKey::random();

        // Prepare transactions
        let transaction_a = TransactionBuilder::new()
            .with_body(TransactionBody::Raw(b"Hello, World! x1".to_vec()))
            .sign(&validator)
            .unwrap();

        let transaction_b = TransactionBuilder::new()
            .with_body(TransactionBody::Raw(b"Hello, World! x2".to_vec()))
            .sign(&validator)
            .unwrap();

        let transaction_c = TransactionBuilder::new()
            .with_body(TransactionBody::Raw(b"Hello, World! x3".to_vec()))
            .sign(&validator)
            .unwrap();

        // Prepare blocks
        let block_a = BlockBuilder::build_root(&validator);

        let block_b = BlockBuilder::chained(&block_a)
            .add_transaction(transaction_a.clone())
            .sign(&validator);

        let block_c = BlockBuilder::chained(&block_b).sign(&validator);

        let block_d = BlockBuilder::chained(&block_c)
            .add_transaction(transaction_b.clone())
            .add_transaction(transaction_c.clone())
            .sign(&validator);

        // Prepare indexes
        let blocks_index = ChunkedBlocksIndex::open(
            path.join("blocks"),
            2
        ).await.map_err(TransactionsFileError::BlocksIndex)?;

        let blocks_index = Arc::new(blocks_index);

        let transactions_index = TransactionsFile::open(
            path.join("transactions"),
            blocks_index.clone()
        ).await?;

        // Run the tests
        assert!(!transactions_index.has_transaction(&Hash::MIN).await?);
        assert!(!transactions_index.has_transaction(&Hash::MAX).await?);

        assert!(!transactions_index.has_transaction(&transaction_a.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_b.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_c.get_hash()).await?);

        assert!(transactions_index.get_transaction(&Hash::MIN).await?.is_none());
        assert!(transactions_index.get_transaction(&Hash::MAX).await?.is_none());

        assert!(transactions_index.get_transaction(&transaction_a.get_hash()).await?.is_none());
        assert!(transactions_index.get_transaction(&transaction_b.get_hash()).await?.is_none());
        assert!(transactions_index.get_transaction(&transaction_c.get_hash()).await?.is_none());

        // Push A
        blocks_index.insert_block(block_a).await.map_err(TransactionsFileError::BlocksIndex)?;

        assert!(!transactions_index.has_transaction(&transaction_a.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_b.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_c.get_hash()).await?);

        // Push B
        blocks_index.insert_block(block_b.clone()).await.map_err(TransactionsFileError::BlocksIndex)?;

        assert!(transactions_index.has_transaction(&transaction_a.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_b.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_c.get_hash()).await?);

        assert_eq!(transactions_index.get_transaction(&transaction_a.get_hash()).await?, Some((
            transaction_a.clone(),
            block_b.clone()
        )));

        // Push C
        blocks_index.insert_block(block_c).await.map_err(TransactionsFileError::BlocksIndex)?;

        assert!(transactions_index.has_transaction(&transaction_a.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_b.get_hash()).await?);
        assert!(!transactions_index.has_transaction(&transaction_c.get_hash()).await?);

        // Push D
        blocks_index.insert_block(block_d.clone()).await.map_err(TransactionsFileError::BlocksIndex)?;

        assert!(transactions_index.has_transaction(&transaction_a.get_hash()).await?);
        assert!(transactions_index.has_transaction(&transaction_b.get_hash()).await?);
        assert!(transactions_index.has_transaction(&transaction_c.get_hash()).await?);

        assert_eq!(transactions_index.get_transaction(&transaction_a.get_hash()).await?, Some((
            transaction_a.clone(),
            block_b.clone()
        )));

        assert_eq!(transactions_index.get_transaction(&transaction_b.get_hash()).await?, Some((
            transaction_b.clone(),
            block_d.clone()
        )));

        assert_eq!(transactions_index.get_transaction(&transaction_c.get_hash()).await?, Some((
            transaction_c.clone(),
            block_d.clone()
        )));

        Ok(())
    }
}
