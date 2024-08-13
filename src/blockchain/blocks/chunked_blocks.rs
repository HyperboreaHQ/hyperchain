use std::path::PathBuf;
use std::collections::HashSet;

use serde_json::Value as Json;

use hyperborealib::exports::tokio;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum ChunkedBlocksIndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] AsJsonError),

    #[error(transparent)]
    Serialize(#[from] serde_json::Error)
}

/// Basic blocks index implementation.
///
/// This struct will squash several blocks
/// in a single chunk file and store in
/// in the given folder.
///
/// This should be enough for small scale applications.
pub struct ChunkedBlocksIndex {
    folder: PathBuf,
    chunk_size: u64
}

impl ChunkedBlocksIndex {
    /// Create or open blocks index
    /// stored in a given folder.
    ///
    /// Chunk size specifies amount of blocks
    /// that needs to be squashed into a single file.
    pub async fn open(folder: impl Into<PathBuf>, chunk_size: u64) -> Result<Self, ChunkedBlocksIndexError> {
        let folder: PathBuf = folder.into();

        if !folder.exists() {
            tokio::fs::create_dir_all(&folder).await?;
        }

        Ok(Self {
            folder,
            chunk_size
        })
    }
}

#[async_trait::async_trait]
impl BlocksIndex for ChunkedBlocksIndex {
    type Error = ChunkedBlocksIndexError;

    async fn get_block(&self, number: u64) -> Result<Option<Block>, Self::Error> {
        let chunk_number = number / self.chunk_size;

        let chunk_path = self.folder.join(format!("chunk-{chunk_number}.json"));

        // Block doesn't exist if the chunk doesn't exist
        if !chunk_path.exists() {
            return Ok(None);
        }

        // Read chunk where the block should be stored
        let chunk = tokio::fs::read(&chunk_path).await?;
        let chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

        // Search for the block
        let block = chunk.iter()
            .flat_map(Block::from_json)
            .find(|block| block.number == number);

        Ok(block)
    }

    async fn push_block(&self, block: Block) -> Result<bool, Self::Error> {
        let chunk_number = block.number / self.chunk_size;

        let chunk_path = self.folder.join(format!("chunk-{chunk_number}.json"));

        // Create new chunk file if one doesn't exist already
        if !chunk_path.exists() {
            tokio::fs::write(&chunk_path, serde_json::to_string_pretty(&[
                block.to_json()?
            ])?).await?;

            return Ok(true);
        }

        // Otherwise update existing chunk file
        let chunk = tokio::fs::read(&chunk_path).await?;
        let mut chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

        // Do not update the file if block wasn't added
        if !chunk.insert(block.to_json()?) {
            return Ok(false);
        }

        tokio::fs::write(&chunk_path, serde_json::to_string_pretty(&chunk)?).await?;

        Ok(true)
    }

    async fn get_tail_block(&self) -> Result<Option<Block>, Self::Error> {
        // Read all the chunks
        let mut entries = tokio::fs::read_dir(&self.folder).await?;

        // Search for the latest chunk
        let mut last_chunk = None;

        while let Some(entry) = entries.next_entry().await? {
            if entry.path().is_file() && Some(entry.file_name()) > last_chunk {
                last_chunk = Some(entry.file_name());
            }
        }

        // Return None if no chunks found
        let Some(last_chunk) = last_chunk else {
            return Ok(None);
        };

        // Search for the latest block
        let chunk = tokio::fs::read(self.folder.join(last_chunk)).await?;

        let block = serde_json::from_slice::<Vec<Json>>(&chunk)?
            .iter()
            .flat_map(Block::from_json)
            .max_by(|a, b| a.number.cmp(&b.number));

        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index() -> Result<(), ChunkedBlocksIndexError> {
        use hyperborealib::crypto::asymmetric::SecretKey;

        use crate::block::prelude::*;

        let path = std::env::temp_dir()
            .join(".hyperchain.chunked-blocks-test");

        if path.exists() {
            tokio::fs::remove_dir_all(&path).await?;
        }

        // Prepare blocks
        let validator = SecretKey::random();

        let block_a = BlockBuilder::build_root(&validator);
        let block_b = BlockBuilder::chained(&block_a).sign(&validator);
        let block_c = BlockBuilder::chained(&block_b).sign(&validator);
        let block_d = BlockBuilder::chained(&block_c).sign(&validator);

        // Run the tests
        let index = ChunkedBlocksIndex::open(path, 2).await?;

        assert!(index.get_block(0).await?.is_none());
        assert!(index.get_block(1).await?.is_none());
        assert!(index.get_block(2).await?.is_none());

        assert!(index.get_root_block().await?.is_none());
        assert!(index.get_tail_block().await?.is_none());

        // Push A
        assert!(index.push_block(block_a.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert!(index.get_block(1).await?.is_none());

        assert_eq!(index.get_root_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_a.clone()));

        assert!(index.get_next_block(&block_a).await?.is_none());

        // Push B and C
        assert!(index.push_block(block_b.clone()).await?);
        assert!(index.push_block(block_c.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert_eq!(index.get_block(1).await?, Some(block_b.clone()));
        assert_eq!(index.get_block(2).await?, Some(block_c.clone()));

        assert_eq!(index.get_root_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_c.clone()));

        // Push D
        assert!(index.push_block(block_d.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert_eq!(index.get_block(1).await?, Some(block_b.clone()));
        assert_eq!(index.get_block(2).await?, Some(block_c.clone()));
        assert_eq!(index.get_block(3).await?, Some(block_d.clone()));

        assert_eq!(index.get_root_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_d.clone()));

        assert_eq!(index.get_next_block(&block_a).await?, Some(block_b.clone()));
        assert_eq!(index.get_next_block(&block_b).await?, Some(block_c.clone()));
        assert_eq!(index.get_next_block(&block_c).await?, Some(block_d.clone()));
        assert!(index.get_next_block(&block_d).await?.is_none());

        Ok(())
    }
}
