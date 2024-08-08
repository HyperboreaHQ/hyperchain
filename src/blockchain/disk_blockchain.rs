use std::path::PathBuf;
use std::num::ParseIntError;
use std::io::SeekFrom;

use hyperborealib::exports::tokio::io::AsyncSeekExt;
use serde_json::Value as Json;

use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::rest_api::{AsJson, AsJsonError};
use hyperborealib::exports::tokio;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::io::{BufReader, Lines};

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum DiskBlockchainError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    ParseInt(#[from] ParseIntError),

    #[error(transparent)]
    CryptographyError(#[from] CryptographyError),

    #[error(transparent)]
    SerializeError(#[from] serde_json::Error),

    #[error(transparent)]
    JsonError(#[from] AsJsonError)
}

#[derive(Debug)]
pub struct DiskBlockchain {
    folder: PathBuf
}

impl DiskBlockchain {
    /// Open existing blockchain or create a new one.
    pub async fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let folder: PathBuf = path.into();

        if !folder.exists() {
            tokio::fs::create_dir_all(&folder.join("blocks")).await?;

            tokio::fs::write(folder.join("authorities"), &[]).await?;
            tokio::fs::write(folder.join("index"), &[]).await?;
        }

        Ok(Self {
            folder
        })
    }

    async fn file_iter(&self, name: &str, offset: SeekFrom) -> std::io::Result<Lines<BufReader<File>>> {
        let mut file = File::open(self.folder.join(name)).await?;

        file.seek(offset).await?;

        let reader = BufReader::new(file);

        Ok(reader.lines())
    }

    async fn file_append(&self, name: &str, line: &str) -> std::io::Result<()> {
        let path = self.folder.join(name);

        if !path.exists() {
            tokio::fs::write(&path, &[]).await?;
        }

        let mut file = File::options()
            .append(true)
            .open(path)
            .await?;

        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;

        Ok(())
    }

    async fn file_delete(&self, name: &str, line: &str) -> std::io::Result<()> {
        let original_path = self.folder.join(name);
        let truncated_path = self.folder.join(format!("{name}.truncated"));

        let mut truncated = File::create(&truncated_path).await?;

        let mut lines = self.file_iter(name, SeekFrom::Start(0)).await?;

        while let Some(file_line) = lines.next_line().await? {
            if file_line != line {
                truncated.write_all(file_line.as_bytes()).await?;
                truncated.write_all(b"\n").await?;
            }
        }

        truncated.flush().await?;

        drop(truncated);
        drop(lines);

        tokio::fs::rename(truncated_path, original_path).await?;

        Ok(())
    }

    async fn block_read(&self, hash: u64) -> Result<Option<Block>, DiskBlockchainError> {
        let path = self.folder.join("blocks")
            .join(format!("{:x}.json", hash));

        if !path.exists() {
            return Ok(None);
        }

        let block = tokio::fs::read(path).await?;
        let block = serde_json::from_slice::<Json>(&block)?;

        Ok(Some(Block::from_json(&block)?))
    }

    async fn block_write(&self, block: Block) -> Result<(), DiskBlockchainError> {
        let path = self.folder.join("blocks")
            .join(format!("{:x}.json", block.hash()));

        let block = serde_json::to_string_pretty(&block.to_json()?)?;

        tokio::fs::write(path, block).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Blockchain for DiskBlockchain {
    type Error = DiskBlockchainError;

    async fn get_authorities(&self) -> Result<Vec<PublicKey>, Self::Error> {
        let Ok(mut list) = self.file_iter("authorities", SeekFrom::Start(0)).await else {
            return Ok(vec![]);
        };

        let mut authorities = Vec::new();

        while let Some(line) = list.next_line().await? {
            let authority = PublicKey::from_base64(line)?;

            authorities.push(authority);
        }

        Ok(authorities)
    }

    async fn is_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        let Ok(mut list) = self.file_iter("authorities", SeekFrom::Start(0)).await else {
            return Ok(false);
        };

        let validator = validator.to_base64();

        while let Some(line) = list.next_line().await? {
            if line == validator {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn add_authority(&self, validator: PublicKey) -> Result<bool, Self::Error> {
        self.file_append("authorities", &validator.to_base64()).await?;

        Ok(true)
    }

    async fn delete_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        self.file_delete("authorities", &validator.to_base64()).await?;

        Ok(true)
    }

    async fn get_root(&self) -> Result<Option<Block>, Self::Error> {
        let Ok(mut list) = self.file_iter("index", SeekFrom::Start(0)).await else {
            return Ok(None);
        };

        let root = list.next_line().await?
            .map(|line| u64::from_str_radix(&line, 16))
            .transpose()?;

        match root {
            Some(root) => {
                let block = self.block_read(root).await?;

                Ok(block)
            }

            None => Ok(None)
        }
    }

    async fn get_tail(&self) -> Result<Option<Block>, Self::Error> {
        let Ok(mut list) = self.file_iter("index", SeekFrom::End(-17)).await else {
            return Ok(None);
        };

        let mut hash = None;

        while let Some(line) = list.next_line().await? {
            hash = Some(u64::from_str_radix(&line, 16)?);
        }

        match hash {
            Some(hash) => self.block_read(hash).await,
            None => Ok(None)
        }
    }

    async fn get_block(&self, hash: u64) -> Result<Option<Block>, Self::Error> {
        self.block_read(hash).await
    }

    async fn get_next_block(&self, hash: u64) -> Result<Option<Block>, Self::Error> {
        let Ok(mut list) = self.file_iter("index", SeekFrom::End(-17)).await else {
            return Ok(None);
        };

        let mut found = false;

        while let Some(line) = list.next_line().await? {
            let curr_hash = u64::from_str_radix(&line, 16)?;

            if found {
                let block = self.block_read(curr_hash).await?;

                return Ok(block);
            }

            else if curr_hash == hash {
                found = true;
            }
        }

        Ok(None)
    }

    async fn push_block(&self, block: Block) -> Result<(), Self::Error> {
        self.file_append("index", &format!("{:x}", block.hash())).await?;
        self.block_write(block).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hyperborealib::crypto::asymmetric::SecretKey;

    use super::*;

    #[tokio::test]
    async fn authorities() -> Result<(), DiskBlockchainError> {
        let path = std::env::temp_dir().join(".hyperchain.disk-blockchain-test.authorities");

        let authorities = [
            SecretKey::random(),
            SecretKey::random(),
            SecretKey::random()
        ];

        let blockchain = DiskBlockchain::open(path).await?;

        blockchain.add_authority(authorities[0].public_key()).await?;
        blockchain.add_authority(authorities[1].public_key()).await?;

        assert_eq!(blockchain.get_authorities().await?, &[
            authorities[0].public_key(),
            authorities[1].public_key()
        ]);

        blockchain.delete_authority(&authorities[0].public_key()).await?;

        assert!(!blockchain.is_authority(&authorities[0].public_key()).await?);
        assert!(blockchain.is_authority(&authorities[1].public_key()).await?);

        blockchain.add_authority(authorities[2].public_key()).await?;

        assert_eq!(blockchain.get_authorities().await?, &[
            authorities[1].public_key(),
            authorities[2].public_key()
        ]);

        Ok(())
    }

    #[tokio::test]
    async fn blocks() -> Result<(), DiskBlockchainError> {
        use crate::block::BlockBuilder;

        let path = std::env::temp_dir().join(".hyperchain.disk-blockchain-test.blocks");
        let validator = SecretKey::random();

        let blockchain = DiskBlockchain::open(path).await?;

        let block_a = BlockBuilder::build_root(b"Block A", &validator);
        let block_b = BlockBuilder::build_chained(block_a.hash(), b"Block B", &validator);
        let block_c = BlockBuilder::build_chained(block_b.hash(), b"Block C", &validator);

        blockchain.push_block(block_a.clone()).await?;
        blockchain.push_block(block_b.clone()).await?;
        blockchain.push_block(block_c.clone()).await?;

        assert_eq!(blockchain.get_root().await?, Some(block_a.clone()));
        assert_eq!(blockchain.get_tail().await?, Some(block_c.clone()));

        assert_eq!(blockchain.get_block(block_b.hash()).await?, Some(block_b.clone()));
        assert_eq!(blockchain.get_block(0).await?, None);

        assert_eq!(blockchain.get_next_block(block_a.hash()).await?, Some(block_b.clone()));
        assert_eq!(blockchain.get_next_block(block_b.hash()).await?, Some(block_c));
        assert_eq!(blockchain.get_next_block(0).await?, None);

        assert_eq!(blockchain.validate(None).await?, BlockchainValidationResult::Valid);
        assert_eq!(blockchain.validate(Some(0)).await?, BlockchainValidationResult::UnknownBlockHash(0));

        Ok(())
    }
}
