use std::path::Path;

use hyperborealib::crypto::asymmetric::PublicKey;
use rusqlite::{Connection, Error};

use super::*;

#[derive(Debug)]
pub struct SqliteBlockchain {
    connection: Connection
}

impl SqliteBlockchain {
    pub fn create(path: impl AsRef<Path>, authority: PublicKey) -> Result<Self, Error> {
        let blockchain = Self::open(path)?;

        blockchain.regenerate()?;
        blockchain.add_authority(authority)?;

        Ok(blockchain)
    }

    /// Open existing database or create a new one.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            connection: Connection::open(path.as_ref())?
        })
    }

    /// Delete all the content and create an empty
    /// database layout.
    pub fn regenerate(&self) -> Result<(), Error> {
        self.connection.execute("
            drop table if exists blocks;
            drop table if exists authorities;

            create table blocks (
                hash          BIGINT,
                prev_hash     BIGINT,
                created_at    BIGINT,
                random_seed   BIGINT,
                data          BLOB,
                validator     BLOB,
                sign          BLOB,

                primary key (hash),
                foreign key (prev_hash) references blocks (hash) on delete set NULL,
                foreign key (validator) references authorities (public_key)
            );

            create table authorities (
                public_key   BLOB,

                primary key (public_key)
            );
        ", [])?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Blockchain for SqliteBlockchain {
    type Error = rusqlite::Error;

    async fn get_authorities(&self) -> Result<Vec<PublicKey>, Self::Error> {
        let mut authorities = Vec::new();

        let query = self.connection.prepare("select (public_key) from authorities")?;

        for public_key in query.query_map([], |row| row.get::<_, Vec<u8>>(0))?.flatten() {
            authorities.push(PublicKey::from_bytes(&public_key));
        }

        authorities
    }

    async fn is_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        Ok(self.get_authorities().await?.contains(validator))
    }

    async fn add_authority(&self, validator: PublicKey) -> Result<bool, Self::Error>;

    async fn delete_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error>;

    async fn get_root(&self) -> Result<Option<Block>, Self::Error>;

    async fn get_tail(&self) -> Result<Option<Block>, Self::Error>;

    async fn get_block(&self, hash: u64) -> Result<Option<Block>, Self::Error>;

    async fn get_next_block(&self, hash: u64) -> Result<Option<Block>, Self::Error>;

    async fn set_root(&self, block: Block) -> Result<(), Self::Error>;

    async fn push_block(&self, block: Block) -> Result<(), Self::Error>;
}
