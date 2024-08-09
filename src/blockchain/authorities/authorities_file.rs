use std::collections::HashSet;
use std::path::PathBuf;

use hyperborealib::exports::tokio;

use super::*;

/// Basic authorities list implementation.
/// 
/// This struct will manage a single text file
/// with authorities listed there in separate
/// lines.
/// 
/// This should be more than enough for
/// most of use cases.
pub struct AuthoritiesFile {
    path: PathBuf
}

impl AuthoritiesFile {
    /// Open or create authorities file.
    pub async fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path: PathBuf = path.into();

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        if !path.exists() {
            tokio::fs::write(&path, []).await?;
        }

        Ok(Self {
            path
        })
    }

    async fn update_file(&self, authorities: HashSet<PublicKey>) -> std::io::Result<()> {
        let authorities = authorities.iter()
            .map(PublicKey::to_base64)
            .fold(String::new(), |authorities, authority| {
                format!("{authorities}{authority}\n")
            });

        tokio::fs::write(&self.path, authorities).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl AuthoritiesIndex for AuthoritiesFile {
    type Error = std::io::Error;

    async fn get_authorities(&self) -> Result<HashSet<PublicKey>, Self::Error> {
        let authorities = tokio::fs::read_to_string(&self.path).await?
            .lines()
            .flat_map(PublicKey::from_base64)
            .collect::<HashSet<_>>();

        Ok(authorities)
    }

    async fn insert_authority(&self, validator: PublicKey) -> Result<bool, Self::Error> {
        let mut authorities = self.get_authorities().await?;

        // Do not do anything if authority is not added
        if !authorities.insert(validator) {
            return Ok(false);
        }

        // Otherwise update the file
        self.update_file(authorities).await?;

        Ok(true)
    }

    async fn delete_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        let mut authorities = self.get_authorities().await?;

        // Do not do anything if authority is not deleted
        if !authorities.remove(validator) {
            return Ok(false);
        }

        // Otherwise update the file
        self.update_file(authorities).await?;

        Ok(true)
    }

    async fn is_authority(&self, validator: &PublicKey) -> Result<bool, Self::Error> {
        let authorities = tokio::fs::read_to_string(&self.path).await?;
        let validator = validator.to_base64();

        Ok(authorities.contains(&validator))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[tokio::test]
    async fn index() -> std::io::Result<()> {
        use hyperborealib::crypto::asymmetric::SecretKey;

        let path = std::env::temp_dir()
            .join(".hyperchain.authorities-file-test");

        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }

        let authorities = [
            SecretKey::random(),
            SecretKey::random(),
            SecretKey::random()
        ];

        let index = AuthoritiesFile::new(path).await?;

        assert!(index.get_authorities().await?.is_empty());

        assert!(!index.delete_authority(&authorities[0].public_key()).await?);
        assert!(!index.delete_authority(&authorities[1].public_key()).await?);
        assert!(!index.delete_authority(&authorities[2].public_key()).await?);

        assert!(!index.is_authority(&authorities[0].public_key()).await?);
        assert!(!index.is_authority(&authorities[1].public_key()).await?);
        assert!(!index.is_authority(&authorities[2].public_key()).await?);

        assert!(index.insert_authority(authorities[0].public_key()).await?);
        assert!(index.insert_authority(authorities[1].public_key()).await?);

        assert_eq!(index.get_authorities().await?, HashSet::from([
            authorities[0].public_key(),
            authorities[1].public_key()
        ]));

        assert!(index.is_authority(&authorities[0].public_key()).await?);
        assert!(index.is_authority(&authorities[1].public_key()).await?);
        assert!(!index.is_authority(&authorities[2].public_key()).await?);

        assert!(index.delete_authority(&authorities[0].public_key()).await?);
        assert!(index.delete_authority(&authorities[1].public_key()).await?);
        assert!(!index.delete_authority(&authorities[2].public_key()).await?);

        assert!(index.get_authorities().await?.is_empty());

        assert!(!index.is_authority(&authorities[0].public_key()).await?);
        assert!(!index.is_authority(&authorities[1].public_key()).await?);
        assert!(!index.is_authority(&authorities[2].public_key()).await?);

        Ok(())
    }
}
