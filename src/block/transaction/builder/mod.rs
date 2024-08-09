use serde::{Serialize, Deserialize};

use hyperborealib::crypto::asymmetric::SecretKey;

use hyperborealib::time::timestamp;
use hyperborealib::crypto::utils::safe_random_u64;

use super::*;

pub(crate) mod message;
pub(crate) mod announcement;

pub use message::*;
pub use announcement::*;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionBuilder {
    // Metadata
    random_seed: u64,
    created_at: u64,

    // Body
    body: Option<TransactionBody>
}

impl TransactionBuilder {
    pub fn new() -> Self {
        Self {
            random_seed: safe_random_u64(),
            created_at: timestamp(),
            body: None
        }
    }

    #[inline]
    /// Change transaction's body.
    pub fn with_body(mut self, body: TransactionBody) -> Self {
        self.body = Some(body);

        self
    }

    /// Build transaction by signing its content.
    pub fn sign(&mut self, author: &SecretKey) -> Option<Transaction> {
        let body = self.body.take()?;

        let mut transaction = Transaction {
            hash: Hash::default(),
            random_seed: self.random_seed,
            created_at: self.created_at,
            author: author.public_key(),
            body,
            sign: vec![]
        };

        let hash = transaction.calculate_hash();
        let sign = author.create_signature(hash.as_bytes());

        transaction.hash = hash;
        transaction.sign = sign;

        Some(transaction)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn get_message() -> (Transaction, SecretKey) {
        let secret = SecretKey::random();

        let transaction = TransactionBuilder::new()
            .with_body(message::tests::get_body().0)
            .sign(&secret)
            .unwrap();

        (transaction, secret)
    }

    pub fn get_announcement() -> (Transaction, SecretKey) {
        let secret = SecretKey::random();

        let transaction = TransactionBuilder::new()
            .with_body(announcement::tests::get_body().0)
            .sign(&secret)
            .unwrap();

        (transaction, secret)
    }

    #[test]
    fn validate() -> Result<(), TransactionValidationError> {
        let transactions = [
            get_message(),
            get_announcement()
        ];

        for (transaction, author) in transactions {
            assert_eq!(transaction.author, author.public_key());

            assert!(transaction.validate()?.is_valid());
        }

        Ok(())
    }
}
