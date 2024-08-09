use serde::{Serialize, Deserialize};

use hyperborealib::crypto::asymmetric::SecretKey;

use super::*;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnnouncementTransactionBuilder {
    content: Vec<u8>
}

impl AnnouncementTransactionBuilder {
    /// Build new `announcement` transaction body.
    /// 
    /// ```
    /// use hyperborealib::crypto::asymmetric::SecretKey;
    /// use hyperchain::block::AnnouncementTransactionBuilder;
    /// 
    /// let secret = SecretKey::random();
    /// 
    /// let transaction_body = AnnouncementTransactionBuilder::new()
    ///     .with_content(b"Hello, World!")
    ///     .sign(&secret);
    /// ```
    pub fn new() -> Self {
        Self {
            content: vec![]
        }
    }

    #[inline]
    /// Change announcement's content.
    pub fn with_content(mut self, content: impl Into<Vec<u8>>) -> Self {
        self.content = content.into();

        self
    }

    /// Build `announcement` transaction by signing its content.
    pub fn sign(self, from: &SecretKey) -> TransactionBody {
        let sign = from.create_signature(&self.content);

        TransactionBody::Announcement {
            from: from.public_key(),
            content: self.content.clone(),
            sign
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn get_body() -> (TransactionBody, SecretKey) {
        let secret = SecretKey::random();

        let transaction = AnnouncementTransactionBuilder::new()
            .with_content(b"Hello, World!")
            .sign(&secret);

        (transaction, secret)
    }

    #[test]
    fn build() {
        let (transaction, secret) = get_body();

        let TransactionBody::Announcement { from, content, .. } = transaction else {
            panic!("Invalid transaction body");
        };

        assert_eq!(from, secret.public_key());
        assert_eq!(content, b"Hello, World!");
    }
}
