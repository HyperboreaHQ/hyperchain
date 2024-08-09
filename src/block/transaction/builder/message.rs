use serde::{Serialize, Deserialize};

use hyperborealib::rest_api::types::MessageEncoding;
use hyperborealib::crypto::asymmetric::SecretKey;

use super::*;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageTransactionBuilder {
    receiver: Option<PublicKey>,
    format: MessageEncoding,
    content: Vec<u8>
}

impl MessageTransactionBuilder {
    /// Build new `message` transaction body.
    /// 
    /// ```
    /// use hyperborealib::crypto::asymmetric::SecretKey;
    /// use hyperchain::block::MessageTransactionBuilder;
    /// 
    /// let secret = SecretKey::random();
    /// 
    /// let transaction_body = MessageTransactionBuilder::new()
    ///     .with_receiver(secret.public_key())
    ///     .with_content(b"Hello, World!")
    ///     .sign(&secret);
    /// ```
    pub fn new() -> Self {
        Self {
            receiver: None,
            format: MessageEncoding::default(),
            content: vec![]
        }
    }

    #[inline]
    /// Change message's receiver.
    pub fn with_receiver(mut self, receiver: impl Into<PublicKey>) -> Self {
        self.receiver = Some(receiver.into());

        self
    }

    #[inline]
    /// Change message's format.
    pub fn with_format(mut self, format: impl Into<MessageEncoding>) -> Self {
        self.format = format.into();

        self
    }

    #[inline]
    /// Change message's content.
    pub fn with_content(mut self, content: impl Into<Vec<u8>>) -> Self {
        self.content = content.into();

        self
    }

    /// Build `message` transaction by signing its content.
    pub fn sign(mut self, from: &SecretKey) -> Option<TransactionBody> {
        let receiver = self.receiver.take()?;

        let sign = from.create_signature(&self.content);

        Some(TransactionBody::Message {
            from: from.public_key(),
            to: receiver,
            format: self.format,
            content: self.content,
            sign
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn get_body() -> (TransactionBody, SecretKey) {
        let secret = SecretKey::random();

        let transaction = MessageTransactionBuilder::new()
            .with_receiver(secret.public_key())
            .with_content(b"Hello, World!")
            .sign(&secret)
            .unwrap();

        (transaction, secret)
    }

    #[test]
    fn build() {
        let (transaction, secret) = get_body();

        let TransactionBody::Message { from, to, content, .. } = transaction else {
            panic!("Invalid transaction body");
        };

        assert_eq!(from, secret.public_key());
        assert_eq!(to, secret.public_key());
        assert_eq!(content, b"Hello, World!");
    }
}
