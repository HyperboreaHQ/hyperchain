use serde::{Serialize, Deserialize};

use hyperborealib::crypto::asymmetric::SecretKey;
use hyperborealib::crypto::compression::CompressionLevel;

use hyperborealib::rest_api::types::{
    MessageEncoding,
    MessagesError
};

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum MessageTransactionBuildError {
    #[error(transparent)]
    Message(#[from] MessagesError),

    #[error("Message's receiver not specified")]
    NoReceiver
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageTransactionBuilder {
    receiver: Option<PublicKey>,
    format: MessageEncoding,
    content: Vec<u8>,

    compress_level: CompressionLevel,
    encryption_salt: Option<Vec<u8>>
}

impl MessageTransactionBuilder {
    /// Build new `message` transaction body.
    ///
    /// ```
    /// use hyperborealib::prelude::*;
    /// use hyperchain::prelude::*;
    ///
    /// let secret = SecretKey::random();
    ///
    /// let transaction_body = MessageTransactionBuilder::new()
    ///     .with_receiver(secret.public_key())
    ///     .with_content(b"Hello, World!")
    ///     .build(&secret);
    /// ```
    pub fn new() -> Self {
        Self {
            receiver: None,
            format: MessageEncoding::default(),
            content: vec![],

            compress_level: CompressionLevel::default(),
            encryption_salt: None
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

    #[inline]
    /// Change message's compression level.
    pub fn with_compression_level(mut self, level: impl Into<CompressionLevel>) -> Self {
        self.compress_level = level.into();

        self
    }

    #[inline]
    /// Change message's encryption salt.
    pub fn with_encryption_salt(mut self, salt: impl Into<Vec<u8>>) -> Self {
        self.encryption_salt = Some(salt.into());

        self
    }

    /// Build `message` transaction by signing its content.
    pub fn build(mut self, from: &SecretKey) -> Result<TransactionBody, MessageTransactionBuildError> {
        let Some(receiver) = self.receiver.take() else {
            return Err(MessageTransactionBuildError::NoReceiver);
        };

        let secret = from.create_shared_secret(&receiver, self.encryption_salt.as_deref());

        Ok(TransactionBody::Message {
            from: from.public_key(),
            to: receiver,
            format: self.format,
            content: self.format.forward(&self.content, &secret, self.compress_level)?
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
            .build(&secret)
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

        // After building transaction's content will be encoded
        // into base64 by default (check out MessageEncoding struct / "format" value)
        assert_eq!(base64::decode(content).as_deref(), Ok(b"Hello, World!".as_slice()));
    }
}
