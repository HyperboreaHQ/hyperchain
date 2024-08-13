use std::str::FromStr;

use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::types::MessageEncoding;
use hyperborealib::crypto::asymmetric::PublicKey;
use hyperborealib::crypto::encoding::base64;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::hash::Hash;

use super::TransactionType;

#[derive(Debug, Clone, PartialEq, Eq, std::hash::Hash, Serialize, Deserialize)]
pub enum TransactionBody {
    Raw(Vec<u8>),

    Message {
        from: PublicKey,
        to: PublicKey,
        format: MessageEncoding,
        content: String
    },

    Announcement {
        from: PublicKey,
        format: MessageEncoding,
        content: String
    }
}

impl TransactionBody {
    #[inline]
    pub fn transaction_type(&self) -> TransactionType {
        TransactionType::from(self)
    }

    /// Calculate hash of the transaction body.
    ///
    /// This is a relatively heavy function and
    /// it should not be called often.
    pub fn hash(&self) -> Hash {
        let mut hasher = blake3::Hasher::new();

        match self {
            Self::Raw(bytes) => {
                hasher.update(bytes);
            }

            Self::Message { from, to, format, content } => {
                hasher.update(&from.to_bytes());
                hasher.update(&to.to_bytes());
                hasher.update(format.to_string().as_bytes());
                hasher.update(content.as_bytes());
            }

            Self::Announcement { from, format, content } => {
                hasher.update(&from.to_bytes());
                hasher.update(format.to_string().as_bytes());
                hasher.update(content.as_bytes());
            }
        }

        hasher.finalize().into()
    }
}

impl AsJson for TransactionBody {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        let transaction = match self {
            Self::Raw(bytes) => json!(base64::encode(bytes)),

            Self::Message { from, to, format, content } => {
                json!({
                    "from": from.to_base64(),
                    "to": to.to_base64(),
                    "format": format.to_string(),
                    "content": content
                })
            }

            Self::Announcement { from, format, content } => {
                json!({
                    "from": from.to_base64(),
                    "format": format.to_string(),
                    "content": content
                })
            }
        };

        Ok(json!({
            "type": self.transaction_type().to_string(),
            "body": transaction
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(transaction_type) = json.get("type").and_then(Json::as_str) else {
            return Err(AsJsonError::FieldNotFound("type"));
        };

        let Some(transaction_body) = json.get("body") else {
            return Err(AsJsonError::FieldNotFound("body"));
        };

        match TransactionType::from_str(transaction_type) {
            Ok(TransactionType::Raw) => {
                let bytes = transaction_body.as_str()
                    .map(base64::decode)
                    .ok_or_else(|| AsJsonError::FieldValueInvalid("body"))??;

                Ok(Self::Raw(bytes))
            }

            Ok(TransactionType::Message) => {
                Ok(Self::Message {
                    from: transaction_body.get("from")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.from"))??,

                    to: transaction_body.get("to")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.to"))??,

                    format: transaction_body.get("format")
                        .and_then(Json::as_str)
                        .map(MessageEncoding::from_str)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.format"))?
                        .map_err(|err| AsJsonError::Other(err.into()))?,

                    content: transaction_body.get("content")
                        .and_then(Json::as_str)
                        .map(String::from)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.content"))?
                })
            }

            Ok(TransactionType::Announcement) => {
                Ok(Self::Announcement {
                    from: transaction_body.get("from")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.from"))??,

                    format: transaction_body.get("format")
                        .and_then(Json::as_str)
                        .map(MessageEncoding::from_str)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.format"))?
                        .map_err(|err| AsJsonError::Other(err.into()))?,

                    content: transaction_body.get("content")
                        .and_then(Json::as_str)
                        .map(String::from)
                        .ok_or_else(|| AsJsonError::FieldValueInvalid("body.content"))?
                })
            }

            Err(()) => Err(AsJsonError::FieldValueInvalid("type"))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::block::transaction::builder::message::tests::get_body as get_message;
    use crate::block::transaction::builder::announcement::tests::get_body as get_announcement;

    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let transactions = [
            TransactionBody::Raw(b"Hello, World!".to_vec()),

            get_message().0,
            get_announcement().0
        ];

        for transaction in transactions {
            assert_eq!(TransactionBody::from_json(&transaction.to_json()?)?, transaction);
        }

        Ok(())
    }
}
