use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::Transaction;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Announce transaction.
/// 
/// Channel: `hyperchain/<name>/v1/announce/transaction`.
pub struct AnnounceTransaction {
    pub transaction: Transaction
}

impl AsJson for AnnounceTransaction {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "transaction": self.transaction.to_json()?
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self {
                transaction: json.get("transaction")
                    .map(Transaction::from_json)
                    .ok_or_else(|| AsJsonError::FieldNotFound("transaction"))??
            }),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::block::transaction::builder::tests::{
        get_message,
        get_announcement
    };

    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let announcements = [
            AnnounceTransaction {
                transaction: get_message().0
            },

            AnnounceTransaction {
                transaction: get_announcement().0
            }
        ];

        for announcement in announcements {
            assert_eq!(AnnounceTransaction::from_json(&announcement.to_json()?)?, announcement);
        }

        Ok(())
    }
}
