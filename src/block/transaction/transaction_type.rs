use serde::{Serialize, Deserialize};

use super::TransactionBody;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionType {
    Message,
    Announcement
}

impl std::fmt::Display for TransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message      => write!(f, "message"),
            Self::Announcement => write!(f, "announcement")
        }
    }
}

impl std::str::FromStr for TransactionType {
    type Err = ();

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "message"      => Ok(Self::Message),
            "announcement" => Ok(Self::Announcement),

            _ => Err(())
        }
    }
}

impl From<&TransactionBody> for TransactionType {
    fn from(value: &TransactionBody) -> Self {
        match value {
            TransactionBody::Message { .. } => TransactionType::Message,
            TransactionBody::Announcement { .. } => TransactionType::Announcement
        }
    }
}
