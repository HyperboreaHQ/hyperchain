use serde::{Serialize, Deserialize};

use super::TransactionBody;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionType {
    Raw,
    Message,
    Announcement
}

impl std::fmt::Display for TransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Raw          => write!(f, "raw"),
            Self::Message      => write!(f, "message"),
            Self::Announcement => write!(f, "announcement")
        }
    }
}

impl std::str::FromStr for TransactionType {
    type Err = ();

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        match str {
            "raw"          => Ok(Self::Raw),
            "message"      => Ok(Self::Message),
            "announcement" => Ok(Self::Announcement),

            _ => Err(())
        }
    }
}

impl From<&TransactionBody> for TransactionType {
    fn from(value: &TransactionBody) -> Self {
        match value {
            TransactionBody::Raw { .. }          => Self::Raw,
            TransactionBody::Message { .. }      => Self::Message,
            TransactionBody::Announcement { .. } => Self::Announcement
        }
    }
}
