use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::block::{
    Transaction,
    Hash
};

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Request staged transactions.
/// 
/// Channel: `hyperchain/<name>/v1/request/get_transactions`.
pub struct GetTransactionsRequest {
    /// List of known transactions hashes.
    pub known_transactions: Vec<Hash>
}

impl AsJson for GetTransactionsRequest {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,

            "known_transactions": self.known_transactions.iter()
                .map(Hash::to_base64)
                .collect::<Vec<_>>()
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self {
                known_transactions: json.get("known_transactions")
                    .and_then(Json::as_array)
                    .map(|transactions| {
                        transactions.iter()
                            .flat_map(Json::as_str)
                            .map(Hash::from_base64)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .ok_or_else(|| AsJsonError::FieldNotFound("known_transactions"))?
                    .map_err(|err| AsJsonError::Other(err.into()))?
            }),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Response staged transactions.
/// 
/// Channel: `hyperchain/<name>/v1/response/get_transactions`.
pub struct GetTransactionsResponse {
    pub transactions: Vec<Transaction>
}

impl AsJson for GetTransactionsResponse {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,

            "transactions": self.transactions.iter()
                .map(Transaction::to_json)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self {
                transactions: json.get("transactions")
                    .and_then(Json::as_array)
                    .map(|transactions| {
                        transactions.iter()
                            .map(Transaction::from_json)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .ok_or_else(|| AsJsonError::FieldNotFound("transactions"))??
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
    fn serialize_request() -> Result<(), AsJsonError> {
        let request = GetTransactionsRequest {
            known_transactions: vec![
                Hash::MIN,
                Hash::MAX
            ]
        };

        assert_eq!(GetTransactionsRequest::from_json(&request.to_json()?)?, request);

        Ok(())
    }

    #[test]
    fn serialize_response() -> Result<(), AsJsonError> {
        let response = GetTransactionsResponse {
            transactions: vec![
                get_message().0,
                get_announcement().0
            ]
        };

        assert_eq!(GetTransactionsResponse::from_json(&response.to_json()?)?, response);

        Ok(())
    }
}
