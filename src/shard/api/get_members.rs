use std::collections::HashSet;

use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::shard::ShardMember;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Request shard members.
///
/// Channel: `hyperchain/<name>/v1/request/get_members`.
pub struct GetMembersRequest;

impl AsJson for GetMembersRequest {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Response shard members.
///
/// Channel: `hyperchain/<name>/v1/response/get_members`.
pub struct GetMembersResponse {
    pub members: HashSet<ShardMember>
}

impl AsJson for GetMembersResponse {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,

            "members": self.members.iter()
                .map(ShardMember::to_json)
                .collect::<Result<Vec<_>, _>>()?,
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => Ok(Self {
                members: json.get("members")
                    .and_then(Json::as_array)
                    .map(|members| {
                        members.iter()
                            .map(ShardMember::from_json)
                            .collect::<Result<HashSet<_>, _>>()
                    })
                    .ok_or_else(|| AsJsonError::FieldNotFound("members"))??
            }),

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::shard::member::tests::get_member;

    use super::*;

    #[test]
    fn serialize_request() -> Result<(), AsJsonError> {
        let request = GetMembersRequest;

        assert_eq!(GetMembersRequest::from_json(&request.to_json()?)?, request);

        Ok(())
    }

    #[test]
    fn serialize_response() -> Result<(), AsJsonError> {
        let response = GetMembersResponse {
            members: HashSet::from([
                get_member(),
                get_member(),
                get_member()
            ])
        };

        assert_eq!(GetMembersResponse::from_json(&response.to_json()?)?, response);

        Ok(())
    }
}
