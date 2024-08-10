use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use crate::shard::ShardMember;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Announce member join or leave.
/// 
/// Channel: `hyperchain/<name>/v1/announce/member`.
pub enum AnnounceMember {
    Join(ShardMember),
    Leave(ShardMember)
}

impl AsJson for AnnounceMember {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        match self {
            Self::Join(member) => Ok(json!({
                "format": 1,
                "status": "join",
                "member": member.to_json()?
            })),

            Self::Leave(member) => Ok(json!({
                "format": 1,
                "status": "leave",
                "member": member.to_json()?
            }))
        }
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(status) = json.get("status").and_then(Json::as_str) else {
                    return Err(AsJsonError::FieldNotFound("status"));
                };

                let member = json.get("member")
                    .map(ShardMember::from_json)
                    .ok_or_else(|| AsJsonError::FieldNotFound("member"))??;

                match status {
                    "join"  => Ok(Self::Join(member)),
                    "leave" => Ok(Self::Leave(member)),

                    _ => Err(AsJsonError::FieldValueInvalid("status"))
                }
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::shard::tests::get_member;

    use super::*;

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let announcements = [
            AnnounceMember::Join(get_member()),
            AnnounceMember::Leave(get_member())
        ];

        for announcement in announcements {
            assert_eq!(AnnounceMember::from_json(&announcement.to_json()?)?, announcement);
        }

        Ok(())
    }
}
