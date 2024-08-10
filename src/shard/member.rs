use serde::{Serialize, Deserialize};
use serde_json::{json, Value as Json};

use hyperborealib::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
/// Information about the shard member.
pub struct ShardMember {
    pub client_public: PublicKey,
    pub server_public: PublicKey,
    pub server_address: String
}

impl AsJson for ShardMember {
    fn to_json(&self) -> Result<Json, AsJsonError> {
        Ok(json!({
            "format": 1,
            "client": {
                "public_key": self.client_public.to_base64()
            },
            "server": {
                "public_key": self.server_public.to_base64(),
                "address": self.server_address
            }
        }))
    }

    fn from_json(json: &Json) -> Result<Self, AsJsonError> where Self: Sized {
        let Some(format) = json.get("format").and_then(Json::as_u64) else {
            return Err(AsJsonError::FieldNotFound("format"));
        };

        match format {
            1 => {
                let Some(client) = json.get("client") else {
                    return Err(AsJsonError::FieldNotFound("client"));
                };

                let Some(server) = json.get("server") else {
                    return Err(AsJsonError::FieldNotFound("server"));
                };

                Ok(Self {
                    client_public: client.get("public_key")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldNotFound("client.public_key"))??,

                    server_public: server.get("public_key")
                        .and_then(Json::as_str)
                        .map(PublicKey::from_base64)
                        .ok_or_else(|| AsJsonError::FieldNotFound("server.public_key"))??,

                    server_address: server.get("address")
                        .and_then(Json::as_str)
                        .map(String::from)
                        .ok_or_else(|| AsJsonError::FieldNotFound("server.address"))?
                })
            }

            version => Err(AsJsonError::InvalidStandard(version))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub fn get_member() -> ShardMember {
        let client_secret = SecretKey::random();
        let server_secret = SecretKey::random();

        ShardMember {
            client_public: client_secret.public_key(),
            server_public: server_secret.public_key(),
            server_address: String::from("Hello, World!")
        }
    }

    #[test]
    fn serialize() -> Result<(), AsJsonError> {
        let member = get_member();

        assert_eq!(ShardMember::from_json(&member.to_json()?)?, member);

        Ok(())
    }
}
