use std::collections::HashSet;

use serde::{Serialize, Deserialize};

use hyperborealib::http::HttpClient;
use hyperborealib::crypto::compression::CompressionLevel;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use hyperborealib::rest_api::middleware::{
    ConnectedClient,
    Error as MiddlewareError
};

use hyperborealib::rest_api::types::{
    Message,
    MessageEncoding,
    MessagesError
};

mod member;

mod remote;
mod owned;

pub use member::*;

pub mod api;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Information about the network shard.
pub struct ShardInfo {
    name: String,
    owner: ShardMember,
    members: HashSet<ShardMember>
}

impl ShardInfo {
    #[inline]
    /// Get shard's name.
    /// 
    /// It is used in hyperborea inbox's channel
    /// to distinguish different shards.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    /// Get shard's owner.
    pub fn owner(&self) -> &ShardMember {
        &self.owner
    }

    #[inline]
    /// Get shard's members.
    pub fn members(&self) -> &HashSet<ShardMember> {
        &self.members
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error(transparent)]
    Middleware(#[from] MiddlewareError),

    #[error(transparent)]
    Message(#[from] MessagesError),

    #[error(transparent)]
    Json(#[from] AsJsonError),

    #[error(transparent)]
    Serialize(#[from] serde_json::Error)
}

#[derive(Debug)]
/// Shard builder.
pub struct Shard<T: HttpClient> {
    middleware: ConnectedClient<T>,
    name: String
}

impl<T: HttpClient> Shard<T> {
    #[inline]
    /// Open shard connection builder with given client middleware.
    pub fn from_middleware(middleware: ConnectedClient<T>) -> Self {
        Self {
            middleware,
            name: String::from("default")
        }
    }

    #[inline]
    /// Change shard's name.
    pub fn with_name(mut self, name: impl ToString) -> Self {
        self.name = name.to_string();

        self
    }

    /// Try connecting to the remote shard.
    pub async fn connect(self, owner: ShardMember) -> Result<(), ShardError> {
        // Prepare connect request
        let request = api::ConnectRequest;

        let encoding = MessageEncoding::default();

        let message = Message::create(
            self.middleware.driver_ref().secret_key(),
            &owner.client_public,
            serde_json::to_vec(&request.to_json()?)?,
            encoding,
            CompressionLevel::Fast
        )?;

        // Send request to the owner
        self.middleware.send(
            &owner.server_address,
            owner.client_public.clone(),
            format!("hyperchain/{}/request/connect", &self.name),
            message
        ).await?;

        // Await connect response
        loop {
            let messages = self.middleware.poll(
                format!("hyperchain/{}/response/connect", &self.name),
                None
            ).await?.0;

            for message in messages {
                // Process messages from the owner only
                if message.sender.client.public_key == owner.client_public {
                    
                }
            }
        }
    }
}
