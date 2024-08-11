use std::collections::HashSet;

use serde_json::Value as Json;

use hyperborealib::crypto::prelude::*;
use hyperborealib::http::HttpClient;

use hyperborealib::exports::tokio;

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

use crate::block::BlockValidationError;

mod member;
mod info;
mod owned;
mod remote;

pub use member::*;
pub use info::*;
pub use owned::*;
pub use remote::*;

pub mod api;

#[derive(Debug, thiserror::Error)]
pub enum ShardError {
    #[error(transparent)]
    Middleware(#[from] MiddlewareError),

    #[error(transparent)]
    Message(#[from] MessagesError),

    #[error(transparent)]
    Json(#[from] AsJsonError),

    #[error(transparent)]
    Serialize(#[from] serde_json::Error),

    #[error(transparent)]
    Validation(#[from] BlockValidationError),

    #[error("Invalid block obtained")]
    InvalidBlock
}

pub(crate) async fn send<T: AsJson, F: HttpClient>(
    middleware: &ConnectedClient<F>,
    member: &ShardMember,
    channel: impl std::fmt::Display,
    message: T
) -> Result<(), ShardError> {
    // Prepare message
    let encoding = MessageEncoding::new(
        Encoding::Base64,
        Encryption::None,
        Compression::Brotli
    );

    let message = Message::create(
        middleware.driver_ref().secret_key(),
        &member.client_public,
        serde_json::to_vec(&message.to_json()?)?,
        encoding,
        CompressionLevel::Balanced
    )?;

    // Send message to the member
    middleware.send(
        &member.server_address,
        member.client_public.clone(),
        channel,
        message
    ).await?;

    Ok(())
}

pub(crate) async fn poll<T: AsJson, F: HttpClient>(
    middleware: &ConnectedClient<F>,
    channel: impl AsRef<str>
) -> Result<T, ShardError> {
    let mut delay = 0;

    // Await message
    loop {
        let messages = middleware.poll(channel.as_ref(), Some(1))
            .await?.0;

        // If message polled
        if let Some(message) = messages.first() {
            // Read the message
            let message = message.message.read(
                middleware.driver_ref().secret_key(),
                &message.sender.client.public_key,
            )?;

            // Deserialize JSON
            let message = serde_json::from_slice::<Json>(&message)?;

            // Deserialize the response
            return Ok(T::from_json(&message)?);
        }

        // Increase timeout by 1 second
        delay += 1;

        // Wait some time before polling message again
        tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
    }
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
    pub async fn connect(self, owner: ShardMember) -> Result<Option<RemoteShard<T>>, ShardError> {
        send(
            &self.middleware,
            &owner,
            format!("hyperchain/{}/v1/request/connect", &self.name),
            api::ConnectRequest
        ).await?;

        let response = poll(
            &self.middleware,
            format!("hyperchain/{}/v1/response/connect", &self.name)
        ).await?;

        match response {
            api::ConnectResponse::Connected {
                members,
                root_block,
                tail_block,
                transactions
            } => {
                Ok(Some(RemoteShard {
                    middleware: self.middleware,

                    info: ShardInfo {
                        name: self.name,
                        owner,
                        members,
                    },

                    root_block,
                    tail_block,

                    staged_transactions: transactions
                        .iter()
                        .map(|transaction| transaction.get_hash())
                        .collect(),

                    transactions_handler: None,
                    block_handler: None,

                    transactions_pool: transactions,
                    blocks_pool: HashSet::new()
                }))
            }

            api::ConnectResponse::Aborted => Ok(None)
        }
    }
}
