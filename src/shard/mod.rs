use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde_json::Value as Json;

use hyperborealib::crypto::prelude::*;
use hyperborealib::http::HttpClient;

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
pub mod message;
pub mod backend;

pub use member::*;
use message::*;
use backend::*;

pub mod prelude {
    pub use super::{
        ShardError,
        Shard
    };

    pub use super::member::*;
    pub use super::message::*;
    pub use super::backend::*;
}

use crate::block::BlockValidationError;
use crate::block::transaction::TransactionValidationError;

#[derive(Debug, thiserror::Error)]
pub enum ShardError<E> {
    #[error(transparent)]
    Middleware(#[from] MiddlewareError),

    #[error(transparent)]
    Message(#[from] MessagesError),

    #[error(transparent)]
    Json(#[from] AsJsonError),

    #[error(transparent)]
    Serialize(#[from] serde_json::Error),

    #[error(transparent)]
    BlockValidation(#[from] BlockValidationError),

    #[error(transparent)]
    TransactionValidation(#[from] TransactionValidationError),

    #[error("Shard backend error: {0}")]
    ShardBackend(E)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardOptions {
    /// Encoding used to transfer hyperborea messages.
    pub encoding_format: MessageEncoding,

    /// Compression level used for hyperborea messages.
    ///
    /// Default is balanced.
    pub compression_level: CompressionLevel,

    /// If true, shard will accept incoming subscriptions
    /// and re-send status updates from other subscribed members.
    ///
    /// Default is true.
    pub accept_subscriptions: bool,

    /// Maximal amount of clients which can subscribe to you.
    ///
    /// Default is 32.
    pub max_subscribers: usize,

    /// Maximal amount of time since last heartbeat message
    /// of the shard subscriber. If more time passed since last
    /// heartbeat update, the client will be removed from the
    /// subscribers list.
    ///
    /// Default is 10 minutes.
    pub max_in_heartbeat_delay: Duration,

    /// Minimal amount of time since last heartbeat message
    /// we send to other shards we're subscribed to.
    ///
    /// Default is 5 minutes.
    pub min_out_heartbeat_delay: Duration
}

impl Default for ShardOptions {
    fn default() -> Self {
        Self {
            encoding_format: MessageEncoding::new(
                Encoding::Base64,
                Encryption::None,
                Compression::Brotli
            ),
            compression_level: CompressionLevel::Balanced,
            accept_subscriptions: true,
            max_subscribers: 32,
            max_in_heartbeat_delay: Duration::from_secs(10 * 60),
            min_out_heartbeat_delay: Duration::from_secs(5 * 60)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Shard<T: HttpClient, F: ShardBackend> {
    /// Hyperborea client middleware used to send and poll messages.
    middleware: ConnectedClient<T>,

    /// Name of the shard.
    name: String,

    /// Backend of the shard.
    backend: F,

    /// List of shard members to which we are subscribed.
    subscriptions: HashMap<ShardMember, Instant>,

    /// List of shard members which are subscribed to us.
    subscribers: HashMap<ShardMember, Instant>,

    /// Shard options.
    options: ShardOptions
}

impl<T: HttpClient, F: ShardBackend> Shard<T, F> {
    #[inline]
    /// Create new shard with given connected hyperborea middleware.
    pub fn new(middleware: ConnectedClient<T>, name: impl ToString, backend: F) -> Self {
        Self {
            middleware,
            name: name.to_string(),
            backend,
            subscriptions: HashMap::new(),
            subscribers: HashMap::new(),
            options: ShardOptions::default()
        }
    }

    #[inline]
    /// Change shard options.
    pub fn with_options(&mut self, options: ShardOptions) -> &mut Self {
        self.options = options;

        self
    }

    async fn send(&self, member: &ShardMember, message: impl Into<ShardMessage>) -> Result<(), ShardError<F::Error>> {
        let message: ShardMessage = message.into();

        let message = Message::create(
            self.middleware.driver_ref().secret_key(),
            &member.client_public,
            serde_json::to_vec(&message.to_json()?)?,
            self.options.encoding_format,
            self.options.compression_level
        )?;

        // Send message to the member.
        self.middleware.send(
            &member.server_address,
            member.client_public.clone(),
            format!("hyperchain/v1/{}", &self.name),
            message
        ).await?;

        Ok(())
    }

    /// Send shard subscription message.
    pub async fn subscribe(&mut self, shard: ShardMember) -> Result<(), ShardError<F::Error>> {
        self.send(&shard, ShardMessage::Subscribe).await?;

        self.subscriptions.insert(shard, Instant::now());

        Ok(())
    }

    /// Send shard unsubscription message.
    pub async fn unsubscribe(&mut self, shard: &ShardMember) -> Result<(), ShardError<F::Error>> {
        self.send(shard, ShardMessage::Subscribe).await?;

        self.subscriptions.remove(shard);

        Ok(())
    }

    /// Send shard heartbeat message.
    pub async fn heartbeat(&mut self, shard: ShardMember) -> Result<(), ShardError<F::Error>> {
        self.send(&shard, ShardMessage::Heartbeat).await?;

        self.subscriptions.insert(shard, Instant::now());

        Ok(())
    }

    /// Poll shard updates and process them.
    pub async fn update(&mut self) -> Result<(), ShardError<F::Error>> {
        // Handle messages.
        let messages = self.middleware.poll(format!("hyperchain/v1/{}", &self.name), None).await?.0;

        for message in messages {
            let update = message.message.read(
                self.middleware.driver_ref().secret_key(),
                &message.sender.client.public_key
            )?;

            // Try to deserialize the message
            if let Ok(update) = serde_json::from_slice::<Json>(&update) {
                // Try to parse the message from json object
                if let Ok(update) = ShardMessage::from_json(&update) {
                    // TODO: handle errors nicer
                    match update {
                        ShardMessage::Subscribe => {
                            if self.options.accept_subscriptions {
                                self.subscribers.insert(ShardMember::from(message.sender), Instant::now());
                            }
                        }

                        ShardMessage::Unsubscribe => {
                            self.subscribers.remove(&ShardMember::from(message.sender));
                        }

                        ShardMessage::Heartbeat => {
                            let member = ShardMember::from(message.sender);

                            if self.subscribers.contains_key(&member) {
                                self.subscribers.insert(member, Instant::now());
                            }
                        }

                        ShardMessage::Update(update) => {
                            match update {
                                // Handle shard update message.
                                ShardUpdate::Status { root_block, tail_block, .. } => {
                                    // Handle root block.
                                    if let Some(root) = root_block {
                                        if root.validate()?.is_valid() {
                                            self.backend.handle_block(root).await
                                                .map_err(ShardError::ShardBackend)?;
                                        }
                                    }

                                    // Handle tail block.
                                    if let Some(tail) = tail_block {
                                        if tail.validate()?.is_valid() {
                                            self.backend.handle_block(tail).await
                                                .map_err(ShardError::ShardBackend)?;
                                        }
                                    }
                                }

                                // Handle blocks announcement.
                                ShardUpdate::AnnounceBlocks { mut blocks } => {
                                    // Handle blocks.
                                    let mut valid_blocks = Vec::with_capacity(blocks.len());

                                    for block in blocks.drain(..) {
                                        if block.validate()?.is_valid() {
                                            valid_blocks.push(block.clone());

                                            self.backend.handle_block(block).await
                                                .map_err(ShardError::ShardBackend)?;
                                        }
                                    }

                                    // Re-send valid blocks to subscribers.
                                    let message = ShardMessage::Update(ShardUpdate::AnnounceBlocks {
                                        blocks: valid_blocks
                                    });

                                    for (member, last_update) in self.subscribers.clone() {
                                        if last_update.elapsed() > self.options.max_in_heartbeat_delay {
                                            self.subscribers.remove(&member);
                                        }

                                        // Send the announced block to the subscriber
                                        // and remove it if sending has failed.
                                        else if let Err(_err) = self.send(&member, message.clone()).await {
                                            self.subscribers.remove(&member);
                                        }
                                    }
                                }

                                // Handle transactions announcement.
                                ShardUpdate::AnnounceTransactions { mut transactions } => {
                                    // Handle transactions.
                                    let mut valid_transactions = Vec::with_capacity(transactions.len());

                                    for transaction in transactions.drain(..) {
                                        if transaction.validate()?.is_valid() {
                                            valid_transactions.push(transaction.clone());

                                            self.backend.handle_transaction(transaction).await
                                                .map_err(ShardError::ShardBackend)?;
                                        }
                                    }

                                    // Re-send valid transactions to subscribers.
                                    let message = ShardMessage::Update(ShardUpdate::AnnounceTransactions {
                                        transactions: valid_transactions
                                    });

                                    for (member, last_update) in self.subscribers.clone() {
                                        if last_update.elapsed() > self.options.max_in_heartbeat_delay {
                                            self.subscribers.remove(&member);
                                        }

                                        // Send the announced transaction to the subscriber
                                        // and remove it if sending has failed.
                                        else if let Err(_err) = self.send(&member, message.clone()).await {
                                            self.subscribers.remove(&member);
                                        }
                                    }
                                }

                                // Handle ask blocks request.
                                ShardUpdate::AskBlocks { from_number, max_amount } => {
                                    let blocks = self.backend.get_blocks(from_number, max_amount).await
                                        .map_err(ShardError::ShardBackend)?;

                                    // Send the response and ignore if it has failed.
                                    let _ = self.send(
                                        &ShardMember::from(message.sender),
                                        ShardUpdate::AnnounceBlocks {
                                            blocks
                                        }
                                    ).await;
                                }

                                // Handle ask transactions request.
                                ShardUpdate::AskTransactions { known_transactions } => {
                                    let transactions = self.backend.get_transactions(known_transactions).await
                                        .map_err(ShardError::ShardBackend)?;

                                    // Send the response and ignore if it has failed.
                                    let _ = self.send(
                                        &ShardMember::from(message.sender),
                                        ShardUpdate::AnnounceTransactions {
                                            transactions
                                        }
                                    ).await;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Send heartbeats.
        for (member, last_update) in self.subscriptions.clone() {
            if last_update.elapsed() > self.options.min_out_heartbeat_delay {
                // Unsubscribe from the client if heartbeat has failed.
                if self.heartbeat(member.clone()).await.is_err() {
                    let _ = self.unsubscribe(&member).await;
                }
            }
        }

        Ok(())
    }
}
