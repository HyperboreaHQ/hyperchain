use std::collections::{HashMap, HashSet, VecDeque};
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
    MessageInfo,
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

use crate::prelude::*;

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

    /// If true, then shard will remember latest status messages
    /// for every connected member. This info will be used to
    /// not to send some announcements, or to send them if
    /// member doesn't have some data. This reduces network
    /// use in cost of increased RAM consumption.
    ///
    /// Default is true.
    pub remember_subscribers_statuses: bool,

    /// If true, then shard will send blocks which are
    /// not known to a client when this client announces
    /// his shard status.
    ///
    /// Default is true.
    pub send_blocks_diff_on_statuses: bool,

    /// Maximal amount of blocks to send in a status diff.
    ///
    /// Default is 16.
    pub max_blocks_diff_size: usize,

    /// If true, then shard will send transactions
    /// which are not known to a client when this client
    /// announces his shard status.
    ///
    /// Default is true.
    pub send_transactions_diff_on_statuses: bool,

    /// Maximal amount of transactions to send in a status diff.
    ///
    /// Default is 64.
    pub max_transactions_diff_size: usize,

    /// Maximal amount of processed blocks hashes to remember.
    ///
    /// This is needed to prevent infinite blocks processing loops.
    ///
    /// Default value is calculated to use roughly 1 MiB of RAM (~32k).
    pub max_handled_blocks_memory: usize,

    /// Maximal amount of processed transactions hashes to remember.
    ///
    /// This is needed to prevent infinite transactions processing loops.
    ///
    /// Default value is calculated to use roughly 4 MiB of RAM (~128k).
    pub max_handled_transactions_memory: usize,

    /// Maximal amount of time since last heartbeat message
    /// of the shard subscriber. If more time passed since last
    /// heartbeat update, the client will be removed from the
    /// subscribers list.
    ///
    /// Default is 5 minutes.
    pub max_in_heartbeat_delay: Duration,

    /// Minimal amount of time since last heartbeat message
    /// we send to other shards we're subscribed to.
    ///
    /// Default is 2 minutes.
    pub min_out_heartbeat_delay: Duration,

    /// Minimal amount of time since last status update
    /// message we send to other shards.
    ///
    /// Default is 5 minutes.
    pub min_out_status_delay: Duration
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

            remember_subscribers_statuses: true,

            send_blocks_diff_on_statuses: true,
            max_blocks_diff_size: 16,

            send_transactions_diff_on_statuses: true,
            max_transactions_diff_size: 64,

            max_handled_blocks_memory: 1024 * 1024 / Hash::BYTES,
            max_handled_transactions_memory: 4 * 1024 * 1024 / Hash::BYTES,

            max_in_heartbeat_delay: Duration::from_secs(5 * 60),
            min_out_heartbeat_delay: Duration::from_secs(2 * 60),
            min_out_status_delay: Duration::from_secs(5 * 60)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardMemberStatus {
    pub head_block: Option<Block>,
    pub tail_block: Option<Block>,
    pub staged_transactions: HashSet<Hash>,
    pub last_in_heartbeat: Instant,
    pub last_out_heartbeat: Instant,
    pub last_out_status: Instant
}

impl ShardMemberStatus {
    /// Almost random large enough value to substract from time instants.
    const TIME_PAST_SUB: Duration = Duration::from_secs(60 * 60 * 24 * 120);

    /// Build new shard member status.
    pub fn new() -> Self {
        // Get old ass time instant to automatically send status and heartbeat
        // messages once we call update method next time.
        let past = Instant::now()
            .checked_sub(Self::TIME_PAST_SUB)
            .unwrap_or_else(Instant::now);

        Self {
            head_block: None,
            tail_block: None,
            staged_transactions: HashSet::new(),
            last_in_heartbeat: past,
            last_out_heartbeat: past,
            last_out_status: past
        }
    }

    /// Check if given block is known to the current client.
    pub fn is_block_known(&self, block: &Block) -> bool {
        match (&self.head_block, &self.tail_block) {
            (Some(head_block), Some(tail_block)) => {
                block.number() >= head_block.number() && block.number() <= tail_block.number()
            }

            // Compare only hashes to increase performance.
            (Some(known_block), None) |
            (None, Some(known_block)) => known_block.get_hash() == block.get_hash(),

            (None, None) => false
        }
    }

    // Check if given transaction is known to the current client.
    pub fn is_transaction_known(&self, transaction: &Transaction) -> bool {
        self.staged_transactions.contains(&transaction.get_hash())
    }
}

#[derive(Debug, Clone)]
pub struct Shard<T: HttpClient, F: ShardBackend + Send + Sync> {
    /// Hyperborea client middleware used to send and poll messages.
    middleware: ConnectedClient<T>,

    /// Name of the shard.
    name: String,

    /// Backend of the shard.
    backend: F,

    /// Queue of the messages polled from the hyperborea server.
    messages: VecDeque<MessageInfo>,

    /// List of blocks that were handled by the shards API.
    /// It is needed to prevent infinite processing loops.
    handled_blocks: HashSet<Hash>,

    /// List of transactions that were handled by the shards API.
    /// It is needed to prevent infinite processing loops.
    handled_transactions: HashSet<Hash>,

    /// List of shard members to which we are subscribed.
    subscriptions: HashMap<ShardMember, ShardMemberStatus>,

    /// List of shard members which are subscribed to us.
    subscribers: HashMap<ShardMember, ShardMemberStatus>,

    /// Shard options.
    options: ShardOptions
}

impl<T: HttpClient, F: ShardBackend + Send + Sync> Shard<T, F> {
    #[inline]
    /// Create new shard with given connected hyperborea middleware.
    pub fn new(middleware: ConnectedClient<T>, name: impl ToString, backend: F) -> Self {
        Self {
            middleware,
            name: name.to_string(),
            backend,
            messages: VecDeque::new(),
            handled_blocks: HashSet::new(),
            handled_transactions: HashSet::new(),
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

    #[inline]
    /// Get reference to the shard's backend implementation
    pub fn backend_ref(&mut self) -> &mut F {
        &mut self.backend
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

        // Remove this member from list of subcribers to prevent
        // announcement loops.
        self.subscribers.remove(&shard);

        // Insert this member to the list of our subscriptions.
        self.subscriptions.insert(shard, ShardMemberStatus::new());

        Ok(())
    }

    /// Send shard unsubscription message.
    pub async fn unsubscribe(&mut self, shard: &ShardMember) -> Result<(), ShardError<F::Error>> {
        self.send(shard, ShardMessage::Subscribe).await?;

        self.subscriptions.remove(shard);

        Ok(())
    }

    /// Send shard heartbeat message.
    pub async fn send_heartbeat(&mut self, shard: ShardMember) -> Result<(), ShardError<F::Error>> {
        self.send(&shard, ShardMessage::Heartbeat).await?;

        let status = match self.subscriptions.remove(&shard) {
            Some(mut status) => {
                status.last_out_heartbeat = Instant::now();

                status
            }

            None => ShardMemberStatus::new()
        };

        self.subscriptions.insert(shard, status);

        Ok(())
    }

    /// Send shard status update message.
    pub async fn send_status(&mut self, shard: &ShardMember) -> Result<(), ShardError<F::Error>> {
        let message = ShardUpdate::Status {
            head_block: self.backend.get_head_block().await
                .map_err(ShardError::ShardBackend)?,

            tail_block: self.backend.get_tail_block().await
                .map_err(ShardError::ShardBackend)?,

            staged_transactions: self.backend.get_staged_transactions().await
                .map_err(ShardError::ShardBackend)?,

            subscriptions: self.subscriptions.keys()
                .cloned()
                .collect(),

            subscribers: self.subscribers.keys()
                .cloned()
                .collect()
        };

        self.send(shard, message).await?;

        Ok(())
    }

    /// Announce block to the shard members.
    pub async fn announce_block(&mut self, block: Block) -> Result<(), ShardError<F::Error>> {
        // Handle new block.
        self.backend.handle_block(block.clone()).await
            .map_err(ShardError::ShardBackend)?;

        // Iterate over list of sub members.
        let members = self.subscriptions.keys().cloned()
            .chain(self.subscribers.keys().cloned())
            .collect::<Vec<_>>();

        // Prepare announcement message.
        let message = ShardUpdate::AnnounceBlocks {
            blocks: vec![block]
        };

        for member in members {
            // Remove this member from subscribers/subscriptions
            // if announcement has failed.
            if self.send(&member, message.clone()).await.is_err() {
                self.subscribers.remove(&member);
                self.subscriptions.remove(&member);
            }
        }

        Ok(())
    }

    /// Announce transaction to the shard members.
    pub async fn announce_transaction(&mut self, transaction: Transaction) -> Result<(), ShardError<F::Error>> {
        // Handle new transaction.
        self.backend.handle_transaction(transaction.clone()).await
            .map_err(ShardError::ShardBackend)?;

        // Iterate over list of sub members.
        let members = self.subscriptions.keys().cloned()
            .chain(self.subscribers.keys().cloned())
            .collect::<Vec<_>>();

        // Prepare announcement message.
        let message = ShardUpdate::AnnounceTransactions {
            transactions: vec![transaction]
        };

        for member in members {
            // Remove this member from subscribers/subscriptions
            // if announcement has failed.
            if self.send(&member, message.clone()).await.is_err() {
                self.subscribers.remove(&member);
                self.subscriptions.remove(&member);
            }
        }

        Ok(())
    }

    /// Poll shard updates and process them.
    pub async fn update(&mut self) -> Result<(), ShardError<F::Error>> {
        // Poll new messages from the hyperborea server
        // if the local queue is empty.
        if self.messages.is_empty() {
            loop {
                let (messages, remained) = self.middleware.poll(
                    format!("hyperchain/v1/{}", &self.name),
                    None
                ).await?;

                // If 0 messages were returned - we suspect
                // that this server is returning fake (or just wrong)
                // remained messages number.
                if messages.is_empty() {
                    break;
                }

                self.messages.extend(messages);

                // Stop polling messages from the server
                // if none remained.
                if remained == 0 {
                    break;
                }
            }
        }

        // Handle the first message in the queue.
        if let Some(message) = self.messages.pop_front() {
            // Decode the message.
            let update = message.message.read(
                self.middleware.driver_ref().secret_key(),
                &message.sender.client.public_key
            )?;

            // Deserialize decoded bytes.
            let update = serde_json::from_slice::<Json>(&update)?;
            let update = ShardMessage::from_json(&update)?;

            // Get info about the shard member from the message info.
            let member = ShardMember::from(message.sender);

            // Process the update message.
            match update {
                // Client wants to subscribe to our shard.
                ShardMessage::Subscribe => {
                    // Allow subscription only if:
                    let allow_subscription =
                        // 1. Incoming subscriptions are allowed.
                        self.options.accept_subscriptions &&

                        // 2. We did not exceed maximal amount of allowed subscriptions.
                        self.options.max_subscribers > self.subscribers.len() &&

                        // 3. We are not subscribed to this member ourselves.
                        !self.subscriptions.contains_key(&member);

                    if allow_subscription {
                        let mut status = self.subscribers.remove(&member)
                            .unwrap_or_else(ShardMemberStatus::new);

                        // Handle "subscribe" message as heartbeat.
                        status.last_in_heartbeat = Instant::now();

                        self.subscribers.insert(member.clone(), status);
                    }
                }

                // Client wants to unsubscribe from our shard.
                ShardMessage::Unsubscribe => {
                    self.subscribers.remove(&member);
                }

                // Client sends keep alive message.
                ShardMessage::Heartbeat => {
                    // Update last incoming heartbeat timestamp
                    // if this client is our shard's member.
                    if let Some(status) = self.subscribers.get_mut(&member) {
                        status.last_in_heartbeat = Instant::now();
                    }
                }

                // Client sends an API message.
                ShardMessage::Update(update) => {
                    // Process messages from subscribed members only.
                    if self.subscribers.contains_key(&member) || self.subscriptions.contains_key(&member) {
                        match update {
                            // Handle shard update message.
                            ShardUpdate::Status {
                                head_block,
                                tail_block,
                                staged_transactions,
                                ..
                            } => {
                                // Handle head block.
                                if let Some(head_block) = head_block.clone() {
                                    // Process it only if it's valid.
                                    if head_block.validate()?.is_valid() {
                                        self.backend.handle_block(head_block).await
                                            .map_err(ShardError::ShardBackend)?;
                                    }
                                }

                                // Handle tail block.
                                if let Some(tail_block) = tail_block.clone() {
                                    // Process it only if it's valid.
                                    if tail_block.validate()?.is_valid() {
                                        self.backend.handle_block(tail_block).await
                                            .map_err(ShardError::ShardBackend)?;
                                    }
                                }

                                // Remove duplicate staged transactions.
                                let staged_transactions = HashSet::from_iter(staged_transactions);

                                // Update the status of this member if this feature is enabled.
                                if self.options.remember_subscribers_statuses {
                                    let status = self.subscribers.get_mut(&member)
                                        .or_else(|| self.subscriptions.get_mut(&member));

                                    if let Some(status) = status {
                                        status.head_block = head_block.clone();
                                        status.tail_block = tail_block.clone();
                                        status.staged_transactions = staged_transactions.clone();
                                    }
                                }

                                // Send the client missing blocks if this feature is enabled.
                                if self.options.send_blocks_diff_on_statuses {
                                    let our_head_block = self.backend.get_head_block().await
                                        .map_err(ShardError::ShardBackend)?;

                                    let our_tail_block = self.backend.get_tail_block().await
                                        .map_err(ShardError::ShardBackend)?;

                                    let mut diff_blocks = Vec::new();

                                    match (our_head_block, our_tail_block) {
                                        (Some(mut our_head_block), Some(our_tail_block)) => {
                                            // Store our head block if remote is missing.
                                            if head_block.is_none() {
                                                diff_blocks.push(our_head_block.clone());
                                            }

                                            // Store our tail block if remote is missing.
                                            if tail_block.is_none() {
                                                diff_blocks.push(our_tail_block.clone());
                                            }

                                            // If remote client doesn't have a tail block - we will
                                            // just send all the blocks to it.
                                            let mut tail_block = match tail_block {
                                                Some(block) => block,
                                                None => our_head_block.clone()
                                            };

                                            // [our_head] <blocks> [remote_head]
                                            // ^^^^^^^^^^^^^^^^^^^ find and store these blocks
                                            while Some(&our_head_block) < head_block.as_ref() {
                                                if diff_blocks.len() >= self.options.max_blocks_diff_size {
                                                    break;
                                                }

                                                diff_blocks.push(our_head_block.clone());

                                                let our_next_block = self.backend.get_next_block(&our_head_block).await
                                                    .map_err(ShardError::ShardBackend)?;

                                                match our_next_block {
                                                    Some(block) => our_head_block = block,
                                                    None => break
                                                }
                                            }

                                            // [remote_tail] <blocks> [our_tail]
                                            //               ^^^^^^^^^^^^^^^^^^^ find and store these blocks
                                            while tail_block < our_tail_block {
                                                if diff_blocks.len() >= self.options.max_blocks_diff_size {
                                                    break;
                                                }

                                                diff_blocks.push(tail_block.clone());

                                                let next_block = self.backend.get_next_block(&tail_block).await
                                                    .map_err(ShardError::ShardBackend)?;

                                                match next_block {
                                                    Some(block) => tail_block = block,
                                                    None => break
                                                }
                                            }
                                        }

                                        (Some(block), None) |
                                        (None, Some(block)) => {
                                            if Some(&block) < head_block.as_ref() || Some(&block) > tail_block.as_ref() {
                                                diff_blocks.push(block);
                                            }
                                        }

                                        (None, None) => ()
                                    }

                                    // Send prepared diff.
                                    let _ = self.send(&member, ShardUpdate::AnnounceBlocks {
                                        blocks: diff_blocks
                                    }).await;
                                }

                                // Send the client missing transactions if this feature is enabled.
                                if self.options.send_transactions_diff_on_statuses {
                                    let mut diff_transactions = Vec::new();

                                    // Get list of our staged transactions.
                                    let our_staged_transactions = self.backend.get_staged_transactions().await
                                        .map_err(ShardError::ShardBackend)?;

                                    // Iterate over them and if it's unknown to a client - store it.
                                    for hash in our_staged_transactions {
                                        if staged_transactions.contains(&hash) {
                                            let transaction = self.backend.get_staged_transaction(&hash).await
                                                .map_err(ShardError::ShardBackend)?;

                                            if let Some(transaction) = transaction {
                                                diff_transactions.push(transaction);
                                            }
                                        }
                                    }

                                    // Send prepared diff.
                                    let _ = self.send(&member, ShardUpdate::AnnounceTransactions {
                                        transactions: diff_transactions
                                    }).await;
                                }
                            }

                            // Handle blocks announcement.
                            ShardUpdate::AnnounceBlocks { mut blocks } => {
                                let mut valid_blocks = Vec::with_capacity(blocks.len());

                                // Sort announced blocks in ascending order.
                                // This should optimize blocks indexing.
                                blocks.sort_by_key(|block| block.number());

                                // Iterate over announced blocks.
                                for block in blocks.drain(..) {
                                    // Skip already processed blocks.
                                    // Its hash might be invalid but if it's invalid - then
                                    // we don't need to process it at all.
                                    if self.handled_blocks.contains(&block.get_hash()) {
                                        continue;
                                    }

                                    // Keep only valid ones.
                                    if block.validate()?.is_valid() {
                                        // Handle valid blocks individually.
                                        self.backend.handle_block(block.clone()).await
                                            .map_err(ShardError::ShardBackend)?;

                                        // Clear handled blocks history if we've exceeded
                                        // maximal allowed size. This is done this way
                                        // to not to keep order of hashes and to keep speed high.
                                        if self.handled_blocks.len() >= self.options.max_handled_blocks_memory {
                                            self.handled_blocks.clear();
                                        }

                                        // Remember the block's hash to not to process it again later.
                                        self.handled_blocks.insert(block.get_hash());

                                        valid_blocks.push(block);
                                    }
                                }

                                // Re-send valid blocks to subscribers.
                                let members = self.subscriptions.keys().cloned()
                                    .chain(self.subscribers.keys().cloned())
                                    .filter(|subscriber| subscriber != &member)
                                    .collect::<Vec<_>>();

                                for subscriber in members {
                                    let status = self.subscriptions.get(&member)
                                        .or_else(|| self.subscribers.get(&member));

                                    if let Some(status) = status {
                                        // Prepare list of blocks that are unknown to this member.
                                        let sub_blocks = valid_blocks.iter()
                                            .filter(|block| {
                                                !status.is_block_known(block)
                                            })
                                            .cloned()
                                            .collect::<Vec<_>>();

                                        // Skip the member if they know all these blocks.
                                        if sub_blocks.is_empty() {
                                            continue;
                                        }

                                        // Send these blocks to the member.
                                        let result = self.send(&subscriber, ShardUpdate::AnnounceBlocks {
                                            blocks: sub_blocks
                                        }).await;

                                        // Remove this member from subscribers/subscriptions
                                        // if announcement has failed.
                                        if result.is_err() {
                                            self.subscribers.remove(&subscriber);
                                            self.subscriptions.remove(&subscriber);
                                        }
                                    }
                                }
                            }

                            // Handle transactions announcement.
                            ShardUpdate::AnnounceTransactions { mut transactions } => {
                                // Handle transactions.
                                let mut valid_transactions = Vec::with_capacity(transactions.len());

                                // TODO: provide some way of sorting transactions before staging them.
                                // this is important because announced transactions have their own
                                // ordering while we would probably like to re-order them using
                                // our own rules set.

                                // Iterate over announced transactions.
                                for transaction in transactions.drain(..) {
                                    // Skip already processed transactions.
                                    // Its hash might be invalid but if it's invalid - then
                                    // we don't need to process it at all.
                                    if self.handled_transactions.contains(&transaction.get_hash()) {
                                        continue;
                                    }

                                    // Keep only valid ones.
                                    if transaction.validate()?.is_valid() {
                                        // Handle valid blocks individually.
                                        self.backend.handle_transaction(transaction.clone()).await
                                            .map_err(ShardError::ShardBackend)?;

                                        // Clear handled transactions history if we've exceeded
                                        // maximal allowed size. This is done this way
                                        // to not to keep order of hashes and to keep speed high.
                                        if self.handled_transactions.len() >= self.options.max_handled_transactions_memory {
                                            self.handled_transactions.clear();
                                        }

                                        // Remember the block's hash to not to process it again later.
                                        self.handled_transactions.insert(transaction.get_hash());

                                        valid_transactions.push(transaction);
                                    }
                                }

                                // Re-send valid transactions to subscribers.
                                let members = self.subscriptions.keys().cloned()
                                    .chain(self.subscribers.keys().cloned())
                                    .filter(|subscriber| subscriber != &member)
                                    .collect::<Vec<_>>();

                                for subscriber in members {
                                    let status = self.subscriptions.get(&member)
                                        .or_else(|| self.subscribers.get(&member));

                                    if let Some(status) = status {
                                        // Prepare list of transactions that are unknown to this member.
                                        let sub_transactions = valid_transactions.iter()
                                            .filter(|transaction| {
                                                !status.is_transaction_known(transaction)
                                            })
                                            .cloned()
                                            .collect::<Vec<_>>();

                                        // Skip the member if they know all these transactions.
                                        if sub_transactions.is_empty() {
                                            continue;
                                        }

                                        // Send these transactions to the member.
                                        let result = self.send(&subscriber, ShardUpdate::AnnounceTransactions {
                                            transactions: sub_transactions
                                        }).await;

                                        // Remove this member from subscribers/subscriptions
                                        // if announcement has failed.
                                        if result.is_err() {
                                            self.subscribers.remove(&subscriber);
                                            self.subscriptions.remove(&subscriber);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Perform timer checks.
        let members = self.subscriptions.keys().cloned()
            .chain(self.subscribers.keys().cloned())
            .collect::<Vec<_>>();

        for member in members {
            let status = self.subscriptions.get(&member)
                .or_else(|| self.subscribers.get(&member))
                .cloned();

            if let Some(status) = status {
                // Send heartbeats.
                if status.last_out_heartbeat.elapsed() > self.options.min_out_heartbeat_delay {
                    // Unsubscribe from the client if heartbeat has failed.
                    if self.send_heartbeat(member.clone()).await.is_err() {
                        self.subscribers.remove(&member);
                        self.subscriptions.remove(&member);

                        continue;
                    }
                }

                // Remove clients which did not send heartbeat messages
                // for requested amount of time.
                if status.last_in_heartbeat.elapsed() > self.options.max_in_heartbeat_delay {
                    self.subscribers.remove(&member);
                    self.subscriptions.remove(&member);

                    continue;
                }

                // Send status updates.
                if status.last_out_status.elapsed() > self.options.min_out_status_delay {
                    // Remove the client if we couldn't sent them a status update.
                    if self.send_status(&member).await.is_err() {
                        self.subscribers.remove(&member);
                        self.subscriptions.remove(&member);

                        continue;
                    }
                }
            }
        }

        Ok(())
    }
}
