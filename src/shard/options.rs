use std::time::Duration;

use hyperborealib::crypto::prelude::*;
use hyperborealib::rest_api::types::*;

use crate::prelude::*;

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

    /// Maximal amount of clients to which we can subscribe.
    ///
    /// Default is 32.
    pub max_subscriptions: usize,

    /// If true, then shard will remember latest status messages
    /// for every connected member. This info will be used to
    /// not to send some announcements, or to send them if
    /// member doesn't have some data. This reduces network
    /// use in cost of increased RAM consumption.
    ///
    /// Default is true.
    pub remember_subscribers_statuses: bool,

    /// Send list of shard members which are subscribed to you
    /// to a client which has tried to subscribe on you
    /// but failed due to limited number of allowed subcriptions.
    ///
    /// If enabled, this will allow this client to connect to
    /// some of the members of your own shard, thus connecting
    /// it to the mesh network.
    ///
    /// Default is true.
    pub announce_members_on_failed_subscription: bool,

    /// Subscribe to clients which are announced by
    /// the shards owners to which you are subscribed.
    ///
    /// Announcements from clients on which you are not
    /// subscribed will be ignored.
    ///
    /// Default is true.
    pub subscribe_on_announced_members: bool,

    /// Use secure random numbers generator to choose
    /// announced members to which the shard should subscribe.
    ///
    /// If disabled, shard will subscribe on members
    /// in the same order as how they were announced.
    ///
    /// This option is used only when `subscribe_on_announced_members`
    /// is enabled.
    ///
    /// Default is true.
    pub randomly_choose_announced_members: bool,

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
            max_subscriptions: 32,

            remember_subscribers_statuses: true,
            announce_members_on_failed_subscription: true,
            subscribe_on_announced_members: true,
            randomly_choose_announced_members: true,

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
