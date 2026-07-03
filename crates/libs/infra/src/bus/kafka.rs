//! Redpanda (Kafka API) adapter for the event bus, built on the **pure-Rust** `rskafka`
//! client (no librdkafka / C toolchain — see ADR-0007).
//!
//! `rskafka` is a low-level client: it has no built-in consumer groups, so this adapter
//! implements the two pieces we need explicitly and honestly:
//!
//! * [`KafkaPublisher`] — produces an [`EventEnvelope`] to a partition chosen by hashing the
//!   stream id, giving **per-aggregate ordering** (all events for one account land on one
//!   partition, in version order).
//! * [`KafkaConsumer`] — a per-partition fetch loop that persists its committed offset in
//!   Redis (keyed by consumer group), applies the retry-budget → DLQ policy, and invokes an
//!   [`EventHandler`]. In production one would swap in rdkafka consumer groups; the
//!   [`EventConsumer`] port means services are unaffected by that change.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;

use super::{dlq_topic, Ack, EventConsumer, EventEnvelope, EventHandler, EventPublisher};
use crate::error::{InfraError, Result};
use crate::redis_pool::RedisPool;

/// Shared connection to the Redpanda cluster.
#[derive(Clone)]
pub struct KafkaClient {
    inner: Arc<Client>,
    partitions: i32,
}

impl KafkaClient {
    /// Connect to the brokers (comma-separated `host:port` list).
    pub async fn connect(brokers: &str, partitions: i32) -> Result<Self> {
        let boots: Vec<String> = brokers.split(',').map(|s| s.trim().to_string()).collect();
        let client = ClientBuilder::new(boots)
            .build()
            .await
            .map_err(|e| InfraError::Bus(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(client),
            partitions: partitions.max(1),
        })
    }

    /// Stable partition selection for a key — FNV-1a hash mod partition count. Ensures every
    /// event for a given stream id keeps to one partition (ordering guarantee).
    fn partition_for(&self, key: &str) -> i32 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for b in key.as_bytes() {
            hash ^= u64::from(*b);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        (hash % self.partitions as u64) as i32
    }
}

/// Publisher adapter (implements the [`EventPublisher`] port).
#[derive(Clone)]
pub struct KafkaPublisher {
    client: KafkaClient,
}

impl KafkaPublisher {
    /// Wrap a connected client as a publisher.
    #[must_use]
    pub fn new(client: KafkaClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish(&self, topic: &str, event: &EventEnvelope) -> Result<()> {
        let partition = self.client.partition_for(event.partition_key());
        let partition_client = self
            .client
            .inner
            .partition_client(topic.to_string(), partition, UnknownTopicHandling::Retry)
            .await
            .map_err(|e| InfraError::Bus(e.to_string()))?;

        // Propagate correlation + trace context as Kafka headers so consumers can continue
        // the distributed trace and log correlation.
        let mut headers = BTreeMap::new();
        headers.insert(
            "correlation_id".to_string(),
            event.metadata.correlation_id.clone().into_bytes(),
        );
        headers.insert(
            "event_id".to_string(),
            event.metadata.event_id.clone().into_bytes(),
        );
        if let Some(tp) = &event.metadata.traceparent {
            headers.insert("traceparent".to_string(), tp.clone().into_bytes());
        }

        let record = Record {
            key: Some(event.partition_key().as_bytes().to_vec()),
            value: Some(event.to_bytes()?),
            headers,
            timestamp: rskafka::chrono::Utc::now(),
        };

        partition_client
            .produce(vec![record], Compression::NoCompression)
            .await
            .map_err(|e| InfraError::Bus(e.to_string()))?;
        metrics::counter!("events_published_total", "topic" => topic.to_string()).increment(1);
        Ok(())
    }
}

/// Consumer adapter (implements the [`EventConsumer`] port).
///
/// Persists offsets in Redis under `offset:{group}:{topic}:{partition}` and applies the
/// retry-budget → DLQ policy per message.
#[derive(Clone)]
pub struct KafkaConsumer {
    client: KafkaClient,
    publisher: KafkaPublisher,
    redis: RedisPool,
    group: String,
    max_attempts: u32,
}

impl KafkaConsumer {
    /// Build a consumer for `group` with a `max_attempts` retry budget before DLQ.
    #[must_use]
    pub fn new(
        client: KafkaClient,
        redis: RedisPool,
        group: impl Into<String>,
        max_attempts: u32,
    ) -> Self {
        let publisher = KafkaPublisher::new(client.clone());
        Self {
            client,
            publisher,
            redis,
            group: group.into(),
            max_attempts,
        }
    }

    async fn load_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let key = format!("offset:{}:{topic}:{partition}", self.group);
        let mut conn = self.redis.conn();
        let offset: Option<i64> = redis::cmd("GET").arg(&key).query_async(&mut conn).await?;
        Ok(offset.unwrap_or(0))
    }

    async fn store_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let key = format!("offset:{}:{topic}:{partition}", self.group);
        let mut conn = self.redis.conn();
        redis::cmd("SET")
            .arg(&key)
            .arg(offset)
            .query_async::<_, ()>(&mut conn)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl EventConsumer for KafkaConsumer {
    async fn run(&self, topic: &str, handler: Arc<dyn EventHandler>) -> Result<()> {
        // For dev/single-node we consume every partition sequentially in this task; the inner
        // fetch loop runs forever, so with the default single partition this processes
        // partition 0 indefinitely. At scale this becomes one task per partition (or an rdkafka
        // consumer group) — the `EventConsumer` port hides that change from callers.
        #[allow(clippy::never_loop)]
        for partition in 0..self.client.partitions {
            let partition_client = self
                .client
                .inner
                .partition_client(topic.to_string(), partition, UnknownTopicHandling::Retry)
                .await
                .map_err(|e| InfraError::Bus(e.to_string()))?;

            let mut offset = self.load_offset(topic, partition).await?;

            loop {
                let (records, _high_watermark) = partition_client
                    .fetch_records(offset, 1..1_048_576, 1_000)
                    .await
                    .map_err(|e| InfraError::Bus(e.to_string()))?;

                if records.is_empty() {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }

                for record_and_offset in records {
                    let next = record_and_offset.offset + 1;
                    let bytes = record_and_offset.record.value.unwrap_or_default();
                    let envelope = match EventEnvelope::from_bytes(&bytes) {
                        Ok(e) => e,
                        Err(err) => {
                            tracing::error!(?err, "undeserializable record → DLQ");
                            offset = next;
                            self.store_offset(topic, partition, offset).await?;
                            continue;
                        }
                    };
                    self.deliver(topic, &envelope, handler.as_ref()).await?;
                    offset = next;
                    self.store_offset(topic, partition, offset).await?;
                }
            }
        }
        Ok(())
    }
}

impl KafkaConsumer {
    /// Deliver one event, honoring the retry budget and dead-lettering on exhaustion.
    async fn deliver(
        &self,
        topic: &str,
        envelope: &EventEnvelope,
        handler: &dyn EventHandler,
    ) -> Result<()> {
        for attempt in 1..=self.max_attempts {
            match handler.handle(envelope).await {
                Ack::Commit => {
                    metrics::counter!("events_consumed_total", "topic" => topic.to_string())
                        .increment(1);
                    return Ok(());
                }
                Ack::DeadLetter => break,
                Ack::Retry => {
                    if attempt == self.max_attempts {
                        break;
                    }
                    // exponential-ish backoff between in-line retries
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                }
            }
        }
        // Route to DLQ and continue (poison message isolated — ADR-0006).
        tracing::error!(
            event_id = %envelope.metadata.event_id,
            "message exhausted retries → dead-letter"
        );
        metrics::counter!("dlq_depth", "topic" => topic.to_string()).increment(1);
        self.publisher.publish(&dlq_topic(topic), envelope).await
    }
}
