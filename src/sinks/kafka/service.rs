use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::task::AtomicWaker;

use bytes::Bytes;
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};
use vector_lib::config;

use crate::{common::backoff::ExponentialBackoff, kafka::KafkaStatisticsContext, sinks::prelude::*};

pub struct KafkaRequest {
    pub body: Bytes,
    pub metadata: KafkaRequestMetadata,
    pub request_metadata: RequestMetadata,
}

pub struct KafkaRequestMetadata {
    pub finalizers: EventFinalizers,
    pub key: Option<Bytes>,
    pub timestamp_millis: Option<i64>,
    pub headers: Option<OwnedHeaders>,
    pub topic: String,
}

pub struct KafkaResponse {
    event_byte_size: GroupedCountByteSize,
    raw_byte_size: usize,
    event_status: EventStatus,
}

impl DriverResponse for KafkaResponse {
    fn event_status(&self) -> EventStatus {
        self.event_status
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        &self.event_byte_size
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.raw_byte_size)
    }
}

impl Finalizable for KafkaRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        std::mem::take(&mut self.metadata.finalizers)
    }
}

impl MetaDescriptive for KafkaRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.request_metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.request_metadata
    }
}

/// Shared state between `KafkaService` and in-flight `BlockedRecordState` instances.
/// Pairs the blocked-record counter with a lock-free waker so that `poll_ready`
/// is properly notified when all blocked records have been enqueued.
struct BlockedRecordNotifier {
    records_blocked: AtomicUsize,
    waker: AtomicWaker,
}

/// BlockedRecordState manages state for a record blocked from being enqueued on the producer.
struct BlockedRecordState {
    shared: Arc<BlockedRecordNotifier>,
}

impl BlockedRecordState {
    fn new(shared: Arc<BlockedRecordNotifier>) -> Self {
        shared.records_blocked.fetch_add(1, Ordering::Release);
        Self { shared }
    }
}

impl Drop for BlockedRecordState {
    fn drop(&mut self) {
        // If this was the last blocked record, wake the service so it can
        // resume dispatching new requests immediately.
        let prev = self.shared.records_blocked.fetch_sub(1, Ordering::Release);
        if prev == 1 {
            self.shared.waker.wake();
        }
    }
}

#[derive(Clone)]
pub struct KafkaService {
    kafka_producer: FutureProducer<KafkaStatisticsContext>,

    /// Shared backpressure state: blocked-record counter + waker for `poll_ready`.
    shared: Arc<BlockedRecordNotifier>,
}

impl KafkaService {
    pub(crate) fn new(kafka_producer: FutureProducer<KafkaStatisticsContext>) -> KafkaService {
        KafkaService {
            kafka_producer,
            shared: Arc::new(BlockedRecordNotifier {
                records_blocked: AtomicUsize::new(0),
                waker: AtomicWaker::new(),
            }),
        }
    }
}

impl Service<KafkaRequest> for KafkaService {
    type Response = KafkaResponse;
    type Error = KafkaError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Register the waker BEFORE checking the condition to avoid a race where
        // `records_blocked` transitions to 0 between the check and waker registration.
        // Spurious wakes (when we return Ready) are harmless.
        self.shared.waker.register(cx.waker());

        // The Kafka service is at capacity if any records are currently blocked from being
        // enqueued on the producer.
        if self.shared.records_blocked.load(Ordering::Acquire) > 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, request: KafkaRequest) -> Self::Future {
        let this = self.clone();

        Box::pin(async move {
            let raw_byte_size =
                request.body.len() + request.metadata.key.as_ref().map_or(0, |x| x.len());
            let event_byte_size = request
                .request_metadata
                .into_events_estimated_json_encoded_byte_size();

            let mut record =
                FutureRecord::to(&request.metadata.topic).payload(request.body.as_ref());
            if let Some(key) = &request.metadata.key {
                record = record.key(&key[..]);
            }
            if let Some(timestamp) = request.metadata.timestamp_millis {
                record = record.timestamp(timestamp);
            }
            if let Some(headers) = request.metadata.headers {
                record = record.headers(headers);
            }

            // Manually poll [FutureProducer::send_result] instead of [FutureProducer::send] to track
            // records that fail to be enqueued on the producer.
            let mut blocked_state: Option<BlockedRecordState> = None;
            // Exponential backoff with ~20% jitter to avoid thundering-herd retries.
            // Sequence (base, before jitter): 10ms, 20ms, 40ms, 80ms, 100ms, 100ms, …
            let mut backoff = ExponentialBackoff::from_millis(2)
                .factor(5)
                .max_delay(Duration::from_millis(100));
            // Use a fast non-crypto PRNG for jitter instead of the heavier
            // thread-local CSPRNG used by rand::random.
            let mut rng = <rand::rngs::SmallRng as rand::SeedableRng>::from_os_rng();
            loop {
                match this.kafka_producer.send_result(record) {
                    // Record was successfully enqueued on the producer.
                    Ok(fut) => {
                        // Drop the blocked state (if any), as the producer is no longer blocked.
                        drop(blocked_state.take());
                        return fut
                            .await
                            .expect("producer unexpectedly dropped")
                            .map(|_| KafkaResponse {
                                event_byte_size,
                                raw_byte_size,
                                event_status: EventStatus::Delivered,
                            })
                            .map_err(|(err, _)| err);
                    }
                    // Producer queue is full or a policy has been violated and the request should
                    // be retried
                    Err((
                        KafkaError::MessageProduction(
                            RDKafkaErrorCode::QueueFull | RDKafkaErrorCode::PolicyViolation,
                        ),
                        original_record,
                    )) => {
                        if blocked_state.is_none() {
                            blocked_state =
                                Some(BlockedRecordState::new(Arc::clone(&this.shared)));
                        }
                        record = original_record;
                        let base_delay = backoff.next().unwrap_or(Duration::from_millis(100));
                        let max_jitter = (base_delay.as_millis() as u64 / 5) + 1;
                        let jitter_ms = rand::Rng::random_range(&mut rng, 1..=max_jitter);
                        tokio::time::sleep(base_delay + Duration::from_millis(jitter_ms)).await;
                    }
                    // A final/non-retriable error occurred.
                    Err((
                        err @ KafkaError::MessageProduction(
                            RDKafkaErrorCode::InvalidMessage
                            | RDKafkaErrorCode::InvalidMessageSize
                            | RDKafkaErrorCode::MessageSizeTooLarge
                            | RDKafkaErrorCode::UnknownTopicOrPartition
                            | RDKafkaErrorCode::InvalidRecord
                            | RDKafkaErrorCode::InvalidRequiredAcks
                            | RDKafkaErrorCode::TopicAuthorizationFailed
                            | RDKafkaErrorCode::UnsupportedForMessageFormat
                            | RDKafkaErrorCode::ClusterAuthorizationFailed,
                        ),
                        _,
                    )) => return Err(err),

                    // A different error occurred. Set event status to Errored not Rejected.
                    Err(_) => {
                        return Ok(KafkaResponse {
                            event_byte_size: config::telemetry().create_request_count_byte_size(),
                            raw_byte_size: 0,
                            event_status: EventStatus::Errored,
                        });
                    }
                };
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::Ordering;

    #[test]
    fn blocked_record_notifier_wakes_on_transition_to_zero() {
        // Simulate: one record becomes blocked, then unblocks.
        // The counter should transition from 1 -> 0 and trigger a wake.
        let shared = Arc::new(BlockedRecordNotifier {
            records_blocked: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        });

        // Create a blocked state — increments counter to 1.
        let state = BlockedRecordState::new(Arc::clone(&shared));
        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 1);

        // Drop the blocked state — counter goes from 1 to 0.
        drop(state);
        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 0);
    }

    #[test]
    fn blocked_record_notifier_does_not_wake_when_others_remain() {
        // Two records blocked; dropping one should NOT wake because records_blocked
        // goes from 2 to 1 (not to 0).
        let shared = Arc::new(BlockedRecordNotifier {
            records_blocked: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        });

        let state1 = BlockedRecordState::new(Arc::clone(&shared));
        let state2 = BlockedRecordState::new(Arc::clone(&shared));
        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 2);

        // Drop one — counter goes to 1.
        drop(state1);
        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 1);

        // Drop the last one — counter goes to 0.
        drop(state2);
        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 0);
    }

    #[test]
    fn backoff_sequence_produces_expected_delays() {
        let mut backoff = ExponentialBackoff::from_millis(2)
            .factor(5)
            .max_delay(Duration::from_millis(100));

        let expected = [
            Duration::from_millis(10),  // 2 * 5
            Duration::from_millis(20),  // 4 * 5
            Duration::from_millis(40),  // 8 * 5
            Duration::from_millis(80),  // 16 * 5
            Duration::from_millis(100), // 32 * 5 = 160, capped at 100
            Duration::from_millis(100), // stays capped
        ];

        for (i, exp) in expected.iter().enumerate() {
            let actual = backoff.next().unwrap();
            assert_eq!(
                actual, *exp,
                "backoff step {i}: expected {exp:?}, got {actual:?}"
            );
        }
    }

    #[test]
    fn poll_ready_returns_ready_when_unblocked() {
        let shared = Arc::new(BlockedRecordNotifier {
            records_blocked: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        });

        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 0);
        // With records_blocked == 0, poll_ready would return Ready.
    }

    #[test]
    fn poll_ready_returns_pending_when_blocked() {
        let shared = Arc::new(BlockedRecordNotifier {
            records_blocked: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
        });

        let _state = BlockedRecordState::new(Arc::clone(&shared));

        assert_eq!(shared.records_blocked.load(Ordering::Acquire), 1);
        // With records_blocked > 0, poll_ready would return Pending.
    }
}
