/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! The worker lifecycle behind the HTTP API, the counterpart of the Java `LocalWorker`.
//!
//! Each producer sends on its own OS thread with its own uniform schedule at the worker's rate
//! divided by the producer count. A send is due at `start + i * interval`; the thread sleeps
//! while the next send is far off and spins for the last microseconds. Publish delay is the lag
//! from the due time to the send, so a worker that falls behind charges the lag to the result,
//! as schedule-corrected latency (ADR 0009) requires.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};

use crate::stats::{self, ProducerSlot, Stats, lock};
use crate::synthetic::{self, Config, Fanout, Flow, Topology};

/// A failed worker call; the API answers 400 for a client error and 500 otherwise, as the Java
/// `WorkerHandler` does.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Client(String),
    Server(String),
}

pub type Result<T> = std::result::Result<T, Error>;

fn client<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Client(message.into()))
}

#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunPhase {
    Warmup,
    BacklogBuild,
    BacklogDrain,
    Measure,
    DeliveryDrain,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TopicsInfo {
    pub number_of_topics: i64,
    pub number_of_partitions_per_topic: i64,
}

#[derive(Deserialize, Debug)]
pub struct TopicSubscription {
    pub topic: String,
    pub subscription: String,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ConsumerAssignment {
    pub topics_subscriptions: Vec<TopicSubscription>,
    pub verify_delivery: bool,
    pub partition_telemetry: bool,
}

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProducerWorkAssignment {
    // byte[] values, which Jackson writes as base64
    pub payload_data: Option<Vec<String>>,
    pub publish_rate: f64,
    // The loopback driver ignores keys, so the key distribution is accepted and not generated
    pub key_distributor_type: Option<String>,
    pub payload_weights: Option<Vec<i64>>,
    pub verify_delivery: bool,
    pub headers: Option<serde_json::Value>,
    pub partition_telemetry: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct WorkerHealth {
    pub ready: bool,
    pub driver: &'static str,
    pub exit_code: Option<i32>,
    pub last_error: Option<String>,
    pub clock_micros: u64,
    pub runtime: &'static str,
}

/// The value of `runtime` in `/health`, which the coordinator records in `ResultMetadata`.
pub const RUNTIME: &str = "rust";

/// The rate of a run; producers notice a change by its generation.
struct Rate {
    bits: AtomicU64,
    generation: AtomicU64,
}

impl Rate {
    fn get(&self) -> f64 {
        f64::from_bits(self.bits.load(Ordering::Acquire))
    }

    fn set(&self, rate: f64) {
        self.bits.store(rate.to_bits(), Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }
}

struct Producer {
    topic: String,
    slot: ProducerSlot,
    // The producer's round-robin cursor; distinct starts keep producers on different consumers
    cursor: u64,
}

#[derive(Default)]
struct RunState {
    producers: Vec<Producer>,
    threads: Vec<JoinHandle<()>>,
    loaded: bool,
}

struct Run {
    config: Config,
    topology: Arc<Topology>,
    stats: Stats,
    flow: Arc<Flow>,
    stop: Arc<AtomicBool>,
    // Set by the DELIVERY_DRAIN phase: producers stop, consumers catch up
    sends_stopped: Arc<AtomicBool>,
    rate: Arc<Rate>,
    state: Mutex<RunState>,
}

/// Picks payload sizes by weight with a small xorshift generator; the loopback driver reads only
/// the payload length.
#[derive(Clone)]
struct PayloadSampler {
    sizes: Vec<usize>,
    // Cumulative weights; empty for a uniform pick
    cumulative: Vec<u64>,
}

impl PayloadSampler {
    fn new(payloads: &[Vec<u8>], weights: Option<&[i64]>) -> Result<Self> {
        if payloads.is_empty() {
            return client("At least one payload is required");
        }
        let sizes = payloads.iter().map(Vec::len).collect();
        let mut cumulative = Vec::new();
        if let Some(weights) = weights {
            if weights.len() != payloads.len() || weights.iter().any(|w| *w <= 0) {
                return client("payloadWeights must give one positive weight per payload");
            }
            let mut sum = 0u64;
            for weight in weights {
                sum += *weight as u64;
                cumulative.push(sum);
            }
        }
        Ok(PayloadSampler { sizes, cumulative })
    }

    fn next(&self, random: &mut u64) -> usize {
        if self.sizes.len() == 1 {
            return self.sizes[0];
        }
        let value = xorshift(random);
        let index = match self.cumulative.last() {
            None => (value % self.sizes.len() as u64) as usize,
            Some(total) => {
                let pick = value % total;
                self.cumulative.partition_point(|c| *c <= pick)
            }
        };
        self.sizes[index]
    }
}

fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn seed(salt: u64) -> u64 {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos() as u64);
    (nanos ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1
}

/// The worker: at most one run, from `initialize-driver` to `stop-all`.
#[derive(Default)]
pub struct Worker {
    run: Mutex<Option<Arc<Run>>>,
    // Serializes driver initialization and stopping, as the Java worker's stop lock does
    lifecycle: Mutex<()>,
}

impl Worker {
    fn require_run(&self) -> Result<Arc<Run>> {
        lock(&self.run).clone().ok_or_else(|| Error::Server("No driver is initialized".into()))
    }

    pub fn initialize_driver(&self, yaml: &[u8]) -> Result<()> {
        let _lifecycle = lock(&self.lifecycle);
        if lock(&self.run).is_some() {
            return Err(Error::Server("Stop the previous driver before initializing another".into()));
        }
        // Parsed into a JSON tree, so the driver class can be read before the driver's own options
        let tree: serde_json::Value =
            serde_saphyr::from_slice(yaml).map_err(|e| Error::Client(format!("Invalid driver configuration: {e}")))?;
        let class = tree.get("driverClass").and_then(serde_json::Value::as_str).unwrap_or_default().to_owned();
        if class.is_empty() {
            return client("Driver configuration must specify a driverClass");
        }
        if !synthetic::DRIVER_CLASSES.contains(&class.as_str()) {
            return client(format!(
                "Driver class {class} is not compiled into worker-rs; it supports {}",
                synthetic::DRIVER_CLASSES.join(", ")
            ));
        }
        let config: Config = serde_json::from_value(tree)
            .map_err(|e| Error::Client(format!("Invalid synthetic driver configuration: {e}")))?;
        config.validate().map_err(Error::Client)?;
        let run = Run {
            topology: Arc::default(),
            stats: Stats::default(),
            flow: Arc::default(),
            stop: Arc::default(),
            sends_stopped: Arc::default(),
            rate: Arc::new(Rate { bits: AtomicU64::new(1f64.to_bits()), generation: AtomicU64::new(0) }),
            state: Mutex::default(),
            config,
        };
        if run.config.receive_rate > 0.0 {
            let receiver = synthetic::start_receiver(
                run.config.receive_rate,
                run.config.receive_payload_size as usize,
                run.topology.clone(),
                run.flow.clone(),
                run.stop.clone(),
            );
            lock(&run.state).threads.push(receiver);
        }
        eprintln!("Initialized driver {class} with {:?}", run.config);
        *lock(&self.run) = Some(Arc::new(run));
        Ok(())
    }

    pub fn set_phase(&self, phase: RunPhase) {
        eprintln!("Run phase: {phase:?}");
        if phase == RunPhase::DeliveryDrain {
            if let Some(run) = lock(&self.run).as_ref() {
                run.sends_stopped.store(true, Ordering::Release);
            }
        }
    }

    pub fn create_topics(&self, info: &TopicsInfo) -> Result<Vec<String>> {
        let run = self.require_run()?;
        if info.number_of_topics < 0 || info.number_of_partitions_per_topic <= 0 {
            return client("numberOfTopics must not be negative and numberOfPartitionsPerTopic must be positive");
        }
        let mut random = seed(info.number_of_topics as u64);
        let topics: Vec<String> =
            (0..info.number_of_topics).map(|i| format!("synthetic-{i:07}-{}", random_suffix(&mut random))).collect();
        topics.iter().for_each(|t| run.topology.create_topic(t));
        Ok(topics)
    }

    pub fn create_producers(&self, topics: &[String]) -> Result<()> {
        let run = self.require_run()?;
        let mut state = lock(&run.state);
        if state.loaded {
            return Err(Error::Server("Producers cannot be added after the load started".into()));
        }
        for topic in topics {
            run.topology.create_topic(topic);
            let cursor = state.producers.len() as u64;
            state.producers.push(Producer { topic: topic.clone(), slot: run.stats.producer_slot(), cursor });
        }
        Ok(())
    }

    pub fn create_consumers(&self, assignment: &ConsumerAssignment) -> Result<()> {
        let run = self.require_run()?;
        if assignment.verify_delivery {
            return client("worker-rs does not support verifyDelivery yet; remove it from the workload");
        }
        if assignment.partition_telemetry {
            return client(
                "Driver synthetic does not support partition telemetry; remove 'partitionTelemetry' from the workload",
            );
        }
        for subscription in &assignment.topics_subscriptions {
            run.topology.add_consumer(&subscription.topic, &subscription.subscription, run.stats.consumer_slot());
        }
        Ok(())
    }

    /// Sends one 10-byte message per producer, as the Java readiness probe does; it counts towards
    /// the totals and is delivered like any other send.
    pub fn probe_producers(&self) -> Result<()> {
        let run = self.require_run()?;
        let mut state = lock(&run.state);
        for producer in &mut state.producers {
            if run.stop.load(Ordering::Acquire) {
                return Err(Error::Server("Worker is stopping".into()));
            }
            if run.config.delivers() {
                synthetic::deliver(&run.topology.fanout(&producer.topic), producer.cursor, 10, &run.flow);
            }
            producer.cursor += 1;
            run.stats.record_probe();
        }
        Ok(())
    }

    pub fn start_load(&self, assignment: &ProducerWorkAssignment) -> Result<()> {
        let run = self.require_run()?;
        if assignment.verify_delivery {
            return client("worker-rs does not support verifyDelivery yet; remove it from the workload");
        }
        if assignment.headers.is_some() {
            return client("Driver synthetic does not support message headers; remove 'headers' from the workload");
        }
        if assignment.partition_telemetry {
            return client(
                "Driver synthetic does not support partition telemetry; remove 'partitionTelemetry' from the workload",
            );
        }
        let rate = valid_rate(assignment.publish_rate)?;
        let payloads = assignment
            .payload_data
            .iter()
            .flatten()
            .map(|encoded| STANDARD.decode(encoded))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Client(format!("Invalid payloadData: {e}")))?;
        let sampler = PayloadSampler::new(&payloads, assignment.payload_weights.as_deref())?;
        let mut state = lock(&run.state);
        if state.loaded {
            return Err(Error::Server("The load was already started".into()));
        }
        state.loaded = true;
        run.rate.set(rate);
        eprintln!(
            "Starting load with {} producers, publish rate: {rate} msg/s, payloads: {}",
            state.producers.len(),
            payloads.len()
        );
        let count = state.producers.len();
        let mut threads = Vec::with_capacity(count);
        for (index, producer) in state.producers.iter().enumerate() {
            let send = Send {
                fanout: if run.config.delivers() { run.topology.fanout(&producer.topic) } else { Vec::new() },
                slot: producer.slot.clone(),
                cursor: producer.cursor,
                producers: count,
                sampler: sampler.clone(),
                rate: run.rate.clone(),
                flow: run.flow.clone(),
                stop: run.stop.clone(),
                sends_stopped: run.sends_stopped.clone(),
            };
            let thread = thread::Builder::new()
                .name(format!("producer-{index}"))
                .spawn(move || send.run(seed(index as u64)))
                .map_err(|e| Error::Server(format!("Cannot start producer thread: {e}")))?;
            threads.push(thread);
        }
        state.threads.extend(threads);
        Ok(())
    }

    pub fn adjust_publish_rate(&self, rate: f64) -> Result<()> {
        let run = self.require_run()?;
        if !rate.is_finite() {
            return client("publishRate must be finite");
        }
        run.rate.set(rate.max(1.0));
        Ok(())
    }

    pub fn pause_consumers(&self) -> Result<()> {
        self.require_run()?.flow.paused.store(true, Ordering::Release);
        Ok(())
    }

    pub fn resume_consumers(&self) -> Result<()> {
        self.require_run()?.flow.paused.store(false, Ordering::Release);
        Ok(())
    }

    pub fn period_stats(&self) -> Result<stats::PeriodStats> {
        Ok(self.require_run()?.stats.period_stats())
    }

    pub fn cumulative_latencies(&self) -> Result<stats::CumulativeLatencies> {
        Ok(self.require_run()?.stats.cumulative_latencies())
    }

    pub fn counters_stats(&self) -> Result<stats::CountersStats> {
        Ok(self.require_run()?.stats.counters())
    }

    pub fn reset_stats(&self) -> Result<()> {
        self.require_run()?.stats.reset_latencies();
        Ok(())
    }

    /// Stops the load and the driver and forgets the run; a no-op without one.
    pub fn stop_all(&self) -> Result<()> {
        let _lifecycle = lock(&self.lifecycle);
        let Some(run) = lock(&self.run).clone() else {
            return Ok(());
        };
        run.stop.store(true, Ordering::Release);
        run.flow.stopping.store(true, Ordering::Release);
        let threads = std::mem::take(&mut lock(&run.state).threads);
        let mut panicked = 0;
        for thread in threads {
            panicked += usize::from(thread.join().is_err());
        }
        *lock(&self.run) = None;
        eprintln!("Stopped all");
        if panicked > 0 {
            return Err(Error::Server(format!("{panicked} send threads panicked")));
        }
        Ok(())
    }

    pub fn health(&self) -> WorkerHealth {
        let running = lock(&self.run).is_some();
        WorkerHealth {
            ready: true,
            driver: if running { "RUNNING" } else { "NOT_STARTED" },
            exit_code: None,
            last_error: None,
            clock_micros: SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64),
            runtime: RUNTIME,
        }
    }
}

fn valid_rate(rate: f64) -> Result<f64> {
    if !rate.is_finite() || rate <= 0.0 {
        return client("publishRate must be finite and greater than 0");
    }
    Ok(rate)
}

const NAME_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";

// A Kafka-safe 7-character suffix, as the Java worker's topic names carry
fn random_suffix(random: &mut u64) -> String {
    (0..7).map(|_| NAME_CHARS[(xorshift(random) % NAME_CHARS.len() as u64) as usize] as char).collect()
}

/// One producer's send loop.
struct Send {
    fanout: Fanout,
    slot: ProducerSlot,
    cursor: u64,
    producers: usize,
    sampler: PayloadSampler,
    rate: Arc<Rate>,
    flow: Arc<Flow>,
    stop: Arc<AtomicBool>,
    sends_stopped: Arc<AtomicBool>,
}

impl Send {
    fn run(self, mut random: u64) {
        let origin = Instant::now();
        let nanos = |t: Instant| t.duration_since(origin).as_nanos() as u64;
        let mut cursor = self.cursor;
        let mut generation = u64::MAX;
        let mut interval = 0f64;
        let mut start = origin;
        let mut index = 0u64;
        while !self.stop.load(Ordering::Relaxed) && !self.sends_stopped.load(Ordering::Relaxed) {
            let current = self.rate.generation.load(Ordering::Acquire);
            if current != generation {
                // Re-based on a rate change: send n after it is due n new intervals later
                generation = current;
                interval = 1e9 * self.producers as f64 / self.rate.get();
                start = Instant::now();
                index = 0;
            }
            let due = start + Duration::from_nanos((index as f64 * interval) as u64);
            let reached = synthetic::wait_until(due, &|| {
                self.stop.load(Ordering::Relaxed) || self.rate.generation.load(Ordering::Relaxed) != generation
            });
            if !reached {
                continue;
            }
            index += 1;
            let bytes = self.sampler.next(&mut random);
            let sent = Instant::now();
            synthetic::deliver(&self.fanout, cursor, bytes, &self.flow);
            cursor += 1;
            let acknowledged = Instant::now();
            lock(&self.slot).record_send(bytes, nanos(due), nanos(sent), nanos(acknowledged));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYNTHETIC: &[u8] = b"name: s\ndriverClass: io.github.mihkels.omb.performance.SyntheticDriver\n";

    #[test]
    fn requires_an_initialized_driver_and_a_known_class() {
        let worker = Worker::default();
        assert_eq!(worker.counters_stats().unwrap_err(), Error::Server("No driver is initialized".into()));
        assert!(matches!(worker.initialize_driver(b"name: x\n"), Err(Error::Client(_))));
        assert!(matches!(worker.initialize_driver(b"driverClass: com.example.Kafka\n"), Err(Error::Client(_))));
        assert!(matches!(worker.initialize_driver(b"driverClass: synthetic\nmode: async\n"), Err(Error::Client(_))));
        worker.initialize_driver(SYNTHETIC).unwrap();
        assert!(matches!(worker.initialize_driver(SYNTHETIC), Err(Error::Server(_))));
        assert_eq!(worker.health().driver, "RUNNING");
        worker.stop_all().unwrap();
        assert_eq!(worker.health().driver, "NOT_STARTED");
        worker.stop_all().unwrap();
    }

    #[test]
    fn sends_at_the_requested_rate_and_delivers_to_consumers() {
        let worker = Worker::default();
        worker.initialize_driver(SYNTHETIC).unwrap();
        let topics =
            worker.create_topics(&TopicsInfo { number_of_topics: 1, number_of_partitions_per_topic: 1 }).unwrap();
        assert!(topics[0].starts_with("synthetic-0000000-"));
        worker
            .create_consumers(&ConsumerAssignment {
                topics_subscriptions: vec![TopicSubscription { topic: topics[0].clone(), subscription: "s".into() }],
                ..ConsumerAssignment::default()
            })
            .unwrap();
        worker.create_producers(&[topics[0].clone(), topics[0].clone()]).unwrap();
        worker.probe_producers().unwrap();
        assert_eq!(worker.counters_stats().unwrap().messages_received, 2);
        worker
            .start_load(&ProducerWorkAssignment {
                payload_data: Some(vec![STANDARD.encode([0u8; 100])]),
                publish_rate: 10_000.0,
                ..ProducerWorkAssignment::default()
            })
            .unwrap();
        thread::sleep(Duration::from_millis(500));
        worker.set_phase(RunPhase::DeliveryDrain);
        thread::sleep(Duration::from_millis(50));
        let counters = worker.counters_stats().unwrap();
        // 10k msg/s for about half a second, plus the two probes
        assert!((4_000..6_500).contains(&counters.messages_sent), "{counters:?}");
        assert_eq!(counters.messages_sent, counters.messages_received);
        let period = worker.period_stats().unwrap();
        assert_eq!(period.messages_sent, counters.messages_sent - 2);
        assert_eq!(period.bytes_sent, 100 * period.messages_sent);
        assert!(matches!(worker.start_load(&ProducerWorkAssignment::default()), Err(Error::Client(_))));
        worker.stop_all().unwrap();
    }

    #[test]
    fn refuses_unsupported_load_options() {
        let worker = Worker::default();
        worker.initialize_driver(SYNTHETIC).unwrap();
        let payload = Some(vec![STANDARD.encode([0u8; 10])]);
        for assignment in [
            ProducerWorkAssignment { payload_data: payload.clone(), publish_rate: 0.0, ..Default::default() },
            ProducerWorkAssignment { payload_data: None, publish_rate: 1.0, ..Default::default() },
            ProducerWorkAssignment {
                payload_data: payload.clone(),
                publish_rate: 1.0,
                verify_delivery: true,
                ..Default::default()
            },
            ProducerWorkAssignment {
                payload_data: payload.clone(),
                publish_rate: 1.0,
                payload_weights: Some(vec![1, 2]),
                ..Default::default()
            },
        ] {
            assert!(matches!(worker.start_load(&assignment), Err(Error::Client(_))), "{assignment:?}");
        }
        worker.stop_all().unwrap();
    }

    #[test]
    fn picks_payloads_by_weight() {
        let sampler = PayloadSampler::new(&[vec![0; 1], vec![0; 2]], Some(&[3, 1])).unwrap();
        let mut random = seed(7);
        let large = (0..40_000).filter(|_| sampler.next(&mut random) == 2).count();
        assert!((9_000..11_000).contains(&large), "{large}");
    }
}
