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

//! The loopback driver, the equivalent of `performance-harness`'s `SyntheticDriver`. It is a
//! harness fixture, not a broker: a send is delivered inline on the sending thread, once per
//! subscription, round-robin within each subscription from a per-producer cursor with a distinct
//! start. Each consumer sits behind its own lock, as the Java consumer is `synchronized`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::stats::{ConsumerSlot, lock};

/// The driver class names that select this driver. The Java class name lets one driver file run
/// against both workers.
pub const DRIVER_CLASSES: [&str; 2] = ["io.github.mihkels.omb.performance.SyntheticDriver", "synthetic"];

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase", default)]
pub struct Config {
    pub mode: String,
    pub queue_capacity: i64,
    pub threads: i64,
    pub receive_rate: f64,
    pub receive_payload_size: i64,
    pub close_timeout_millis: i64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            mode: "sync".into(),
            queue_capacity: 1024,
            threads: 1,
            receive_rate: 0.0,
            receive_payload_size: 1024,
            close_timeout_millis: 5000,
        }
    }
}

impl Config {
    /// Validates like the Java driver, and refuses the modes this driver does not implement.
    pub fn validate(&self) -> Result<(), String> {
        if !["sync", "async", "send-only"].contains(&self.mode.as_str())
            || self.queue_capacity <= 0
            || self.threads <= 0
            || !self.receive_rate.is_finite()
            || self.receive_rate < 0.0
            || self.receive_payload_size <= 0
            || self.close_timeout_millis <= 0
        {
            return Err("Invalid synthetic driver configuration".into());
        }
        if self.mode == "async" {
            return Err("worker-rs supports synthetic mode sync and send-only, not async".into());
        }
        Ok(())
    }

    pub fn delivers(&self) -> bool {
        self.mode != "send-only"
    }
}

/// Consumer flow control shared by every delivery of a run.
#[derive(Default)]
pub struct Flow {
    pub paused: AtomicBool,
    pub stopping: AtomicBool,
}

/// The consumers of every subscription of one topic, as a send sees them.
pub type Fanout = Vec<Vec<ConsumerSlot>>;

#[derive(Default)]
struct Topic {
    // Subscription names in creation order, and their members
    subscriptions: Vec<(String, Vec<ConsumerSlot>)>,
}

/// Topics, subscriptions and consumers of one run.
#[derive(Default)]
pub struct Topology {
    topics: RwLock<HashMap<String, Topic>>,
    // Every consumer, for the independent receive stream
    all: RwLock<Arc<Vec<ConsumerSlot>>>,
}

impl Topology {
    pub fn create_topic(&self, name: &str) {
        self.topics.write().unwrap_or_else(|e| e.into_inner()).entry(name.into()).or_default();
    }

    pub fn add_consumer(&self, topic: &str, subscription: &str, slot: ConsumerSlot) {
        let mut topics = self.topics.write().unwrap_or_else(|e| e.into_inner());
        let subscriptions = &mut topics.entry(topic.into()).or_default().subscriptions;
        match subscriptions.iter_mut().find(|(name, _)| name == subscription) {
            Some((_, members)) => members.push(slot.clone()),
            None => subscriptions.push((subscription.into(), vec![slot.clone()])),
        }
        let mut all = self.all.write().unwrap_or_else(|e| e.into_inner());
        let mut next = all.as_ref().clone();
        next.push(slot);
        *all = Arc::new(next);
    }

    /// The consumers a send to `topic` reaches now.
    pub fn fanout(&self, topic: &str) -> Fanout {
        let topics = self.topics.read().unwrap_or_else(|e| e.into_inner());
        topics
            .get(topic)
            .map(|t| t.subscriptions.iter().map(|(_, members)| members.clone()).collect())
            .unwrap_or_default()
    }

    fn all(&self) -> Arc<Vec<ConsumerSlot>> {
        self.all.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

pub fn wall_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Delivers one message to one consumer, waiting while consumers are paused. Returns false when
/// the run stopped while waiting.
pub fn deliver_one(slot: &ConsumerSlot, bytes: usize, publish_millis: u64, flow: &Flow) -> bool {
    while flow.paused.load(Ordering::Acquire) {
        if flow.stopping.load(Ordering::Acquire) {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
    lock(slot).record_receive(bytes, publish_millis, wall_millis());
    true
}

/// Delivers one send to each subscription, to the member picked by `sequence`.
pub fn deliver(fanout: &Fanout, sequence: u64, bytes: usize, flow: &Flow) {
    let publish_millis = wall_millis();
    for members in fanout {
        if !members.is_empty() {
            let member = &members[(sequence % members.len() as u64) as usize];
            if !deliver_one(member, bytes, publish_millis, flow) {
                return;
            }
        }
    }
}

/// Waits until `due` on the monotonic clock: sleeps while far off, then spins. Returns false as
/// soon as `abort` returns true.
pub fn wait_until(due: Instant, abort: &dyn Fn() -> bool) -> bool {
    loop {
        if abort() {
            return false;
        }
        let now = Instant::now();
        if now >= due {
            return true;
        }
        let gap = due - now;
        if gap > Duration::from_micros(50) {
            thread::sleep((gap - Duration::from_micros(20)).min(Duration::from_millis(10)));
        } else {
            std::hint::spin_loop();
        }
    }
}

/// The independent receive stream of a positive `receiveRate`: messages at that total rate,
/// shared round-robin across the run's consumers, for consumer workers in distributed runs.
pub fn start_receiver(
    rate: f64,
    payload_size: usize,
    topology: Arc<Topology>,
    flow: Arc<Flow>,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    let receiver = move || {
        let interval = 1e9 / rate;
        let start = Instant::now();
        let mut index: u64 = 0;
        loop {
            let due = start + Duration::from_nanos((index as f64 * interval) as u64);
            if !wait_until(due, &|| stop.load(Ordering::Relaxed)) {
                return;
            }
            let consumers = topology.all();
            if !consumers.is_empty() {
                let slot = &consumers[(index % consumers.len() as u64) as usize];
                if !deliver_one(slot, payload_size, wall_millis(), &flow) {
                    return;
                }
            }
            index += 1;
        }
    };
    thread::Builder::new().name("synthetic-receive".into()).spawn(receiver).expect("spawn receive thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Stats;

    #[test]
    fn refuses_async_and_invalid_configuration() {
        let config: Config = serde_saphyr::from_str("name: x\nmode: async\n").unwrap();
        assert!(config.validate().unwrap_err().contains("not async"));
        let config: Config = serde_saphyr::from_str("threads: 0\n").unwrap();
        assert!(config.validate().is_err());
        let config: Config = serde_saphyr::from_str("mode: send-only\nreceiveRate: 10\n").unwrap();
        assert!(config.validate().is_ok());
        assert!(!config.delivers());
    }

    #[test]
    fn fans_out_once_per_subscription_and_round_robin_within_one() {
        let stats = Stats::default();
        let topology = Topology::default();
        topology.create_topic("t");
        let a1 = stats.consumer_slot();
        let a2 = stats.consumer_slot();
        let b = stats.consumer_slot();
        topology.add_consumer("t", "a", a1.clone());
        topology.add_consumer("t", "a", a2.clone());
        topology.add_consumer("t", "b", b.clone());
        let fanout = topology.fanout("t");
        let flow = Flow::default();
        for sequence in 0..4 {
            deliver(&fanout, sequence, 8, &flow);
        }
        assert_eq!(lock(&a1).received, 2);
        assert_eq!(lock(&a2).received, 2);
        assert_eq!(lock(&b).received, 4);
        assert_eq!(lock(&b).bytes, 32);
        assert!(topology.fanout("missing").is_empty());
    }

    #[test]
    fn a_paused_delivery_gives_up_when_the_run_stops() {
        let stats = Stats::default();
        let slot = stats.consumer_slot();
        let flow = Flow::default();
        flow.paused.store(true, Ordering::Release);
        flow.stopping.store(true, Ordering::Release);
        assert!(!deliver_one(&slot, 1, 0, &flow));
        assert_eq!(lock(&slot).received, 0);
    }
}
