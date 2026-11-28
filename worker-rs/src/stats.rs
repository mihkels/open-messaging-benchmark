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

//! Run statistics in the shapes the Java coordinator reads (`worker/commands` in
//! `benchmark-framework`): `PeriodStats`, `CumulativeLatencies` and `CountersStats`.
//!
//! Each producer and consumer records into its own slot behind an uncontended mutex. Readers swap
//! a slot's histograms for empty spares under the lock and merge them outside it, so a snapshot
//! holds a send thread for a pointer swap only. As in the Java `LatencyMetric`, each drained value
//! goes into both a per-interval and a cumulative pending view.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hdrhistogram::Histogram;
use hdrhistogram::serialization::{Serializer, V2DeflateSerializer};
use serde::Serialize;

/// Highest publish latency tracked, as in the Java worker: 60 s in microseconds.
pub const PUBLISH_MAX_MICROS: u64 = 60_000_000;
/// Highest end-to-end latency tracked, as in the Java worker: 12 h in microseconds.
pub const E2E_MAX_MICROS: u64 = 12 * 3_600 * 1_000_000;
const SIGNIFICANT_DIGITS: u8 = 4;

/// Locks a mutex, ignoring poisoning: a panicked send thread leaves plain counters behind.
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

pub fn histogram(highest: u64) -> Histogram<u64> {
    Histogram::new_with_bounds(1, highest, SIGNIFICANT_DIGITS).expect("valid histogram bounds")
}

/// Records like the Java worker: values above the highest trackable value are clamped to it.
pub fn record(histogram: &mut Histogram<u64>, micros: u64) {
    histogram.saturating_record(micros.min(histogram.high()));
}

/// What one producer recorded. Counters only grow; histograms are swapped out by readers.
pub struct ProducerRecord {
    pub sent: u64,
    pub bytes: u64,
    pub publish: Histogram<u64>,
    pub delay: Histogram<u64>,
    pub schedule: Histogram<u64>,
}

impl ProducerRecord {
    fn new() -> Self {
        ProducerRecord {
            sent: 0,
            bytes: 0,
            publish: histogram(PUBLISH_MAX_MICROS),
            delay: histogram(PUBLISH_MAX_MICROS),
            schedule: histogram(PUBLISH_MAX_MICROS),
        }
    }

    /// Records one acknowledged send, with times in nanoseconds on one monotonic clock.
    pub fn record_send(&mut self, bytes: usize, intended: u64, sent: u64, acknowledged: u64) {
        self.sent += 1;
        self.bytes += bytes as u64;
        record(&mut self.publish, acknowledged.saturating_sub(sent) / 1_000);
        record(&mut self.delay, sent.saturating_sub(intended) / 1_000);
        record(&mut self.schedule, acknowledged.saturating_sub(intended) / 1_000);
    }
}

/// What one consumer recorded.
pub struct ConsumerRecord {
    pub received: u64,
    pub bytes: u64,
    pub e2e: Histogram<u64>,
}

impl ConsumerRecord {
    fn new() -> Self {
        ConsumerRecord { received: 0, bytes: 0, e2e: histogram(E2E_MAX_MICROS) }
    }

    /// Records one received message. Times are wall-clock milliseconds, as the driver API carries
    /// them; a negative latency (clock skew across hosts) is counted but not recorded.
    pub fn record_receive(&mut self, bytes: usize, publish_millis: u64, now_millis: u64) {
        self.received += 1;
        self.bytes += bytes as u64;
        if now_millis >= publish_millis {
            record(&mut self.e2e, (now_millis - publish_millis) * 1_000);
        }
    }
}

pub type ProducerSlot = Arc<Mutex<ProducerRecord>>;
pub type ConsumerSlot = Arc<Mutex<ConsumerRecord>>;

struct Latencies {
    publish: Histogram<u64>,
    delay: Histogram<u64>,
    schedule: Histogram<u64>,
    e2e: Histogram<u64>,
}

impl Latencies {
    fn new() -> Self {
        Latencies {
            publish: histogram(PUBLISH_MAX_MICROS),
            delay: histogram(PUBLISH_MAX_MICROS),
            schedule: histogram(PUBLISH_MAX_MICROS),
            e2e: histogram(E2E_MAX_MICROS),
        }
    }

    fn take(&mut self) -> Latencies {
        std::mem::replace(self, Latencies::new())
    }
}

#[derive(Clone, Copy, Default)]
struct Totals {
    sent: u64,
    sent_bytes: u64,
    received: u64,
    received_bytes: u64,
}

struct View {
    interval: Latencies,
    cumulative: Latencies,
    // Totals at the previous period snapshot
    last: Totals,
    spare_producer: ProducerRecord,
    spare_consumer: ConsumerRecord,
}

/// The statistics of one run.
pub struct Stats {
    producers: Mutex<Vec<ProducerSlot>>,
    consumers: Mutex<Vec<ConsumerSlot>>,
    // Readiness probes count towards the totals only, as in the Java worker
    probes_sent: AtomicU64,
    view: Mutex<View>,
}

impl Default for Stats {
    fn default() -> Self {
        Stats {
            producers: Mutex::new(Vec::new()),
            consumers: Mutex::new(Vec::new()),
            probes_sent: AtomicU64::new(0),
            view: Mutex::new(View {
                interval: Latencies::new(),
                cumulative: Latencies::new(),
                last: Totals::default(),
                spare_producer: ProducerRecord::new(),
                spare_consumer: ConsumerRecord::new(),
            }),
        }
    }
}

impl Stats {
    pub fn producer_slot(&self) -> ProducerSlot {
        let slot = Arc::new(Mutex::new(ProducerRecord::new()));
        lock(&self.producers).push(slot.clone());
        slot
    }

    pub fn consumer_slot(&self) -> ConsumerSlot {
        let slot = Arc::new(Mutex::new(ConsumerRecord::new()));
        lock(&self.consumers).push(slot.clone());
        slot
    }

    pub fn record_probe(&self) {
        self.probes_sent.fetch_add(1, Ordering::Relaxed);
    }

    // Moves every slot's histograms into the pending views and returns the current totals
    fn drain(&self, view: &mut View) -> Totals {
        let mut totals = Totals::default();
        let producers = lock(&self.producers).clone();
        for slot in producers {
            let spare = &mut view.spare_producer;
            {
                let mut record = lock(&slot);
                totals.sent += record.sent;
                totals.sent_bytes += record.bytes;
                std::mem::swap(&mut record.publish, &mut spare.publish);
                std::mem::swap(&mut record.delay, &mut spare.delay);
                std::mem::swap(&mut record.schedule, &mut spare.schedule);
            }
            for (pending, drained) in [
                (&mut view.interval.publish, &spare.publish),
                (&mut view.interval.delay, &spare.delay),
                (&mut view.interval.schedule, &spare.schedule),
                (&mut view.cumulative.publish, &spare.publish),
                (&mut view.cumulative.delay, &spare.delay),
                (&mut view.cumulative.schedule, &spare.schedule),
            ] {
                pending.add(drained).expect("histograms share bounds");
            }
            spare.publish.reset();
            spare.delay.reset();
            spare.schedule.reset();
        }
        let consumers = lock(&self.consumers).clone();
        for slot in consumers {
            let spare = &mut view.spare_consumer;
            {
                let mut record = lock(&slot);
                totals.received += record.received;
                totals.received_bytes += record.bytes;
                std::mem::swap(&mut record.e2e, &mut spare.e2e);
            }
            view.interval.e2e.add(&spare.e2e).expect("histograms share bounds");
            view.cumulative.e2e.add(&spare.e2e).expect("histograms share bounds");
            spare.e2e.reset();
        }
        totals
    }

    pub fn period_stats(&self) -> PeriodStats {
        let mut view = lock(&self.view);
        let totals = self.drain(&mut view);
        let last = std::mem::replace(&mut view.last, totals);
        let latencies = view.interval.take();
        let probes = self.probes_sent.load(Ordering::Relaxed);
        PeriodStats {
            messages_sent: totals.sent - last.sent,
            message_send_errors: 0,
            bytes_sent: totals.sent_bytes - last.sent_bytes,
            messages_received: totals.received - last.received,
            bytes_received: totals.received_bytes - last.received_bytes,
            total_messages_sent: totals.sent + probes,
            total_message_send_errors: 0,
            total_messages_received: totals.received,
            publish_latency: encode(&latencies.publish),
            publish_delay_latency: encode(&latencies.delay),
            end_to_end_latency: encode(&latencies.e2e),
            publish_schedule_latency: encode(&latencies.schedule),
        }
    }

    pub fn cumulative_latencies(&self) -> CumulativeLatencies {
        let mut view = lock(&self.view);
        self.drain(&mut view);
        let latencies = view.cumulative.take();
        CumulativeLatencies {
            publish_latency: encode(&latencies.publish),
            publish_delay_latency: encode(&latencies.delay),
            publish_schedule_latency: encode(&latencies.schedule),
            end_to_end_latency: encode(&latencies.e2e),
        }
    }

    pub fn counters(&self) -> CountersStats {
        let mut sent = self.probes_sent.load(Ordering::Relaxed);
        for slot in lock(&self.producers).iter() {
            sent += lock(slot).sent;
        }
        let mut received = 0;
        for slot in lock(&self.consumers).iter() {
            received += lock(slot).received;
        }
        CountersStats {
            messages_sent: sent,
            message_send_errors: 0,
            messages_received: received,
            verified_sent: 0,
            delivered: 0,
            duplicated: 0,
            out_of_order: 0,
        }
    }

    /// Drops the latencies recorded so far, keeping the counters, like the Java `resetLatencies`.
    pub fn reset_latencies(&self) {
        let mut view = lock(&self.view);
        self.drain(&mut view);
        view.interval = Latencies::new();
        view.cumulative = Latencies::new();
    }
}

/// HdrHistogram V2 compressed encoding in base64, as Jackson writes the Java `byte[]`.
pub fn encode(histogram: &Histogram<u64>) -> String {
    let mut bytes = Vec::new();
    V2DeflateSerializer::new().serialize(histogram, &mut bytes).expect("in-memory histogram encoding");
    STANDARD.encode(bytes)
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PeriodStats {
    pub messages_sent: u64,
    pub message_send_errors: u64,
    pub bytes_sent: u64,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub total_messages_sent: u64,
    pub total_message_send_errors: u64,
    pub total_messages_received: u64,
    pub publish_latency: String,
    pub publish_delay_latency: String,
    pub end_to_end_latency: String,
    pub publish_schedule_latency: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CumulativeLatencies {
    pub publish_latency: String,
    pub publish_delay_latency: String,
    pub publish_schedule_latency: String,
    pub end_to_end_latency: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CountersStats {
    pub messages_sent: u64,
    pub message_send_errors: u64,
    pub messages_received: u64,
    pub verified_sent: u64,
    pub delivered: u64,
    pub duplicated: u64,
    pub out_of_order: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hdrhistogram::serialization::Deserializer;

    fn decode(encoded: &str) -> Histogram<u64> {
        let bytes = STANDARD.decode(encoded).unwrap();
        Deserializer::new().deserialize(&mut bytes.as_slice()).unwrap()
    }

    #[test]
    fn period_stats_report_each_value_once_and_cumulative_keeps_them() {
        let stats = Stats::default();
        let producer = stats.producer_slot();
        let consumer = stats.consumer_slot();
        lock(&producer).record_send(100, 0, 2_000, 5_000);
        lock(&consumer).record_receive(100, 10, 12);
        stats.record_probe();

        let first = stats.period_stats();
        assert_eq!(first.messages_sent, 1);
        assert_eq!(first.total_messages_sent, 2);
        assert_eq!(first.bytes_sent, 100);
        assert_eq!(first.messages_received, 1);
        assert_eq!(decode(&first.publish_latency).len(), 1);
        assert_eq!(decode(&first.publish_delay_latency).max(), 2);
        assert_eq!(decode(&first.publish_schedule_latency).max(), 5);
        assert_eq!(decode(&first.end_to_end_latency).max(), 2_000);

        let second = stats.period_stats();
        assert_eq!(second.messages_sent, 0);
        assert_eq!(second.total_messages_sent, 2);
        assert_eq!(decode(&second.publish_latency).len(), 0);

        let cumulative = stats.cumulative_latencies();
        assert_eq!(decode(&cumulative.publish_latency).len(), 1);
        assert_eq!(decode(&stats.cumulative_latencies().publish_latency).len(), 0);
        assert_eq!(stats.counters().messages_sent, 2);
    }

    #[test]
    fn reset_drops_latencies_but_keeps_counters() {
        let stats = Stats::default();
        let producer = stats.producer_slot();
        lock(&producer).record_send(10, 0, 0, 1_000);
        stats.reset_latencies();
        let period = stats.period_stats();
        assert_eq!(period.messages_sent, 1);
        assert_eq!(decode(&period.publish_latency).len(), 0);
        assert_eq!(decode(&stats.cumulative_latencies().publish_latency).len(), 0);
    }

    #[test]
    fn clamps_high_values_and_skips_negative_end_to_end() {
        let mut record = ConsumerRecord::new();
        record.record_receive(1, 20, 10);
        assert_eq!(record.received, 1);
        assert_eq!(record.e2e.len(), 0);
        let mut publish = histogram(PUBLISH_MAX_MICROS);
        record_value_above_max(&mut publish);
        assert_eq!(publish.len(), 1);
    }

    fn record_value_above_max(h: &mut Histogram<u64>) {
        record(h, PUBLISH_MAX_MICROS * 10);
        assert!(h.max() >= PUBLISH_MAX_MICROS);
    }
}
