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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.utils.PaddingDecimalFormat;
import io.openmessaging.benchmark.worker.commands.CumulativeLatencies;
import java.text.DecimalFormat;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The console lines a workload run prints for each report interval and at its end. */
final class ResultLog {
    private static final Logger log = LoggerFactory.getLogger(ResultLog.class);

    private static final DecimalFormat rateFormat = new PaddingDecimalFormat("0.0", 7);
    private static final DecimalFormat throughputFormat = new PaddingDecimalFormat("0.0", 4);
    private static final DecimalFormat dec = new PaddingDecimalFormat("0.0", 4);

    private ResultLog() {}

    @SuppressWarnings("checkstyle:LineLength")
    static void period(PeriodReport period) {
        Histogram publish = period.publishLatency();
        Histogram delay = period.publishDelayLatency();
        Histogram schedule = period.publishScheduleLatency();
        if (log.isInfoEnabled()) {
            log.info(
                    "Pub rate {} msg/s / {} MB/s | Pub err {} err/s | Cons rate {} msg/s / {} MB/s | Backlog: {} K | Pub Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {} | Pub Delay Latency (us) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {} | Pub Schedule Latency (ms) avg: {} - 50%: {} - 99%: {} - 99.9%: {} - Max: {}",
                    rateFormat.format(period.publishRate()),
                    throughputFormat.format(period.publishThroughputMb()),
                    rateFormat.format(period.publishErrorRate()),
                    rateFormat.format(period.consumeRate()),
                    throughputFormat.format(period.consumeThroughputMb()),
                    dec.format(period.backlog() / 1000.0),
                    dec.format(publish.getMean() / 1000.0),
                    dec.format(publish.getValueAtPercentile(50) / 1000.0),
                    dec.format(publish.getValueAtPercentile(99) / 1000.0),
                    dec.format(publish.getValueAtPercentile(99.9) / 1000.0),
                    throughputFormat.format(publish.getMaxValue() / 1000.0),
                    dec.format(delay.getMean()),
                    dec.format(delay.getValueAtPercentile(50)),
                    dec.format(delay.getValueAtPercentile(99)),
                    dec.format(delay.getValueAtPercentile(99.9)),
                    throughputFormat.format(delay.getMaxValue()),
                    dec.format(schedule.getMean() / 1000.0),
                    dec.format(schedule.getValueAtPercentile(50) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(99) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(99.9) / 1000.0),
                    throughputFormat.format(schedule.getMaxValue() / 1000.0));
        }
    }

    @SuppressWarnings("checkstyle:LineLength")
    static void aggregate(CumulativeLatencies aggregate) {
        Histogram publish = aggregate.publishLatency;
        Histogram delay = aggregate.publishDelayLatency;
        Histogram schedule = aggregate.publishScheduleLatency;
        if (log.isInfoEnabled()) {
            log.info(
                    "----- Aggregated Pub Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} | Pub Delay (us)  avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {} | Pub Schedule Latency (ms) avg: {} - 50%: {} - 95%: {} - 99%: {} - 99.9%: {} - 99.99%: {} - Max: {}",
                    dec.format(publish.getMean() / 1000.0),
                    dec.format(publish.getValueAtPercentile(50) / 1000.0),
                    dec.format(publish.getValueAtPercentile(95) / 1000.0),
                    dec.format(publish.getValueAtPercentile(99) / 1000.0),
                    dec.format(publish.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(publish.getValueAtPercentile(99.99) / 1000.0),
                    throughputFormat.format(publish.getMaxValue() / 1000.0),
                    dec.format(delay.getMean()),
                    dec.format(delay.getValueAtPercentile(50)),
                    dec.format(delay.getValueAtPercentile(95)),
                    dec.format(delay.getValueAtPercentile(99)),
                    dec.format(delay.getValueAtPercentile(99.9)),
                    dec.format(delay.getValueAtPercentile(99.99)),
                    throughputFormat.format(delay.getMaxValue()),
                    dec.format(schedule.getMean() / 1000.0),
                    dec.format(schedule.getValueAtPercentile(50) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(95) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(99) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(99.9) / 1000.0),
                    dec.format(schedule.getValueAtPercentile(99.99) / 1000.0),
                    throughputFormat.format(schedule.getMaxValue() / 1000.0));
        }
    }
}
