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

import io.openmessaging.benchmark.worker.commands.PeriodStats;
import org.HdrHistogram.Histogram;

/**
 * What happened during one report interval. Latency histograms are in microseconds.
 *
 * @param publishRate messages published per second
 * @param publishThroughputMb megabytes published per second
 * @param publishErrorRate failed publishes per second
 * @param consumeRate messages consumed per second
 * @param consumeThroughputMb megabytes consumed per second
 * @param backlog messages published but not yet consumed by every subscription
 * @param publishLatency time from send to acknowledgement
 * @param publishDelayLatency delay between the intended and the actual send time
 * @param publishScheduleLatency time from intended send to acknowledgement
 * @param endToEndLatency time from publish to consumption
 */
record PeriodReport(
        double publishRate,
        double publishThroughputMb,
        double publishErrorRate,
        double consumeRate,
        double consumeThroughputMb,
        long backlog,
        Histogram publishLatency,
        Histogram publishDelayLatency,
        Histogram publishScheduleLatency,
        Histogram endToEndLatency) {

    private static final double MEGABYTE = 1024.0 * 1024;

    static PeriodReport of(PeriodStats stats, double elapsedSeconds, int subscriptionsPerTopic) {
        return new PeriodReport(
                stats.messagesSent() / elapsedSeconds,
                stats.bytesSent() / elapsedSeconds / MEGABYTE,
                stats.messageSendErrors() / elapsedSeconds,
                stats.messagesReceived() / elapsedSeconds,
                stats.bytesReceived() / elapsedSeconds / MEGABYTE,
                Math.max(
                        0L, subscriptionsPerTopic * stats.totalMessagesSent() - stats.totalMessagesReceived()),
                stats.publishLatency(),
                stats.publishDelayLatency(),
                stats.publishScheduleLatency(),
                stats.endToEndLatency());
    }
}
