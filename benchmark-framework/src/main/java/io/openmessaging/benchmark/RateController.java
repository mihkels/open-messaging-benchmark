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

import static java.util.concurrent.TimeUnit.SECONDS;

import io.openmessaging.benchmark.utils.Env;

class RateController {
    private static final long ONE_SECOND_IN_NANOS = SECONDS.toNanos(1);
    private final long publishBacklogLimit;
    private final long receiveBacklogLimit;
    private final double minRampingFactor;
    private final double maxRampingFactor;
    // Each published message is received once per subscription
    private final int subscriptions;

    private double rampingFactor;

    private long previousTotalPublished = 0;
    private long previousTotalReceived = 0;

    // What the last step saw and decided, for the result's rate-probe trace
    private String decision = "";
    private long receiveBacklog;
    private long publishBacklog;

    RateController(int subscriptions) {
        this.subscriptions = subscriptions;
        publishBacklogLimit = Env.getLong("PUBLISH_BACKLOG_LIMIT", 1_000);
        receiveBacklogLimit = Env.getLong("RECEIVE_BACKLOG_LIMIT", 1_000);
        minRampingFactor = Env.getDouble("MIN_RAMPING_FACTOR", 0.01);
        maxRampingFactor = Env.getDouble("MAX_RAMPING_FACTOR", 1);
        rampingFactor = maxRampingFactor;
    }

    double getRampingFactor() {
        return rampingFactor;
    }

    double nextRate(double rate, long periodNanos, long totalPublished, long totalReceived) {
        long expected = (long) ((rate / ONE_SECOND_IN_NANOS) * periodNanos);
        long published = totalPublished - previousTotalPublished;
        long received = totalReceived - previousTotalReceived;

        previousTotalPublished = totalPublished;
        previousTotalReceived = totalReceived;

        // A producer-only workload has nothing to receive
        receiveBacklog = subscriptions == 0 ? 0 : subscriptions * totalPublished - totalReceived;
        publishBacklog = expected - published;
        if (receiveBacklog > receiveBacklogLimit) {
            decision = "receiveBacklog";
            // Both count one copy per subscription; scale back to published messages
            return nextRate(
                    periodNanos, received / subscriptions, expected, receiveBacklog / subscriptions);
        }

        if (publishBacklog > publishBacklogLimit) {
            decision = "publishBacklog";
            return nextRate(periodNanos, published, expected, publishBacklog);
        }

        decision = "rampUp";
        rampUp();

        return rate + (rate * rampingFactor);
    }

    // How the last step chose its rate: rampUp, receiveBacklog or publishBacklog
    String decision() {
        return decision;
    }

    // Messages published but not yet received at the last step
    long receiveBacklog() {
        return receiveBacklog;
    }

    // Messages the last step expected to publish but did not
    long publishBacklog() {
        return publishBacklog;
    }

    private double nextRate(long periodNanos, long actual, long expected, long backlog) {
        rampDown();
        long nextExpected = Math.max(0, expected - backlog);
        double nextExpectedRate = rate(nextExpected, periodNanos);
        double actualRate = rate(actual, periodNanos);
        // Never 0: ramp-up multiplies the rate, so 0 could never climb again
        return Math.max(1, Math.min(actualRate, nextExpectedRate));
    }

    private double rate(long count, long periodNanos) {
        return (count / (double) periodNanos) * ONE_SECOND_IN_NANOS;
    }

    private void rampUp() {
        rampingFactor = Math.min(maxRampingFactor, rampingFactor * 2);
    }

    private void rampDown() {
        rampingFactor = Math.max(minRampingFactor, rampingFactor / 2);
    }
}
