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
package io.github.mihkels.omb.e2e;

/**
 * Totals of one smoke run.
 *
 * @param messagesSent messages acknowledged after the readiness probes
 * @param messageSendErrors sends that failed
 * @param messagesReceived messages received, one per subscription
 * @param endToEndLatencyMillis mean end-to-end latency
 * @param delivered stamped messages received for the first time; zero unless {@code verifyDelivery}
 * @param duplicated stamped messages received again; zero unless {@code verifyDelivery}
 */
public record SmokeResult(
        long messagesSent,
        long messageSendErrors,
        long messagesReceived,
        double endToEndLatencyMillis,
        long delivered,
        long duplicated) {

    public SmokeResult(
            long messagesSent,
            long messageSendErrors,
            long messagesReceived,
            double endToEndLatencyMillis) {
        this(messagesSent, messageSendErrors, messagesReceived, endToEndLatencyMillis, 0, 0);
    }
}
