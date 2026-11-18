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
package io.openmessaging.benchmark.driver.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.junit.jupiter.api.Test;

class NatsPublishTimestampTest {

    @Test
    void readsTheProducerTimestampHeader() {
        assertEquals(42L, NatsBenchmarkDriver.publishTimestamp(message(header("42"))));
    }

    @Test
    void messageWithoutReadableTimestampGivesNoLatencySample() {
        assertEquals(
                ConsumerCallback.NO_PUBLISH_TIMESTAMP, NatsBenchmarkDriver.publishTimestamp(message(null)));
        assertEquals(
                ConsumerCallback.NO_PUBLISH_TIMESTAMP,
                NatsBenchmarkDriver.publishTimestamp(message(new Headers().put("other", "1"))));
        assertEquals(
                ConsumerCallback.NO_PUBLISH_TIMESTAMP,
                NatsBenchmarkDriver.publishTimestamp(message(header("x"))));
    }

    private static Headers header(String value) {
        return new Headers().put(NatsBenchmarkProducer.PUBLISH_TIMESTAMP_HEADER, value);
    }

    private static Message message(Headers headers) {
        return NatsMessage.builder().subject("s").headers(headers).data(new byte[] {1, 2}).build();
    }
}
