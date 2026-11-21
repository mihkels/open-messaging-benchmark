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
package io.github.mihkels.omb.driver.pulsar;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.Failures;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pulsar.client.api.Consumer;

public final class PulsarBenchmarkConsumer implements BenchmarkConsumer {

    private final List<Consumer<ByteBuffer>> consumer;
    private boolean closed;

    public PulsarBenchmarkConsumer(List<Consumer<ByteBuffer>> consumer) {
        this.consumer = List.copyOf(consumer);
        if (this.consumer.isEmpty()) {
            throw new IllegalArgumentException("A Pulsar consumer must own at least one partition");
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        Failures failures = new Failures();
        consumer.forEach(failures::closeRestoringInterruptNow);
        failures.throwIfAny();
    }
}
