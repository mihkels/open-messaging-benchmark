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

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NatsBenchmarkConsumer implements BenchmarkConsumer {
    private final Connection connection;
    private final Dispatcher dispatcher;
    private final AtomicBoolean closed = new AtomicBoolean();

    NatsBenchmarkConsumer(Connection connection, Dispatcher dispatcher) {
        this.connection = connection;
        this.dispatcher = dispatcher;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            connection.closeDispatcher(dispatcher);
        }
    }
}
