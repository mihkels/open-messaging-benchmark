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
package io.openmessaging.benchmark.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class RunCallbacksTest {

    @Test
    void interruptedPausedReceiveReleasesItsSlotAndKeepsTheInterrupt() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(key -> null);
        try {
            RunCallbacks callbacks =
                    new RunCallbacks(
                            new WorkerStats(registry),
                            failure -> {
                                throw new AssertionError(failure);
                            });
            callbacks.pause();

            Thread.currentThread().interrupt();
            callbacks.receive(16, System.currentTimeMillis());

            assertThat(Thread.interrupted()).isTrue();
            assertThat(callbacks.awaitQuiescence(System.nanoTime() + TimeUnit.SECONDS.toNanos(1)))
                    .isTrue();
        } finally {
            registry.close();
        }
    }
}
