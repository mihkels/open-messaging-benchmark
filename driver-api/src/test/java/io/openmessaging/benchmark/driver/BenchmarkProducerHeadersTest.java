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
package io.openmessaging.benchmark.driver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class BenchmarkProducerHeadersTest {

    @Test
    void defaultHeaderSendDropsTheHeadersAndDelegates() {
        CompletableFuture<Void> sent = new CompletableFuture<>();
        byte[] payload = {1};
        BenchmarkProducer producer =
                new BenchmarkProducer() {
                    @Override
                    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] value) {
                        assertSame(payload, value);
                        return sent;
                    }

                    @Override
                    public void close() {}
                };

        assertSame(sent, producer.sendAsync(Optional.empty(), payload, Map.of("h", new byte[] {2})));
    }

    @Test
    void driversDoNotSupportHeadersByDefault() throws Exception {
        try (BenchmarkDriver driver =
                (BenchmarkDriver)
                        java.lang.reflect.Proxy.newProxyInstance(
                                BenchmarkDriver.class.getClassLoader(),
                                new Class<?>[] {BenchmarkDriver.class},
                                (proxy, method, args) ->
                                        method.isDefault()
                                                ? java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, args)
                                                : null)) {
            assertFalse(driver.supportsHeaders());
        }
    }
}
