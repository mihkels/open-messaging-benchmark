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
package io.openmessaging.benchmark.driver.rocketmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;

class RocketMQBenchmarkDriverTest {
    @Test
    void adminStartupFailurePropagatesAndClosesPartialAdmin() throws Exception {
        TestDriver driver = new TestDriver();
        MQClientException startupFailure = new MQClientException("admin failed", null);
        RuntimeException closeFailure = new RuntimeException("shutdown failed");
        doThrow(startupFailure).when(driver.admin).start();
        doThrow(closeFailure).when(driver.admin).shutdown();
        assertThatThrownBy(() -> driver.initialize(configPath(), null))
                .isInstanceOf(IOException.class)
                .hasCause(startupFailure);
        assertThat(startupFailure.getSuppressed()).containsExactly(closeFailure);
        driver.close();
        verify(driver.admin, times(1)).shutdown();
    }

    @Test
    void failedProducerIsClosedAndCanBeRetriedWithoutClosingTheSharedClient() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        DefaultMQProducer failed = mock(DefaultMQProducer.class);
        DefaultMQProducer started = mock(DefaultMQProducer.class);
        driver.producers.add(failed);
        driver.producers.add(started);
        MQClientException startupFailure = new MQClientException("producer failed", null);
        doThrow(startupFailure).when(failed).start();

        assertThatThrownBy(() -> driver.createProducer("topic").join()).hasCause(startupFailure);
        verify(failed).shutdown();
        BenchmarkProducer first = driver.createProducer("topic").join();
        BenchmarkProducer second = driver.createProducer("another").join();
        first.close();
        second.close();
        verify(started, times(1)).start();
        verify(started, never()).shutdown();
        driver.close();
        verify(started, times(1)).shutdown();
        verify(driver.admin, times(1)).shutdown();
    }

    @Test
    void subscriptionAndConsumerStartupFailuresClosePartialConsumers() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        MQClientException subscribeFailure = new MQClientException("subscribe failed", null);
        doThrow(subscribeFailure).when(driver.consumer).subscribe("topic", "*");
        CompletableFuture<BenchmarkConsumer> failedSubscription =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        assertThatThrownBy(failedSubscription::join).hasCause(subscribeFailure);
        verify(driver.consumer).shutdown();

        DefaultMQPushConsumer second = mock(DefaultMQPushConsumer.class);
        driver.consumer = second;
        MQClientException startFailure = new MQClientException("consumer failed", null);
        doThrow(startFailure).when(second).start();
        CompletableFuture<BenchmarkConsumer> failedStart =
                driver.createConsumer("topic", "group", mock(ConsumerCallback.class));
        assertThatThrownBy(failedStart::join).hasCause(startFailure);
        verify(second).shutdown();
        driver.close();
    }

    @Test
    void closingAttemptsAdminEvenWhenSharedProducerShutdownFails() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        DefaultMQProducer shared = mock(DefaultMQProducer.class);
        driver.producers.add(shared);
        driver.createProducer("topic").join();
        RuntimeException failure = new RuntimeException("producer shutdown failed");
        RuntimeException adminFailure = new RuntimeException("admin shutdown failed");
        doThrow(failure).when(shared).shutdown();
        doThrow(adminFailure).when(driver.admin).shutdown();
        assertThatThrownBy(driver::close).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(adminFailure);
        verify(driver.admin).shutdown();
        driver.close();
        verify(shared, times(1)).shutdown();
    }

    private static Path configPath() {
        Path inModule = Path.of("rocketmq.yaml");
        return Files.exists(inModule) ? inModule : Path.of("driver-rocketmq", "rocketmq.yaml");
    }

    private static final class TestDriver extends RocketMQBenchmarkDriver {
        final DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        final Queue<DefaultMQProducer> producers = new ArrayDeque<>();
        DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);

        @Override
        DefaultMQAdminExt newAdmin(RPCHook hook) {
            return admin;
        }

        @Override
        DefaultMQProducer newProducer(String group, RPCHook hook) {
            return producers.remove();
        }

        @Override
        DefaultMQPushConsumer newConsumer(String subscription, RPCHook hook) {
            return consumer;
        }
    }
}
