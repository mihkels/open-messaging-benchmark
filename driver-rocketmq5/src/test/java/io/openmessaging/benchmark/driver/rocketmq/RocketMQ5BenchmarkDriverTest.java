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
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RocketMQ5BenchmarkDriverTest {
    @Test
    void queuesPerMasterCreatesExactlyPartitionsQueuesOnMastersOnly() {
        assertThat(RocketMQ5BenchmarkDriver.queuesPerMaster(1, 3)).containsExactly(1);
        assertThat(RocketMQ5BenchmarkDriver.queuesPerMaster(3, 3)).containsExactly(1, 1, 1);
        assertThat(RocketMQ5BenchmarkDriver.queuesPerMaster(10, 3)).containsExactly(4, 3, 3);
    }

    @Test
    void consumerCallbackReturnsSuccessAndConsumerHandleClosesClient() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        PushConsumerBuilder consumerBuilder = mock(PushConsumerBuilder.class, RETURNS_SELF);
        PushConsumer pushConsumer = mock(PushConsumer.class);
        when(driver.provider.newPushConsumerBuilder()).thenReturn(consumerBuilder);
        when(consumerBuilder.build()).thenReturn(pushConsumer);
        ConsumerCallback callback = mock(ConsumerCallback.class);

        var handle = driver.createConsumer("topic", "group", callback).join();

        ArgumentCaptor<MessageListener> listener = ArgumentCaptor.forClass(MessageListener.class);
        verify(consumerBuilder).setMessageListener(listener.capture());
        MessageView message = mock(MessageView.class);
        ByteBuffer body = ByteBuffer.wrap(new byte[] {1, 2, 3});
        when(message.getBody()).thenReturn(body);
        when(message.getBornTimestamp()).thenReturn(123L);
        assertThat(listener.getValue().consume(message)).isEqualTo(ConsumeResult.SUCCESS);
        verify(callback).messageReceived(body, 123L);

        handle.close();
        verify(pushConsumer).close();
        driver.close();
    }

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
    void closeShutsDownTheOwnedAdminOnce() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        driver.close();
        driver.close();
        verify(driver.admin, times(1)).shutdown();
    }

    @Test
    void subscriptionGroupFailureCompletesConsumerCreationExceptionally() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        RuntimeException failure = new RuntimeException("group failed");
        driver.subscriptionFailure = failure;
        assertThatThrownBy(
                        () -> driver.createConsumer("topic", "group", mock(ConsumerCallback.class)).join())
                .hasCause(failure);
        driver.close();
    }

    @Test
    void clientBuilderFailuresCompleteCreationFuturesExceptionally() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(configPath(), null);
        ProducerBuilder producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
        PushConsumerBuilder consumerBuilder = mock(PushConsumerBuilder.class, RETURNS_SELF);
        when(driver.provider.newProducerBuilder()).thenReturn(producerBuilder);
        when(driver.provider.newPushConsumerBuilder()).thenReturn(consumerBuilder);
        ClientException producerFailure = new ClientException("producer failed");
        ClientException consumerFailure = new ClientException("consumer failed");
        doThrow(producerFailure).when(producerBuilder).build();
        doThrow(consumerFailure).when(consumerBuilder).build();
        assertThatThrownBy(() -> driver.createProducer("topic").join()).hasCause(producerFailure);
        assertThatThrownBy(
                        () -> driver.createConsumer("topic", "group", mock(ConsumerCallback.class)).join())
                .hasCause(consumerFailure);
        driver.close();
    }

    private static Path configPath() {
        Path inModule = Path.of("rocketmq.yaml");
        return Files.exists(inModule) ? inModule : Path.of("driver-rocketmq5", "rocketmq.yaml");
    }

    private static final class TestDriver extends RocketMQ5BenchmarkDriver {
        final DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        final ClientServiceProvider provider = mock(ClientServiceProvider.class);
        RuntimeException subscriptionFailure;

        @Override
        DefaultMQAdminExt newAdmin(RPCHook hook) {
            return admin;
        }

        @Override
        ClientServiceProvider clientProvider() {
            return provider;
        }

        @Override
        public void createSubscriptionGroup(String fullSubName) {
            if (subscriptionFailure != null) {
                throw subscriptionFailure;
            }
        }
    }
}
