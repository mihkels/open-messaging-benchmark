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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class RocketMQBenchmarkDriverOptionsTest {
    @TempDir Path dir;

    @Test
    void appliesEveryOptionalProducerAndConsumerSettingWithAcl() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(
                config(
                        """
                        clusterName: c1
                        namesrvAddr: 127.0.0.1:9876
                        vipChannelEnabled: true
                        maxMessageSize: 1024
                        compressMsgBodyOverHowmuch: 2048
                        autoBatch: true
                        batchMaxBytes: 10
                        batchMaxDelayMs: 11
                        totalBatchMaxBytes: 12
                        enableBackpressure: true
                        backpressureConcurrency: 13
                        accessKey: ak
                        secretKey: sk
                        """),
                null);
        assertThat(driver.isAclEnabled()).isTrue();
        assertThat(driver.hooks).allMatch(h -> h != null);

        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        driver.producer = producer;
        driver.createProducer("topic").join();
        verify(producer).setVipChannelEnabled(true);
        verify(producer).setMaxMessageSize(1024);
        verify(producer).setCompressMsgBodyOverHowmuch(2048);
        verify(producer).setAutoBatch(true);
        verify(producer).batchMaxBytes(10);
        verify(producer).batchMaxDelayMs(11);
        verify(producer).totalBatchMaxBytes(12);
        verify(producer).setEnableBackpressureForAsyncMode(true);
        verify(producer).setBackPressureForAsyncSendNum(13);

        driver.createConsumer("topic", "group", mock(ConsumerCallback.class)).join();
        verify(driver.consumer).setVipChannelEnabled(true);
        driver.close();
        driver.close();
        verify(producer, times(1)).shutdown();
        assertThatThrownBy(() -> driver.createProducer("topic").join())
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void skipsUnsetProducerSettingsAndAclWithoutCredentials() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(
                config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\naccessKey: ' '\n"), null);
        assertThat(driver.isAclEnabled()).isFalse();
        assertThat(driver.hooks).containsOnlyNulls();

        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        driver.producer = producer;
        driver.createProducer("topic").join();
        verify(producer, never()).setVipChannelEnabled(any(Boolean.class));
        verify(producer, never()).setMaxMessageSize(any(Integer.class));
        verify(producer, never()).setAutoBatch(any(Boolean.class));
        driver.createConsumer("topic", "group", mock(ConsumerCallback.class)).join();
        verify(driver.consumer, never()).setVipChannelEnabled(any(Boolean.class));
        driver.close();
    }

    @Test
    void consumerListenerForwardsEveryMessage() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\n"), null);
        ConsumerCallback callback = mock(ConsumerCallback.class);
        BenchmarkConsumer consumer = driver.createConsumer("topic", "group", callback).join();
        ArgumentCaptor<MessageListenerConcurrently> listener =
                ArgumentCaptor.forClass(MessageListenerConcurrently.class);
        verify(driver.consumer).registerMessageListener(listener.capture());

        MessageExt first = new MessageExt();
        first.setBody(new byte[] {1});
        first.setBornTimestamp(5);
        MessageExt second = new MessageExt();
        second.setBody(new byte[] {2});
        second.setBornTimestamp(6);
        assertThat(listener.getValue().consumeMessage(List.of(first, second), null))
                .isEqualTo(ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        verify(callback).messageReceived(new byte[] {1}, 5);
        verify(callback).messageReceived(new byte[] {2}, 6);
        consumer.close();
        driver.close();
    }

    @Test
    void createsBatchTopicOnMastersAndCachesTheBrokerList() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(
                config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\nbatchCQ: true\n"), null);
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:10911", "b2:10911"));

        driver.createTopic("topic", 8).join();
        driver.createTopic("other", 1).join();

        ArgumentCaptor<TopicConfig> topic = ArgumentCaptor.forClass(TopicConfig.class);
        verify(driver.admin, times(3)).createAndUpdateTopicConfig(anyString(), topic.capture());
        TopicConfig first = topic.getAllValues().get(0);
        assertThat(first.getReadQueueNums()).isEqualTo(4);
        assertThat(first.getAttributes()).containsValue("BatchCQ");
        assertThat(topic.getAllValues().get(2).getWriteQueueNums()).isEqualTo(1);
        verify(driver.admin, times(1)).examineBrokerClusterInfo();
        driver.close();
    }

    @Test
    void topicCreationFailsWhenClusterHasNoBrokersOrBrokerRejects() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\n"), null);
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1"));
        assertThatThrownBy(() -> driver.createTopic("topic", 1).join())
                .hasRootCauseMessage("get brokerAddr return null, clusterName: c1");

        driver.cachedBrokerAddr.clear();
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:10911"));
        doThrow(new IllegalStateException("rejected"))
                .when(driver.admin)
                .createAndUpdateTopicConfig(eq("b1:10911"), any(TopicConfig.class));
        assertThatThrownBy(() -> driver.createTopic("topic", 1).join())
                .hasMessageContaining("Failed to create topic [topic] to cluster [c1]");
        driver.close();
    }

    @Test
    void interruptedTopicCreationRestoresInterruptStatus() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\n"), null);
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:10911"));

        InterruptedException interrupted = new InterruptedException("interrupted");
        CountDownLatch adminCallStarted = new CountDownLatch(1);
        CountDownLatch releaseAdminCall = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            adminCallStarted.countDown();
                            releaseAdminCall.await();
                            throw interrupted;
                        })
                .when(driver.admin)
                .createAndUpdateTopicConfig(anyString(), any(TopicConfig.class));

        CompletableFuture<Void> topicCreation = driver.createTopic("topic", 1);
        assertThat(adminCallStarted.await(5, TimeUnit.SECONDS)).isTrue();
        AtomicBoolean interruptRestored = new AtomicBoolean();
        CompletableFuture<Void> observed =
                topicCreation.whenComplete(
                        (ignored, failure) -> interruptRestored.set(Thread.currentThread().isInterrupted()));

        try {
            releaseAdminCall.countDown();
            assertThatThrownBy(observed::join).hasRootCause(interrupted);
            assertThat(interruptRestored).isTrue();
        } finally {
            releaseAdminCall.countDown();
            driver.close();
        }
    }

    @Test
    void factoriesBuildRealClientsWithAndWithoutHook() {
        RocketMQBenchmarkDriver driver = new RocketMQBenchmarkDriver();
        RPCHook hook = mock(RPCHook.class);
        assertThat(driver.newAdmin(null)).isNotNull();
        assertThat(driver.newAdmin(hook)).isNotNull();
        assertThat(driver.newProducer("g", null).getProducerGroup()).isEqualTo("g");
        assertThat(driver.newProducer("g", hook).getProducerGroup()).isEqualTo("g");
        assertThat(driver.newConsumer("s", null).getConsumerGroup()).isEqualTo("s");
        assertThat(driver.newConsumer("s", hook).getConsumerGroup()).isEqualTo("s");
        assertThat(driver.getTopicNamePrefix()).isEqualTo("RocketMQ-Benchmark");
    }

    @Test
    void closeRethrowsAdminFailureWhenProducerWasNeverCreated() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config("clusterName: c1\nnamesrvAddr: 127.0.0.1:9876\n"), null);
        RuntimeException failure = new RuntimeException("admin");
        doThrow(failure).when(driver.admin).shutdown();
        assertThatThrownBy(driver::close).isSameAs(failure);
    }

    @Test
    void runtimeAdminStartupFailureIsRethrownUnchanged() throws Exception {
        TestDriver driver = new TestDriver();
        IllegalStateException failure = new IllegalStateException("boom");
        doThrow(failure).when(driver.admin).start();
        assertThatThrownBy(() -> driver.initialize(config("clusterName: c1\nnamesrvAddr: x:1\n"), null))
                .isSameAs(failure);
        verify(driver.admin).shutdown();
    }

    private Path config(String yaml) throws Exception {
        Path file = dir.resolve("rocketmq-" + System.nanoTime() + ".yaml");
        Files.writeString(
                file,
                "name: RocketMQ\ndriverClass: " + RocketMQBenchmarkDriver.class.getName() + "\n" + yaml);
        return file;
    }

    private static ClusterInfo cluster(String name, String... brokerAddrs) {
        ClusterInfo info = new ClusterInfo();
        Map<String, BrokerData> brokers = new HashMap<>();
        for (int i = 0; i < brokerAddrs.length; i++) {
            HashMap<Long, String> addrs = new HashMap<>();
            addrs.put(0L, brokerAddrs[i]);
            brokers.put("broker-" + i, new BrokerData(name, "broker-" + i, addrs));
        }
        info.setBrokerAddrTable(brokers);
        info.setClusterAddrTable(Map.of(name, Set.copyOf(brokers.keySet())));
        return info;
    }

    private static final class TestDriver extends RocketMQBenchmarkDriver {
        final DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        final DefaultMQPushConsumer consumer = mock(DefaultMQPushConsumer.class);
        final java.util.List<RPCHook> hooks = new java.util.ArrayList<>();
        DefaultMQProducer producer;

        @Override
        DefaultMQAdminExt newAdmin(RPCHook hook) {
            hooks.add(hook);
            return admin;
        }

        @Override
        DefaultMQProducer newProducer(String group, RPCHook hook) {
            hooks.add(hook);
            return producer;
        }

        @Override
        DefaultMQPushConsumer newConsumer(String subscription, RPCHook hook) {
            hooks.add(hook);
            return consumer;
        }
    }
}
