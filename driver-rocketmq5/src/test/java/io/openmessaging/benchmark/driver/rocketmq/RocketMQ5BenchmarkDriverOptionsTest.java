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
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class RocketMQ5BenchmarkDriverOptionsTest {
    private static final String BASE =
            "namesrvAddr: localhost:9876\nclusterName: c1\ngrpcEndpoint: localhost:8081\ndelayTimeInSec: 5\n";

    @TempDir Path dir;

    @Test
    void aclCredentialsAreUsedForAdminProducerAndConsumer() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(
                config(
                        BASE
                                + "sendDelayMsg: true\nadminAccessKey: aak\nadminSecretKey: ask\n"
                                + "accessKey: ak\nsecretKey: sk\n"),
                null);
        assertThat(driver.isAdminAclEnabled()).isTrue();
        assertThat(driver.isAclEnabled()).isTrue();
        assertThat(driver.hooks).singleElement().isNotNull();
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:10911"));

        ProducerBuilder producerBuilder = producerBuilder(driver);
        assertThat(driver.createProducer("topic").join()).isNotNull();
        ArgumentCaptor<ClientConfiguration> config = ArgumentCaptor.forClass(ClientConfiguration.class);
        verify(producerBuilder).setClientConfiguration(config.capture());
        assertThat(config.getValue().getCredentialsProvider()).isPresent();

        PushConsumerBuilder consumerBuilder = consumerBuilder(driver);
        driver.createConsumer("ns%topic", "sub", mock(ConsumerCallback.class)).join();
        verify(consumerBuilder).setConsumerGroup("topic_sub");
        verify(consumerBuilder).setClientConfiguration(config.capture());
        assertThat(config.getValue().getCredentialsProvider()).isPresent();
        ArgumentCaptor<SubscriptionGroupConfig> group =
                ArgumentCaptor.forClass(SubscriptionGroupConfig.class);
        verify(driver.admin).createAndUpdateSubscriptionGroupConfig(anyString(), group.capture());
        assertThat(group.getValue().getGroupName()).isEqualTo("topic_sub");
        driver.close();
    }

    @Test
    void withoutCredentialsNoHookOrCredentialProviderIsSet() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(
                config(BASE + "sendDelayMsg: false\nadminAccessKey: ' '\naccessKey: ak\n"), null);
        assertThat(driver.isAdminAclEnabled()).isFalse();
        assertThat(driver.isAclEnabled()).isFalse();
        assertThat(driver.hooks).containsOnlyNulls();
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:10911"));

        ProducerBuilder producerBuilder = producerBuilder(driver);
        driver.createProducer("topic").join();
        ArgumentCaptor<ClientConfiguration> config = ArgumentCaptor.forClass(ClientConfiguration.class);
        verify(producerBuilder).setClientConfiguration(config.capture());
        assertThat(config.getValue().getCredentialsProvider()).isEmpty();

        PushConsumerBuilder consumerBuilder = consumerBuilder(driver);
        driver.createConsumer("topic", "sub", mock(ConsumerCallback.class)).join();
        verify(consumerBuilder).setConsumerGroup("topic_sub");
        driver.close();
    }

    @Test
    void createsTopicOfTheConfiguredTypeOnEveryBroker() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config(BASE + "sendDelayMsg: true\n"), null);
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:1", "b2:1"));
        driver.createTopic("topic", 6).join();
        driver.createTopic("other", 1).join();

        ArgumentCaptor<TopicConfig> topic = ArgumentCaptor.forClass(TopicConfig.class);
        verify(driver.admin, times(4)).createAndUpdateTopicConfig(anyString(), topic.capture());
        assertThat(topic.getAllValues().get(0).getReadQueueNums()).isEqualTo(3);
        assertThat(topic.getAllValues().get(0).getAttributes()).containsEntry("+message.type", "DELAY");
        assertThat(topic.getAllValues().get(3).getWriteQueueNums()).isEqualTo(1);
        verify(driver.admin, times(1)).examineBrokerClusterInfo();
        driver.close();

        TestDriver normal = new TestDriver();
        normal.initialize(config(BASE + "sendDelayMsg: false\n"), null);
        when(normal.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1", "b1:1"));
        normal.createTopic("topic", 1).join();
        verify(normal.admin).createAndUpdateTopicConfig(anyString(), topic.capture());
        assertThat(topic.getValue().getAttributes()).containsEntry("+message.type", "NORMAL");
        normal.close();
    }

    @Test
    void topicAndSubscriptionFailuresAreWrapped() throws Exception {
        TestDriver driver = new TestDriver();
        driver.initialize(config(BASE + "sendDelayMsg: false\n"), null);
        when(driver.admin.examineBrokerClusterInfo()).thenReturn(cluster("c1"));
        assertThatThrownBy(() -> driver.createTopic("topic", 1).join())
                .hasMessageContaining("Failed to create topic [topic] to cluster [c1]")
                .hasRootCauseMessage("get brokerAddr return null, clusterName: c1");
        driver.cachedBrokerAddr.clear();
        when(driver.admin.examineBrokerClusterInfo()).thenThrow(new IllegalStateException("down"));
        assertThatThrownBy(() -> driver.createSubscriptionGroup("g"))
                .hasMessageContaining("Failed to create subscription [g] to cluster [c1]");

        driver.cachedBrokerAddr.clear();
        doReturn(cluster("c1", "b1:1")).when(driver.admin).examineBrokerClusterInfo();
        doThrow(new IllegalStateException("rejected"))
                .when(driver.admin)
                .createAndUpdateSubscriptionGroupConfig(anyString(), any(SubscriptionGroupConfig.class));
        assertThatThrownBy(
                        () -> driver.createConsumer("topic", "sub", mock(ConsumerCallback.class)).join())
                .hasMessageContaining("Failed to create subscription [topic_sub]");
        driver.close();
    }

    @Test
    void runtimeAdminStartupFailureIsRethrownAfterShutdown() throws Exception {
        TestDriver driver = new TestDriver();
        IllegalStateException failure = new IllegalStateException("boom");
        doThrow(failure).when(driver.admin).start();
        doThrow(failure).when(driver.admin).shutdown();
        assertThatThrownBy(() -> driver.initialize(config(BASE + "sendDelayMsg: false\n"), null))
                .isSameAs(failure);
        assertThat(failure.getSuppressed()).isEmpty();
        driver.close();
    }

    @Test
    void realFactoriesAndPrefix() {
        RocketMQ5BenchmarkDriver driver = new RocketMQ5BenchmarkDriver();
        assertThat(driver.newAdmin(null)).isNotNull();
        assertThat(driver.newAdmin(mock(RPCHook.class))).isNotNull();
        assertThat(driver.clientProvider()).isNotNull();
        assertThat(driver.getTopicNamePrefix()).isEqualTo("Benchmark");
        driver.close();
    }

    private static ProducerBuilder producerBuilder(TestDriver driver) throws Exception {
        ProducerBuilder builder = mock(ProducerBuilder.class, RETURNS_SELF);
        when(driver.provider.newProducerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(mock(Producer.class));
        return builder;
    }

    private static PushConsumerBuilder consumerBuilder(TestDriver driver) throws Exception {
        PushConsumerBuilder builder = mock(PushConsumerBuilder.class, RETURNS_SELF);
        when(driver.provider.newPushConsumerBuilder()).thenReturn(builder);
        when(builder.build()).thenReturn(mock(PushConsumer.class));
        return builder;
    }

    private Path config(String yaml) throws Exception {
        Path file = dir.resolve("rocketmq5-" + System.nanoTime() + ".yaml");
        Files.writeString(
                file,
                "name: RocketMQ5\ndriverClass: " + RocketMQ5BenchmarkDriver.class.getName() + "\n" + yaml);
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

    private static final class TestDriver extends RocketMQ5BenchmarkDriver {
        final DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        final ClientServiceProvider provider = mock(ClientServiceProvider.class);
        final List<RPCHook> hooks = new ArrayList<>();

        @Override
        DefaultMQAdminExt newAdmin(RPCHook hook) {
            hooks.add(hook);
            return admin;
        }

        @Override
        ClientServiceProvider clientProvider() {
            return provider;
        }
    }
}
