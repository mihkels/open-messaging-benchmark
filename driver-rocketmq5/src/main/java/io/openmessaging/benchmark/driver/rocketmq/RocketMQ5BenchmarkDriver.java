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

import com.google.common.io.BaseEncoding;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.rocketmq.client.RocketMQClient5Config;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.shaded.commons.lang3.StringUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class RocketMQ5BenchmarkDriver implements BenchmarkDriver {
    private RocketMQClient5Config rmqClientConfig;
    private DefaultMQAdminExt rmqAdmin;

    @Override
    public synchronized void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException {
        this.rmqClientConfig = readConfig(configurationFile);
        RPCHook rpcHook = null;
        if (isAdminAclEnabled()) {
            rpcHook =
                    new AclClientRPCHook(
                            new SessionCredentials(
                                    this.rmqClientConfig.adminAccessKey, this.rmqClientConfig.adminSecretKey));
        }
        DefaultMQAdminExt admin = newAdmin(rpcHook);
        try {
            admin.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
            admin.setInstanceName("AdminInstance-" + getRandomString());
            admin.start();
        } catch (MQClientException e) {
            shutdownAfterFailure(admin, e);
            throw new IOException("Failed to start the RocketMQ admin tool", e);
        } catch (RuntimeException e) {
            shutdownAfterFailure(admin, e);
            throw e;
        }
        this.rmqAdmin = admin;
    }

    DefaultMQAdminExt newAdmin(RPCHook hook) {
        return hook == null ? new DefaultMQAdminExt() : new DefaultMQAdminExt(hook);
    }

    ClientServiceProvider clientProvider() {
        return ClientServiceProvider.loadService();
    }

    Map<String, Set<String>> cachedBrokerAddr = new ConcurrentHashMap<>();

    private synchronized Set<String> fetchMasterAndSlaveAddrByClusterName(
            final MQAdminExt adminExt, final String clusterName)
            throws RemotingConnectException,
                    RemotingSendRequestException,
                    RemotingTimeoutException,
                    MQBrokerException,
                    InterruptedException {
        Set<String> brokerList = cachedBrokerAddr.get(clusterName);
        if (brokerList == null) {
            brokerList =
                    CommandUtil.fetchMasterAndSlaveAddrByClusterName(
                            adminExt, this.rmqClientConfig.clusterName);
            cachedBrokerAddr.put(clusterName, brokerList);
            if (brokerList.isEmpty()) {
                throw new RuntimeException("get brokerAddr return null, clusterName: " + clusterName);
            }
        }
        return brokerList;
    }

    @Override
    public String getTopicNamePrefix() {
        return "Benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.runAsync(
                () -> {
                    TopicConfig topicConfig = new TopicConfig();
                    topicConfig.setOrder(false);
                    topicConfig.setPerm(6);
                    topicConfig.setReadQueueNums(partitions);
                    topicConfig.setWriteQueueNums(partitions);
                    topicConfig.setTopicName(topic);
                    Map<String, String> properties = new HashMap<>();
                    String topicType =
                            rmqClientConfig.sendDelayMsg
                                    ? TopicMessageType.DELAY.getValue()
                                    : TopicMessageType.NORMAL.getValue();
                    properties.put("+message.type", topicType);
                    topicConfig.setAttributes(properties);

                    try {
                        Set<String> brokerList =
                                fetchMasterAndSlaveAddrByClusterName(
                                        this.rmqAdmin, this.rmqClientConfig.clusterName);
                        topicConfig.setReadQueueNums(Math.max(1, partitions / brokerList.size()));
                        topicConfig.setWriteQueueNums(Math.max(1, partitions / brokerList.size()));

                        for (String brokerAddr : brokerList) {
                            this.rmqAdmin.createAndUpdateTopicConfig(brokerAddr, topicConfig);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Failed to create topic [%s] to cluster [%s]",
                                        topic, this.rmqClientConfig.clusterName),
                                e);
                    }
                });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        try {
            ClientServiceProvider provider = clientProvider();
            ClientConfigurationBuilder builder =
                    ClientConfiguration.newBuilder().setEndpoints(this.rmqClientConfig.grpcEndpoint);
            if (isAclEnabled()) {
                builder.setCredentialProvider(
                        new StaticSessionCredentialsProvider(
                                this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
            }
            ClientConfiguration configuration = builder.build();
            Producer rmqProducer =
                    provider
                            .newProducerBuilder()
                            .setTopics(topic)
                            .setClientConfiguration(configuration)
                            .build();
            if (this.rmqClientConfig.sendDelayMsg) {
                return CompletableFuture.completedFuture(
                        new RocketMQ5BenchmarkProducer(
                                rmqProducer, topic, true, this.rmqClientConfig.delayTimeInSec));
            }
            return CompletableFuture.completedFuture(new RocketMQ5BenchmarkProducer(rmqProducer, topic));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public void createSubscriptionGroup(String fullSubName) {
        try {
            Set<String> brokerList =
                    fetchMasterAndSlaveAddrByClusterName(this.rmqAdmin, this.rmqClientConfig.clusterName);
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setGroupName(fullSubName);

            for (String brokerAddr : brokerList) {
                this.rmqAdmin.createAndUpdateSubscriptionGroupConfig(brokerAddr, subscriptionGroupConfig);
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to create subscription [%s] to cluster [%s]",
                            fullSubName, this.rmqClientConfig.clusterName),
                    e);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        try {
            // Prefix the group with the topic to avoid subscription relationship conflicts.
            String subPrefix;
            if (topic.contains("%")) {
                subPrefix = topic.split("%")[1];
            } else {
                subPrefix = topic;
            }
            String fullSubName = String.format("%s_%s", subPrefix, subscriptionName);
            createSubscriptionGroup(fullSubName);

            ClientServiceProvider provider = clientProvider();
            ClientConfigurationBuilder builder =
                    ClientConfiguration.newBuilder().setEndpoints(this.rmqClientConfig.grpcEndpoint);
            if (isAclEnabled()) {
                builder.setCredentialProvider(
                        new StaticSessionCredentialsProvider(
                                this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
            }
            ClientConfiguration configuration = builder.build();
            FilterExpression filterExpression = new FilterExpression("*", FilterExpressionType.TAG);
            PushConsumer rmqConsumer =
                    provider
                            .newPushConsumerBuilder()
                            .setClientConfiguration(configuration)
                            // Set the consumer group name.
                            .setConsumerGroup(fullSubName)
                            // Set the subscription for the consumer.
                            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
                            .setMessageListener(
                                    messageView -> {
                                        // Handle the received message and return consume result.
                                        consumerCallback.messageReceived(
                                                messageView.getBody(), messageView.getBornTimestamp());
                                        return ConsumeResult.SUCCESS;
                                    })
                            .build();
            return CompletableFuture.completedFuture(new RocketMQ5BenchmarkConsumer(rmqConsumer));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public boolean isAclEnabled() {
        return !(StringUtils.isAnyBlank(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey)
                || StringUtils.isAnyEmpty(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
    }

    public boolean isAdminAclEnabled() {
        return !(StringUtils.isAnyBlank(
                        this.rmqClientConfig.adminAccessKey, this.rmqClientConfig.adminSecretKey)
                || StringUtils.isAnyEmpty(
                        this.rmqClientConfig.adminAccessKey, this.rmqClientConfig.adminSecretKey));
    }

    @Override
    public synchronized void close() {
        if (rmqAdmin != null) {
            DefaultMQAdminExt admin = rmqAdmin;
            rmqAdmin = null;
            admin.shutdown();
        }
    }

    private static void shutdownAfterFailure(DefaultMQAdminExt admin, Throwable failure) {
        try {
            admin.shutdown();
        } catch (RuntimeException closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper
                    .readerFor(RocketMQClient5Config.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static RocketMQClient5Config readConfig(Path configurationFile) throws IOException {
        return reader.readValue(configurationFile.toFile());
    }

    private static final Random random = new Random();

    private static String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return BaseEncoding.base64Url().omitPadding().encode(buffer);
    }
}
