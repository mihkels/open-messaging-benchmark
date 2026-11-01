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

import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.rocketmq.client.RocketMQClientConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicAttributes;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class RocketMQBenchmarkDriver implements BenchmarkDriver {
    private DefaultMQAdminExt rmqAdmin;
    private RocketMQClientConfig rmqClientConfig;
    DefaultMQProducer rmqProducer;
    private RPCHook rpcHook;
    private boolean closed;

    @Override
    public synchronized void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException {
        this.rmqClientConfig = readConfig(configurationFile);
        if (isAclEnabled()) {
            rpcHook =
                    new AclClientRPCHook(
                            new SessionCredentials(
                                    this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
        }
        DefaultMQAdminExt admin = newAdmin(rpcHook);
        try {
            admin.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
            admin.setInstanceName("AdminInstance-" + getRandomString());
            admin.start();
        } catch (MQClientException e) {
            shutdownAfterFailure(admin::shutdown, e);
            throw new IOException("Failed to start the RocketMQ admin tool", e);
        } catch (RuntimeException e) {
            shutdownAfterFailure(admin::shutdown, e);
            throw e;
        }
        this.rmqAdmin = admin;
    }

    DefaultMQAdminExt newAdmin(RPCHook hook) {
        return hook == null ? new DefaultMQAdminExt() : new DefaultMQAdminExt(hook);
    }

    DefaultMQProducer newProducer(String group, RPCHook hook) {
        return hook == null ? new DefaultMQProducer(group) : new DefaultMQProducer(group, hook);
    }

    DefaultMQPushConsumer newConsumer(String subscription, RPCHook hook) {
        return hook == null
                ? new DefaultMQPushConsumer(subscription)
                : new DefaultMQPushConsumer(subscription, hook, new AllocateMessageQueueAveragely());
    }

    @Override
    public String getTopicNamePrefix() {
        return "RocketMQ-Benchmark";
    }

    Map<String, Set<String>> cachedBrokerAddr = new ConcurrentHashMap<>();

    int fetchCnt = 0;

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
        if (fetchCnt++ % 100 == 0) {
            log.info("fetch brokerAddr count: " + fetchCnt);
        }
        return brokerList;
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
                    if (Boolean.TRUE.equals(this.rmqClientConfig.batchCQ)) {
                        topicConfig
                                .getAttributes()
                                .put("+" + TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), "BatchCQ");
                    }

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
    public synchronized CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("RocketMQ driver is closed"));
        }
        if (rmqProducer == null) {
            DefaultMQProducer producer = null;
            try {
                producer = newProducer("ProducerGroup_" + getRandomString(), rpcHook);
                producer.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
                producer.setInstanceName("ProducerInstance" + getRandomString());
                if (null != this.rmqClientConfig.vipChannelEnabled) {
                    producer.setVipChannelEnabled(this.rmqClientConfig.vipChannelEnabled);
                }
                if (null != this.rmqClientConfig.maxMessageSize) {
                    producer.setMaxMessageSize(this.rmqClientConfig.maxMessageSize);
                }
                if (null != this.rmqClientConfig.compressMsgBodyOverHowmuch) {
                    producer.setCompressMsgBodyOverHowmuch(this.rmqClientConfig.compressMsgBodyOverHowmuch);
                }

                if (null != this.rmqClientConfig.autoBatch) {
                    producer.setAutoBatch(this.rmqClientConfig.autoBatch);
                }
                if (null != this.rmqClientConfig.batchMaxBytes) {
                    producer.batchMaxBytes(this.rmqClientConfig.batchMaxBytes);
                }
                if (null != this.rmqClientConfig.batchMaxDelayMs) {
                    producer.batchMaxDelayMs(this.rmqClientConfig.batchMaxDelayMs);
                }
                if (null != this.rmqClientConfig.totalBatchMaxBytes) {
                    producer.totalBatchMaxBytes(this.rmqClientConfig.totalBatchMaxBytes);
                }
                if (null != this.rmqClientConfig.enableBackpressure) {
                    producer.setEnableBackpressureForAsyncMode(this.rmqClientConfig.enableBackpressure);
                }
                if (null != this.rmqClientConfig.backpressureConcurrency) {
                    producer.setBackPressureForAsyncSendNum(this.rmqClientConfig.backpressureConcurrency);
                }
                producer.start();
                rmqProducer = producer;
            } catch (Exception e) {
                if (producer != null) {
                    shutdownAfterFailure(producer::shutdown, e);
                }
                return CompletableFuture.failedFuture(e);
            }
        }

        return CompletableFuture.completedFuture(new RocketMQBenchmarkProducer(rmqProducer, topic));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        DefaultMQPushConsumer rmqConsumer = null;
        try {
            rmqConsumer = newConsumer(subscriptionName, rpcHook);
            rmqConsumer.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
            rmqConsumer.setInstanceName("ConsumerInstance" + getRandomString());
            if (null != this.rmqClientConfig.vipChannelEnabled) {
                rmqConsumer.setVipChannelEnabled(this.rmqClientConfig.vipChannelEnabled);
            }
            rmqConsumer.subscribe(topic, "*");
            rmqConsumer.registerMessageListener(
                    (MessageListenerConcurrently)
                            (msgs, context) -> {
                                for (MessageExt message : msgs) {
                                    consumerCallback.messageReceived(message.getBody(), message.getBornTimestamp());
                                }
                                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                            });
            rmqConsumer.start();
        } catch (Exception e) {
            if (rmqConsumer != null) {
                shutdownAfterFailure(rmqConsumer::shutdown, e);
            }
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.completedFuture(new RocketMQBenchmarkConsumer(rmqConsumer));
    }

    public boolean isAclEnabled() {
        return !(StringUtils.isAnyBlank(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey)
                || StringUtils.isAnyEmpty(this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        DefaultMQProducer producer = rmqProducer;
        DefaultMQAdminExt admin = rmqAdmin;
        rmqProducer = null;
        rmqAdmin = null;
        RuntimeException failure = null;
        if (producer != null) {
            try {
                producer.shutdown();
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        if (admin != null) {
            try {
                admin.shutdown();
            } catch (RuntimeException e) {
                if (failure == null) {
                    failure = e;
                } else if (failure != e) {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void shutdownAfterFailure(Runnable shutdown, Throwable failure) {
        try {
            shutdown.run();
        } catch (RuntimeException closeFailure) {
            if (closeFailure != failure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper
                    .readerFor(RocketMQClientConfig.class)
                    .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private static RocketMQClientConfig readConfig(Path configurationFile) throws IOException {
        try (var input = Files.newInputStream(configurationFile)) {
            return reader.readValue(input);
        }
    }

    private static final Random random = new Random();

    private static String getRandomString() {
        byte[] buffer = new byte[5];
        random.nextBytes(buffer);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(buffer);
    }

    private static final Logger log = LoggerFactory.getLogger(RocketMQBenchmarkDriver.class);
}
