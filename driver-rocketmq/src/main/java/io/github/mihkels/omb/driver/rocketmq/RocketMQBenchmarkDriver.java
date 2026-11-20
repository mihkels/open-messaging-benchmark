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
package io.github.mihkels.omb.driver.rocketmq;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.Failures;
import io.github.mihkels.omb.driver.ResourceNames;
import io.github.mihkels.omb.driver.rocketmq.client.RocketMQClientConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQBenchmarkDriver implements BenchmarkDriver {
    private @Nullable DefaultMQAdminExt rmqAdmin;
    private RocketMQClientConfig rmqClientConfig;
    private String clusterName;
    @Nullable DefaultMQProducer rmqProducer;
    private @Nullable RPCHook rpcHook;
    private boolean closed;

    @Override
    public synchronized void initialize(final Path configurationFile, final MeterRegistry statsLogger)
            throws IOException {
        this.rmqClientConfig = readConfig(configurationFile);
        String configuredCluster = rmqClientConfig.clusterName;
        if (configuredCluster == null) {
            throw new IllegalArgumentException("RocketMQ config needs clusterName");
        }
        this.clusterName = configuredCluster;
        if (isAclEnabled()) {
            rpcHook =
                    new AclClientRPCHook(
                            new SessionCredentials(
                                    this.rmqClientConfig.accessKey, this.rmqClientConfig.secretKey));
        }
        DefaultMQAdminExt admin = newAdmin(rpcHook);
        try {
            admin.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
            admin.setInstanceName("AdminInstance-" + ResourceNames.randomSuffix());
            admin.start();
        } catch (MQClientException e) {
            Failures.closeAfterFailure(e, admin::shutdown);
            throw new IOException("Failed to start the RocketMQ admin tool", e);
        } catch (RuntimeException e) {
            Failures.closeAfterFailure(e, admin::shutdown);
            throw e;
        }
        this.rmqAdmin = admin;
    }

    DefaultMQAdminExt newAdmin(@Nullable RPCHook hook) {
        return hook == null ? new DefaultMQAdminExt() : new DefaultMQAdminExt(hook);
    }

    DefaultMQProducer newProducer(String group, @Nullable RPCHook hook) {
        return hook == null ? new DefaultMQProducer(group) : new DefaultMQProducer(group, hook);
    }

    DefaultMQPushConsumer newConsumer(String subscription, @Nullable RPCHook hook) {
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

    private synchronized Set<String> fetchMasterAddrByClusterName(
            final MQAdminExt adminExt, final String clusterName)
            throws RemotingConnectException,
                    RemotingSendRequestException,
                    RemotingTimeoutException,
                    MQBrokerException,
                    InterruptedException {
        if (fetchCnt++ % 100 == 0) {
            log.info("fetch brokerAddr count: {}", fetchCnt);
        }
        Set<String> cached = cachedBrokerAddr.get(clusterName);
        if (cached != null) {
            return cached;
        }
        Set<String> brokerList = CommandUtil.fetchMasterAddrByClusterName(adminExt, this.clusterName);
        if (brokerList.isEmpty()) {
            throw new IllegalStateException("get brokerAddr return null, clusterName: " + clusterName);
        }
        cachedBrokerAddr.put(clusterName, brokerList);
        return brokerList;
    }

    @Override
    public CompletableFuture<Void> createTopic(final String topic, final int partitions) {
        return CompletableFuture.runAsync(() -> {
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setOrder(false);
            topicConfig.setPerm(6);
            topicConfig.setReadQueueNums(partitions);
            topicConfig.setWriteQueueNums(partitions);
            topicConfig.setTopicName(topic);
            if (this.rmqClientConfig.batchCQ) {
                topicConfig
                        .getAttributes()
                        .put("+" + TopicAttributes.QUEUE_TYPE_ATTRIBUTE.getName(), "BatchCQ");
            }

            try {
                DefaultMQAdminExt admin = admin();
                String[] masters =
                        fetchMasterAddrByClusterName(admin, this.clusterName).toArray(String[]::new);
                createOnMasters(admin, masters, topicConfig, partitions);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(
                        String.format("Failed to create topic [%s] to cluster [%s]", topic, this.clusterName),
                        e);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format("Failed to create topic [%s] to cluster [%s]", topic, this.clusterName),
                        e);
            }
        });
    }

    static int queuesForMaster(int partitions, int masters, int index) {
        return partitions / masters + (index < partitions % masters ? 1 : 0);
    }

    // Masters only: slaves sync topic config from their master.
    private static void createOnMasters(
            MQAdminExt admin, String[] masters, TopicConfig topicConfig, int partitions)
            throws InterruptedException, MQBrokerException, MQClientException, RemotingException {
        for (int i = 0; i < Math.min(partitions, masters.length); i++) {
            topicConfig.setReadQueueNums(queuesForMaster(partitions, masters.length, i));
            topicConfig.setWriteQueueNums(topicConfig.getReadQueueNums());
            admin.createAndUpdateTopicConfig(masters[i], topicConfig);
        }
    }

    private synchronized DefaultMQAdminExt admin() {
        DefaultMQAdminExt admin = rmqAdmin;
        if (admin == null) {
            throw new IllegalStateException("RocketMQ driver is closed");
        }
        return admin;
    }

    @Override
    public synchronized CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("RocketMQ driver is closed"));
        }
        DefaultMQProducer shared = rmqProducer;
        if (shared == null) {
            DefaultMQProducer producer = null;
            try {
                producer = newProducer("ProducerGroup_" + ResourceNames.randomSuffix(), rpcHook);
                configureProducer(producer);
                producer.start();
                rmqProducer = producer;
                shared = producer;
            } catch (Exception e) {
                if (producer != null) {
                    Failures.closeAfterFailure(e, producer::shutdown);
                }
                return CompletableFuture.failedFuture(e);
            }
        }

        return CompletableFuture.completedFuture(new RocketMQBenchmarkProducer(shared, topic));
    }

    private void configureProducer(DefaultMQProducer producer) {
        producer.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
        producer.setInstanceName("ProducerInstance" + ResourceNames.randomSuffix());
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
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            final String topic, final String subscriptionName, final ConsumerCallback consumerCallback) {
        DefaultMQPushConsumer rmqConsumer = null;
        try {
            rmqConsumer = newConsumer(subscriptionName, rpcHook);
            rmqConsumer.setNamesrvAddr(this.rmqClientConfig.namesrvAddr);
            rmqConsumer.setInstanceName("ConsumerInstance" + ResourceNames.randomSuffix());
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
                Failures.closeAfterFailure(e, rmqConsumer::shutdown);
            }
            return CompletableFuture.failedFuture(e);
        }

        return CompletableFuture.completedFuture(new RocketMQBenchmarkConsumer(rmqConsumer));
    }

    public boolean isAclEnabled() {
        return !rmqClientConfig.accessKey.isBlank() && !rmqClientConfig.secretKey.isBlank();
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
        Failures failures = new Failures();
        if (producer != null) {
            failures.close(producer::shutdown);
        }
        if (admin != null) {
            failures.close(admin::shutdown);
        }
        failures.throwIfAny(RuntimeException.class);
    }

    private static RocketMQClientConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, RocketMQClientConfig.class);
    }

    private static final Logger log = LoggerFactory.getLogger(RocketMQBenchmarkDriver.class);
}
