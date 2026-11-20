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
package io.github.mihkels.omb.driver.pulsar;

import io.github.mihkels.omb.driver.BenchmarkConsumer;
import io.github.mihkels.omb.driver.BenchmarkDriver;
import io.github.mihkels.omb.driver.BenchmarkProducer;
import io.github.mihkels.omb.driver.ConfiguredClasses;
import io.github.mihkels.omb.driver.ConsumerCallback;
import io.github.mihkels.omb.driver.DriverConfigs;
import io.github.mihkels.omb.driver.Failures;
import io.github.mihkels.omb.driver.ResourceCreator;
import io.github.mihkels.omb.driver.ResourceNames;
import io.github.mihkels.omb.driver.pulsar.config.PulsarClientConfig.PersistenceConfiguration;
import io.github.mihkels.omb.driver.pulsar.config.PulsarConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

@SuppressWarnings("unused")
public class PulsarBenchmarkDriver implements BenchmarkDriver {

    private volatile @Nullable PulsarClient client;
    private volatile @Nullable PulsarAdmin adminClient;

    private PulsarConfig config;

    private String namespace;
    private volatile @Nullable ProducerBuilder<byte[]> producerBuilder;

    // A file: URI anywhere a value starts, as in "file:///etc/shadow" or "privateKey":"file:/x"
    private static final Pattern FILE_REFERENCE = Pattern.compile("(?i)(?<![a-z0-9])file:");

    /**
     * Checks an authentication plugin from the driver config before Pulsar instantiates it. The
     * config arrives over the worker API, and Pulsar would construct any named class before checking
     * its type. Plugins such as {@code AuthenticationToken} also read {@code file:} references and
     * send the file to the broker, which the caller chooses, so those are refused: put the token
     * itself in {@code authentication.data}.
     *
     * @param plugin the authentication plugin class name
     * @param data the plugin parameters, or {@code null}
     */
    static void checkAuthentication(String plugin, @Nullable String data) {
        try {
            ConfiguredClasses.load(
                    plugin, Authentication.class, PulsarBenchmarkDriver.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown authentication plugin " + plugin, e);
        }
        if (data != null && FILE_REFERENCE.matcher(data).find()) {
            throw new IllegalArgumentException(
                    "authentication.data must not reference local files (file:); give the token or"
                            + " credentials inline");
        }
    }

    @Override
    public void initialize(Path configurationFile, MeterRegistry statsLogger) throws IOException {
        this.config = readConfig(configurationFile);
        String serviceUrl = required(config.client.serviceUrl, "client.serviceUrl");
        String httpUrl = required(config.client.httpUrl, "client.httpUrl");

        ClientBuilder clientBuilder =
                PulsarClient.builder()
                        .ioThreads(config.client.ioThreads)
                        .connectionsPerBroker(config.client.connectionsPerBroker)
                        .serviceUrl(serviceUrl)
                        .maxConcurrentLookupRequests(config.client.maxConcurrentLookupRequests)
                        .maxLookupRequests(Integer.MAX_VALUE)
                        .memoryLimit(config.client.clientMemoryLimitMB, SizeUnit.MEGA_BYTES)
                        .operationTimeout(10, TimeUnit.MINUTES)
                        .listenerThreads(Runtime.getRuntime().availableProcessors());

        if (serviceUrl.startsWith("pulsar+ssl")) {
            clientBuilder
                    .allowTlsInsecureConnection(config.client.tlsAllowInsecureConnection)
                    .enableTlsHostnameVerification(config.client.tlsEnableHostnameVerification)
                    .tlsTrustCertsFilePath(config.client.tlsTrustCertsFilePath);
        }

        PulsarAdminBuilder pulsarAdminBuilder = PulsarAdmin.builder().serviceHttpUrl(httpUrl);
        if (httpUrl.startsWith("https")) {
            pulsarAdminBuilder
                    .allowTlsInsecureConnection(config.client.tlsAllowInsecureConnection)
                    .enableTlsHostnameVerification(config.client.tlsEnableHostnameVerification)
                    .tlsTrustCertsFilePath(config.client.tlsTrustCertsFilePath);
        }

        if (!config.client.authentication.plugin.isEmpty()) {
            checkAuthentication(config.client.authentication.plugin, config.client.authentication.data);
            clientBuilder.authentication(
                    config.client.authentication.plugin, config.client.authentication.data);
            pulsarAdminBuilder.authentication(
                    config.client.authentication.plugin, config.client.authentication.data);
        }

        try {
            client = clientBuilder.build();

            log.info("Created Pulsar client");

            PulsarAdmin admin = pulsarAdminBuilder.build();
            adminClient = admin;

            log.info("Created Pulsar admin client");

            producerBuilder =
                    configureProducer(
                            client.newProducer(),
                            config.producer.batchingEnabled,
                            config.producer.batchingMaxPublishDelayMs,
                            config.producer.batchingMaxBytes,
                            config.producer.blockIfQueueFull,
                            config.producer.pendingQueueSize);

            namespace =
                    createNamespace(admin, required(config.client.namespacePrefix, "client.namespacePrefix"));
        } catch (IOException | RuntimeException failure) {
            Failures failures = new Failures();
            failures.add(failure);
            closeClients(failures);
            throw failure;
        }
    }

    // Batches are bounded by delay and bytes only, and sends never time out, so slow brokers show up
    // as latency rather than send failures. Shared with the KoP driver so both measure the same way.
    public static ProducerBuilder<byte[]> configureProducer(
            ProducerBuilder<byte[]> builder,
            boolean batchingEnabled,
            int batchingMaxPublishDelayMs,
            int batchingMaxBytes,
            boolean blockIfQueueFull,
            int pendingQueueSize) {
        return builder
                .enableBatching(batchingEnabled)
                .batchingMaxPublishDelay(batchingMaxPublishDelayMs, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(Integer.MAX_VALUE)
                .batchingMaxBytes(batchingMaxBytes)
                .blockIfQueueFull(blockIfQueueFull)
                .sendTimeout(0, TimeUnit.MILLISECONDS)
                .maxPendingMessages(pendingQueueSize);
    }

    // The persistence configuration is logged once per run, so eager formatting costs nothing
    // measurable
    @SuppressWarnings("java:S2629")
    private String createNamespace(PulsarAdmin adminClient, String namespacePrefix)
            throws IOException {
        try {
            // Create namespace and set the configuration
            String tenant = namespacePrefix.split("/")[0];
            String cluster = config.client.clusterName;
            ensureTenant(adminClient, tenant, cluster);
            log.info("Created Pulsar tenant {} with allowed cluster {}", tenant, cluster);

            String namespace = namespacePrefix + "-" + ResourceNames.randomSuffix();
            adminClient.namespaces().createNamespace(namespace);
            log.info("Created Pulsar namespace {}", namespace);

            PersistenceConfiguration p = config.client.persistence;
            adminClient
                    .namespaces()
                    .setPersistence(
                            namespace, new PersistencePolicies(p.ensembleSize, p.writeQuorum, p.ackQuorum, 1.0));

            adminClient
                    .namespaces()
                    .setBacklogQuota(
                            namespace,
                            BacklogQuota.builder()
                                    .limitSize(-1L)
                                    .limitTime(-1)
                                    .retentionPolicy(RetentionPolicy.producer_exception)
                                    .build());
            adminClient.namespaces().setDeduplicationStatus(namespace, p.deduplicationEnabled);
            log.info(
                    "Applied persistence configuration for namespace {}/{}/{}: {}",
                    tenant,
                    cluster,
                    namespace,
                    writer.writeValueAsString(p));
            return namespace;
        } catch (PulsarAdminException e) {
            throw new IOException(e);
        }
    }

    private static void ensureTenant(PulsarAdmin adminClient, String tenant, @Nullable String cluster)
            throws PulsarAdminException {
        if (adminClient.tenants().getTenants().contains(tenant)) {
            return;
        }
        try {
            adminClient
                    .tenants()
                    .createTenant(
                            tenant,
                            TenantInfo.builder().adminRoles(Set.of()).allowedClusters(Set.of(cluster)).build());
        } catch (ConflictException _) {
            // Ignore. This can happen when multiple workers are initializing at the same time
        } catch (PulsarAdminException e) {
            // Some metadata stores report the concurrent creation as a server error instead of
            // a conflict, so only fail when the tenant really does not exist
            if (!adminClient.tenants().getTenants().contains(tenant)) {
                throw e;
            }
        }
    }

    @Override
    public boolean supportsHeaders() {
        return true;
    }

    @Override
    public String getTopicNamePrefix() {
        return config.client.topicType + "://" + namespace + "/test";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        try {
            if (partitions == 1) {
                // No-op
                return CompletableFuture.completedFuture(null);
            }
            return requireOpen(adminClient).topics().createPartitionedTopicAsync(topic, partitions);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        try {
            ProducerBuilder<byte[]> producerBuilder = requireOpen(this.producerBuilder);
            return ResourceCreator.<String, Producer<byte[]>, BenchmarkProducer>compose(
                    CompletableFuture.completedFuture(List.of(topic)),
                    name -> producerBuilder.clone().topic(name).createAsync(),
                    producers -> new PulsarBenchmarkProducer(producers.getFirst()));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        try {
            return ResourceCreator.<String, Consumer<ByteBuffer>, BenchmarkConsumer>compose(
                    requireOpen(client).getPartitionsForTopic(topic, true),
                    partition -> createInternalConsumer(partition, subscriptionName, consumerCallback),
                    PulsarBenchmarkConsumer::new);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    CompletableFuture<Consumer<ByteBuffer>> createInternalConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        return requireOpen(client)
                .newConsumer(Schema.BYTEBUFFER)
                .priorityLevel(0)
                .subscriptionType(config.consumer.subscriptionType)
                .messageListener((c, msg) -> {
                    try {
                        consumerCallback.messageReceived(msg.getValue(), msg.getPublishTime());
                        c.acknowledgeAsync(msg);
                    } finally {
                        msg.release();
                    }
                })
                .topic(topic)
                .subscriptionName(subscriptionName)
                .receiverQueueSize(config.consumer.receiverQueueSize)
                .maxTotalReceiverQueueSizeAcrossPartitions(Integer.MAX_VALUE)
                .poolMessages(true)
                .subscribeAsync();
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down Pulsar benchmark driver");

        Failures failures = new Failures();
        closeClients(failures);
        failures.throwIfAny();

        log.info("Pulsar benchmark driver successfully shut down");
    }

    private void closeClients(Failures failures) {
        PulsarClient clientToClose;
        PulsarAdmin adminToClose;
        synchronized (this) {
            clientToClose = client;
            adminToClose = adminClient;
            client = null;
            adminClient = null;
            producerBuilder = null;
        }
        failures.closeRestoringInterruptNow(clientToClose);
        failures.closeRestoringInterruptNow(adminToClose);
    }

    private static String required(@Nullable String value, String key) {
        if (value == null) {
            throw new IllegalArgumentException("Pulsar driver config needs " + key);
        }
        return value;
    }

    private static <T> T requireOpen(@Nullable T resource) {
        if (resource == null) {
            throw new IllegalStateException(
                    "Pulsar benchmark driver is not initialized or already closed");
        }
        return resource;
    }

    private static PulsarConfig readConfig(Path configurationFile) throws IOException {
        return DriverConfigs.read(configurationFile, PulsarConfig.class);
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
    private static final Logger log = LoggerFactory.getLogger(PulsarBenchmarkDriver.class);
}
