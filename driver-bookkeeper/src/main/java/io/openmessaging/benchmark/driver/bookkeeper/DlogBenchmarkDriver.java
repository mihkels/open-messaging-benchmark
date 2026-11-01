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
package io.openmessaging.benchmark.driver.bookkeeper;

import dlshade.org.apache.bookkeeper.stats.CachingStatsLogger;
import dlshade.org.apache.distributedlog.DLSN;
import dlshade.org.apache.distributedlog.DistributedLogConfiguration;
import dlshade.org.apache.distributedlog.api.AsyncLogWriter;
import dlshade.org.apache.distributedlog.api.DistributedLogManager;
import dlshade.org.apache.distributedlog.api.LogReader;
import dlshade.org.apache.distributedlog.api.namespace.Namespace;
import dlshade.org.apache.distributedlog.api.namespace.NamespaceBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreator;
import io.openmessaging.benchmark.driver.bookkeeper.stats.StatsLoggerAdaptor;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Benchmark driver testing distributedlog. */
public class DlogBenchmarkDriver implements BenchmarkDriver {

    private static final Logger log = LoggerFactory.getLogger(DlogBenchmarkProducer.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private static final ObjectReader reader =
            mapper.readerFor(Config.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Config config;
    private Namespace namespace;

    public DlogBenchmarkDriver() {}

    DlogBenchmarkDriver(Namespace namespace) {
        this.namespace = namespace;
    }

    @Override
    public void initialize(Path configurationFile, MeterRegistry meterRegistry) throws IOException {
        config = reader.readValue(Files.newInputStream(configurationFile));

        DistributedLogConfiguration conf = new DistributedLogConfiguration();
        try {
            PropertiesConfiguration propsConf = new PropertiesConfiguration();
            propsConf.read(new StringReader(config.dlogConf));
            conf.loadConf(propsConf);
        } catch (ConfigurationException e) {
            log.error("Failed to load dlog configuration : \n{}\n", config.dlogConf, e);
            throw new IOException("Failed to load configuration : \n" + config.dlogConf + "\n", e);
        }
        URI dlogUri = URI.create(config.dlogUri);

        // BookKeeper client metrics are not exported.
        org.apache.bookkeeper.stats.StatsLogger noOpStatsLogger =
                new org.apache.bookkeeper.stats.NullStatsLogger();
        dlshade.org.apache.bookkeeper.stats.StatsLogger dlStatsLogger =
                new CachingStatsLogger(new StatsLoggerAdaptor(noOpStatsLogger));

        namespace =
                NamespaceBuilder.newBuilder().conf(conf).uri(dlogUri).statsLogger(dlStatsLogger).build();

        log.info("Initialized distributedlog namespace at {}", dlogUri);
    }

    @Override
    public String getTopicNamePrefix() {
        return "test-stream";
    }

    private static String getFullyQualifiedPartitionedStreamName(String topic, int partitionIdx) {
        return String.format("%s/%06d", topic, partitionIdx);
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        namespace.createLog(topic);
                        if (partitions > 1) {
                            for (int i = 0; i < partitions; i++) {
                                namespace.createLog(getFullyQualifiedPartitionedStreamName(topic, i));
                            }
                        }
                        log.info("Successfully create topic {} with {} partitions", topic, partitions);
                    } catch (IOException ioe) {
                        log.error("Failed to create topic {} with {} partitions", topic, partitions, ioe);
                        throw new RuntimeException(ioe);
                    }
                });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(String topic) {
        CompletableFuture<DistributedLogManager> manager = openLog(topic);
        CompletableFuture<AsyncLogWriter> writer =
                manager.thenComposeAsync(DistributedLogManager::openAsyncLogWriter);
        CompletableFuture<AutoCloseable> ownedManager = manager.thenApply(dlm -> dlm);
        // Abandoned writer cleanup must not block a DistributedLog completion thread.
        CompletableFuture<AutoCloseable> ownedWriter = writer.thenApplyAsync(OwnedWriter::new);
        return ResourceCreator.compose(
                CompletableFuture.completedFuture(List.of(ownedManager, ownedWriter)),
                Function.identity(),
                resources ->
                        new DlogBenchmarkProducer(
                                (DistributedLogManager) resources.get(0),
                                ((OwnedWriter) resources.get(1)).writer()));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        CompletableFuture<DistributedLogManager> manager = openLog(topic);
        CompletableFuture<LogReader> initialReader =
                manager.thenApplyAsync(
                        dlm -> {
                            try {
                                return Objects.requireNonNull(
                                        dlm.openLogReader(DLSN.InitialDLSN), "initial log reader");
                            } catch (IOException e) {
                                throw new CompletionException(e);
                            }
                        });
        CompletableFuture<AutoCloseable> ownedManager = manager.thenApply(dlm -> dlm);
        CompletableFuture<AutoCloseable> ownedReader = initialReader.thenApply(reader -> reader);
        return ResourceCreator.compose(
                CompletableFuture.completedFuture(List.of(ownedManager, ownedReader)),
                Function.identity(),
                resources ->
                        new DlogBenchmarkConsumer(
                                (DistributedLogManager) resources.get(0),
                                (LogReader) resources.get(1),
                                consumerCallback));
    }

    private CompletableFuture<DistributedLogManager> openLog(String topic) {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        return Objects.requireNonNull(namespace.openLog(topic), "log manager");
                    } catch (IOException e) {
                        throw new CompletionException(e);
                    }
                });
    }

    private record OwnedWriter(AsyncLogWriter writer) implements AutoCloseable {
        private OwnedWriter {
            Objects.requireNonNull(writer, "log writer");
        }

        @Override
        public void close() throws Exception {
            writer.asyncClose().get();
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down BookKeeper benchmark driver");

        if (null != namespace) {
            namespace.close();
        }

        log.info("BookKeeper benchmark driver successfully shut down");
    }
}
