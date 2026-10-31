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
package io.openmessaging.benchmark.driver.nsq;

import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQProducer;
import com.github.brainlag.nsq.lookup.DefaultNSQLookup;
import com.github.brainlag.nsq.lookup.NSQLookup;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

public class NsqBenchmarkDriver implements BenchmarkDriver {
    private static final int NSQD_HTTP_PORT = 4151;

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private NsqConfig config;

    @Override
    public void initialize(Path configurationFile, PrometheusMeterRegistry statsLogger)
            throws IOException {
        config = reader.readValue(Files.newInputStream(configurationFile));
        log.info("read config file," + config.toString());
    }

    @Override
    public String getTopicNamePrefix() {
        return "Nsq-Benchmark";
    }

    @Override
    public CompletableFuture<Void> createTopic(String topic, int partitions) {
        // Create the topic up front so nsqd registers it with nsqlookupd before consumers look it up;
        // otherwise consumers only find it at their next periodic lookup
        log.info("Creating topic {} on nsqd {} (partitions are ignored)", topic, config.nsqdHost);
        HttpRequest request =
                HttpRequest.newBuilder(
                                URI.create(
                                        "http://"
                                                + config.nsqdHost
                                                + ":"
                                                + NSQD_HTTP_PORT
                                                + "/topic/create?topic="
                                                + URLEncoder.encode(topic, StandardCharsets.UTF_8)))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
        return httpClient
                .sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenAccept(
                        response -> {
                            if (response.statusCode() != 200) {
                                throw new IllegalStateException(
                                        "Failed to create NSQ topic "
                                                + topic
                                                + ": HTTP "
                                                + response.statusCode()
                                                + " "
                                                + response.body());
                            }
                        });
    }

    @Override
    public CompletableFuture<BenchmarkProducer> createProducer(final String topic) {
        NSQProducer nsqProducer = new NSQProducer();
        nsqProducer.addAddress(config.nsqdHost, 4150);
        nsqProducer.start();
        log.info("start a nsq producer");

        return CompletableFuture.completedFuture(new NsqBenchmarkProducer(nsqProducer, topic));
    }

    @Override
    public CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback) {
        // Channel can be treat as subscriptionName
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(config.lookupHost, 4161);
        NSQConsumer nsqConsumer =
                new NSQConsumer(
                        lookup,
                        topic,
                        subscriptionName,
                        (message) -> {
                            // now mark the message as finished.
                            consumerCallback.messageReceived(
                                    message.getMessage(), message.getTimestamp().getTime());
                            message.finished();

                            // or you could requeue it, which indicates a failure and puts it back on the queue.
                            // message.requeue();
                        });

        nsqConsumer.start();
        log.info("start a nsq consumer");

        return CompletableFuture.completedFuture(new NsqBenchmarkConsumer(nsqConsumer));
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    private static final Logger log = LoggerFactory.getLogger(NsqBenchmarkDriver.class);
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final ObjectReader reader =
            mapper.readerFor(NsqConfig.class).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
}
