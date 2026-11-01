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

import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.lookup.NSQLookup;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

final class NsqLookup implements NSQLookup {
    private static final Logger log = LoggerFactory.getLogger(NsqLookup.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final HttpClient httpClient;
    private final Duration timeout;
    private final String nsqdHost;
    private final int nsqdHttpPort;
    private final Set<String> addresses = new HashSet<>();
    private volatile ServerAddress publishingNode;

    NsqLookup(HttpClient httpClient, Duration timeout, String nsqdHost, int nsqdHttpPort) {
        this.httpClient = httpClient;
        this.timeout = timeout;
        this.nsqdHost = nsqdHost;
        this.nsqdHttpPort = nsqdHttpPort;
    }

    @Override
    public synchronized void addLookupAddress(String address, int port) {
        String base = address.startsWith("http") ? address : "http://" + address;
        addresses.add(base + ":" + port);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        List<String> endpoints;
        synchronized (this) {
            endpoints = new ArrayList<>(addresses);
        }
        Set<ServerAddress> producers = new HashSet<>();
        for (String endpoint : endpoints) {
            try {
                URI uri =
                        URI.create(
                                endpoint + "/lookup?topic=" + URLEncoder.encode(topic, StandardCharsets.UTF_8));
                HttpResponse<String> response = request(uri);
                if (response.statusCode() != 200) {
                    log.debug("NSQ lookup for {} returned HTTP {}", topic, response.statusCode());
                    continue;
                }
                JsonNode nodes = mapper.readTree(response.body()).findValue("producers");
                if (nodes == null || !nodes.isArray()) {
                    throw new IllegalStateException("Missing producers in NSQ lookup response");
                }
                Set<ServerAddress> endpointProducers = new HashSet<>();
                for (JsonNode node : nodes) {
                    endpointProducers.add(serverAddress(node));
                }
                producers.addAll(endpointProducers);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return producers;
            } catch (Exception failure) {
                log.debug("Unable to query NSQ lookup endpoint {} for {}", endpoint, topic, failure);
            }
        }
        return producers;
    }

    boolean includesPublishingNode(Set<ServerAddress> discovered) {
        ServerAddress expected = publishingNode;
        if (expected == null) {
            try {
                URI uri = URI.create("http://" + nsqdHost + ":" + nsqdHttpPort + "/info");
                HttpResponse<String> response = request(uri);
                if (response.statusCode() != 200) {
                    log.debug("NSQ publisher identity returned HTTP {}", response.statusCode());
                    return false;
                }
                expected = serverAddress(mapper.readTree(response.body()));
                if (discovered.contains(expected)) {
                    publishingNode = expected;
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return false;
            } catch (Exception failure) {
                log.debug("Unable to read NSQ publisher identity", failure);
                return false;
            }
        }
        return discovered.contains(expected);
    }

    private HttpResponse<String> request(URI uri) throws Exception {
        CompletableFuture<HttpResponse<String>> pending =
                httpClient.sendAsync(
                        HttpRequest.newBuilder(uri).timeout(timeout).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
        try {
            return pending.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception failure) {
            pending.cancel(true);
            throw failure;
        }
    }

    private static ServerAddress serverAddress(JsonNode node) {
        String host = node.path("broadcast_address").asString("");
        int port = node.path("tcp_port").asInt(0);
        if (host.isEmpty() || port < 1 || port > 65535) {
            throw new IllegalStateException("Invalid NSQ producer identity");
        }
        return new ServerAddress(host, port);
    }
}
