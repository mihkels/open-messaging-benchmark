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

import static org.assertj.core.api.Assertions.assertThat;

import com.github.brainlag.nsq.ServerAddress;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class NsqLookupTest {
    @Test
    void requiresThePublishersAdvertisedIdentityInRealLookupResults() throws Exception {
        AtomicReference<String> body =
                new AtomicReference<>(
                        "{\"producers\":[{\"broadcast_address\":\"other\",\"tcp_port\":4150}]}");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/lookup", exchange -> respond(exchange, body.get()));
        server.createContext(
                "/info",
                exchange -> respond(exchange, "{\"broadcast_address\":\"publisher\",\"tcp_port\":4150}"));
        server.start();
        try (HttpClient http = HttpClient.newHttpClient()) {
            NsqLookup lookup =
                    new NsqLookup(http, Duration.ofSeconds(2), "127.0.0.1", server.getAddress().getPort());
            lookup.addLookupAddress("127.0.0.1", server.getAddress().getPort());
            Set<ServerAddress> other = lookup.lookup("topic");
            assertThat(other).containsExactly(new ServerAddress("other", 4150));
            assertThat(lookup.includesPublishingNode(other)).isFalse();

            body.set(
                    "{\"producers\":[{\"broadcast_address\":\"publisher\",\"tcp_port\":4150},"
                            + "{\"broadcast_address\":\"other\",\"tcp_port\":4150}]}");
            Set<ServerAddress> complete = lookup.lookup("topic");
            assertThat(complete)
                    .containsExactlyInAnyOrder(
                            new ServerAddress("publisher", 4150), new ServerAddress("other", 4150));
            assertThat(lookup.includesPublishingNode(complete)).isTrue();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void malformedLookupDoesNotExposeAFragmentOfItsNodeSet() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/lookup",
                exchange ->
                        respond(
                                exchange,
                                "{\"producers\":[{\"broadcast_address\":\"good\",\"tcp_port\":4150},"
                                        + "{\"broadcast_address\":\"bad\",\"tcp_port\":0}]}"));
        server.start();
        try (HttpClient http = HttpClient.newHttpClient()) {
            NsqLookup lookup =
                    new NsqLookup(http, Duration.ofSeconds(2), "127.0.0.1", server.getAddress().getPort());
            lookup.addLookupAddress("127.0.0.1", server.getAddress().getPort());
            assertThat(lookup.lookup("topic")).isEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void interruptionCancelsAnInProgressLookupAndRemainsVisible() throws Exception {
        CountDownLatch requestReceived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/lookup",
                exchange -> {
                    requestReceived.countDown();
                    try {
                        release.await();
                        respond(exchange, "{\"producers\":[]}");
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (HttpClient http = HttpClient.newHttpClient()) {
            NsqLookup lookup =
                    new NsqLookup(http, Duration.ofSeconds(5), "127.0.0.1", server.getAddress().getPort());
            lookup.addLookupAddress("127.0.0.1", server.getAddress().getPort());
            AtomicReference<Thread> thread = new AtomicReference<>();
            AtomicBoolean interrupted = new AtomicBoolean();
            Future<?> result =
                    worker.submit(() -> {
                        thread.set(Thread.currentThread());
                        assertThat(lookup.lookup("topic")).isEmpty();
                        interrupted.set(Thread.currentThread().isInterrupted());
                    });
            try {
                assertThat(requestReceived.await(5, TimeUnit.SECONDS)).isTrue();
                thread.get().interrupt();
                result.get(5, TimeUnit.SECONDS);
                assertThat(interrupted).isTrue();
            } finally {
                release.countDown();
            }
        } finally {
            worker.shutdownNow();
            server.stop(0);
        }
    }

    @Test
    void rejectedOrMalformedLookupResponsesYieldNoProducers() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/lookup",
                exchange -> {
                    String topic = exchange.getRequestURI().getQuery().substring("topic=".length());
                    switch (topic) {
                        case "unavailable" -> respond(exchange, 503, "{}");
                        case "missing" -> respond(exchange, "{\"channels\":[]}");
                        case "object" -> respond(exchange, "{\"producers\":{}}");
                        case "nohost" -> respond(exchange, "{\"producers\":[{\"tcp_port\":4150}]}");
                        case "bigport" ->
                                respond(
                                        exchange, "{\"producers\":[{\"broadcast_address\":\"h\",\"tcp_port\":70000}]}");
                        default ->
                                respond(
                                        exchange, "{\"producers\":[{\"broadcast_address\":\"h\",\"tcp_port\":4150}]}");
                    }
                });
        server.start();
        try (HttpClient http = HttpClient.newHttpClient()) {
            NsqLookup lookup =
                    new NsqLookup(http, Duration.ofSeconds(2), "127.0.0.1", server.getAddress().getPort());
            lookup.addLookupAddress("http://127.0.0.1", server.getAddress().getPort());

            assertThat(lookup.lookup("unavailable")).isEmpty();
            assertThat(lookup.lookup("missing")).isEmpty();
            assertThat(lookup.lookup("object")).isEmpty();
            assertThat(lookup.lookup("nohost")).isEmpty();
            assertThat(lookup.lookup("bigport")).isEmpty();
            assertThat(lookup.lookup("ok")).containsExactly(new ServerAddress("h", 4150));
        } finally {
            server.stop(0);
        }
    }

    @Test
    void publisherIdentityIsCachedOnceSeenAndUnavailableIdentityIsNotPresent() throws Exception {
        AtomicReference<Integer> status = new AtomicReference<>(503);
        AtomicInteger infoRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/info",
                exchange -> {
                    infoRequests.incrementAndGet();
                    respond(
                            exchange, status.get(), "{\"broadcast_address\":\"publisher\",\"tcp_port\":4150}");
                });
        server.start();
        try (HttpClient http = HttpClient.newHttpClient()) {
            NsqLookup lookup =
                    new NsqLookup(http, Duration.ofSeconds(2), "127.0.0.1", server.getAddress().getPort());
            Set<ServerAddress> discovered = Set.of(new ServerAddress("publisher", 4150));

            assertThat(lookup.includesPublishingNode(discovered)).isFalse();
            status.set(200);
            assertThat(lookup.includesPublishingNode(discovered)).isTrue();
            assertThat(lookup.includesPublishingNode(discovered)).isTrue();
            assertThat(lookup.includesPublishingNode(Set.of())).isFalse();
            assertThat(infoRequests.get()).isEqualTo(2);
        } finally {
            server.stop(0);
        }
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        respond(exchange, 200, body);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (exchange) {
            exchange.getResponseBody().write(bytes);
        }
    }
}
