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
package io.github.mihkels.omb;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.TreeSet;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Expands a {@code dns://host:port} worker address, such as a Kubernetes headless Service, into one
 * {@code http://ip:port} URL per A/AAAA record.
 */
final class WorkerDiscovery {
    static final String PREFIX = "dns://";
    private static final long POLL_MILLIS = 1000;

    @FunctionalInterface
    interface Resolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private WorkerDiscovery() {}

    static boolean isDns(String worker) {
        return worker.startsWith(PREFIX);
    }

    static List<String> resolve(String worker, int expected, Duration timeout)
            throws InterruptedException {
        return resolve(
                worker,
                expected,
                timeout,
                InetAddress::getAllByName,
                System::currentTimeMillis,
                Thread::sleep);
    }

    // The JDK caches positive lookups for about 30s, so a partial answer can delay reaching
    // `expected` by up to that; changing networkaddress.cache.ttl would affect the whole JVM.
    static List<String> resolve(
            String worker,
            int expected,
            Duration timeout,
            Resolver resolver,
            LongSupplier clock,
            Sleeper sleeper)
            throws InterruptedException {
        URI uri = parse(worker);
        String host = uri.getHost();
        long deadline = clock.getAsLong() + timeout.toMillis();
        while (true) {
            List<String> urls = lookup(resolver, host, uri.getPort());
            if (urls.size() >= expected) {
                log.info("Resolved {} workers from {}: {}", urls.size(), worker, urls);
                return urls;
            }
            if (clock.getAsLong() >= deadline) {
                throw new IllegalStateException(
                        String.format(
                                Locale.ROOT,
                                "Found %d of %d expected workers at %s after %s: %s",
                                urls.size(),
                                expected,
                                worker,
                                timeout,
                                urls));
            }
            log.info("Found {} of {} expected workers at {}, retrying", urls.size(), expected, worker);
            sleeper.sleep(POLL_MILLIS);
        }
    }

    private static URI parse(String worker) {
        try {
            URI uri = new URI(worker);
            String path = uri.getRawPath();
            if (uri.getHost() != null
                    && uri.getPort() >= 0
                    && (path == null || path.isEmpty())
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null) {
                return uri;
            }
        } catch (URISyntaxException _) {
            // reported below
        }
        throw new IllegalArgumentException(
                "Worker DNS address must look like dns://<host>:<port>, got " + worker);
    }

    // Sorted and deduplicated so the worker order is stable between lookups
    private static List<String> lookup(Resolver resolver, String host, int port) {
        InetAddress[] addresses;
        try {
            addresses = resolver.resolve(host);
        } catch (UnknownHostException _) {
            // A headless Service has no records until its first pod is ready
            return List.of();
        }
        TreeSet<String> urls = new TreeSet<>();
        for (InetAddress address : addresses) {
            String ip = address.getHostAddress();
            String hostPart = address instanceof Inet6Address ? "[" + ip.replace("%", "%25") + "]" : ip;
            urls.add("http://" + hostPart + ":" + port);
        }
        return List.copyOf(urls);
    }

    private static final Logger log = LoggerFactory.getLogger(WorkerDiscovery.class);
}
