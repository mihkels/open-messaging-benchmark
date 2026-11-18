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
package io.openmessaging.benchmark.worker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.javalin.Javalin;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.openmessaging.benchmark.metrics.otlp.OtlpExport;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

/** A benchmark worker that listen for tasks to perform. */
public final class BenchmarkWorker {

    private BenchmarkWorker() {
        throw new UnsupportedOperationException(
                "BenchmarkWorker is a CLI utility and cannot be instantiated");
    }

    @SuppressWarnings("unused")
    static class Arguments {

        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        private boolean help;

        @Parameter(
                names = {"-p", "--port"},
                description = "HTTP port to listen on")
        private int httpPort = 8080;

        @Parameter(
                names = {"-b", "--bind"},
                description = "Address to listen on; use 127.0.0.1 to accept only local connections")
        private String bindAddress = "0.0.0.0";

        @Parameter(
                names = {"-sp", "--stats-port"},
                description = "Deprecated and ignored; accepted so existing launch scripts keep working")
        // Deprecated since 0.0.3. Kept so existing launch scripts still parse; only read to warn.
        private @Nullable Integer statsPort;

        @Parameter(
                names = "--token-file",
                description =
                        "File holding the shared bearer token that every request must send; overrides the "
                                + BearerTokenAuth.TOKEN_ENV
                                + " environment variable. Required unless --bind is a loopback address or"
                                + " --insecure-no-auth is set")
        private @Nullable Path tokenFile;

        @Parameter(
                names = "--insecure-no-auth",
                description =
                        "Allow an unauthenticated API on a non-loopback --bind address; anyone who can reach"
                                + " the port can run arbitrary driver configurations on this worker")
        private boolean insecureNoAuth;

        @Parameter(
                names = "--protect-metrics",
                description = "Require the bearer token on /metrics too (it is open by default)")
        private boolean protectMetrics;

        @Parameter(
                names = "--tls-cert",
                description =
                        "PEM certificate chain; with --tls-key, the API is served over HTTPS instead of HTTP."
                                + " Create one with bin/benchmark-tls-cert")
        private @Nullable Path tlsCert;

        @Parameter(
                names = "--tls-key",
                description = "PEM private key (PKCS#8 or PKCS#1 RSA) for --tls-cert")
        private @Nullable Path tlsKey;

        public boolean isHelp() {
            return help;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public String getBindAddress() {
            return bindAddress;
        }
    }

    /**
     * Builds the request authenticator from the arguments and environment.
     *
     * @param arguments the parsed command line
     * @param environment the process environment
     * @return the authenticator, or {@code null} when no token is configured
     */
    static @Nullable BearerTokenAuth authFor(Arguments arguments, Map<String, String> environment) {
        return BearerTokenAuth.resolveToken(environment, arguments.tokenFile)
                .map(token ->
                        new BearerTokenAuth(
                                token,
                                arguments.protectMetrics
                                        ? Set.of(WorkerHandler.HEALTH)
                                        : Set.of(WorkerHandler.HEALTH, HttpWorkerClient.METRICS_PATH)))
                .orElse(null);
    }

    /**
     * Checks that a worker without a token is reachable only from this host, unless the operator
     * opted out with {@code --insecure-no-auth}.
     *
     * @param arguments the parsed command line
     * @param auth the authenticator from {@link #authFor}, or {@code null} when no token is
     *     configured
     * @return {@code null} when the worker may start, otherwise the reason it may not
     * @throws UnknownHostException when the bind address does not resolve
     */
    static @Nullable String refuseUnauthenticated(Arguments arguments, @Nullable BearerTokenAuth auth)
            throws UnknownHostException {
        if (auth != null
                || arguments.insecureNoAuth
                || InetAddress.getByName(arguments.bindAddress).isLoopbackAddress()) {
            return null;
        }
        return "No worker token configured and --bind "
                + arguments.bindAddress
                + " is not a loopback address. Set "
                + BearerTokenAuth.TOKEN_ENV
                + " or --token-file, bind to 127.0.0.1, or pass --insecure-no-auth to run the API"
                + " unauthenticated";
    }

    /**
     * Checks that TLS is either fully configured or not at all.
     *
     * @param arguments the parsed command line
     * @return {@code null} when the TLS options are consistent, otherwise the reason they are not
     */
    static @Nullable String refuseIncompleteTls(Arguments arguments) {
        if ((arguments.tlsCert == null) != (arguments.tlsKey == null)) {
            return "--tls-cert and --tls-key must be given together";
        }
        return null;
    }

    /**
     * The supervisor metrics followed by the driver metrics, if the driver can be scraped.
     *
     * @param supervisor the supervisor metrics
     * @param driver scrapes the driver process
     * @return the metrics; the supervisor metrics alone when the driver scrape fails
     */
    static String metrics(String supervisor, Supplier<String> driver) {
        try {
            return supervisor + driver.get();
        } catch (RuntimeException e) {
            // Scraped every few seconds; /health reports what went wrong with the driver
            log.debug("Driver metrics unavailable", e);
            return supervisor;
        }
    }

    // WorkerHandler lives for the whole process; closed by the shutdown hook
    @SuppressWarnings("java:S2095")
    static void main(String[] args)
            throws InterruptedException, IOException, GeneralSecurityException {
        final Arguments arguments = new Arguments();
        JCommander jc = new JCommander(arguments);
        jc.setProgramName("benchmark-worker");

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jc.usage();
            System.exit(-1);
        }

        if (arguments.help) {
            jc.usage();
            System.exit(0);
        }

        if (arguments.statsPort != null) {
            log.warn("--stats-port is deprecated and ignored; metrics are served on --port");
        }

        PrometheusMeterRegistry prometheusRegistry =
                new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MeterRegistry registry = OtlpExport.withOtlp(prometheusRegistry, System.getenv());
        new JvmResourceMetrics().bindTo(registry, "supervisor");

        BearerTokenAuth auth = authFor(arguments, System.getenv());
        String refusal = refuseIncompleteTls(arguments);
        if (refusal == null) {
            refusal = refuseUnauthenticated(arguments, auth);
        }
        if (refusal != null) {
            System.err.println(refusal);
            System.exit(-1);
        }
        if (auth == null) {
            log.warn(
                    "No worker token configured (set {} or --token-file); anyone who can reach port {}"
                            + " can control this worker",
                    BearerTokenAuth.TOKEN_ENV,
                    arguments.getHttpPort());
        } else if (arguments.protectMetrics) {
            log.info(
                    "Bearer token required on every request except {}, including {}",
                    WorkerHandler.HEALTH,
                    HttpWorkerClient.METRICS_PATH);
        } else {
            log.info(
                    "Bearer token required on every request except {} and {}",
                    WorkerHandler.HEALTH,
                    HttpWorkerClient.METRICS_PATH);
        }

        // Dump configuration variables
        log.atInfo()
                .setMessage("Starting benchmark with config: {}")
                .addArgument(() -> writer.writeValueAsString(arguments))
                .log();

        // Drivers run in a child process with only their own plugin bundle on the classpath
        ProcessWorker worker = new ProcessWorker();
        WorkerHandler handler = new WorkerHandler(worker, auth);
        var connector =
                arguments.tlsCert != null && arguments.tlsKey != null
                        ? WorkerTls.connector(
                                arguments.tlsCert,
                                arguments.tlsKey,
                                arguments.getBindAddress(),
                                arguments.getHttpPort())
                        : cleartext(arguments.getBindAddress(), arguments.getHttpPort());
        if (arguments.tlsCert != null) {
            log.info("Serving the worker API over HTTPS with {}", arguments.tlsCert);
        }
        Supplier<String> allMetrics = () -> metrics(prometheusRegistry.scrape(), worker::scrapeMetrics);
        Javalin app =
                Javalin.create(
                                config -> {
                                    handler.configure(config);
                                    // Javalin adds its HTTP connector only when none was added
                                    config.jetty.addConnector(connector);
                                    config.routes.get(
                                            HttpWorkerClient.METRICS_PATH, ctx -> ctx.result(allMetrics.get()));
                                })
                        .start(arguments.getBindAddress(), arguments.getHttpPort());

        // Stop server and driver process on JVM shutdown and keep the process alive
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    app.stop();
                                    try {
                                        handler.close();
                                    } catch (Exception e) {
                                        log.warn("Failed to stop driver process", e);
                                    }
                                    // Publishes the last OTLP step
                                    registry.close();
                                },
                                "benchmark-worker-shutdown"));
        Thread.currentThread().join();
    }

    /**
     * Builds the plain-text connector, which serves HTTP/1.1 and h2c, by upgrade or prior knowledge,
     * on one port.
     *
     * @param host the address to listen on
     * @param port the port to listen on, or 0 for any free port
     * @return the connector factory, for {@code config.jetty.addConnector}
     */
    static BiFunction<Server, HttpConfiguration, Connector> cleartext(String host, int port) {
        return (server, http) -> {
            ServerConnector connector =
                    new ServerConnector(
                            server, new HttpConnectionFactory(http), new HTTP2CServerConnectionFactory(http));
            connector.setHost(host);
            connector.setPort(port);
            return connector;
        };
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(BenchmarkWorker.class);
}
