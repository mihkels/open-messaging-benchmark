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
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
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
                                + " environment variable. Without either, the API is unauthenticated")
        private @Nullable Path tokenFile;

        @Parameter(
                names = "--protect-metrics",
                description = "Require the bearer token on /metrics too (it is open by default)")
        private boolean protectMetrics;

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
                                token, arguments.protectMetrics ? Set.of() : Set.of(HttpWorkerClient.METRICS_PATH)))
                .orElse(null);
    }

    // WorkerHandler lives for the whole process; closed by the shutdown hook
    @SuppressWarnings("java:S2095")
    static void main(String[] args) throws InterruptedException {
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
        new JvmResourceMetrics().bindTo(prometheusRegistry, "supervisor");

        BearerTokenAuth auth = authFor(arguments, System.getenv());
        if (auth == null) {
            log.warn(
                    "No worker token configured (set {} or --token-file); anyone who can reach port {}"
                            + " can control this worker",
                    BearerTokenAuth.TOKEN_ENV,
                    arguments.getHttpPort());
        } else if (arguments.protectMetrics) {
            log.info(
                    "Bearer token required on every request, including {}", HttpWorkerClient.METRICS_PATH);
        } else {
            log.info("Bearer token required on every request except {}", HttpWorkerClient.METRICS_PATH);
        }

        // Dump configuration variables
        log.atInfo()
                .setMessage("Starting benchmark with config: {}")
                .addArgument(() -> writer.writeValueAsString(arguments))
                .log();

        // Drivers run in a child process with only their own plugin bundle on the classpath
        ProcessWorker worker = new ProcessWorker();
        WorkerHandler handler = new WorkerHandler(worker, auth);
        Javalin app =
                Javalin.create(
                                config -> {
                                    handler.configure(config);
                                    config.routes.get(
                                            HttpWorkerClient.METRICS_PATH,
                                            ctx -> ctx.result(prometheusRegistry.scrape() + worker.scrapeMetrics()));
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
                                },
                                "benchmark-worker-shutdown"));
        Thread.currentThread().join();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(BenchmarkWorker.class);
}
