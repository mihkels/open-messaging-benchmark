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
                names = {"-sp", "--stats-port"},
                description = "Deprecated and ignored; accepted so existing launch scripts keep working")
        // Deprecated since 0.0.3. Kept so existing launch scripts still parse; only read to warn.
        private Integer statsPort;

        public boolean isHelp() {
            return help;
        }

        public int getHttpPort() {
            return httpPort;
        }
    }

    // WorkerHandler lives for the whole process; closed by the shutdown hook
    @SuppressWarnings("java:S2095")
    static void main(String[] args) throws Exception {
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

        // Dump configuration variables
        log.atInfo()
                .setMessage("Starting benchmark with config: {}")
                .addArgument(() -> writer.writeValueAsString(arguments))
                .log();

        // Drivers run in a child process with only their own plugin bundle on the classpath
        ProcessWorker worker = new ProcessWorker();
        WorkerHandler handler = new WorkerHandler(worker);
        Javalin app =
                Javalin.create(
                                config -> {
                                    handler.configure(config);
                                    config.routes.get(
                                            "/metrics",
                                            ctx -> ctx.result(prometheusRegistry.scrape() + worker.scrapeMetrics()));
                                })
                        .start(arguments.getHttpPort());

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
