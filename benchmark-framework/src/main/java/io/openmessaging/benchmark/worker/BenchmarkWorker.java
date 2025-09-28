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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.javalin.Javalin;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A benchmark worker that listen for tasks to perform. */
public class BenchmarkWorker {

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
                description = "Stats port to listen on")
        private int statsPort = 8081;

        public boolean isHelp() {
            return help;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public int getStatsPort() {
            return statsPort;
        }
    }

    public static void main(String[] args) throws Exception {
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
            System.exit(-1);
        }

        if (arguments.statsPort == arguments.httpPort) {
            System.err.println("Stats port must be different from HTTP port");
            jc.usage();
            System.exit(-1);
        }

        // Replace BookKeeper stats with Micrometer
        PrometheusMeterRegistry prometheusRegistry =
                new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        // Dump configuration variables
        log.info("Starting benchmark with config: {}", writer.writeValueAsString(arguments));

        // Start web server (no try-with-resources)
        Javalin app = Javalin.create().start(arguments.getHttpPort());

        // Add Prometheus metrics endpoint
        app.get("/metrics", ctx -> ctx.result(prometheusRegistry.scrape()));

        new WorkerHandler(app, prometheusRegistry);

        // Stop server on JVM shutdown and keep process alive
        Runtime.getRuntime().addShutdownHook(new Thread(app::stop, "benchmark-worker-shutdown"));
        Thread.currentThread().join();
    }

    private static final ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();

    private static final Logger log = LoggerFactory.getLogger(BenchmarkWorker.class);
}
