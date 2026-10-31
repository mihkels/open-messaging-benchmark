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

import io.javalin.Javalin;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point of a driver process started by {@link ProcessWorker}. Runs a {@link LocalWorker} on
 * the driver's own classpath and serves it over HTTP on a loopback port, which is written to the
 * file given with {@code --port-file}. The process exits when its standard input is closed, so it
 * never outlives its supervisor.
 */
public final class DriverProcessMain {

    private static final Logger log = LoggerFactory.getLogger(DriverProcessMain.class);
    static final String INITIALIZE_DRIVER_LOCAL = "/initialize-driver-local";
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);

    private DriverProcessMain() {}

    public static void main(String[] args) throws Exception {
        Path portFile = null;
        for (int i = 0; i < args.length; i++) {
            if ("--port-file".equals(args[i]) && i + 1 < args.length) {
                portFile = Path.of(args[++i]);
            }
        }
        if (portFile == null) {
            System.err.println("Usage: DriverProcessMain --port-file <file>");
            System.exit(2);
        }

        Thread supervisorMonitor =
                Thread.ofPlatform()
                        .daemon()
                        .name("driver-supervisor-monitor")
                        .start(DriverProcessMain::exitWhenSupervisorDisconnects);

        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        LocalWorker worker = new LocalWorker(registry);
        WorkerHandler handler = new WorkerHandler(worker);
        Javalin app =
                Javalin.create(
                        config -> {
                            config.startup.showJavalinBanner = false;
                            handler.registerRoutes(config.routes);
                            config.routes.get("/metrics", ctx -> ctx.result(registry.scrape()));
                            config.routes.post(
                                    INITIALIZE_DRIVER_LOCAL, ctx -> worker.initializeDriver(Path.of(ctx.body())));
                        });
        app.start("127.0.0.1", 0);

        Runnable shutdown =
                () -> {
                    try {
                        worker.stopAll();
                    } catch (RuntimeException e) {
                        log.warn("Failed to stop driver", e);
                    }
                    try {
                        handler.close();
                    } catch (Exception e) {
                        log.warn("Failed to close worker", e);
                    }
                    app.stop();
                };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdown, "driver-process-shutdown"));

        Path tmp = portFile.resolveSibling(portFile.getFileName() + ".tmp");
        Files.writeString(tmp, Integer.toString(app.port()), StandardCharsets.US_ASCII);
        Files.move(tmp, portFile, StandardCopyOption.ATOMIC_MOVE);
        log.info("Driver process listening on 127.0.0.1:{}", app.port());

        supervisorMonitor.join();
    }

    private static void exitWhenSupervisorDisconnects() {
        try {
            // The supervisor never writes; EOF means it closed the pipe or died
            int read = 0;
            while (read != -1) {
                read = System.in.read();
            }
        } catch (IOException e) {
            log.debug("Supervisor pipe failed", e);
        }
        log.info("Supervisor disconnected, stopping driver process");
        Thread.ofPlatform()
                .daemon()
                .name("driver-shutdown-watchdog")
                .start(
                        () -> {
                            try {
                                Thread.sleep(SHUTDOWN_TIMEOUT);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                            log.error("Driver shutdown exceeded {}; forcing exit", SHUTDOWN_TIMEOUT);
                            Runtime.getRuntime().halt(1);
                        });
        System.exit(0);
    }
}
