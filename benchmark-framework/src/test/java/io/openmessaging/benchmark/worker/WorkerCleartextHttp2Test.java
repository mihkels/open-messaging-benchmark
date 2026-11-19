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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.javalin.Javalin;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The plain-text worker port serves HTTP/1.1 and h2c side by side. */
class WorkerCleartextHttp2Test {
    private static final String LOOPBACK = "127.0.0.1";

    private final Worker worker = mock(Worker.class);
    private final AtomicReference<String> protocol = new AtomicReference<>();
    private Javalin app;
    private String host;

    @BeforeEach
    void start() {
        WorkerHandler handler = new WorkerHandler(worker, null);
        app =
                Javalin.create(config -> {
                    config.startup.showJavalinBanner = false;
                    handler.configure(config);
                    config.routes.before(ctx -> protocol.set(ctx.protocol()));
                    config.jetty.addConnector(BenchmarkWorker.cleartext(LOOPBACK, 0));
                });
        app.start(LOOPBACK, 0);
        host = "http://" + LOOPBACK + ":" + app.port();
    }

    @AfterEach
    void stop() {
        app.stop();
    }

    @Test
    void coordinatorSpeaksH2cWithPriorKnowledge() throws Exception {
        try (HttpWorkerClient client = HttpWorkerClient.forRemoteWorker(host, null, null)) {
            client.stopAll();
        }
        verify(worker).stopAll();
        assertThat(protocol.get()).isEqualTo("HTTP/2.0");
    }

    @Test
    void driverProcessClientKeepsHttp11() throws Exception {
        try (HttpWorkerClient client = new HttpWorkerClient(host)) {
            client.stopAll();
        }
        verify(worker).stopAll();
        assertThat(protocol.get()).isEqualTo("HTTP/1.1");
    }
}
