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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import io.javalin.Javalin;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.asynchttpclient.AsyncHttpClient;
import org.junit.jupiter.api.Test;

class HttpWorkerClientLifecycleTest {
    @Test
    void boundsAStalledClientClose() throws Exception {
        AsyncHttpClient client = mock(AsyncHttpClient.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                while (true) {
                                    try {
                                        release.await();
                                        break;
                                    } catch (InterruptedException ignoredInterrupt) {
                                        // Simulate a client that does not respond to cancellation.
                                    }
                                }
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(client)
                .close();
        HttpWorkerClient worker = new HttpWorkerClient(client, "http://worker", Duration.ofMillis(150));
        try {
            assertThatThrownBy(worker::close)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Timed out closing HTTP worker client");
            assertThat(entered.getCount()).isZero();
        } finally {
            release.countDown();
            assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void preservesClientCloseFailure() throws Exception {
        AsyncHttpClient client = mock(AsyncHttpClient.class);
        doThrow(new IOException("close failed")).when(client).close();
        HttpWorkerClient worker = new HttpWorkerClient(client, "http://worker", Duration.ofSeconds(2));
        assertThatThrownBy(worker::close).isInstanceOf(IOException.class).hasMessage("close failed");
    }

    @Test
    void preservesInterruptWhenClosingTheClient() throws Exception {
        AsyncHttpClient client = mock(AsyncHttpClient.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            try {
                                release.await();
                            } catch (InterruptedException ignoredInterrupt) {
                                // Simulate an interruptible client close.
                            } finally {
                                exited.countDown();
                            }
                            return null;
                        })
                .when(client)
                .close();
        HttpWorkerClient worker = new HttpWorkerClient(client, "http://worker", Duration.ofSeconds(2));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread closing =
                Thread.ofVirtual()
                        .start(
                                () -> {
                                    try {
                                        worker.close();
                                    } catch (Throwable t) {
                                        failure.set(t);
                                        interrupted.set(Thread.currentThread().isInterrupted());
                                    }
                                });
        try {
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            closing.interrupt();
            closing.join(2000);
            assertThat(closing.isAlive()).isFalse();
            assertThat(failure.get()).isInstanceOf(InterruptedException.class);
            assertThat(interrupted).isTrue();
        } finally {
            release.countDown();
            assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void allowsLongRunningStopRequestsWithoutJettyIdleTimeout() throws Exception {
        Worker worker = mock(Worker.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        ignored -> {
                            entered.countDown();
                            release.await();
                            return null;
                        })
                .when(worker)
                .stopAll();
        try (WorkerHandler handler = new WorkerHandler(worker)) {
            Javalin app =
                    Javalin.create(
                            config -> {
                                config.startup.showJavalinBanner = false;
                                config.jetty.modifyHttpConfiguration(http -> http.setIdleTimeout(100));
                                handler.configure(config);
                            });
            try {
                app.start("127.0.0.1", 0);
                try (HttpWorkerClient remote = new HttpWorkerClient("http://127.0.0.1:" + app.port())) {
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    Thread request =
                            Thread.ofVirtual()
                                    .start(
                                            () -> {
                                                try {
                                                    remote.stopAll();
                                                } catch (Throwable t) {
                                                    failure.set(t);
                                                }
                                            });
                    try {
                        assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                        request.join(250);
                        assertThat(request.isAlive()).isTrue();
                        release.countDown();
                        request.join(3000);
                        assertThat(request.isAlive()).isFalse();
                        assertThat(failure.get()).isNull();
                    } finally {
                        release.countDown();
                        request.join(3000);
                    }
                }
            } finally {
                app.stop();
            }
        }
    }

    @Test
    void includesSuppressedCleanupFailuresInHttpErrors() throws Exception {
        Worker worker = mock(Worker.class);
        IllegalStateException failure = new IllegalStateException("primary close failure");
        failure.addSuppressed(new IOException("additional close failure"));
        doThrow(failure).when(worker).stopAll();
        try (WorkerHandler handler = new WorkerHandler(worker)) {
            Javalin app =
                    Javalin.create(
                            config -> {
                                config.startup.showJavalinBanner = false;
                                handler.configure(config);
                            });
            try {
                app.start("127.0.0.1", 0);
                try (HttpWorkerClient remote = new HttpWorkerClient("http://127.0.0.1:" + app.port())) {
                    assertThatThrownBy(remote::stopAll)
                            .hasStackTraceContaining("primary close failure")
                            .hasStackTraceContaining("suppressed: java.io.IOException: additional close failure");
                }
            } finally {
                app.stop();
            }
        }
    }
}
