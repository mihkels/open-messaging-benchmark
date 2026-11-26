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
package io.github.mihkels.omb.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.beust.jcommander.JCommander;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Serves the worker API over HTTPS with self-signed certificates made by openssl, as in production.
 */
class WorkerTlsTest {
    private static final String TOKEN = "correct-horse-battery-staple";
    private static final String LOOPBACK = "127.0.0.1";

    @TempDir static Path dir;
    private static Path cert;
    private static Path key;

    private final Worker worker = mock(Worker.class);
    private Javalin app;
    private final List<HttpWorkerClient> clients = new ArrayList<>();
    private final AtomicReference<String> protocol = new AtomicReference<>();

    @BeforeAll
    static void createCertificate() throws Exception {
        assumeTrue(openssl("version") == 0, "openssl is not installed");
        cert = dir.resolve("worker.crt");
        key = dir.resolve("worker.key");
        // Same command as bin/benchmark-tls-cert
        assertThat(selfSigned("ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", key, cert)).isZero();
    }

    @AfterEach
    void stop() throws Exception {
        for (HttpWorkerClient client : clients) {
            client.close();
        }
        if (app != null) {
            app.stop();
        }
    }

    private void start(Path certificate, Path privateKey) throws Exception {
        var https = WorkerTls.connector(certificate, privateKey, LOOPBACK, 0);
        WorkerHandler handler = new WorkerHandler(worker, new BearerTokenAuth(TOKEN, Set.of()));
        app =
                Javalin.create(config -> {
                    config.startup.showJavalinBanner = false;
                    handler.configure(config);
                    config.routes.before(ctx -> protocol.set(ctx.protocol()));
                    config.jetty.addConnector(https);
                });
        app.start(LOOPBACK, 0);
    }

    private HttpWorkerClient client(String scheme, Path caFile) {
        HttpWorkerClient client =
                HttpWorkerClient.forRemoteWorker(
                        scheme + "://" + LOOPBACK + ":" + app.port(), TOKEN, caFile);
        clients.add(client);
        return client;
    }

    @Test
    void coordinatorTrustingTheWorkerCertificateTalksHttps() throws Exception {
        start(cert, key);
        client("https", cert).stopAll();
        verify(worker).stopAll();
    }

    @Test
    void coordinatorNegotiatesHttp2() throws Exception {
        start(cert, key);
        client("https", cert).stopAll();
        assertThat(protocol.get()).isEqualTo("HTTP/2.0");
    }

    @Test
    void clientsWithoutAlpnGetHttp11() throws Exception {
        start(cert, key);
        KeyStore trusted = KeyStore.getInstance("PKCS12");
        trusted.load(null, null);
        try (InputStream in = Files.newInputStream(cert)) {
            trusted.setCertificateEntry(
                    "worker", CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        TrustManagerFactory trust =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trust.init(trusted);
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(null, trust.getTrustManagers(), null);
        try (Socket socket = tls.getSocketFactory().createSocket(LOOPBACK, app.port())) {
            socket.setSoTimeout(5_000);
            socket
                    .getOutputStream()
                    .write(
                            ("GET " + WorkerHandler.HEALTH + " HTTP/1.1\r\nHost: " + LOOPBACK + "\r\n\r\n")
                                    .getBytes(StandardCharsets.US_ASCII));
            String reply = new String(socket.getInputStream().readNBytes(12), StandardCharsets.US_ASCII);
            // Any status line proves the HTTP/1.1 fallback; the mock worker cannot answer 200
            assertThat(reply).startsWith("HTTP/1.1 ");
        }
    }

    @Test
    void coordinatorRejectsAWorkerWithAnotherCertificate() throws Exception {
        Path otherCert = dir.resolve("other.crt");
        Path otherKey = dir.resolve("other.key");
        assertThat(selfSigned("ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", otherKey, otherCert))
                .isZero();
        start(otherCert, otherKey);
        HttpWorkerClient pinnedToFirst = client("https", cert);
        assertThatThrownBy(pinnedToFirst::stopAll).hasStackTraceContaining("SSL");
    }

    @Test
    void tlsWorkerDoesNotAnswerPlainHttp() throws Exception {
        start(cert, key);
        try (Socket socket = new Socket(LOOPBACK, app.port())) {
            socket.setSoTimeout(5_000);
            socket
                    .getOutputStream()
                    .write("GET /health HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            String reply = new String(socket.getInputStream().readNBytes(12), StandardCharsets.US_ASCII);
            assertThat(reply).doesNotStartWith("HTTP/1.1 200");
        }
    }

    @Test
    void loadsPkcs1RsaKeysAsHelmAndCertManagerWriteThem() throws Exception {
        Path rsaKey = dir.resolve("rsa.key");
        Path rsaCert = dir.resolve("rsa.crt");
        assertThat(selfSigned("rsa:2048", null, null, rsaKey, rsaCert)).isZero();
        Path pkcs1 = dir.resolve("rsa-pkcs1.key");
        assertThat(openssl("rsa", "-in", rsaKey.toString(), "-traditional", "-out", pkcs1.toString()))
                .isZero();
        assertThat(Files.readString(pkcs1)).startsWith("-----BEGIN RSA PRIVATE KEY-----");
        start(rsaCert, pkcs1);
        client("https", rsaCert).stopAll();
        verify(worker).stopAll();
    }

    @Test
    void explainsHowToConvertUnsupportedKeys() throws Exception {
        Path sec1 = dir.resolve("sec1.key");
        assertThat(openssl("ec", "-in", key.toString(), "-out", sec1.toString())).isZero();
        assertThat(Files.readString(sec1)).startsWith("-----BEGIN EC PRIVATE KEY-----");
        assertThatThrownBy(() -> WorkerTls.privateKey(sec1))
                .isInstanceOf(GeneralSecurityException.class)
                .hasMessageContaining("openssl pkcs8 -topk8");
    }

    @Test
    void certificateAndKeyMustBeGivenTogether() {
        BenchmarkWorker.Arguments onlyCert = parse("--tls-cert", "a.crt");
        assertThat(BenchmarkWorker.refuseIncompleteTls(onlyCert))
                .hasValueSatisfying(value -> assertThat(value).contains("together"));
        assertThat(BenchmarkWorker.refuseIncompleteTls(parse("--tls-cert", "a", "--tls-key", "b")))
                .isEmpty();
        assertThat(BenchmarkWorker.refuseIncompleteTls(parse())).isEmpty();
    }

    @Test
    void rejectsAnUnreadableCaFile() throws IOException {
        Path notPem = dir.resolve("not-a-cert.pem");
        Files.writeString(notPem, "hello");
        assertThatThrownBy(() -> HttpWorkerClient.clientConfig(notPem, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(notPem.toString());
    }

    private static BenchmarkWorker.Arguments parse(String... args) {
        BenchmarkWorker.Arguments arguments = new BenchmarkWorker.Arguments();
        JCommander.newBuilder().addObject(arguments).build().parse(args);
        return arguments;
    }

    private static int selfSigned(
            String newKey, String option, String value, Path keyFile, Path certFile) throws Exception {
        List<String> command = new ArrayList<>(List.of("req", "-x509", "-newkey", newKey));
        if (option != null) {
            command.addAll(List.of(option, value));
        }
        command.addAll(
                List.of(
                        "-nodes",
                        "-days",
                        "1",
                        "-subj",
                        "/CN=omb-worker",
                        "-keyout",
                        keyFile.toString(),
                        "-out",
                        certFile.toString()));
        return openssl(command.toArray(String[]::new));
    }

    private static int openssl(String... args) throws Exception {
        List<String> command = new ArrayList<>(List.of("openssl"));
        command.addAll(List.of(args));
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            process.getInputStream().readAllBytes();
            return process.waitFor(30, TimeUnit.SECONDS) ? process.exitValue() : -1;
        } catch (IOException _) {
            return -1;
        }
    }
}
