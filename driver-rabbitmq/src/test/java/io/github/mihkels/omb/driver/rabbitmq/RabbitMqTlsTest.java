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
package io.github.mihkels.omb.driver.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.ConnectionFactory;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins that {@code amqps://} connections refuse a broker whose certificate the JVM does not trust.
 * The driver passes {@code amqpUris} to {@code setUri}; amqp-client 5.x then uses the JVM's default
 * trust store with host name verification, unlike old clients that trusted every certificate.
 */
class RabbitMqTlsTest {

    @Test
    void amqpsRefusesAnUntrustedBrokerCertificate(@TempDir Path dir) throws Exception {
        try (SSLServerSocket server = untrustedServer(dir)) {
            CompletableFuture<Boolean> handshake = acceptOneHandshake(server);
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri("amqps://u:p@localhost:" + server.getLocalPort());
            assertThat(factory.isSSL()).isTrue();
            factory.setAutomaticRecoveryEnabled(false);
            factory.setConnectionTimeout(5_000);
            factory.setHandshakeTimeout(5_000);
            assertThatThrownBy(factory::newConnection).isNotNull();
            // A trust-all client would complete the handshake before failing on the AMQP protocol
            assertThat(handshake.get(15, TimeUnit.SECONDS)).isFalse();
        }
    }

    // A TLS server with a fresh self-signed certificate; completes with whether a handshake succeeded
    private static SSLServerSocket untrustedServer(Path dir) throws Exception {
        Path keyStore = dir.resolve("server.p12");
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Process process =
                new ProcessBuilder(
                                keytool,
                                "-genkeypair",
                                "-alias",
                                "server",
                                "-keyalg",
                                "EC",
                                "-dname",
                                "CN=localhost",
                                "-ext",
                                "san=dns:localhost,ip:127.0.0.1",
                                "-validity",
                                "1",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                keyStore.toString(),
                                "-storepass",
                                "changeit")
                        .redirectErrorStream(true)
                        .start();
        process.getInputStream().readAllBytes();
        assertThat(process.waitFor(60, TimeUnit.SECONDS) && process.exitValue() == 0).isTrue();
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (FileInputStream in = new FileInputStream(keyStore.toFile())) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keys.init(store, "changeit".toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keys.getKeyManagers(), null, null);
        return (SSLServerSocket) context.getServerSocketFactory().createServerSocket(0);
    }

    private static CompletableFuture<Boolean> acceptOneHandshake(SSLServerSocket server) {
        return CompletableFuture.supplyAsync(() -> {
            try (SSLSocket socket = (SSLSocket) server.accept()) {
                socket.setSoTimeout(10_000);
                socket.startHandshake();
                return true;
            } catch (Exception _) {
                return false;
            }
        });
    }
}
