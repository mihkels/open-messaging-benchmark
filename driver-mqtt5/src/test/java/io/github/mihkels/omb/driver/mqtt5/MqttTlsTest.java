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
package io.github.mihkels.omb.driver.mqtt5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
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

/** The driver must refuse a TLS broker whose certificate the JVM does not trust. */
class MqttTlsTest {

    @Test
    void plainTcpStaysPlain() {
        assertThat(MqttBenchmarkDriver.transportConfig("tcp://broker").getSslConfig()).isEmpty();
        assertThat(MqttBenchmarkDriver.transportConfig("broker:1883").getSslConfig()).isEmpty();
    }

    @Test
    void tlsSchemesEnableTls() {
        for (String uri : new String[] {"ssl://broker", "tls://broker", "MQTTS://broker:8883"}) {
            assertThat(MqttBenchmarkDriver.transportConfig(uri).getSslConfig()).as(uri).isPresent();
        }
    }

    @Test
    void mqttsRefusesAnUntrustedBrokerCertificate(@TempDir Path dir) throws Exception {
        try (SSLServerSocket server = untrustedServer(dir)) {
            CompletableFuture<Boolean> handshake = acceptOneHandshake(server);
            Mqtt5BlockingClient client =
                    Mqtt5Client.builder()
                            .identifier("tls-test")
                            .transportConfig(
                                    MqttBenchmarkDriver.transportConfig("mqtts://localhost:" + server.getLocalPort()))
                            .buildBlocking();
            assertThatThrownBy(client::connect).isNotNull();
            // Without TLS the client would send a plain CONNECT and no handshake would start either,
            // so also check the transport asked for TLS
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
