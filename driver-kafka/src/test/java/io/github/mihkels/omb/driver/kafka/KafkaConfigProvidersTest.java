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
package io.github.mihkels.omb.driver.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.DirectoryConfigProvider;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Driver processes set {@code org.apache.kafka.automatic.config.providers=none} (see {@code
 * DriverProcessMain}). This checks that the Kafka client then refuses a driver config that declares
 * a provider instead of reading the referenced file.
 */
class KafkaConfigProvidersTest {

    private static final String KEY = "org.apache.kafka.automatic.config.providers";
    private String before;

    @BeforeEach
    void saveProperty() {
        before = System.getProperty(KEY);
    }

    @AfterEach
    void restoreProperty() {
        if (before == null) {
            System.clearProperty(KEY);
        } else {
            System.setProperty(KEY, before);
        }
    }

    @Test
    void configProvidersAreRejectedWhenDisabled(@TempDir Path dir) {
        System.setProperty(KEY, "none");
        ConfigException e = assertThrows(ConfigException.class, () -> clientId(dir));
        assertTrue(e.getMessage().contains("is not allowed"), e.getMessage());
    }

    @Test
    void providerReferencesAreResolvedByDefault(@TempDir Path dir) throws Exception {
        // Shows the test above exercises a real file read
        System.clearProperty(KEY);
        assertEquals("secret-value", clientId(dir));
    }

    private static String reference(Path dir) {
        return "${d:" + dir + ":secret}";
    }

    private static String clientId(Path dir) throws Exception {
        Files.writeString(dir.resolve("secret"), "secret-value");
        Properties props = new Properties();
        props.put("config.providers", "d");
        props.put("config.providers.d.class", DirectoryConfigProvider.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, reference(dir));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new ProducerConfig(props).getString(ProducerConfig.CLIENT_ID_CONFIG);
    }
}
