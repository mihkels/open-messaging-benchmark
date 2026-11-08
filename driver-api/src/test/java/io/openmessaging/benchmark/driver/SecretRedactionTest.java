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
package io.openmessaging.benchmark.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SecretRedactionTest {

    static class ClientConfig {
        public String serverUri = "tcp://broker:1883";
        public String username = "bench";
        public String password = "mqtt-password";
    }

    static class DriverConfig {
        public ClientConfig client = new ClientConfig();
        public String producerConfig =
                "acks=all\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule"
                        + " required username=\"u\" password=\"kafka-password\";\n";
        public int qos = 1;
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "password",
                "dbPassword",
                "authToken",
                "sasl.jaas.config",
                "api_key",
                "authentication",
                "Authorization",
                "redisPass",
                "db.pass",
                "adminPwd",
                "REDIS_PASS"
            })
    void flagsSecretKeys(String key) {
        assertTrue(SecretRedaction.isSecretKey(key), key);
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "username",
                "bootstrap.servers",
                "sasl.mechanism",
                "auth",
                "key",
                "",
                "bypassCache",
                "passive",
                "passThrough",
                "redisUser"
            })
    void keepsOrdinaryKeys(String key) {
        assertFalse(SecretRedaction.isSecretKey(key), key);
    }

    @Test
    void redactedJsonMasksTopLevelNestedAndEmbeddedSecrets() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("password", "top-password");
        config.put("client", new ClientConfig());

        String json = SecretRedaction.redactedJson(config);

        assertFalse(json.contains("top-password"), json);
        assertFalse(json.contains("mqtt-password"), json);
        assertTrue(json.contains("\"password\" : \"[REDACTED]\""), json);
        assertTrue(json.contains("\"username\" : \"bench\""), json);
        assertTrue(json.contains("tcp://broker:1883"), json);
    }

    @Test
    void redactedJsonMasksSaslJaasConfigInsidePropertiesString() {
        String json = SecretRedaction.redactedJson(new DriverConfig());

        assertFalse(json.contains("kafka-password"), json);
        assertFalse(json.contains("mqtt-password"), json);
        assertTrue(json.contains("sasl.jaas.config=[REDACTED]"), json);
        assertTrue(json.contains("acks=all"), json);
        assertTrue(json.contains("\"qos\" : 1"), json);
    }

    @Test
    void masksSecretPropertyLinesAndKeepsOthers() {
        String properties =
                "bootstrap.servers=kafka:9092\n"
                        + "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule"
                        + " required username=\"u\" password=\"x\";\n"
                        + "  ssl.keystore.password : hunter2\r\n"
                        + "redis.pass=hunter3\n"
                        + "sasl.mechanism=PLAIN\n";

        assertEquals(
                "bootstrap.servers=kafka:9092\n"
                        + "sasl.jaas.config=[REDACTED]\n"
                        + "  ssl.keystore.password : [REDACTED]\r\n"
                        + "redis.pass=[REDACTED]\n"
                        + "sasl.mechanism=PLAIN\n",
                SecretRedaction.redactString(properties));
    }

    @Test
    void keepsSecretKeyLinesWithoutValueAndKeysWithSpaces() {
        assertEquals("password:", SecretRedaction.redactString("password:"));
        assertEquals("password =  ", SecretRedaction.redactString("password =  "));
        assertEquals("my password: none", SecretRedaction.redactString("my password: none"));
        assertEquals("=token", SecretRedaction.redactString("=token"));
    }

    @Test
    void masksUriPasswords() {
        assertEquals(
                "amqp://user:[REDACTED]@rabbit:5672/vhost",
                SecretRedaction.redactString("amqp://user:pass@rabbit:5672/vhost"));
        assertEquals(
                "redis://:[REDACTED]@redis:6379", SecretRedaction.redactString("redis://:pass@redis:6379"));
        assertEquals(
                "url: nats://a:[REDACTED]@h1,nats://b:[REDACTED]@h2",
                SecretRedaction.redactString("url: nats://a:p1@h1,nats://b:p2@h2"));
        assertEquals(
                "--uri=\"amqp://u:[REDACTED]@h\"", SecretRedaction.redactString("--uri=\"amqp://u:p@h\""));
    }

    @Test
    void keepsUrisWithoutPasswords() {
        for (String uri :
                List.of(
                        "amqp://user@rabbit:5672",
                        "pulsar://broker:6650",
                        "http://host:8080/a@b",
                        "amqp://user:@host",
                        "tcp://",
                        "no scheme here")) {
            assertEquals(uri, SecretRedaction.redactString(uri));
        }
    }

    @Test
    void masksJsonObjectStringsRecursively() {
        String json =
                "{\"brokerURL\":\"tcp://u:p@artemis:61616\",\"password\":\"x\","
                        + "\"nested\":{\"userName\":\"u\",\"apiKey\":\"k\"},\"list\":[\"a\"]}";

        assertEquals(
                "{\"brokerURL\":\"tcp://u:[REDACTED]@artemis:61616\",\"password\":\"[REDACTED]\","
                        + "\"nested\":{\"userName\":\"u\",\"apiKey\":\"[REDACTED]\"},\"list\":[\"a\"]}",
                SecretRedaction.redactString(json));
    }

    @Test
    void fallsBackToLineMaskingForInvalidOrNonObjectJson() {
        assertEquals("{password=[REDACTED]", SecretRedaction.redactString("{password=x"));
        assertEquals("{password: [REDACTED]", SecretRedaction.redactString("{password: x}"));
        assertEquals("[1, 2]", SecretRedaction.redactString("[1, 2]"));
    }

    @Test
    void redactKeepsNonStringValuesAndNulls() {
        assertNull(SecretRedaction.redactString(null));
        assertEquals("", SecretRedaction.redactString(""));
        assertEquals(42, SecretRedaction.redact(42));
        assertNull(SecretRedaction.redact(null));
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(1, Arrays.asList("redis://:p@h", null));
        map.put("token", null);

        assertEquals(
                Map.of("1", Arrays.asList("redis://:[REDACTED]@h", null), "token", "[REDACTED]"),
                SecretRedaction.redactMap(map));
    }
}
