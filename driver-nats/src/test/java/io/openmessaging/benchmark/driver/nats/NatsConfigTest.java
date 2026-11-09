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
package io.openmessaging.benchmark.driver.nats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.nats.client.api.StorageType;
import io.openmessaging.benchmark.driver.DriverConfigs;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NatsConfigTest {

    @Test
    void missingKeysKeepFileStorageAndUnlimitedBytes(@TempDir Path temp) throws Exception {
        Path config = temp.resolve("nats.yaml");
        Files.writeString(config, "natsHostUrl: localhost:4222\nreplicationFactor: 3\n");

        NatsConfig nats = DriverConfigs.read(config, NatsConfig.class);

        assertEquals("localhost:4222", nats.natsHostUrl);
        assertEquals(3, nats.replicationFactor);
        assertEquals(StorageType.File, nats.storageType);
        assertEquals(-1, nats.maxBytes);
    }
}
