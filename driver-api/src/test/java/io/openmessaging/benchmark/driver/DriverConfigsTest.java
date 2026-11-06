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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DriverConfigsTest {
    @TempDir Path dir;

    @Test
    void readsKnownYamlPropertiesAndIgnoresHostProperties() throws IOException {
        Path config = dir.resolve("driver.yaml");
        Files.writeString(config, "name: example\nworkers: 3\ndriverClass: ignored.Driver\n");

        assertEquals(new DriverConfig("example", 3), DriverConfigs.read(config, DriverConfig.class));
    }

    @Test
    void propagatesFileReadFailures() {
        assertThrows(
                IOException.class,
                () -> DriverConfigs.read(dir.resolve("missing.yaml"), DriverConfig.class));
    }

    private record DriverConfig(String name, int workers) {}
}
