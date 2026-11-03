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
package io.openmessaging.benchmark.driver.pravega.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.openmessaging.benchmark.driver.DriverConfigs;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PravegaConfigTest {

    @Test
    void missingClientSectionUsesClientDefaults(@TempDir Path temp) throws Exception {
        Path config = temp.resolve("pravega.yaml");
        Files.writeString(config, "createScope: false\n");

        PravegaClientConfig client = DriverConfigs.read(config, PravegaConfig.class).client();

        assertThat(client.controllerURI()).isEqualTo("tcp://localhost:9090");
        assertThat(client.scopeName()).isEqualTo("openmessaging-benchmark");
    }

    @Test
    void partialClientSectionKeepsDefaultsForMissingKeys(@TempDir Path temp) throws Exception {
        Path config = temp.resolve("pravega.yaml");
        Files.writeString(config, "client:\n  controllerURI: tcp://pravega:9090\n");

        PravegaClientConfig client = DriverConfigs.read(config, PravegaConfig.class).client();

        assertThat(client.controllerURI()).isEqualTo("tcp://pravega:9090");
        assertThat(client.scopeName()).isEqualTo("openmessaging-benchmark");
    }
}
