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

import io.micrometer.core.instrument.Tags;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DriverProcessMainTest {

    @Test
    void parsesTagArguments() {
        assertThat(DriverProcessMain.commonTags(List.of("run_id=r-1", "driver=Kafka", "empty=")))
                .isEqualTo(Tags.of("run_id", "r-1", "driver", "Kafka", "empty", ""));
    }

    @ParameterizedTest
    @CsvSource({
        "org.apache.kafka.automatic.config.providers, none",
        "org.apache.commons.configuration2.interpol.ConfigurationInterpolator.defaultPrefixLookups, ''"
    })
    void disablesConfigFileLookupsUnlessTheOperatorSetThem(String key, String disabled) {
        String before = System.getProperty(key);
        try {
            System.clearProperty(key);
            DriverProcessMain.disableConfigFileLookups();
            assertThat(System.getProperty(key)).isEqualTo(disabled);

            System.setProperty(key, "OPERATOR_CHOICE");
            DriverProcessMain.disableConfigFileLookups();
            assertThat(System.getProperty(key)).isEqualTo("OPERATOR_CHOICE");
        } finally {
            if (before == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, before);
            }
        }
    }

    @Test
    void rejectsARepeatedFileFlag() {
        Path first = DriverProcessMain.once(null, "--port-file", "/tmp/a");
        assertThat(first).isEqualTo(Path.of("/tmp/a"));
        assertThatThrownBy(() -> DriverProcessMain.once(first, "--port-file", "/tmp/b"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--port-file");
    }

    @Test
    void rejectsTagsWithoutKey() {
        List<String> withoutKey = List.of("=value");
        List<String> withoutSeparator = List.of("novalue");
        assertThatThrownBy(() -> DriverProcessMain.commonTags(withoutKey))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> DriverProcessMain.commonTags(withoutSeparator))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
