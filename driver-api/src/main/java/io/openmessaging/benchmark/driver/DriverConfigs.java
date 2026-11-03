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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectReader;
import tools.jackson.dataformat.yaml.YAMLFactory;

/** Reads driver configuration files. */
public final class DriverConfigs {
    private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private DriverConfigs() {}

    /**
     * Reads a YAML driver configuration, ignoring properties the configuration class does not
     * declare, such as the {@code driverClass} and {@code name} read by the benchmark host.
     *
     * @param configurationFile the configuration file
     * @param type the configuration class
     * @param enabled additional deserialization features to enable
     * @return the configuration
     * @param <T> the configuration type
     * @throws IOException if the file cannot be read
     */
    public static <T> T read(Path configurationFile, Class<T> type, DeserializationFeature... enabled)
            throws IOException {
        ObjectReader reader =
                mapper.readerFor(type).without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        for (DeserializationFeature feature : enabled) {
            reader = reader.with(feature);
        }
        try (InputStream input = Files.newInputStream(configurationFile)) {
            return reader.readValue(input);
        }
    }
}
