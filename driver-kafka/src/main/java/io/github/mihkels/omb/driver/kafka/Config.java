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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public record Config(
        @JsonProperty("replicationFactor") short replicationFactor,
        @JsonProperty("topicConfig") String topicConfig,
        @JsonProperty("commonConfig") String commonConfig,
        @JsonProperty("producerConfig") String producerConfig,
        @JsonProperty("consumerConfig") String consumerConfig) {
    @JsonCreator
    public Config {
        // Jackson passes null for a missing key; empty text adds no properties
        topicConfig = Objects.requireNonNullElse(topicConfig, "");
        commonConfig = Objects.requireNonNullElse(commonConfig, "");
        producerConfig = Objects.requireNonNullElse(producerConfig, "");
        consumerConfig = Objects.requireNonNullElse(consumerConfig, "");
    }

    // Default constructor for Jackson deserialization when no arguments are provided
    @SuppressWarnings("unused")
    public Config() {
        this((short) 0, "", "", "", "");
    }
}
