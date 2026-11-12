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
package io.openmessaging.benchmark.utils.payload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * Parameters for generated payloads.
 *
 * @param type the generator
 * @param targetCompressionRatio compressed size divided by original size, in (0, 1]
 * @param poolSize number of distinct payloads per message size
 */
public record PayloadGenerator(Type type, double targetCompressionRatio, int poolSize) {

    /** The available generators. */
    public enum Type {
        /** Random segments mixed with repeats, calibrated to a zstd compression ratio. */
        COMPRESSIBLE
    }

    public PayloadGenerator {
        if (type == null) {
            throw new IllegalArgumentException("payloadGenerator type is required");
        }
        if (!Double.isFinite(targetCompressionRatio)
                || targetCompressionRatio <= 0
                || targetCompressionRatio > 1) {
            throw new IllegalArgumentException(
                    "payloadGenerator targetCompressionRatio must be in (0, 1]");
        }
        if (poolSize < 1) {
            throw new IllegalArgumentException("payloadGenerator poolSize must be positive");
        }
    }

    @JsonCreator
    public static PayloadGenerator fromJson(
            @JsonProperty("type") @Nullable Type type,
            @JsonProperty("targetCompressionRatio") @Nullable Double targetCompressionRatio,
            @JsonProperty("poolSize") @Nullable Integer poolSize) {
        if (type == null || targetCompressionRatio == null || poolSize == null) {
            throw new IllegalArgumentException(
                    "payloadGenerator requires type, targetCompressionRatio and poolSize");
        }
        return new PayloadGenerator(type, targetCompressionRatio, poolSize);
    }
}
