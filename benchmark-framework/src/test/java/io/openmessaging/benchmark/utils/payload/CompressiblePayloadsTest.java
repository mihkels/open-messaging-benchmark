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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CompressiblePayloadsTest {

    @ParameterizedTest
    @ValueSource(doubles = {0.2, 0.45, 0.7, 1.0})
    void poolCompressesWithinFivePercentOfTheTarget(double target) {
        List<byte[]> pool = CompressiblePayloads.pool(1024, target, 100, new Random(42));

        assertThat(CompressiblePayloads.ratio(PayloadTemplates.of(pool, null)))
                .isCloseTo(target, withinPercentage(5));
    }

    @Test
    void largePayloadsReachTheTargetToo() {
        List<byte[]> pool = CompressiblePayloads.pool(256 * 1024, 0.3, 4, new Random(7));

        assertThat(CompressiblePayloads.ratio(PayloadTemplates.of(pool, null)))
                .isCloseTo(0.3, withinPercentage(5));
    }

    @Test
    void payloadsHaveTheRequestedSizeAndAreDistinct() {
        List<byte[]> pool = CompressiblePayloads.pool(1000, 0.45, 50, new Random(1));

        assertThat(pool).hasSize(50).allSatisfy(payload -> assertThat(payload).hasSize(1000));
        Set<ByteBuffer> distinct = new HashSet<>();
        pool.forEach(payload -> distinct.add(ByteBuffer.wrap(payload)));
        assertThat(distinct).hasSize(50);
    }

    @Test
    void emptyPayloadsNeedNoCalibration() {
        List<byte[]> pool = CompressiblePayloads.pool(0, 0.5, 3, new Random(1));

        assertThat(pool).hasSize(3).allSatisfy(payload -> assertThat(payload).isEmpty());
        assertThat(CompressiblePayloads.ratio(PayloadTemplates.of(pool, null))).isEqualTo(1.0);
    }

    @Test
    void ratioWeighsEachPayloadByHowOftenItIsSent() {
        byte[] zeros = new byte[4096];
        byte[] random = new byte[4096];
        new Random(3).nextBytes(random);
        double zerosSize = Zstd.compress(zeros, CompressiblePayloads.ZSTD_LEVEL).length;
        double randomSize = Zstd.compress(random, CompressiblePayloads.ZSTD_LEVEL).length;

        double ratio =
                CompressiblePayloads.ratio(PayloadTemplates.of(List.of(zeros, random), new int[] {3, 1}));

        assertThat(ratio).isEqualTo((3 * zerosSize + randomSize) / (4 * 4096.0));
    }

    @Test
    void generatorRejectsRatiosOutsideZeroToOne() {
        for (double ratio : new double[] {0, -0.1, 1.01, Double.NaN}) {
            assertThatThrownBy(() -> new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, ratio, 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("payloadGenerator targetCompressionRatio must be in (0, 1]");
        }
        assertThat(new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 1.0, 10).poolSize())
                .isEqualTo(10);
    }

    @Test
    void generatorRejectsAnEmptyPoolAndMissingFields() {
        assertThatThrownBy(() -> new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 0.5, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("payloadGenerator poolSize must be positive");
        assertThatThrownBy(
                        () -> PayloadGenerator.fromJson(PayloadGenerator.Type.COMPRESSIBLE, 0.5, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("payloadGenerator requires type, targetCompressionRatio and poolSize");
    }
}
