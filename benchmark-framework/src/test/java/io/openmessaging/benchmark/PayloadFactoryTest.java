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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withinPercentage;

import io.openmessaging.benchmark.utils.payload.CompressiblePayloads;
import io.openmessaging.benchmark.utils.payload.PayloadGenerator;
import io.openmessaging.benchmark.utils.payload.PayloadTemplates;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PayloadFactoryTest {

    @TempDir Path tempDir;

    @Test
    void createsRandomizedPayloadPoolWithConfiguredSizeAndZeroPadding() {
        Workload workload = new Workload();
        workload.messageSize = 10;
        workload.useRandomizedPayloads = true;
        workload.randomBytesRatio = 0.5;
        workload.randomizedPayloadPoolSize = 3;

        PayloadTemplates payloads = PayloadFactory.create(workload);

        assertThat(payloads.weights()).isNull();
        assertThat(payloads.templates()).hasSize(3);
        assertThat(payloads.templates())
                .allSatisfy(payload -> {
                    assertThat(payload).hasSize(10);
                    assertThat(Arrays.copyOf(payload, 5)).isNotEqualTo(new byte[5]);
                    assertThat(Arrays.copyOfRange(payload, 5, 10)).containsOnly((byte) 0);
                });
    }

    @Test
    void createsPayloadsForEachMessageSizeBucket() {
        Workload workload = new Workload();
        workload.messageSizeDistribution = Map.of("0-10", 2, "10-20", 1);
        workload.useRandomizedPayloads = true;
        workload.randomBytesRatio = 0;

        PayloadTemplates payloads = PayloadFactory.create(workload);

        assertThat(payloads.templates())
                .extracting(payload -> payload.length)
                .containsExactlyInAnyOrder(5, 15);
        assertThat(payloads.templates())
                .allSatisfy(payload -> assertThat(payload).containsOnly((byte) 0));
        assertThat(payloads.weights()).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void generatesACompressiblePoolOfTheMessageSize() {
        Workload workload = new Workload();
        workload.messageSize = 1024;
        workload.payloadGenerator = new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 0.45, 20);

        PayloadTemplates payloads = PayloadFactory.create(workload);

        assertThat(payloads.weights()).isNull();
        assertThat(payloads.templates())
                .hasSize(20)
                .allSatisfy(payload -> assertThat(payload).hasSize(1024));
        assertThat(CompressiblePayloads.ratio(payloads)).isCloseTo(0.45, withinPercentage(5));
    }

    @Test
    void generatesAPoolPerSizeBucketWithTheBucketWeight() {
        Workload workload = new Workload();
        Map<String, Integer> sizes = new LinkedHashMap<>();
        sizes.put("1KB-1KB", 3);
        sizes.put("4KB-4KB", 1);
        workload.messageSizeDistribution = sizes;
        workload.payloadGenerator = new PayloadGenerator(PayloadGenerator.Type.COMPRESSIBLE, 0.3, 5);

        PayloadTemplates payloads = PayloadFactory.create(workload);

        assertThat(payloads.templates())
                .extracting(payload -> payload.length)
                .containsExactly(1024, 1024, 1024, 1024, 1024, 4096, 4096, 4096, 4096, 4096);
        assertThat(payloads.weights()).containsExactly(3, 3, 3, 3, 3, 1, 1, 1, 1, 1);
        assertThat(CompressiblePayloads.ratio(payloads)).isCloseTo(0.3, withinPercentage(5));
    }

    @Test
    void loadsConfiguredPayloadFile() throws Exception {
        Path payloadFile = tempDir.resolve("payload.bin");
        byte[] expected = {1, 2, 3};
        Files.write(payloadFile, expected);
        Workload workload = new Workload();
        workload.messageSize = expected.length;
        workload.payloadFile = payloadFile.toString();

        assertThat(PayloadFactory.create(workload).templates()).containsExactly(expected);
    }

    @Test
    void aFileBasedWorkloadWithoutAPayloadFileFailsWithTheMissingKey() {
        Workload workload = new Workload();
        workload.name = "no-payload";
        workload.messageSize = 10;

        assertThatThrownBy(() -> PayloadFactory.create(workload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Workload no-payload must set payloadFile, payloadGenerator,"
                                + " useRandomizedPayloads or messageSizeDistribution");
    }
}
