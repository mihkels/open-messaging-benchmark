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
package io.openmessaging.benchmark.e2e;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class ArtifactResolverIT extends BaseE2eIT {
    @Test
    void rejectsACorruptCachedArtifact() throws Exception {
        Path artifact = tempDir.resolve("provider.jar");
        Files.writeString(artifact, "not the pinned artifact");
        assertThatThrownBy(
                        () ->
                                ArtifactResolver.verify(
                                        artifact, "0000000000000000000000000000000000000000000000000000000000000000"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SHA-256 mismatch");
    }
}
