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

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WorkerTimeoutsTest {
    private static final Duration DEFAULT = Duration.ofMinutes(10);

    @Test
    void usesTheDefaultWhenUnset() {
        assertThat(WorkerTimeouts.parse("X", null, DEFAULT)).isEqualTo(DEFAULT);
        assertThat(WorkerTimeouts.parse("X", " ", DEFAULT)).isEqualTo(DEFAULT);
    }

    @Test
    void parsesSeconds() {
        assertThat(WorkerTimeouts.parse("X", " 1800 ", DEFAULT)).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void rejectsValuesThatAreNotPositiveWholeSeconds() {
        assertThatThrownBy(() -> WorkerTimeouts.parse("OMB_X", "10m", DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OMB_X must be a whole number of seconds: 10m");
        assertThatThrownBy(() -> WorkerTimeouts.parse("OMB_X", "0", DEFAULT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("OMB_X must be positive");
    }

    @Test
    void systemPropertyOverridesTheDefault() {
        System.setProperty(WorkerTimeouts.STOP_PROPERTY, "900");
        try {
            assertThat(WorkerTimeouts.stop()).isEqualTo(Duration.ofMinutes(15));
            // The server must keep a request open as long as the longest client timeout
            assertThat(WorkerTimeouts.serverIdle()).isEqualTo(Duration.ofMinutes(15));
        } finally {
            System.clearProperty(WorkerTimeouts.STOP_PROPERTY);
        }
    }
}
