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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.openmessaging.benchmark.Workload;
import org.junit.jupiter.api.Test;

class ResultAssertionsIT extends BaseE2eIT {
    @Test
    void acceptsDeliveredMessagesForEverySubscription() {
        Workload workload = createSimpleWorkload();
        workload.subscriptionsPerTopic = 2;
        assertThatCode(() -> validateResults(new SmokeResult(10, 0, 20, 1.0), workload))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingMessagesAndPublishErrors() {
        Workload workload = createSimpleWorkload();
        assertThatThrownBy(() -> validateResults(new SmokeResult(9, 0, 10, 1.0), workload))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> validateResults(new SmokeResult(10, 0, 9, 1.0), workload))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> validateResults(new SmokeResult(10, 1, 10, 1.0), workload))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void rejectsInvalidLatency() {
        Workload workload = createSimpleWorkload();
        assertThatThrownBy(() -> validateResults(new SmokeResult(10, 0, 10, Double.NaN), workload))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> validateResults(new SmokeResult(10, 0, 10, -1.0), workload))
                .isInstanceOf(AssertionError.class);
    }
}
