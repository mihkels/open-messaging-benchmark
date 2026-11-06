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
package io.openmessaging.benchmark.driver.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

class KafkaTopicCreatorBatchFailureTest {
    @Test
    void failedBatchRequestIsRequeuedAndRetried() {
        AdminClient admin = mock(AdminClient.class);
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.values()).thenReturn(Map.of("topic", KafkaFuture.completedFuture(null)));
        when(admin.createTopics(any()))
                .thenThrow(new IllegalStateException("broker unavailable"))
                .thenReturn(created);

        CompletableFuture<Void> result =
                new KafkaTopicCreator(admin, Map.of(), (short) 1)
                        .create(List.of(new TopicInfo("topic", 1)));
        result.join();
        verify(admin, times(2)).createTopics(any());
        assertThat(result.cancel(true)).isFalse();
    }

    @Test
    void wrappedInterruptionStopsCreationWithoutRetrying() {
        AdminClient admin = mock(AdminClient.class);
        RuntimeException wrapped =
                new RuntimeException(new IllegalStateException(new InterruptedException()));
        when(admin.createTopics(any())).thenThrow(wrapped);

        assertThatThrownBy(
                        () ->
                                new KafkaTopicCreator(admin, Map.of(), (short) 1, 1)
                                        .create(List.of(new TopicInfo("topic", 1)))
                                        .join())
                .hasCause(wrapped);
        verify(admin, times(1)).createTopics(any());
        verify(admin, never()).listTopics();
    }

    @Test
    void repeatedCancellationReportsTheFirstOutcome() {
        AdminClient admin = mock(AdminClient.class);
        when(admin.createTopics(any()))
                .thenAnswer(
                        invocation -> {
                            Thread.sleep(10_000);
                            return null;
                        });
        CompletableFuture<Void> result =
                new KafkaTopicCreator(admin, Map.of(), (short) 1)
                        .create(List.of(new TopicInfo("topic", 1)));
        assertThat(result.cancel(true)).isTrue();
        assertThat(result.cancel(true)).isTrue();
        assertThat(result).isCancelled();
    }
}
