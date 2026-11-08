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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaTopicCreatorRetryTest {
    private static final List<TopicInfo> TOPICS =
            List.of(new TopicInfo("a", 1), new TopicInfo("b", 1));

    // Past the one-minute attempt timeout, so an attempt that reads the clock again times out.
    private static final long PAST_ATTEMPT_TIMEOUT_MS = 60_001;

    private final AdminClient admin = mock(AdminClient.class);
    private LongSupplier clock = () -> 0;

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    void existingTopicsAfterFailedAttemptCountAsSuccess() {
        failEveryAttempt();
        listTopicsReturns(Set.of("a", "b", "other"));

        creator().createWithRetry(TOPICS);

        verify(admin, never()).createTopics(any());
        verify(admin, times(1)).listTopics();
    }

    @Test
    void partiallyExistingTopicsAreRetried() {
        // The first attempt times out before creating anything; the clock then stands still.
        long[] readings = {0, PAST_ATTEMPT_TIMEOUT_MS};
        AtomicLong reads = new AtomicLong();
        clock = () -> readings[(int) Math.min(reads.getAndIncrement(), readings.length - 1)];
        listTopicsReturns(Set.of("a"));
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.values())
                .thenReturn(
                        Map.of(
                                "a", KafkaFuture.completedFuture(null),
                                "b", KafkaFuture.completedFuture(null)));
        when(admin.createTopics(any())).thenReturn(created);

        creator().createWithRetry(TOPICS);

        verify(admin, times(1)).listTopics();
        verify(admin, times(1)).createTopics(any());
    }

    @Test
    void failedExistenceChecksExhaustEveryAttempt() throws Exception {
        failEveryAttempt();
        listTopicsThrows(new ExecutionException(new IllegalStateException("metadata unavailable")));

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS))
                .hasMessageStartingWith("Failed to create topics after 6 attempts")
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageStartingWith("Topic creation attempt timed out");
        verify(admin, times(5)).listTopics();
    }

    @Test
    void interruptedExistenceCheckStopsRetrying() throws Exception {
        failEveryAttempt();
        InterruptedException interrupted = new InterruptedException("interrupted");
        listTopicsThrows(interrupted);

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS))
                .hasMessage("Topic creation interrupted")
                .hasCause(interrupted);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(admin, times(1)).listTopics();
    }

    @Test
    void wrappedInterruptionInExistenceCheckStopsRetrying() throws Exception {
        failEveryAttempt();
        ExecutionException wrapped = new ExecutionException(new InterruptedException("interrupted"));
        listTopicsThrows(wrapped);

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS))
                .hasMessage("Topic creation interrupted")
                .hasCause(wrapped);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(admin, times(1)).listTopics();
    }

    private KafkaTopicCreator creator() {
        return new KafkaTopicCreator(admin, Map.of(), (short) 1, 500, 1, clock);
    }

    // Every clock reading is past the previous one by more than the attempt timeout, so each
    // attempt times out before it creates anything.
    private void failEveryAttempt() {
        AtomicLong now = new AtomicLong();
        clock = () -> now.getAndAdd(PAST_ATTEMPT_TIMEOUT_MS);
    }

    private void listTopicsReturns(Set<String> names) {
        ListTopicsResult listed = mock(ListTopicsResult.class);
        when(listed.names()).thenReturn(KafkaFuture.completedFuture(names));
        when(admin.listTopics()).thenReturn(listed);
    }

    @SuppressWarnings("unchecked")
    private void listTopicsThrows(Exception failure) throws Exception {
        KafkaFuture<Set<String>> names = mock(KafkaFuture.class);
        when(names.get(5, TimeUnit.SECONDS)).thenThrow(failure);
        ListTopicsResult listed = mock(ListTopicsResult.class);
        when(listed.names()).thenReturn(names);
        when(admin.listTopics()).thenReturn(listed);
    }
}
