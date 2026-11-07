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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openmessaging.benchmark.driver.BenchmarkDriver.TopicInfo;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class KafkaTopicCreatorRetryTest {
    private static final List<TopicInfo> TOPICS =
            List.of(new TopicInfo("a", 1), new TopicInfo("b", 1));

    private final AdminClient admin = mock(AdminClient.class);
    private final ScheduledExecutorService progress = mock(ScheduledExecutorService.class);

    @AfterEach
    void clearInterrupt() {
        Thread.interrupted();
    }

    @Test
    void existingTopicsAfterFailedAttemptCountAsSuccess() {
        failEveryAttempt();
        listTopicsReturns(Set.of("a", "b", "other"));

        creator().createWithRetry(TOPICS, progress);

        verify(progress, times(1)).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
        verify(admin, times(1)).listTopics();
    }

    @Test
    void partiallyExistingTopicsAreRetried() {
        ScheduledFuture<?> logging = mock(ScheduledFuture.class);
        when(progress.scheduleAtFixedRate(any(), anyLong(), anyLong(), any()))
                .thenThrow(new RejectedExecutionException("first attempt"))
                .thenAnswer(invocation -> logging);
        listTopicsReturns(Set.of("a"));
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.values())
                .thenReturn(
                        Map.of(
                                "a", KafkaFuture.completedFuture(null),
                                "b", KafkaFuture.completedFuture(null)));
        when(admin.createTopics(any())).thenReturn(created);

        creator().createWithRetry(TOPICS, progress);

        verify(progress, times(2)).scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
        verify(admin, times(1)).createTopics(any());
    }

    @Test
    void failedExistenceChecksExhaustEveryAttempt() throws Exception {
        failEveryAttempt();
        listTopicsThrows(new ExecutionException(new IllegalStateException("metadata unavailable")));

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS, progress))
                .hasMessageStartingWith("Failed to create topics after 6 attempts")
                .hasCauseInstanceOf(RejectedExecutionException.class);
        verify(admin, times(5)).listTopics();
    }

    @Test
    void interruptedExistenceCheckStopsRetrying() throws Exception {
        failEveryAttempt();
        InterruptedException interrupted = new InterruptedException("interrupted");
        listTopicsThrows(interrupted);

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS, progress))
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

        assertThatThrownBy(() -> creator().createWithRetry(TOPICS, progress))
                .hasMessage("Topic creation interrupted")
                .hasCause(wrapped);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        verify(admin, times(1)).listTopics();
    }

    private KafkaTopicCreator creator() {
        return new KafkaTopicCreator(admin, Map.of(), (short) 1, 500, 1);
    }

    private void failEveryAttempt() {
        when(progress.scheduleAtFixedRate(any(), anyLong(), anyLong(), any()))
                .thenThrow(new RejectedExecutionException("progress executor unavailable"));
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
