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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;

class KafkaTopicCreatorLifecycleTest {
    @Test
    @SuppressWarnings("unchecked")
    void interruptionIsNotConvertedIntoAnotherTopicCreationAttempt() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        CreateTopicsResult topics = mock(CreateTopicsResult.class);
        KafkaFuture<Void> topic = mock(KafkaFuture.class);
        InterruptedException interrupted = new InterruptedException("interrupted");
        when(admin.createTopics(any())).thenReturn(topics);
        when(topics.values()).thenReturn(Map.of("topic", topic));
        when(topic.get()).thenThrow(interrupted);

        CompletionException failure =
                assertThrows(
                        CompletionException.class,
                        () ->
                                new KafkaTopicCreator(admin, Map.of(), (short) 1, 1)
                                        .create(List.of(new TopicInfo("topic", 1)))
                                        .join());
        assertSame(interrupted, failure.getCause().getCause());
        verify(admin, times(1)).createTopics(any());
        verify(admin, never()).listTopics();
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancellationInterruptsWorkerAndDisposesProgressExecutor() throws Exception {
        AdminClient admin = mock(AdminClient.class);
        CreateTopicsResult topics = mock(CreateTopicsResult.class);
        KafkaFuture<Void> topic = mock(KafkaFuture.class);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Thread> worker = new AtomicReference<>();
        when(admin.createTopics(any())).thenReturn(topics);
        when(topics.values()).thenReturn(Map.of("topic", topic));
        when(topic.get())
                .thenAnswer(
                        ignored -> {
                            worker.set(Thread.currentThread());
                            entered.countDown();
                            try {
                                release.await();
                                return null;
                            } catch (InterruptedException error) {
                                interrupted.countDown();
                                throw error;
                            }
                        });
        CompletableFuture<Void> creation =
                new KafkaTopicCreator(admin, Map.of(), (short) 1, 1)
                        .create(List.of(new TopicInfo("topic", 1)));
        try {
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertTrue(creation.cancel(true));
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            await(() -> !worker.get().isAlive());
            await(
                    () ->
                            Thread.getAllStackTraces().keySet().stream()
                                    .noneMatch(KafkaTopicCreatorLifecycleTest::isProgressThreadAlive));
            verify(admin, times(1)).createTopics(any());
            verify(admin, never()).listTopics();
        } finally {
            release.countDown();
        }
    }

    @Test
    void emptyBatchDoesNotStartKafkaCreation() {
        AdminClient admin = mock(AdminClient.class);
        new KafkaTopicCreator(admin, Map.of(), (short) 1, 1).create(List.of()).join();
        verify(admin, never()).createTopics(any());
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertTrue(condition.getAsBoolean(), "condition not met before timeout");
    }

    private static boolean isProgressThreadAlive(Thread thread) {
        return thread.isAlive() && thread.getName().equals("kafka-topic-progress");
    }
}
