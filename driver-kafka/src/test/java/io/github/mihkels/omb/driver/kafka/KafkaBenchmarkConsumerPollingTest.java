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
package io.github.mihkels.omb.driver.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.mihkels.omb.driver.ConsumerCallback;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class KafkaBenchmarkConsumerPollingTest {
    @Test
    @SuppressWarnings("unchecked")
    void deliversRecordsAndCommitsOffsetsWhenAutoCommitIsOff() throws Exception {
        KafkaConsumer<String, byte[]> client = mock(KafkaConsumer.class);
        when(client.poll(any(Duration.class)))
                .thenReturn(records(new ConsumerRecord<>("t", 3, 4, "k", new byte[] {1})))
                .thenReturn(ConsumerRecords.empty());
        ConsumerCallback callback = mock(ConsumerCallback.class);

        KafkaBenchmarkConsumer consumer =
                new KafkaBenchmarkConsumer(client, new Properties(), callback);
        verify(callback, timeout(2000))
                .messageReceived(new byte[] {1}, ConsumerCallback.NO_PUBLISH_TIMESTAMP, 3);
        verify(client, timeout(2000))
                .commitAsync(Map.of(new TopicPartition("t", 3), new OffsetAndMetadata(5)), null);
        consumer.close();
        verify(client).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void passesPositiveTimestampsAndMapsMissingTimestampToNoPublishTimestamp() throws Exception {
        KafkaConsumer<String, byte[]> client = mock(KafkaConsumer.class);
        ConsumerRecord<String, byte[]> missingTimestamp =
                new ConsumerRecord<>(
                        "t",
                        3,
                        4,
                        ConsumerRecord.NO_TIMESTAMP,
                        TimestampType.NO_TIMESTAMP_TYPE,
                        1,
                        1,
                        "k",
                        new byte[] {1},
                        new RecordHeaders(),
                        Optional.empty());
        ConsumerRecord<String, byte[]> validTimestamp =
                new ConsumerRecord<>(
                        "t",
                        3,
                        5,
                        12345L,
                        TimestampType.CREATE_TIME,
                        1,
                        1,
                        "k",
                        new byte[] {2},
                        new RecordHeaders(),
                        Optional.empty());
        when(client.poll(any(Duration.class)))
                .thenReturn(
                        new ConsumerRecords<>(
                                Map.of(new TopicPartition("t", 3), List.of(missingTimestamp, validTimestamp)),
                                Map.of()))
                .thenReturn(ConsumerRecords.empty());
        ConsumerCallback callback = mock(ConsumerCallback.class);

        KafkaBenchmarkConsumer consumer =
                new KafkaBenchmarkConsumer(client, new Properties(), callback);
        verify(callback, timeout(2000))
                .messageReceived(new byte[] {1}, ConsumerCallback.NO_PUBLISH_TIMESTAMP, 3);
        verify(callback, timeout(2000)).messageReceived(new byte[] {2}, 12345L, 3);
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void autoCommitSkipsManualCommitsAndCallbackFailuresAreLogged() throws Exception {
        KafkaConsumer<String, byte[]> client = mock(KafkaConsumer.class);
        when(client.poll(any(Duration.class)))
                .thenReturn(records(new ConsumerRecord<>("t", 0, 0, "k", new byte[] {1})))
                .thenReturn(ConsumerRecords.empty());
        ConsumerCallback callback = mock(ConsumerCallback.class);
        doThrow(new IllegalStateException("callback"))
                .when(callback)
                .messageReceived(any(byte[].class), anyLong(), anyInt());
        Properties config = new Properties();
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaBenchmarkConsumer consumer = new KafkaBenchmarkConsumer(client, config, callback, 1L);
        verify(callback, timeout(2000)).messageReceived(any(byte[].class), anyLong(), anyInt());
        verify(client, after(50).never()).commitAsync(anyMap(), any());
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void pollFailuresAfterHandoffAreLoggedAndPollingContinues() throws Exception {
        KafkaConsumer<String, byte[]> client = mock(KafkaConsumer.class);
        when(client.poll(any(Duration.class)))
                .thenThrow(new IllegalStateException("broker"))
                .thenThrow(new InterruptException("interrupted"))
                .thenReturn(ConsumerRecords.empty());
        // InterruptException's constructor interrupts the thread that builds it.
        Thread.interrupted();

        KafkaBenchmarkConsumer consumer =
                new KafkaBenchmarkConsumer(client, new Properties(), mock(ConsumerCallback.class), 1L);
        verify(client, timeout(2000).atLeast(3)).poll(any(Duration.class));
        consumer.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void closeRethrowsClientCloseFailureAndIgnoresWakeupFailure() {
        KafkaConsumer<String, byte[]> client = mock(KafkaConsumer.class);
        when(client.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());
        IllegalStateException closeFailure = new IllegalStateException("close");
        doThrow(closeFailure).when(client).close();
        doThrow(new IllegalStateException("wakeup")).when(client).wakeup();

        KafkaBenchmarkConsumer consumer =
                new KafkaBenchmarkConsumer(client, new Properties(), mock(ConsumerCallback.class), 1L);
        assertThatThrownBy(consumer::close).isSameAs(closeFailure);
        assertThatThrownBy(consumer::close).isSameAs(closeFailure);
        verify(client).wakeup();
        verify(client, never()).commitAsync(anyMap(), any());
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    private static ConsumerRecords<String, byte[]> records(ConsumerRecord<String, byte[]> record) {
        return new ConsumerRecords<>(
                Map.of(new TopicPartition(record.topic(), record.partition()), List.of(record)), Map.of());
    }
}
