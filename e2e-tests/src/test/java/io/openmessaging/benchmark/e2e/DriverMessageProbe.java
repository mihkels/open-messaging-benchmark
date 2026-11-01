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

import io.openmessaging.benchmark.driver.BenchmarkDriver;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Standalone probe executed with one installed driver bundle on the broker network. */
public final class DriverMessageProbe implements ConsumerCallback {
    private final BlockingQueue<byte[]> deliveries = new LinkedBlockingQueue<>();
    private final BlockingQueue<Long> timestamps = new LinkedBlockingQueue<>();

    @Override
    public void messageReceived(byte[] payload, long timestamp) {
        timestamps.add(timestamp);
        deliveries.add(Arrays.copyOf(payload, payload.length));
    }

    @Override
    public void messageReceived(ByteBuffer payload, long timestamp) {
        ByteBuffer copy = payload.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        timestamps.add(timestamp);
        deliveries.add(bytes);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 3) {
            throw new IllegalArgumentException(
                    "Expected driver class, config path and optional timestamp check or unmatched config");
        }
        Class<? extends BenchmarkDriver> type =
                Class.forName(args[0]).asSubclass(BenchmarkDriver.class);
        boolean checkTimestamps = args.length == 3 && args[2].equals("--timestamps");
        if (args.length == 3 && !checkTimestamps) {
            verifySelector(type, Path.of(args[1]), Path.of(args[2]));
            return;
        }
        DriverMessageProbe probe = new DriverMessageProbe();
        try (BenchmarkDriver driver = type.getDeclaredConstructor().newInstance()) {
            driver.initialize(Path.of(args[1]), null);
            String topic = driver.getTopicNamePrefix() + "-probe-" + UUID.randomUUID();
            driver.createTopic(topic, 1).get(30, TimeUnit.SECONDS);
            try (var consumer = driver.createConsumer(topic, "probe", probe).get(30, TimeUnit.SECONDS);
                    var producer = driver.createProducer(topic).get(30, TimeUnit.SECONDS)) {
                long sendStarted = System.currentTimeMillis();
                for (int i = 0; i < 10; i++) {
                    producer.sendAsync(Optional.empty(), payload(i)).get(30, TimeUnit.SECONDS);
                }
                for (int i = 0; i < 10; i++) {
                    byte[] received = probe.deliveries.poll(30, TimeUnit.SECONDS);
                    if (!Arrays.equals(payload(i), received)) {
                        throw new AssertionError(
                                "Missing, reordered or corrupted message " + i + ": " + Arrays.toString(received));
                    }
                    Long timestamp = probe.timestamps.poll(30, TimeUnit.SECONDS);
                    if (checkTimestamps
                            && (timestamp == null
                                    || timestamp < sendStarted - 1_000
                                    || timestamp > System.currentTimeMillis() + 1_000)) {
                        throw new AssertionError(
                                "Invalid publish timestamp for message " + i + ": " + timestamp);
                    }
                }
            }
        }
    }

    private static void verifySelector(
            Class<? extends BenchmarkDriver> type, Path matchingConfig, Path unmatchedConfig)
            throws Exception {
        byte[] matching = "matching".getBytes(StandardCharsets.UTF_8);
        byte[] unmatched = "unmatched".getBytes(StandardCharsets.UTF_8);
        DriverMessageProbe probe = new DriverMessageProbe();
        try (BenchmarkDriver matchedDriver = type.getDeclaredConstructor().newInstance();
                BenchmarkDriver unmatchedDriver = type.getDeclaredConstructor().newInstance()) {
            matchedDriver.initialize(matchingConfig, null);
            unmatchedDriver.initialize(unmatchedConfig, null);
            String topic = matchedDriver.getTopicNamePrefix() + "-selector-" + UUID.randomUUID();
            matchedDriver.createTopic(topic, 1).get(30, TimeUnit.SECONDS);
            try (var consumer =
                            matchedDriver.createConsumer(topic, "selector", probe).get(30, TimeUnit.SECONDS);
                    var matchedProducer = matchedDriver.createProducer(topic).get(30, TimeUnit.SECONDS);
                    var unmatchedProducer = unmatchedDriver.createProducer(topic).get(30, TimeUnit.SECONDS)) {
                unmatchedProducer.sendAsync(Optional.empty(), unmatched).get(30, TimeUnit.SECONDS);
                matchedProducer.sendAsync(Optional.empty(), matching).get(30, TimeUnit.SECONDS);
                byte[] received = probe.deliveries.poll(30, TimeUnit.SECONDS);
                if (!Arrays.equals(matching, received)) {
                    throw new AssertionError(
                            "Selector did not deliver only the matching message: " + Arrays.toString(received));
                }
                byte[] extra = probe.deliveries.poll(2, TimeUnit.SECONDS);
                if (extra != null) {
                    throw new AssertionError(
                            "Selector delivered an extra message: " + Arrays.toString(extra));
                }
            }
        }
    }

    private static byte[] payload(int index) {
        return ByteBuffer.allocate(16)
                .putInt(index)
                .putLong(0x1020304050607080L)
                .putInt(0x80ff0000 | index)
                .array();
    }
}
