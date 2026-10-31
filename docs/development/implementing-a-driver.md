<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Implementing a driver

A driver adapts one messaging system to the benchmark. It implements the service provider interface
(SPI) in the
[`driver-api`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-api/src/main/java/io/openmessaging/benchmark/driver)
module. The benchmark generates load, measures it and reports the results. The driver only creates
topics, producers and consumers, and moves bytes.

The running example on this page is the broker-less loopback driver in
[`examples/external-driver-plugin`](https://github.com/mihkels/open-messaging-benchmark/tree/main/examples/external-driver-plugin).
To package and install a driver, see [Packaging a plugin](packaging-a-plugin.md).

## The SPI

|        Type         |                           Kind                           |                                   Purpose                                    |
|---------------------|----------------------------------------------------------|------------------------------------------------------------------------------|
| `BenchmarkDriver`   | interface, extends `AutoCloseable`                       | Entry point: configuration, topics, producers, consumers                     |
| `BenchmarkProducer` | interface, extends `AutoCloseable`                       | `sendAsync(Optional<String> key, byte[] payload)`                            |
| `BenchmarkConsumer` | interface, extends `AutoCloseable`                       | Marker for a subscription; only `close()` is called                          |
| `ConsumerCallback`  | interface                                                | Implemented by the benchmark; the driver calls it for every received message |
| `TopicInfo`         | record `(topic, partitions)`                             | Argument of `createTopics`                                                   |
| `ProducerInfo`      | record `(id, topic)`                                     | Argument of `createProducers`                                                |
| `ConsumerInfo`      | record `(id, topic, subscriptionName, consumerCallback)` | Argument of `createConsumers`                                                |
| `ResourceCreator`   | helper class                                             | Batched creation with retries, for `createProducers` / `createConsumers`     |
| `DriverApiVersion`  | constant `CURRENT = 1`                                   | SPI version that plugin bundles declare                                      |

The records are nested in `BenchmarkDriver`. The full `BenchmarkDriver` interface is:

```java
public interface BenchmarkDriver extends AutoCloseable {
    void initialize(Path configurationFile, PrometheusMeterRegistry statsLogger)
            throws IOException, InterruptedException;

    String getTopicNamePrefix();

    CompletableFuture<Void> createTopic(String topic, int partitions);

    default CompletableFuture<Void> createTopics(List<TopicInfo> topicInfos) { ... }

    CompletableFuture<BenchmarkProducer> createProducer(String topic);

    default CompletableFuture<List<BenchmarkProducer>> createProducers(List<ProducerInfo> producers) { ... }

    CompletableFuture<BenchmarkConsumer> createConsumer(
            String topic, String subscriptionName, ConsumerCallback consumerCallback);

    default CompletableFuture<List<BenchmarkConsumer>> createConsumers(List<ConsumerInfo> consumers) { ... }
}
```

The default `createTopics`, `createProducers` and `createConsumers` call the single-resource method
for every entry and complete when all of them have completed.

## Lifecycle

The driver runs inside the isolated driver JVM, in `LocalWorker` (see
[Architecture](architecture.md)). One benchmark run calls it in this order:

```text
new Driver()                       public no-argument constructor
initialize(configFile, statsLogger)
getTopicNamePrefix()
createTopics(...)                  on the leader worker only, in distributed mode
createConsumers(...)
createProducers(...)
producer.sendAsync(...)            readiness probe, then the measured load
consumerCallback.messageReceived(...)
producer.close() ...               for every producer
consumer.close() ...               for every consumer
close()
```

### Construction and `initialize`

`LocalWorker` loads `driverClass` from the driver YAML with `Class.forName(...)` and calls its public
no-argument constructor. It then calls `initialize(configurationFile, statsLogger)`:

- `configurationFile` is a local copy of the driver YAML. The benchmark reads only `name` and
  `driverClass` from it (unknown properties are ignored), so the driver parses the rest. The in-tree
  drivers read it with Jackson's YAML mapper and disable `FAIL_ON_UNKNOWN_PROPERTIES`, so the shared
  keys do not cause errors.
- `statsLogger` is the driver process's `PrometheusMeterRegistry`. Meters registered there are exposed
  on the worker's `/metrics` endpoint (see [Monitoring](../guides/monitoring.md)).
- Open broker connections and admin clients here. An exception fails driver initialization for the
  whole workload.

The loopback driver only logs:

```java
@Override
public void initialize(Path configurationFile, PrometheusMeterRegistry statsLogger) {
    log.info(
            "Loopback driver initialized from {} using bundled commons-lang3 {}",
            configurationFile,
            StringUtils.defaultString(StringUtils.class.getPackage().getImplementationVersion()));
}
```

### Topic names

`getTopicNamePrefix()` returns the first part of every topic name. The benchmark generates names in
the format `<prefix>-<7-digit index>-<random suffix>`, for example `loopback-0000000-AbCdEfG`. If the
messaging system restricts names, choose a prefix that keeps generated names valid.

### Creating topics

`createTopic(topic, partitions)` creates a topic with the requested number of partitions
(`partitionsPerTopic` in the workload). In distributed mode only the first worker's driver instance
creates topics. Producers and consumers on other workers then use these topics through their own
driver instances, so do not rely on state held in the creating instance.

Override `createTopics(List<TopicInfo>)` when the system can create many topics in one request, as the
Kafka driver does.

### Creating producers and consumers

`createProducer(topic)` and `createConsumer(topic, subscriptionName, consumerCallback)` each return a
future that completes once the resource is usable:

- The benchmark creates consumers before producers.
- Every subscription name must behave as an independent subscription that receives every message on
  the topic. Consumers that share a subscription name split the messages between them.
- The `consumerCallback` passed in is the benchmark's statistics recorder. Keep the reference and call
  it for every message that the consumer receives.

The loopback consumer registers the callback on its subscription and removes it again on `close()`:

```java
Subscription subscription =
        topics
                .computeIfAbsent(topic, t -> new ConcurrentHashMap<>())
                .computeIfAbsent(subscriptionName, s -> new Subscription());
subscription.consumers.add(consumerCallback);
BenchmarkConsumer consumer = () -> subscription.consumers.remove(consumerCallback);
return CompletableFuture.completedFuture(consumer);
```

For systems where creating many clients at once is slow or rate-limited, override `createProducers` /
`createConsumers` and use `ResourceCreator`. It splits the list into batches of `maxBatchSize`, waits
`interBatchDelayMs` between batches, and retries failed entries until all of them succeed, logging
progress every 10 s. The RabbitMQ driver is an example:

```java
return new ResourceCreator<ProducerInfo, BenchmarkProducer>(
                "producer",
                config.producerCreationBatchSize,
                config.producerCreationDelay,
                ps -> ps.stream().collect(toMap(p -> p, p -> createProducer(p.topic()))),
                fc -> {
                    try {
                        return new CreationResult<>(fc.get(), true);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        log.debug(e.getMessage());
                        return new CreationResult<>(null, false);
                    }
                })
        .create(producers);
```

Because `ResourceCreator` retries indefinitely, a permanent failure such as wrong credentials keeps
creation running instead of failing the workload.

## Asynchronous semantics

All creation methods and `sendAsync` return `CompletableFuture`s:

- The worker waits (`join()`) on the futures from `createTopics`, `createProducers` and
  `createConsumers`. An exceptional completion fails the workload.
- Producers run on virtual threads. Each producer thread waits for the shared rate limiter, calls
  `sendAsync`, and loops. Time spent inside `sendAsync` before it returns delays the producer's next
  send, so start the send and return without waiting for the broker.
- The benchmark measures publish latency from the call to `sendAsync` until the returned future
  completes. Complete the future only when the broker has acknowledged the message, according to the
  durability settings of the driver configuration.
- Complete the future exceptionally when a send fails. The benchmark counts it as a publish error and
  logs `Write error on message`. An exception thrown directly from `sendAsync` stops that producer's
  thread for the rest of the run (`Producer thread failed`).
- `key` is empty when the workload uses `NO_KEY`. Otherwise it is the key that the workload's key
  distributor selected.

The loopback producer delivers synchronously and returns a completed future:

```java
@Override
public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
    long now = System.currentTimeMillis();
    subscriptions.values().forEach(s -> s.deliver(payload, now));
    return CompletableFuture.completedFuture(null);
}
```

## End-to-end publish timestamp

End-to-end latency is computed in the consumer callback:

```java
void messageReceived(byte[] payload, long publishTimestamp);
void messageReceived(ByteBuffer payload, long publishTimestamp);
```

- `publishTimestamp` is **epoch milliseconds** (`System.currentTimeMillis()` on the producer side).
  The benchmark subtracts it from its own `System.currentTimeMillis()` on receipt and records the
  difference in microseconds. Passing nanoseconds or microseconds produces meaningless latencies.
- The timestamp must travel with the message from producer to consumer. Producers and consumers can
  run in different driver processes on different hosts, so a timestamp kept in the producer's memory
  does not work. Use a broker-assigned publish/append time when the system provides one, or put the
  send time in a message header or property. For example, the Kafka driver passes the record
  timestamp, and the JMS driver passes a long property set by the producer.
- Values that produce a latency of zero or less are counted as received but are not recorded in the
  latency histograms.
- Call the callback once per message, with the payload or a `ByteBuffer` view of it. The payload
  length is recorded as received bytes.

Comparing clocks across hosts makes end-to-end latency sensitive to clock skew; see
[Latency and backlog](../methodology/latency-and-backlog.md).

## Readiness

The SPI has no readiness method. After producers and consumers are created, the benchmark calls
`sendAsync(Optional.of("key"), new byte[10])` once on every producer. It then waits up to 60 s until
the consumers have received at least `topics × subscriptionsPerTopic` messages, polling every 2 s.
If too few probe messages arrive, the run fails with `Timed out waiting for consumers to be ready`.

The consequences for a driver:

- A consumer must be subscribed and positioned so that it receives messages published after its
  `createConsumer` future completes. A consumer that starts from the latest offset only after a
  delayed assignment can miss the probe.
- The probe is an ordinary message with a 10-byte payload. Consumers must deliver it like any other
  message.

## Pausing consumers

The SPI has no pause or resume method. For backlog workloads (`consumerBacklogSizeGB`), the benchmark
pauses consumption by blocking inside `ConsumerCallback.messageReceived`: while consumers are paused,
the call sleeps in 1 s steps and returns after `resumeConsumers`. The message being delivered has
already been counted at that point.

A driver must therefore tolerate a callback that blocks for a long time. It must not treat a slow
callback as a failure, for example by timing out a session, and should apply back-pressure instead of
buffering messages without limit while the callback blocks.

## Error handling

- Exceptions from `initialize`, from the creation futures, and from `close()` propagate to the
  coordinator, which logs the failure and moves on to the next workload. The worker HTTP layer returns
  the exception chain in the response body.
- Report send failures through the `sendAsync` future, as described above.
- Errors on the consumer side cannot be reported through the SPI. Log them. Missing messages show up
  as a lower consume rate and a growing backlog in the results.

## Resource cleanup

At the end of each workload, `stopAll()` closes every producer, then every consumer, then the driver,
and discards the driver instance. The next workload or driver gets a new instance through the
constructor and `initialize`, in the same driver process unless the driver changes.

- `BenchmarkProducer.close()` should flush or abandon outstanding sends and release the client.
- `BenchmarkConsumer.close()` should stop delivery to the callback and release the client.
- `BenchmarkDriver.close()` should release shared connections, admin clients and thread pools. Leaked
  threads or connections stay alive in the driver process for later workloads.
- The benchmark does not delete topics. Topic names include a random suffix, so later runs do not reuse
  them. Delete them on the broker, or in `close()` if the driver has a reason to.

The loopback driver forgets all of its topics:

```java
@Override
public void close() {
    topics.clear();
}
```

## Driver configuration file

A driver configuration is a YAML file with at least:

```yaml
name: Loopback
driverClass: com.example.omb.loopback.LoopbackBenchmarkDriver
```

`name` appears in results and in the default result file name. `driverClass` selects the driver and
the plugin bundle that provides it. Everything else is driver-specific. Document every key in the
driver's page; see [Documentation](documentation.md#adding-a-driver-page).

## Next steps

- [Packaging a plugin](packaging-a-plugin.md): build the bundle and install it on workers.
- [Testing a driver](testing-a-driver.md): unit tests, Testcontainers tests and the Compose regression
  suite.

