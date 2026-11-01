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
    void initialize(Path configurationFile, MeterRegistry statsLogger)
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

The default batch methods call the corresponding single-resource method for each entry.
Producer and consumer batches succeed after every entry succeeds, or fail as soon as creation fails.

The `initialize` parameter changed from `PrometheusMeterRegistry` to `MeterRegistry` without changing
`driverApiVersion: 1`. Older version-1 bundles are binary-incompatible even though their descriptors
still match. Rebuild the host and all plugin bundles together against the changed `driver-api`; the
descriptor check cannot detect an old bundle. See
[Compatibility rules](packaging-a-plugin.md#compatibility-rules).

The requirements below define the SPI contract. Individual drivers must also meet the stated
failure, cleanup, readiness and payload-ownership requirements.

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
- `statsLogger` is a host-owned Micrometer `MeterRegistry`. The driver process supplies the same
  `PrometheusMeterRegistry` used for worker statistics, so meters registered there are exposed on the
  worker's `/metrics` endpoint (see [Monitoring](../guides/monitoring.md)). Do not close the registry.
- Open broker connections and admin clients here. An exception fails driver initialization for the
  whole workload.

The loopback driver only logs:

```java
@Override
public void initialize(Path configurationFile, MeterRegistry statsLogger) {
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
non-null future that completes with a non-null handle once the resource is usable, or exceptionally
with the cause if creation fails:

- The benchmark creates consumers before producers.
- A consumer is usable only after its subscription is ready to receive messages published after its
  creation future completes. Report connection and subscription failures through that future.
- Every subscription name must behave as an independent subscription that receives every message on
  the topic. Consumers that share a subscription name split the messages between them.
- The `consumerCallback` passed in is the benchmark's statistics recorder. Keep the reference and call
  it for every message that the consumer receives.

Single-resource creation hands a successfully created handle to its caller. For a batch, that caller
is the batch operation: it owns each handle until the batch future succeeds and transfers the list to
the worker. The worker then owns and closes those producer and consumer handles; the driver owns its
shared clients, connections and other shared resources. On creation failure or cancellation, cancel
pending work where possible and close every successfully created handle not handed off, including
handles that complete late. Each handle must be closed by its owner exactly once.

The default `createProducers` and `createConsumers` implementations close successful handles if any
creation fails or the batch is cancelled, including handles that complete after the batch ends. They
do not cancel individual creation futures: cancelling a `CompletableFuture` does not stop its supplier
and can discard a handle the supplier creates later.

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
`interBatchDelayMs` before each batch, and retries each failed entry up to `maxRetries` times after
the first attempt. It logs progress every 10 s, closes untransferred handles on failure or cancellation,
and shuts down its scheduler when the operation ends. The RabbitMQ driver is an example:

```java
return new ResourceCreator<ProducerInfo, BenchmarkProducer>(
                "producer",
                config.producerCreationBatchSize,
                config.producerCreationDelay,
                config.resourceCreationMaxRetries,
                producer -> createProducer(producer.topic()))
        .create(producers);
```

RabbitMQ defaults to 10 retries per failed resource (11 attempts in total). Set
`resourceCreationMaxRetries: 0` to disable retries; negative values are invalid. An exhausted
resource fails the batch with its original creation cause. Interruption is never retried.

## Asynchronous semantics

All creation methods and `sendAsync` return non-null `CompletableFuture`s. Operational failures must
complete them exceptionally with a meaningful cause instead of returning `null`, hanging, or
reporting success:

- The worker waits (`join()`) on the futures from `createTopics`, `createProducers` and
  `createConsumers`. An exceptional completion fails the workload.
- Producers run on virtual threads. Each producer thread waits for the shared rate limiter, calls
  `sendAsync`, and loops. Time spent inside `sendAsync` before it returns delays the producer's next
  send, so start the send and return without waiting for the broker.
- The benchmark measures publish latency from the call to `sendAsync` until the returned future
  completes. Define and document the completion point for each driver and configuration: it may be
  client acceptance or transaction buffering rather than a broker acknowledgement, and a broker
  acknowledgement does not necessarily mean durable storage. For example, Pravega transactional
  sends complete before the transaction commits; see [Pravega semantics](../drivers/pravega.md#semantics).
- Complete the future exceptionally when a send fails. The benchmark counts it as a publish error and
  logs `Write error on message`. An exception thrown directly from `sendAsync` stops that producer's
  thread for the rest of the run (`Producer thread failed`).
- `key` is empty when the workload uses `NO_KEY`. Otherwise it is the key that the workload's key
  distributor selected.
- Cancellation does not guarantee that the broker did not receive the message.

The loopback producer delivers synchronously and returns a completed future:

```java
@Override
public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {
    long now = System.currentTimeMillis();
    subscriptions.values().forEach(s -> s.deliver(payload, now));
    return CompletableFuture.completedFuture(null);
}
```

### Payload ownership

The caller owns the `byte[]` supplied to `sendAsync`; a driver must treat it as read-only. The caller
may share the same array across concurrent sends, but must not mutate it until **all** futures for
those sends have completed. A driver may retain the array while its own send future is pending, but
must stop using it when that future completes, including exceptional completion or cancellation.
Copy into driver-owned storage before mutating bytes or retaining them beyond completion, including
when an asynchronous client may continue using a payload after the future is cancelled.

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
  coordinator. It continues to the next workload only if cleanup leaves the worker safe to reuse;
  otherwise it skips the remaining workloads. Any failure makes the benchmark exit unsuccessfully.
  The worker HTTP layer returns the exception chain in the response body.
- Report send failures through the `sendAsync` future, as described above.
- Errors on the consumer side cannot be reported through the SPI. Log them. Missing messages show up
  as a lower consume rate and a growing backlog in the results.

## Resource cleanup

On normal shutdown at the end of a workload, `stopAll()` closes handed-off producer and consumer
handles, then the driver, and discards the driver instance. `ProcessWorker` starts a fresh isolated
driver process on the next initialization. Handle closes run concurrently with a shared 45-second
deadline; the driver close has a separate 45-second deadline. A timeout is reported as a failure and
the worker is not reused.

- `BenchmarkProducer.close()` must flush or abandon outstanding sends, settle their futures, and
  release per-producer resources.
- `BenchmarkConsumer.close()` must stop delivery to the callback and release the client.
- `BenchmarkDriver.close()` should release shared connections, admin clients and thread pools. Leaked
  threads or connections consume resources until the driver process terminates.
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

