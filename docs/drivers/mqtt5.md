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

# MQTT 5

The MQTT 5 driver benchmarks MQTT 5 brokers with the [HiveMQ MQTT client](https://github.com/hivemq/hivemq-mqtt-client)
(1.3.8). Each benchmark topic is an MQTT topic and each subscription is an MQTT 5 shared subscription group. The
regression suite tests it against Eclipse Mosquitto; other brokers that support MQTT 5 shared subscriptions should
work but are not tested here.

|          Item          |                                          Value                                          |
|------------------------|-----------------------------------------------------------------------------------------|
| Plugin ID              | `mqtt5`                                                                                 |
| Module                 | `driver-mqtt5`                                                                          |
| `driverClass`          | `io.openmessaging.benchmark.driver.mqtt5.MqttBenchmarkDriver`                           |
| Example config shipped | `driver-mqtt5/mqtt5.yaml` (`tcp://localhost:1883`, no authentication)                   |
| Tested broker          | `eclipse-mosquitto:2.0.22`, anonymous listener on 1883 (Compose stack `mqtt5`)          |
| Regression cases       | `mqtt5-contract` (publish/subscribe through Mosquitto)                                  |
| Known limitations      | No TLS or WebSocket; partitions and keys are ignored; latency uses the producer's clock |

## Prerequisites and broker setup

The driver needs an MQTT 5 broker reachable over plain TCP that supports shared subscriptions
(`$share/<group>/<topic>`), and, if authentication is enabled, a user allowed to publish and subscribe below the
configured topic prefix. Topics need no preparation.

The regression suite starts Mosquitto from
[`e2e-tests/compose/stacks/mqtt5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/mqtt5.yaml)
with a listener on all interfaces and anonymous access. A comparable local broker:

```bash
docker run -d --name mqtt -p 1883:1883 eclipse-mosquitto:2.0.22 \
  sh -c "printf 'listener 1883 0.0.0.0\nallow_anonymous true\n' > /tmp/mosquitto.conf && exec mosquitto -c /tmp/mosquitto.conf"
```

Without an explicit listener, Mosquitto 2 only accepts connections from inside its own container. The image is
multi-arch and runs natively on Apple Silicon. This broker has no authentication and no persistence; use it only for
local tests.

## Minimal configuration and benchmark command

The shipped `driver-mqtt5/mqtt5.yaml`:

```yaml
name: MQTT
driverClass: io.openmessaging.benchmark.driver.mqtt5.MqttBenchmarkDriver

client:
  serverUri: tcp://localhost:1883
  username:
  password:
  topicPrefix: benchmark

consumer:
  cleanSession: true
  sessionExpiryInterval: 259200
  receiveMaximum: 256
```

It does not set `qos`, so QoS 1 is used. Change `serverUri` to your broker and fill in `username` and `password` if
the broker requires them.

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image:

```bash
bin/benchmark --drivers driver-mqtt5/mqtt5.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the machine running the `docker run` broker above: `localhost` works.
- Benchmark inside a container: `localhost` is the container itself; use a host name the container can resolve.
- Compose regression suite: [`e2e-tests/compose/drivers/mqtt5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/mqtt5.yaml)
  uses `tcp://mqtt:1883` and `qos: 1`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
Install the plugin bundle on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md), and
[Using your broker](../getting-started/using-your-broker.md) for adapting a shipped configuration.

## Configuration reference

Keys are nested under `client` and `consumer` and read by `MqttConfig`. Unknown keys are ignored.

|               Key                |  Type   |        Default         |                                                                          Description                                                                           |
|----------------------------------|---------|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `client.serverUri`               | string  | `tcp://localhost:1883` | Broker address as `[scheme://]host[:port]`. Only host and port are used; the scheme is ignored. Write the port explicitly: without it the driver uses port 80. |
| `client.username`                | string  | empty                  | User name for MQTT simple authentication. Used only when `password` is also non-empty.                                                                         |
| `client.password`                | string  | empty                  | Password. Used only when `username` is also non-empty.                                                                                                         |
| `client.qos`                     | int     | `1`                    | QoS for publishes and subscriptions: `0`, `1` or `2`. Any other value is treated as `1`.                                                                       |
| `client.topicPrefix`             | string  | `benchmark`            | First level of the topic names, without trailing slash.                                                                                                        |
| `consumer.cleanSession`          | boolean | `true`                 | Clean Start flag of consumer connections.                                                                                                                      |
| `consumer.sessionExpiryInterval` | int     | `259200` (3 days)      | Session expiry in seconds for consumer connections. Used only when `cleanSession` is `false`; otherwise 0.                                                     |
| `consumer.receiveMaximum`        | int     | `256`                  | Receive Maximum of consumer connections: QoS 1 and 2 messages the broker may have unacknowledged per consumer.                                                 |

Producer connections use client defaults for Clean Start, session expiry and Receive Maximum. Keep-alive, TLS and
WebSocket transport are not configurable; TLS and WebSocket are not supported.

## Semantics

**Topics and partitions.** Topic names are `<topicPrefix>/test-<index>-<random>`, for example
`benchmark/test-0000000-AbCdEfG`. `createTopic` does nothing: MQTT topics exist implicitly. MQTT has no partitions;
`partitionsPerTopic` has no effect.

**Producers.** Each producer opens its own MQTT connection with a random client ID (`benchmark_pub_...`) and
publishes to its topic with the configured QoS, without the retain flag. Keys are ignored. A send completes when the
publish is finished for that QoS: immediately after sending for QoS 0, at PUBACK for QoS 1 and at PUBCOMP for QoS 2.
Errors reported by the broker fail the send. The driver owns the message bytes even if the send is canceled.

**Subscriptions and consumers.** Each consumer opens its own connection with a random client ID (`benchmark_sub_...`)
and subscribes to `$share/<subscriptionName>/<topic>` with the configured QoS. Consumers with the same subscription
name form one shared subscription group, and the broker distributes the topic's messages among them. Different
subscriptions are different groups, and each receives every message. MQTT 5 shared subscriptions are therefore what
makes the benchmark's subscription model (several consumers sharing one subscription, several subscriptions per
topic) work on MQTT. Consumer creation completes only after a successful CONNACK and a SUBACK granting every requested
filter. Connection errors, rejected or malformed SUBACKs, and canceled creation fail the operation and disconnect
the partial client.

**Acknowledgement.** Subscriptions use manual acknowledgement: the client sends PUBACK (QoS 1) or completes the
PUBREC/PUBREL/PUBCOMP exchange (QoS 2) after the benchmark callback returns. With QoS 0 there is no acknowledgement.

**Sessions.** With `cleanSession: true` (default) every consumer starts a new session that ends when it disconnects.
With `cleanSession: false` the broker keeps the session for `sessionExpiryInterval` seconds after a disconnect, so
messages for an interrupted consumer are kept until it reconnects. Client IDs are random for every run, so a later
run never resumes an earlier session; expired sessions are removed by the broker. Successfully created clients
reconnect automatically, starting after 100 ms and backing off up to 10 seconds. A disconnect before the handle is
ready fails creation instead of retrying indefinitely.

**End-to-end latency.** Each publish carries the producer's wall-clock time in the user property
`benchmark-publish-timestamp`; the consumer uses it as the publish time. Latency therefore covers the full path from
producer to consumer and includes clock offset between producer and consumer hosts. If the property is missing, the
receive time is used. See [Latency and backlog](../methodology/latency-and-backlog.md).

**Durability.** Determined by the QoS and by broker persistence settings. With QoS 0 messages can be lost; the
regression broker does not persist anything.

**Cleanup.** Each producer or consumer handle waits up to five seconds for its own client to disconnect and reports
disconnect errors or timeouts; producer close also fails pending sends. Closing a consumer from its own message callback
is rejected. Closing a producer from a callback executing synchronously inside a `sendAsync` call on that handle,
or from any of its send-completion callbacks (including asynchronous acknowledgements and close-induced failures),
throws `IllegalStateException` before changing the handle's state. Retry after the call or callback returns, or schedule
close on another thread without waiting for it in the callback. Nothing is created on the broker that outlives the run
except sessions with `cleanSession: false`, which expire after `sessionExpiryInterval`. No retained messages are published.

**Workload features.** Multiple topics, multiple subscriptions per topic and multiple consumers per subscription
work. Partition counts and keys are ignored. For backlog workloads (`consumerBacklogSizeGB`) the framework blocks the
consumer callback; the broker then stops delivering to that consumer once `receiveMaximum` messages are
unacknowledged (QoS 1 and 2) and queues further messages according to its own limits (for Mosquitto,
`max_queued_messages` and `max_queued_bytes`), which may drop messages. See [Workloads](../guides/workloads.md).

## Tuning examples

Delivery guarantee per run:

```yaml
client:
  serverUri: tcp://broker.example.com:1883
  qos: 0
```

QoS 0 has no acknowledgements, QoS 1 one acknowledgement per message, QoS 2 a four-step exchange per message. Use
the same QoS for all brokers you compare and record it with the results; see
[Fair comparisons](../methodology/fair-comparisons.md).

More messages in flight per consumer:

```yaml
consumer:
  cleanSession: true
  receiveMaximum: 1024
```

The broker may apply a lower limit of its own. Each producer and consumer uses its own connection, so the number of
broker connections equals the number of producers plus consumers in the workload.

## Troubleshooting and verification

Run the MQTT 5 regression case (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks mqtt5
e2e-tests/compose/run-e2e.sh --cases mqtt5-contract
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- A connection timeout: check the host, whether `serverUri` includes the port (without it the driver uses port 80),
  and whether the broker listens on a non-loopback interface.
- `ConnAck-ReasonCode: NOT_AUTHORIZED` or `BAD_USER_NAME_OR_PASSWORD`: authentication failed, or the broker
  requires credentials and only one of `username` and `password` is set.
- `Subscription rejected` or `Invalid SUBACK`: consumer creation failed. The broker may not support shared
  subscriptions, the user may lack permission, or the SUBACK may not match the requested filters.
- A TLS port (8883) does not work: the driver only connects over plain TCP.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-mqtt5/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-mqtt5/src/main/java/io/openmessaging/benchmark/driver/mqtt5)
- Shipped config: [`driver-mqtt5/mqtt5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-mqtt5/mqtt5.yaml)
- Regression config: [`e2e-tests/compose/drivers/mqtt5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/mqtt5.yaml)
- Stack file: [`e2e-tests/compose/stacks/mqtt5.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/mqtt5.yaml)

