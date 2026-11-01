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

# MQTT 5 driver

This plugin benchmarks MQTT 5 brokers with the HiveMQ MQTT client. Each benchmark topic is an MQTT topic and each
subscription is an MQTT 5 shared subscription group (`$share/<subscription>/<topic>`).

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/mqtt5/>

- Plugin ID: `mqtt5`
- `driverClass`: `io.openmessaging.benchmark.driver.mqtt5.MqttBenchmarkDriver`
- Shipped config: [`mqtt5.yaml`](mqtt5.yaml) (`tcp://localhost:1883`, no authentication, QoS 1)
- Tested with: `eclipse-mosquitto:2.0.22` ([stack file](../e2e-tests/compose/stacks/mqtt5.yaml))

## Prerequisites

- An MQTT 5 broker with shared subscription support, reachable over plain TCP (no TLS or WebSocket).
- The `mqtt5` plugin bundle installed on the coordinator (local mode) and on every worker.

A local test broker with anonymous access:

```bash
docker run -d --name mqtt -p 1883:1883 eclipse-mosquitto:2.0.22 \
  sh -c "printf 'listener 1883 0.0.0.0\nallow_anonymous true\n' > /tmp/mosquitto.conf && exec mosquitto -c /tmp/mosquitto.conf"
```

## Usage

Edit `mqtt5.yaml`: `client.serverUri` (always include the port), `client.username`/`client.password` if needed,
`client.qos` (0, 1 or 2; default 1), and the consumer options `cleanSession`, `sessionExpiryInterval` and
`receiveMaximum`. Then run from the repository root (after `mvn -DskipTests install`) or from `/benchmark` in the
container image:

```bash
bin/benchmark --drivers driver-mqtt5/mqtt5.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against Mosquitto started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks mqtt5
```

Consumers are ready only after a successful connection and SUBACK for every requested filter. A
connection or subscription failure is reported without retrying indefinitely and disconnects the partial client.
Successfully created clients still reconnect automatically. Handle close waits
up to five seconds for disconnect and reports failures. Closing a consumer from its own message
callback is rejected; schedule close on another thread without waiting in the callback.
Closing a producer from a callback running synchronously inside a send, or from one of its send-completion
callbacks, is rejected; retry after the callback returns or schedule close on another thread without waiting
in the callback.

## Limitations

- `partitionsPerTopic` and message keys are ignored.
- Plain TCP only; the `serverUri` scheme is ignored and a missing port means port 80.
- End-to-end latency uses the producer's clock (user property `benchmark-publish-timestamp`).
- Each producer and consumer uses its own connection.

## Further reading

- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)
- [Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

