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

# Redis

The Redis driver benchmarks [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/). Each benchmark
topic is one stream, written with `XADD`, and each subscription is a consumer group read with `XREADGROUP` and
acknowledged with `XACK`. It uses the [Jedis](https://github.com/redis/jedis) client (8.0.2) against a single
Redis endpoint.

|          Item          |                                                                                       Value                                                                                       |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Plugin ID              | `redis`                                                                                                                                                                           |
| Module                 | `driver-redis`                                                                                                                                                                    |
| `driverClass`          | `io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver`                                                                                                                    |
| Example config shipped | `driver-redis/redis.yaml` (`127.0.0.1:6379`)                                                                                                                                      |
| Tested broker          | `redis:7.4-alpine` (Compose stack `redis`, Redis 7.4)                                                                                                                             |
| Regression cases       | `redis-contract`, `redis-fanout`, `redis-random-payload`                                                                                                                          |
| Known limitations      | Partitions are ignored (one stream per topic); keys are stored but not used; single node only, no Cluster, Sentinel or TLS; synchronous sends; streams are not trimmed or deleted |

## Prerequisites and broker setup

The driver needs one Redis server with Streams support (Redis 5.0 or later; tested with 7.4) reachable on its TCP
port. If the server requires authentication, set `redisPass` and optionally `redisUser` (ACL user). The user needs
the stream commands `XADD`, `XGROUP`, `XREADGROUP` and `XACK`. Nothing has to be created in advance: the driver
creates streams and consumer groups on demand.

The regression suite starts Redis from
[`e2e-tests/compose/stacks/redis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/redis.yaml).
A comparable local server with the same image:

```bash
docker run -d --name redis -p 6379:6379 redis:7.4-alpine
```

The image is multi-arch and runs natively on Apple Silicon. Streams live in memory, so size `maxmemory` and the host
memory for the total data a run writes (see [Cleanup](#semantics) below).

## Minimal configuration and benchmark command

The shipped `driver-redis/redis.yaml`:

```yaml
name: Redis
driverClass: io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver

redisPort: 6379
redisHost: 127.0.0.1
jedisPoolMaxTotal: 8
jedisPoolMaxIdle: 8
```

Change `redisHost` (and `redisPort`) to your server. All four keys shown are required: the driver has no defaults.

Run a local-mode benchmark from the repository root (after a full `mvn -DskipTests install`) or from `/benchmark`
in the container image:

```bash
bin/benchmark --drivers driver-redis/redis.yaml workloads/1-topic-1-partition-1kb.yaml
```

Host names depend on where the benchmark runs:

- Source checkout or distribution on the machine running the `docker run` server above: `127.0.0.1` works.
- Benchmark inside a container: `127.0.0.1` is the container itself; use a host name the container can resolve.
- Compose regression suite: [`e2e-tests/compose/drivers/redis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/redis.yaml)
  uses the service name `redis`.

For remote workers add `--workers` or `--workers-file`; see [Distributed runs](../getting-started/distributed-runs.md).
Install the plugin bundle on every worker, see [Runtime and plugins](../reference/runtime-and-plugins.md), and
[Using your broker](../getting-started/using-your-broker.md) for adapting a shipped configuration.

## Configuration reference

All keys are read by `RedisClientConfig`. Unknown keys are ignored.

|         Key         |  Type   | Default  |                                                    Description                                                    |
|---------------------|---------|----------|-------------------------------------------------------------------------------------------------------------------|
| `redisHost`         | string  | required | Redis host name or IP address.                                                                                    |
| `redisPort`         | integer | required | Redis TCP port.                                                                                                   |
| `jedisPoolMaxTotal` | integer | required | Maximum connections in the Jedis pool of one driver instance (one per worker), shared by producers and consumers. |
| `jedisPoolMaxIdle`  | integer | required | Maximum idle connections kept in the pool.                                                                        |
| `redisPass`         | string  | none     | Password. When unset, no authentication is sent.                                                                  |
| `redisUser`         | string  | none     | ACL user name. Only used when `redisPass` is also set.                                                            |

The connection and socket timeout is fixed at 2000 ms. Other pool settings are the Apache Commons Pool defaults;
in particular a caller waits without a time limit when the pool is exhausted. There are no pass-through client
properties. See the [Jedis documentation](https://redis.io/docs/latest/develop/clients/jedis/) for client behaviour.

## Semantics

**Topics and partitions.** Topic names start with `redis-openmessaging-benchmark` followed by an index and a random
suffix. Each topic is a single stream key. `createTopic` does nothing; the stream is created by the first consumer
group creation (`MKSTREAM`) or the first `XADD`. `partitionsPerTopic` is accepted and ignored: a topic with
16 partitions is still one stream, so per-partition parallelism does not exist with this driver.

**Producers.** A send runs `XADD <stream> * payload <bytes>` on a pooled connection and completes when Redis
replies. The call is synchronous: `sendAsync` returns only after the command has completed, so a producer has at most
one send in flight. No `MAXLEN` trimming is applied. If the workload provides a key, a `key` field is added to the
entry; it is not used for routing, ordering or consumption. (The field currently contains the Java `Optional`
representation of the key, for example `Optional[key-1]`.)

**Subscriptions and consumers.** Each subscription is a consumer group named after the benchmark subscription,
created with `XGROUP CREATE <stream> <group> 0-0 MKSTREAM`; if it already exists (`BUSYGROUP`) it is reused. Each
consumer has a random consumer name and one thread that loops on
`XREADGROUP GROUP <group> <consumer> COUNT 100 BLOCK 1000 STREAMS <stream> >`. Consumers in the same group share the
stream's entries; every group receives every entry, so multiple subscriptions per topic work.

**Acknowledgement.** Each entry is acknowledged with `XACK` immediately after the benchmark callback. Entries that
were delivered but never acknowledged (for example when a worker stops) stay in the group's pending entries list; the
driver does not claim or redeliver them.

**End-to-end latency.** The consumer uses the millisecond part of the stream entry ID as the publish time. Redis
assigns that ID from the server clock when `XADD` runs, so end-to-end latency is measured from the Redis server's
clock, not from the producer's send time. Clock offset between the Redis host and the consumer workers adds
directly to the reported value. See [Latency and backlog](../methodology/latency-and-backlog.md).

**Payloads.** Payloads are written and read as bytes, including non-UTF-8 binary payloads.

**Durability.** Durability is whatever the Redis server is configured for (RDB snapshots, AOF, replication). The
driver does not wait for replication (`WAIT`) or persistence.

**Cleanup.** Producer close waits for its synchronous sends, and consumer close waits for its reader
thread with a bounded timeout. The driver owns the shared pool; handles do not close it. Streams and
consumer groups are not deleted after a run, and streams are not trimmed. Data written in a run
stays in Redis memory until you delete the keys (for example
`redis-cli --scan --pattern 'redis-openmessaging-benchmark*'` and `DEL`, or `FLUSHDB` on a dedicated
server).

**Workload features.** Multiple topics, multiple subscriptions per topic and multiple consumers per subscription
work. Partition counts and keys are ignored. Backlog workloads (`consumerBacklogSizeGB`) are handled by the framework
blocking the consumer callback; a blocked consumer thread stops issuing `XREADGROUP`, so the backlog accumulates in
the stream. See [Workloads](../guides/workloads.md).

## Tuning examples

Pool size:

```yaml
jedisPoolMaxTotal: 64
jedisPoolMaxIdle: 64
```

Every consumer holds a pool connection for up to one second per `XREADGROUP BLOCK 1000`, and every in-progress
`XADD` holds one too. If a worker runs at least as many consumers as `jedisPoolMaxTotal`, producers wait for a free
connection without a time limit. Size the pool above the number of consumers per worker plus the producers per
worker. Larger pools mean more client connections on the Redis server.

Authentication:

```yaml
redisUser: benchmark
redisPass: secret
```

Producer concurrency is controlled by the workload (`producersPerTopic`, topics) rather than by a driver setting,
because each producer sends one command at a time. Because Redis is single-node here, partitions do not add
parallelism; compare against partitioned systems with that in mind, see
[Fair comparisons](../methodology/fair-comparisons.md).

## Troubleshooting and verification

Run the Redis regression cases (Docker required, see the
[Compose suite README](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks redis
e2e-tests/compose/run-e2e.sh --cases redis-contract,redis-fanout,redis-random-payload
```

A passing case is a functional check at a low rate, not a performance result.

Common problems:

- `NullPointerException` during producer or consumer creation: `redisPort`, `jedisPoolMaxTotal` or
  `jedisPoolMaxIdle` is missing from the driver file.
- `Failed to create consumer group ...`: the consumer group could not be created, for example because the key exists
  and is not a stream, or the user lacks permission for `XGROUP`.
- `NOAUTH` or `WRONGPASS`: set `redisPass` (and `redisUser` for ACL users).
- Publishing stalls while consumers run: the pool is exhausted, see the pool size example above.
- `OOM command not allowed when used memory > 'maxmemory'`: streams are never trimmed; delete old benchmark keys or
  raise `maxmemory`.
- High or negative-looking end-to-end latency: clock offset between the Redis server and the workers.

General advice is in [Troubleshooting](../guides/troubleshooting.md); result interpretation in
[Results](../guides/results.md).

Source and configuration files:

- Driver source: [`driver-redis/src/main/java`](https://github.com/mihkels/open-messaging-benchmark/tree/main/driver-redis/src/main/java/io/openmessaging/benchmark/driver/redis)
- Shipped config: [`driver-redis/redis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/driver-redis/redis.yaml)
- Regression config: [`e2e-tests/compose/drivers/redis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/drivers/redis.yaml)
- Stack file: [`e2e-tests/compose/stacks/redis.yaml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/e2e-tests/compose/stacks/redis.yaml)

