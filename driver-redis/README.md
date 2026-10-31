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

# Redis driver

This plugin benchmarks [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/) with the Jedis
client. Each benchmark topic is one stream (`XADD`), and each subscription is a consumer group read with
`XREADGROUP` and acknowledged with `XACK`.

Full documentation: <https://mihkels.github.io/open-messaging-benchmark/drivers/redis/>

- Plugin ID: `redis`
- `driverClass`: `io.openmessaging.benchmark.driver.redis.RedisBenchmarkDriver`
- Shipped config: [`redis.yaml`](redis.yaml) (`127.0.0.1:6379`)
- Tested with: `redis:7.4-alpine` ([stack file](../e2e-tests/compose/stacks/redis.yaml))

## Prerequisites

- One Redis server with Streams support, reachable from the coordinator (local mode) or every worker.
- `redisHost`, `redisPort`, `jedisPoolMaxTotal` and `jedisPoolMaxIdle` set in the driver file (no defaults).
  Set `redisPass`, and optionally `redisUser`, if the server requires authentication.
- The `redis` plugin bundle installed on the coordinator (local mode) and on every worker.

A local test server:

```bash
docker run -d --name redis -p 6379:6379 redis:7.4-alpine
```

## Usage

Edit `redisHost` and `redisPort` in `redis.yaml`, then run from the repository root (after
`mvn -DskipTests install`) or from `/benchmark` in the container image:

```bash
bin/benchmark --drivers driver-redis/redis.yaml workloads/1-topic-1-partition-1kb.yaml
```

Smoke check against a server started by Docker Compose (see [`e2e-tests/compose/README.md`](../e2e-tests/compose/README.md)):

```bash
e2e-tests/compose/run-e2e.sh --stacks redis
```

## Limitations

- `partitionsPerTopic` is ignored: every topic is a single stream.
- Keys are stored in a `key` field but not used for routing or ordering.
- Single endpoint only: no Redis Cluster, Sentinel or TLS.
- Sends are synchronous: each producer has one `XADD` in flight at a time.
- End-to-end latency uses the stream entry ID, which Redis takes from the server clock.
- The pool (`jedisPoolMaxTotal`) is shared by producers and consumers of a worker; each consumer holds a connection
  while blocked in `XREADGROUP`, so size it above the number of consumers per worker.
- Streams and consumer groups are neither trimmed nor deleted after a run.

## Further reading

- [Workloads](https://mihkels.github.io/open-messaging-benchmark/guides/workloads/)
- [Distributed runs](https://mihkels.github.io/open-messaging-benchmark/getting-started/distributed-runs/)
- [Runtime and plugins](https://mihkels.github.io/open-messaging-benchmark/reference/runtime-and-plugins/)
- [Troubleshooting](https://mihkels.github.io/open-messaging-benchmark/guides/troubleshooting/)

