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

# Payloads and keys

Message contents and keys affect compression, batching, partition placement and ordering. OMB
supports three ways to build payloads and three key strategies.

## Payload sources

The coordinator prepares payloads once, before producers start, and sends them to the producer
workers. Producers pick one payload per message, uniformly at random unless weights apply. Exactly
one of these sources is used, in this order of precedence:

### 1. Message size distribution

```yaml
messageSizeDistribution:
  "256-1024": 1
  "1KB-4KB": 3
  "4KB-16KB": 2
```

Each entry is a size range and a relative weight. For each range one payload is created at the
range midpoint (for example 640 bytes for `256-1024`), and each message picks a range with
probability proportional to its weight. In the example, half the messages are 2.5 KiB.

- Sizes are bytes; `B`, `KB` and `MB` suffixes are accepted, case-insensitively, with `1KB = 1024`.
- Write a fixed size as a range with equal ends, for example `"1024-1024"`.
- Weights must be zero or positive and sum to more than zero.
- `messageSize` and `payloadFile` are ignored. The result's `messageSize` is the weighted average of
  the midpoints, and backlog sizes are computed from it.
- With `useRandomizedPayloads: true`, each payload is randomized as described below.

### 2. Randomized payloads

```yaml
messageSize: 1024
useRandomizedPayloads: true
randomBytesRatio: 0.5
randomizedPayloadPoolSize: 1000
```

The coordinator generates `randomizedPayloadPoolSize` payloads of `messageSize` bytes. The first
`randomBytesRatio` of each payload is random and the rest is zeros, so the ratio controls how well
the data compresses: `1.0` is effectively incompressible and `0.0` is all zeros. A pool of distinct
payloads stops brokers or clients from benefiting from identical messages.

### 3. Payload file

```yaml
messageSize: 1024
payloadFile: payload/payload-1Kb.data
```

Every message carries the contents of the file. The file length must equal `messageSize`, otherwise
the run fails with `Payload length mismatch`. The path is resolved from the coordinator's working
directory. The repository ships `payload/payload-{100b,200b,400b,1Kb,2Kb,4Kb}.data`. To create
another size:

```bash
head -c 65536 /dev/urandom > payload/payload-64Kb.data
```

A single repeated payload compresses extremely well. Prefer randomized payloads when a driver
configuration enables compression.

## Keys

`keyDistributor` controls the key attached to each message:

|       Value        |                                         Behavior                                          |
|--------------------|-------------------------------------------------------------------------------------------|
| `NO_KEY` (default) | No key; the client's default partitioning applies, usually round-robin or sticky batching |
| `KEY_ROUND_ROBIN`  | Cycles through 10,000 random 10-character keys                                            |
| `RANDOM_NANO`      | Picks one of the same 10,000 keys based on `System.nanoTime()`                            |

An unknown value falls back to `NO_KEY`. Keys are generated independently in each worker process.

Keys matter because most partitioned systems hash them to choose a partition, and because some
drivers use them for routing or ordering. With keys, per-partition batches are usually smaller,
which raises publish latency compared with `NO_KEY`. How each driver uses keys is described on its
page; some ignore them.

## Comparing systems

Use the same payload source, size, randomness and key strategy for every system in a comparison,
and record them with the results. Compression settings on the driver side interact strongly with
`randomBytesRatio`. See [Fair comparisons](../methodology/fair-comparisons.md).
