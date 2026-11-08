# Changelog

## [0.3.0](https://github.com/mihkels/open-messaging-benchmark/compare/v0.2.0...v0.3.0) (2026-11-08)


### ⚠ BREAKING CHANGES

* the NATS Streaming driverClass is now io.openmessaging.benchmark.driver.natsstreaming.NatsStreamingBenchmarkDriver. Driver YAML files that still use the old natsStreaming class name fail to resolve; update their driverClass.
* the NATS Streaming driverClass is now io.openmessaging.benchmark.driver.natsstreaming.NatsStreamingBenchmarkDriver. Driver YAML files that still use the old natsStreaming class name fail to resolve; update their driverClass.
* plugin bundles must declare `driverApiVersion: 2`. Bundles declaring version 1, including those built for 0.1.0 to 0.2.0, are rejected at discovery. Rebuild external plugins against the current driver-api (initialize takes io.micrometer.core.instrument.MeterRegistry), set driverApiVersion: 2 in plugin.yaml, and reinstall them on every worker.

### Features

* add declarative SLO threshold gates ([#130](https://github.com/mihkels/open-messaging-benchmark/issues/130)) ([9f8593f](https://github.com/mihkels/open-messaging-benchmark/commit/9f8593f6aa0022b14963bb7e0b391e7c8de30d94))
* bump driverApiVersion to 2 after the MeterRegistry SPI change ([#155](https://github.com/mihkels/open-messaging-benchmark/issues/155)) ([c4fb637](https://github.com/mihkels/open-messaging-benchmark/commit/c4fb63722c68ca290f8710f31d7721fc87cd647a)), closes [#108](https://github.com/mihkels/open-messaging-benchmark/issues/108)
* rename the natsStreaming driver package to natsstreaming ([#176](https://github.com/mihkels/open-messaging-benchmark/issues/176)) ([4d8d077](https://github.com/mihkels/open-messaging-benchmark/commit/4d8d077cea5953826ea3475caf2d33c2200b2463))


### Bug Fixes

* create temp files owner-only and run git from fixed paths ([#153](https://github.com/mihkels/open-messaging-benchmark/issues/153)) ([f8de36d](https://github.com/mihkels/open-messaging-benchmark/commit/f8de36dc69125d7e1e98781ba74d9a130186eed5))
* remove super-linear regex backtracking flagged by java:S8786 ([#157](https://github.com/mihkels/open-messaging-benchmark/issues/157)) ([5db1381](https://github.com/mihkels/open-messaging-benchmark/commit/5db1381aa0bf54de97c3fe84b26a11a02a8186a1)), closes [#140](https://github.com/mihkels/open-messaging-benchmark/issues/140)
* resolve remaining Sonar bug and correctness findings ([#171](https://github.com/mihkels/open-messaging-benchmark/issues/171)) ([71e453c](https://github.com/mihkels/open-messaging-benchmark/commit/71e453cdd08940a1e88bb1fdacbee618893b2b99)), closes [#167](https://github.com/mihkels/open-messaging-benchmark/issues/167)
* resolve SonarCloud bug and correctness findings ([#156](https://github.com/mihkels/open-messaging-benchmark/issues/156)) ([1ff7e4b](https://github.com/mihkels/open-messaging-benchmark/commit/1ff7e4b43380682f82cd90353bb9e3cb65fcd72f))


### Refactoring

* **drivers:** reduce cognitive complexity and nested try blocks ([#165](https://github.com/mihkels/open-messaging-benchmark/issues/165)) ([8c746d0](https://github.com/mihkels/open-messaging-benchmark/commit/8c746d0b18cb8d9108462ab246f23ad5f06b9ea8))
* move addDriverError into PluginRegistry.Discovery ([#166](https://github.com/mihkels/open-messaging-benchmark/issues/166)) ([b1fa7ba](https://github.com/mihkels/open-messaging-benchmark/commit/b1fa7bab22db54c7dd1978b710c0407e12aef6cc))
* move RocketMQ, RocketMQ 5 and MQTT producers onto PublicationLifecycle ([#184](https://github.com/mihkels/open-messaging-benchmark/issues/184)) ([d6f8f00](https://github.com/mihkels/open-messaging-benchmark/commit/d6f8f002ddb1369d9f583e28f623523bcf39920e))
* **nsq:** reduce cognitive complexity and nested try blocks ([#164](https://github.com/mihkels/open-messaging-benchmark/issues/164)) ([249a1b9](https://github.com/mihkels/open-messaging-benchmark/commit/249a1b9f0701dfa41f4da5c520d3a47cced971a9))
* **pravega,kafka:** reduce cognitive complexity ([#163](https://github.com/mihkels/open-messaging-benchmark/issues/163)) ([9b6c3fc](https://github.com/mihkels/open-messaging-benchmark/commit/9b6c3fc1b66afbeea2c9fb02ed75664d7c3eec12))
* record by-design Sonar findings in code ([#173](https://github.com/mihkels/open-messaging-benchmark/issues/173)) ([c313d8c](https://github.com/mihkels/open-messaging-benchmark/commit/c313d8c63b91e048eb15b4507c7d6182fdf309dc))
* reduce cognitive complexity in benchmark-framework and driver-api ([#162](https://github.com/mihkels/open-messaging-benchmark/issues/162)) ([d4e844f](https://github.com/mihkels/open-messaging-benchmark/commit/d4e844ff644f5c0d5299b6cbcc7e03edfe08e6db))
* resolve small Sonar code smells in production code ([#160](https://github.com/mihkels/open-messaging-benchmark/issues/160)) ([e673b5d](https://github.com/mihkels/open-messaging-benchmark/commit/e673b5d030588f9f2d818b77705bd82f423d72a5))
* use Java 22 unnamed variables for unused catch and resource vars ([#159](https://github.com/mihkels/open-messaging-benchmark/issues/159)) ([e4fca74](https://github.com/mihkels/open-messaging-benchmark/commit/e4fca7457d898c1510994c63e5347813e6781df6))
* use unnamed variables and record patterns ([#174](https://github.com/mihkels/open-messaging-benchmark/issues/174)) ([f307d42](https://github.com/mihkels/open-messaging-benchmark/commit/f307d424d3045c38589df77bf0593522010bd9ad))


### Documentation

* redesign homepage spacing and visual system ([#142](https://github.com/mihkels/open-messaging-benchmark/issues/142)) ([6cdc0c7](https://github.com/mihkels/open-messaging-benchmark/commit/6cdc0c7e997da9a94acbb2f3330f65bf65f9f824)), closes [#141](https://github.com/mihkels/open-messaging-benchmark/issues/141)


### Dependencies

* Bump bytebuddy.version from 1.18.16 to 1.18.17 ([#133](https://github.com/mihkels/open-messaging-benchmark/issues/133)) ([20755f5](https://github.com/mihkels/open-messaging-benchmark/commit/20755f5f64050d1d3caa0c73a6e45670399a16d1))
* Bump com.diffplug.spotless:spotless-maven-plugin from 3.10.4 to 3.11.0 ([#136](https://github.com/mihkels/open-messaging-benchmark/issues/136)) ([9cbdfa8](https://github.com/mihkels/open-messaging-benchmark/commit/9cbdfa8881945c1549d020b9a1bf720e45309aa5))
* Bump com.google.guava:guava from 33.7.2-jre to 33.8.0-jre ([#135](https://github.com/mihkels/open-messaging-benchmark/issues/135)) ([acae2b4](https://github.com/mihkels/open-messaging-benchmark/commit/acae2b4dd0962a4f98e68a96e0f05aef746ab64f))
* Bump com.hivemq:hivemq-mqtt-client from 1.4.1 to 1.4.2 ([#134](https://github.com/mihkels/open-messaging-benchmark/issues/134)) ([7ede9c5](https://github.com/mihkels/open-messaging-benchmark/commit/7ede9c58c495ad74d17ebaf41ccf5794a7776ba1))
* Bump io.netty:netty-bom from 4.2.19.Final to 4.2.20.Final ([#132](https://github.com/mihkels/open-messaging-benchmark/issues/132)) ([99b0f71](https://github.com/mihkels/open-messaging-benchmark/commit/99b0f71c1a5ba78637b3d9ea7e49facc893ac60b))
* Bump maven in /docker in the base-images group across 1 directory ([#131](https://github.com/mihkels/open-messaging-benchmark/issues/131)) ([6e76864](https://github.com/mihkels/open-messaging-benchmark/commit/6e768646cf1332ea9cf47c6ae94d83aa29e7e3e0))

## [0.2.0](https://github.com/mihkels/open-messaging-benchmark/compare/v0.1.3...v0.2.0) (2026-11-06)


### Features

* add versioned result metadata, summaries, and optional histograms ([#124](https://github.com/mihkels/open-messaging-benchmark/issues/124)) ([50a013b](https://github.com/mihkels/open-messaging-benchmark/commit/50a013bb0d591b72e620a2cfb6b7067b6ecfc2be))
* expose schedule-corrected publish latency ([#125](https://github.com/mihkels/open-messaging-benchmark/issues/125)) ([d29b646](https://github.com/mihkels/open-messaging-benchmark/commit/d29b646e2c3b5e0c98c6caa8e960e09fd546badc))

## [0.1.3](https://github.com/mihkels/open-messaging-benchmark/compare/v0.1.2...v0.1.3) (2026-11-06)


### Bug Fixes

* handle oversized histogram payloads ([#98](https://github.com/mihkels/open-messaging-benchmark/issues/98)) ([081b630](https://github.com/mihkels/open-messaging-benchmark/commit/081b630d72f6d780b010955df58d74872736a7a5))
* preserve RocketMQ admin interrupt status ([#99](https://github.com/mihkels/open-messaging-benchmark/issues/99)) ([ad16302](https://github.com/mihkels/open-messaging-benchmark/commit/ad163028cbb91321c24fe56c27e096d9c0d06056))

## [0.1.2](https://github.com/mihkels/open-messaging-benchmark/compare/v0.1.1...v0.1.2) (2026-11-06)


### Bug Fixes

* Avoid SIGPIPE exits in omb-k8s.sh pipelines, ref: [#86](https://github.com/mihkels/open-messaging-benchmark/issues/86) ([#87](https://github.com/mihkels/open-messaging-benchmark/issues/87)) ([fe3a7e0](https://github.com/mihkels/open-messaging-benchmark/commit/fe3a7e055985f41d14889b5a86bc397ff38412d8))
* Upgrade OS packages in the runtime image, ref: [#65](https://github.com/mihkels/open-messaging-benchmark/issues/65) ([#66](https://github.com/mihkels/open-messaging-benchmark/issues/66)) ([279cd99](https://github.com/mihkels/open-messaging-benchmark/commit/279cd99f5a54803bc0e0971004b397868ef94a5f))


### Documentation

* Add CONTRIBUTING.md pointing to development docs, ref: [#49](https://github.com/mihkels/open-messaging-benchmark/issues/49) ([#67](https://github.com/mihkels/open-messaging-benchmark/issues/67)) ([a3c5a13](https://github.com/mihkels/open-messaging-benchmark/commit/a3c5a13bc426dde9b5257fa39cc5ddfe36991c6d))

## [0.1.1](https://github.com/mihkels/open-messaging-benchmark/compare/v0.1.0...v0.1.1) (2026-11-04)


### Dependencies

* Bump org.apache.rocketmq:rocketmq-tools from 5.1.0 to 5.5.1 ([#52](https://github.com/mihkels/open-messaging-benchmark/issues/52)) ([f9116ec](https://github.com/mihkels/open-messaging-benchmark/commit/f9116ec19f15b6ec61d20b3468acf0bb802f6074))

## [0.1.0](https://github.com/mihkels/open-messaging-benchmark/compare/v0.0.6...v0.1.0) (2026-11-04)


### Features

* Publish Docker images and Helm chart to Docker Hub, add local kind testing, ref: [#24](https://github.com/mihkels/open-messaging-benchmark/issues/24) ([c139d0d](https://github.com/mihkels/open-messaging-benchmark/commit/c139d0db26a156cf790392e9137b178517e0ddb2))
* Publish Docker images and Helm chart to Docker Hub, add local kind testing, ref: NOISSUE ([5c72a7d](https://github.com/mihkels/open-messaging-benchmark/commit/5c72a7d2b237f2a78df8e66b995da6c5d5f44db1))


### Bug Fixes

* Fix Redis ACL auth, Pravega client defaults and workload template defaults, ref: [#27](https://github.com/mihkels/open-messaging-benchmark/issues/27) ([61b5a09](https://github.com/mihkels/open-messaging-benchmark/commit/61b5a0926abf9b14ca0dfae2d15cf7347f95a2b2))
* Redis ACL auth order, Pravega client defaults, workload template defaults, ref: [#27](https://github.com/mihkels/open-messaging-benchmark/issues/27) ([213b1ed](https://github.com/mihkels/open-messaging-benchmark/commit/213b1ed7b981cd8759e5f7cf05469afa7ad9db63))


### Documentation

* Add Google Analytics to docs site, ref: [#30](https://github.com/mihkels/open-messaging-benchmark/issues/30) ([243bae6](https://github.com/mihkels/open-messaging-benchmark/commit/243bae6562573b14d3068687759ccc4d48aad988))
* Add Google Analytics with cookie consent, feedback and custom events, ref: [#30](https://github.com/mihkels/open-messaging-benchmark/issues/30) ([484e299](https://github.com/mihkels/open-messaging-benchmark/commit/484e2994278eda5269b941749b4cfb17946ca17b))
* Add landing page and review docs and README, ref: [#25](https://github.com/mihkels/open-messaging-benchmark/issues/25) ([cc42d5e](https://github.com/mihkels/open-messaging-benchmark/commit/cc42d5e33fed321b84c2f9e916f4d5fd1d2c4db5))
* Docs site landing page, docs review and slimmer README, ref: [#25](https://github.com/mihkels/open-messaging-benchmark/issues/25) ([fec81fa](https://github.com/mihkels/open-messaging-benchmark/commit/fec81fad0f43f3f0742db57ad448e5f7781cd8ca))
* Fix visual issues on docs pages and landing, ref: [#25](https://github.com/mihkels/open-messaging-benchmark/issues/25) ([8dba478](https://github.com/mihkels/open-messaging-benchmark/commit/8dba478e7de0c9c94820b9f17d86b6cb4c6e662e))
* Link site name to home, follow system theme, align landing cards, ref: [#25](https://github.com/mihkels/open-messaging-benchmark/issues/25) ([ae7611e](https://github.com/mihkels/open-messaging-benchmark/commit/ae7611ebf52134393478fc7d9b8c961c70e3a82f))
* Wrap landing terminal lines and even out landing grids, ref: [#25](https://github.com/mihkels/open-messaging-benchmark/issues/25) ([755b66d](https://github.com/mihkels/open-messaging-benchmark/commit/755b66dc45439eb984dda72d3b42f435c9811cf0))


### Dependencies

* bump com.hivemq:hivemq-mqtt-client from 1.3.8 to 1.4.1 ([bbdfd36](https://github.com/mihkels/open-messaging-benchmark/commit/bbdfd361a42e9dd493190fb47361e4c254e137c9))
* bump com.hivemq:hivemq-mqtt-client from 1.3.8 to 1.4.1, ref: NOISSUE ([0c2ac76](https://github.com/mihkels/open-messaging-benchmark/commit/0c2ac7681922bc29cad494475731416c28add7a7))
* bump org.apache.pulsar:pulsar-client-all from 4.2.5 to 5.0.1 ([8ec4a6a](https://github.com/mihkels/open-messaging-benchmark/commit/8ec4a6ad32732bcc75526219dd44d01ad7761779))
* bump org.apache.pulsar:pulsar-client-all from 4.2.5 to 5.0.1, ref: NOISSUE ([d57e24f](https://github.com/mihkels/open-messaging-benchmark/commit/d57e24f634b868fb43644395bfe3dded651e3294))
* bump org.apache.rocketmq:rocketmq-client-java from 5.0.7 to 5.2.2 ([f76d79f](https://github.com/mihkels/open-messaging-benchmark/commit/f76d79f445f09fd1d57d83dca0d34dee4f2db87c))
* bump org.apache.rocketmq:rocketmq-client-java from 5.0.7 to 5.2.2, ref: NOISSUE ([eba6205](https://github.com/mihkels/open-messaging-benchmark/commit/eba620584882961c43844164198ce85fa74d23fb))
