# Changelog

## [0.4.0](https://github.com/mihkels/open-messaging-benchmark/compare/v0.3.0...v0.4.0) (2026-11-10)


### ⚠ BREAKING CHANGES

* the edge, edge-<driver> and sha-<commit> image tags are no longer updated. Use latest, latest-<driver> or a version tag. Existing edge tags stay on Docker Hub until removed by hand.
* workers reject POSTs without an application/json or application/yaml Content-Type, and /set-run-id and /initialize-driver-local take a JSON string body. Coordinators and workers from earlier releases do not interoperate with this one; upgrade every node of a cluster together.

### Features

* add benchmark-report diff command for A/B run comparison, ref: [#40](https://github.com/mihkels/open-messaging-benchmark/issues/40) ([#245](https://github.com/mihkels/open-messaging-benchmark/issues/245)) ([a068b45](https://github.com/mihkels/open-messaging-benchmark/commit/a068b45caa1040918db7d3255071f3801c40e78a))
* add worker --bind option and publish local-test ports on loopback ([#200](https://github.com/mihkels/open-messaging-benchmark/issues/200)) ([a2fc320](https://github.com/mihkels/open-messaging-benchmark/commit/a2fc32038911d002d8c0791c4c30a2bb995ebeae))
* interactive HTML benchmark report replacing create_charts.py ([#334](https://github.com/mihkels/open-messaging-benchmark/issues/334)) ([18214cb](https://github.com/mihkels/open-messaging-benchmark/commit/18214cbcbe9cccd4cf6788d4227ded5aa1bd1335))
* optional bearer token authentication for the worker HTTP API ([#203](https://github.com/mihkels/open-messaging-benchmark/issues/203)) ([5ee9b2b](https://github.com/mihkels/open-messaging-benchmark/commit/5ee9b2bce228825f763a71e221c68d7f1b715611))
* optional JSON logging and explicit run phases ([#48](https://github.com/mihkels/open-messaging-benchmark/issues/48)) ([#335](https://github.com/mihkels/open-messaging-benchmark/issues/335)) ([5507558](https://github.com/mihkels/open-messaging-benchmark/commit/55075582cb3e7303cee62d4a6d88e03a39157b22))


### Bug Fixes

* apply publish-rate changes to running producer tasks, ref: [#266](https://github.com/mihkels/open-messaging-benchmark/issues/266) ([#267](https://github.com/mihkels/open-messaging-benchmark/issues/267)) ([180e4cc](https://github.com/mihkels/open-messaging-benchmark/commit/180e4cc0bca44d8754247fabf5aa83fba661ad33))
* authenticate the supervisor-to-driver-process channel ([#208](https://github.com/mihkels/open-messaging-benchmark/issues/208)) ([a0c05b4](https://github.com/mihkels/open-messaging-benchmark/commit/a0c05b48175594db5d9e91a3444ef521439c56e9))
* complete Pravega transactional sends only after commit ([#308](https://github.com/mihkels/open-messaging-benchmark/issues/308)) ([c5ce052](https://github.com/mihkels/open-messaging-benchmark/commit/c5ce052e437fc3c4d86e7d5dc347915e5aea5f6d))
* create exactly the requested number of RocketMQ queues, on masters only ([#312](https://github.com/mihkels/open-messaging-benchmark/issues/312)) ([c94c2ad](https://github.com/mihkels/open-messaging-benchmark/commit/c94c2add125d240b287a3b713259196cbc38e3fb))
* keep broker credentials out of RabbitMQ topic names and DistributedLog errors ([#316](https://github.com/mihkels/open-messaging-benchmark/issues/316)) ([8f2c8f0](https://github.com/mihkels/open-messaging-benchmark/commit/8f2c8f0e5699ac09c461a149c8d1c587a5192bd5)), closes [#303](https://github.com/mihkels/open-messaging-benchmark/issues/303)
* keep results safe on empty publishes, --output, CSV export and diff identity ([#313](https://github.com/mihkels/open-messaging-benchmark/issues/313)) ([ca3ece4](https://github.com/mihkels/open-messaging-benchmark/commit/ca3ece4b380cedeb3a8722526f766c3332af8733))
* let Artemis consumers share a subscription and reject unsupported DistributedLog layouts ([#314](https://github.com/mihkels/open-messaging-benchmark/issues/314)) ([d86d83b](https://github.com/mihkels/open-messaging-benchmark/commit/d86d83b86e87c6c55830c4ad2ab5120d9838e3c5))
* long-run driver stability for Redis, NSQ, MQTT and Kafka ([#311](https://github.com/mihkels/open-messaging-benchmark/issues/311)) ([5532020](https://github.com/mihkels/open-messaging-benchmark/commit/5532020d1658c78040875139f071fff0e1d4a6b0))
* pin Pulsar client to 4.2.5 so JMS on Pulsar starts again ([#327](https://github.com/mihkels/open-messaging-benchmark/issues/327)) ([d5de56e](https://github.com/mihkels/open-messaging-benchmark/commit/d5de56edb8c3459af6d54ef0e8c214f968e7168c)), closes [#325](https://github.com/mihkels/open-messaging-benchmark/issues/325)
* record RabbitMQ latency in milliseconds and keep 0 ms end-to-end samples ([#310](https://github.com/mihkels/open-messaging-benchmark/issues/310)) ([5eedc5b](https://github.com/mihkels/open-messaging-benchmark/commit/5eedc5b0d3b08cafc1379278638dd7415f13c5f1))
* record worker latency histograms with 4 significant digits ([#328](https://github.com/mihkels/open-messaging-benchmark/issues/328)) ([932b0eb](https://github.com/mihkels/open-messaging-benchmark/commit/932b0ebb8be9fcb173d67a21dadff15b43f9c742)), closes [#326](https://github.com/mihkels/open-messaging-benchmark/issues/326)
* redact config keys that end in pass or pwd, such as redisPass ([#265](https://github.com/mihkels/open-messaging-benchmark/issues/265)) ([6db0e08](https://github.com/mihkels/open-messaging-benchmark/commit/6db0e080348318de57c4e37c425c8742cd3fc4a3)), closes [#254](https://github.com/mihkels/open-messaging-benchmark/issues/254)
* redact credentials in driver config logs and result files ([#202](https://github.com/mihkels/open-messaging-benchmark/issues/202)) ([ac50649](https://github.com/mihkels/open-messaging-benchmark/commit/ac506492712fb9673967579625bcced42b0edf6d))
* reject cross-site simple POSTs to the worker HTTP API ([#199](https://github.com/mihkels/open-messaging-benchmark/issues/199)) ([3d5a13e](https://github.com/mihkels/open-messaging-benchmark/commit/3d5a13e1ae08294db015e8790fdc09eeee27e6f2)), closes [#189](https://github.com/mihkels/open-messaging-benchmark/issues/189)
* stop a blocked Pulsar send from hanging producer close ([#309](https://github.com/mihkels/open-messaging-benchmark/issues/309)) ([ec86d9c](https://github.com/mihkels/open-messaging-benchmark/commit/ec86d9c513ec14679356b205dcbd9e821ddff59a))
* type-check config-supplied class names before loading them ([#197](https://github.com/mihkels/open-messaging-benchmark/issues/197)) ([8e41310](https://github.com/mihkels/open-messaging-benchmark/commit/8e4131014e98d5b9d151b179eadb8cf3bb4951ad)), closes [#191](https://github.com/mihkels/open-messaging-benchmark/issues/191)
* write the Redis message key without the Optional wrapper ([#223](https://github.com/mihkels/open-messaging-benchmark/issues/223)) ([aff5adf](https://github.com/mihkels/open-messaging-benchmark/commit/aff5adfe5e3c437f45012a9f38bd5d937f4b6813)), closes [#206](https://github.com/mihkels/open-messaging-benchmark/issues/206)


### Performance

* cut per-send overhead in PublicationLifecycle and the Redis, NSQ and Kafka hot paths ([#307](https://github.com/mihkels/open-messaging-benchmark/issues/307)) ([909cd37](https://github.com/mihkels/open-messaging-benchmark/commit/909cd37a54bb529ad29f22f3fb35e3fb3a607a61))
* skip the progress lock on the RunCallbacks hot path unless a waiter can exist ([#315](https://github.com/mihkels/open-messaging-benchmark/issues/315)) ([4199f0e](https://github.com/mihkels/open-messaging-benchmark/commit/4199f0ee1f8841e462e02d6c92c9be7a2d31bfc2))


### Refactoring

* add PublicationLifecycle.publish and share producer lifecycle tests ([#271](https://github.com/mihkels/open-messaging-benchmark/issues/271)) ([c713204](https://github.com/mihkels/open-messaging-benchmark/commit/c713204914b5bb6a20d18cd6f003c44df6296adb)), closes [#256](https://github.com/mihkels/open-messaging-benchmark/issues/256)
* change the publish rate in the rate limiter instead of replacing MessageProducer ([#291](https://github.com/mihkels/open-messaging-benchmark/issues/291)) ([5a48148](https://github.com/mihkels/open-messaging-benchmark/commit/5a481488fd053923476e5f8f81405f01b6bfded0))
* consolidate driver-api close primitives and finish the producer close migrations ([#295](https://github.com/mihkels/open-messaging-benchmark/issues/295)) ([180a90e](https://github.com/mihkels/open-messaging-benchmark/commit/180a90eec719e2c8305e03d41804ec85204d76d5)), closes [#283](https://github.com/mihkels/open-messaging-benchmark/issues/283)
* drop BookKeeper stats adaptors, closeAfterFailure copies and unused helpers ([#318](https://github.com/mihkels/open-messaging-benchmark/issues/318)) ([b98826b](https://github.com/mihkels/open-messaging-benchmark/commit/b98826bffeac04630ab01b5726ddfb08adcf6d4a))
* finish the Failures migration in driver close paths ([#293](https://github.com/mihkels/open-messaging-benchmark/issues/293)) ([60041ce](https://github.com/mihkels/open-messaging-benchmark/commit/60041cee4c97d5327708832ac4566b8e64e604ef))
* log redacted driver config once in LocalWorker ([#275](https://github.com/mihkels/open-messaging-benchmark/issues/275)) ([11aaf5e](https://github.com/mihkels/open-messaging-benchmark/commit/11aaf5e77203457fffa4ae01f121626a799ca0f5)), closes [#258](https://github.com/mihkels/open-messaging-benchmark/issues/258)
* log ResultLog and PayloadFactory under their own class names ([#279](https://github.com/mihkels/open-messaging-benchmark/issues/279)) ([c2e80d8](https://github.com/mihkels/open-messaging-benchmark/commit/c2e80d8897dd5798eac1b520681f012c6db2e4f1)), closes [#278](https://github.com/mihkels/open-messaging-benchmark/issues/278)
* move Redis, NSQ, JMS-transaction and NATS producers onto PublicationLifecycle ([#276](https://github.com/mihkels/open-messaging-benchmark/issues/276)) ([653ccca](https://github.com/mihkels/open-messaging-benchmark/commit/653cccab6677064c3c54a5e45bca56d7fe664fc3)), closes [#257](https://github.com/mihkels/open-messaging-benchmark/issues/257)
* redact ResultMetadata driver config once and reuse SecretRedaction ([#270](https://github.com/mihkels/open-messaging-benchmark/issues/270)) ([3c72c41](https://github.com/mihkels/open-messaging-benchmark/commit/3c72c411c13ec457ef6758079076fbfe48186583)), closes [#259](https://github.com/mihkels/open-messaging-benchmark/issues/259)
* resolve SonarCloud findings left by [#311](https://github.com/mihkels/open-messaging-benchmark/issues/311) ([#320](https://github.com/mihkels/open-messaging-benchmark/issues/320)) ([344106e](https://github.com/mihkels/open-messaging-benchmark/commit/344106e9894cc07187b84c8d38ec62cfdb0f173e)), closes [#319](https://github.com/mihkels/open-messaging-benchmark/issues/319)
* resolve the open SonarCloud reliability and maintainability findings ([#281](https://github.com/mihkels/open-messaging-benchmark/issues/281)) ([eeea22c](https://github.com/mihkels/open-messaging-benchmark/commit/eeea22c01b4a49867677d0e427142796a10ae2c9))
* return threshold outcome instead of throwing and store pass as boolean ([#268](https://github.com/mihkels/open-messaging-benchmark/issues/268)) ([b3ac254](https://github.com/mihkels/open-messaging-benchmark/commit/b3ac2542502b4dcf6017321cd716e2e91a8dcdb6)), closes [#263](https://github.com/mihkels/open-messaging-benchmark/issues/263)
* reuse TestResult histogram codec and ObjectMappers in benchmark-report ([#272](https://github.com/mihkels/open-messaging-benchmark/issues/272)) ([320f288](https://github.com/mihkels/open-messaging-benchmark/commit/320f288c4c3ccdee651efb69d681d993a7b3e06e)), closes [#260](https://github.com/mihkels/open-messaging-benchmark/issues/260)
* scrape worker metrics through HttpWorkerClient and share token-file reading ([#274](https://github.com/mihkels/open-messaging-benchmark/issues/274)) ([cef78e5](https://github.com/mihkels/open-messaging-benchmark/commit/cef78e55e915f0786aa7b717364721b4fc2a9a48)), closes [#261](https://github.com/mihkels/open-messaging-benchmark/issues/261)
* share an immutable payload value between ProducerWorkAssignment and PayloadFactory ([#294](https://github.com/mihkels/open-messaging-benchmark/issues/294)) ([8ca627c](https://github.com/mihkels/open-messaging-benchmark/commit/8ca627c5ba557aa380833b72820216bcd732bf27)), closes [#286](https://github.com/mihkels/open-messaging-benchmark/issues/286)
* simplify NSQ consumer, Kafka topic creator, MQTT 5 driver and Redis consumer ([#277](https://github.com/mihkels/open-messaging-benchmark/issues/277)) ([238cf36](https://github.com/mihkels/open-messaging-benchmark/commit/238cf3613c66861f91dba354d42d1c39437c089f))
* small framework, tool and MQTT 5 simplifications ([#292](https://github.com/mihkels/open-messaging-benchmark/issues/292)) ([407c657](https://github.com/mihkels/open-messaging-benchmark/commit/407c65703ef11dcdab789493e98020af0f608ef7)), closes [#287](https://github.com/mihkels/open-messaging-benchmark/issues/287)
* use driver-api Failures instead of local failure aggregators ([#273](https://github.com/mihkels/open-messaging-benchmark/issues/273)) ([68eafc7](https://github.com/mihkels/open-messaging-benchmark/commit/68eafc7bbbc4db589fb488d6f582a212ba2c123c))


### Documentation

* add code of conduct and support guide, ref: [#212](https://github.com/mihkels/open-messaging-benchmark/issues/212) ([#243](https://github.com/mihkels/open-messaging-benchmark/issues/243)) ([d3054f9](https://github.com/mihkels/open-messaging-benchmark/commit/d3054f994185a3509768b81e83f1c4133567bcfa))
* add local quickstarts for every driver ([#324](https://github.com/mihkels/open-messaging-benchmark/issues/324)) ([977fa0c](https://github.com/mihkels/open-messaging-benchmark/commit/977fa0cb8fc62cbcc4d4649233be2aacb88339de))
* Avoid dependency formatting and repeat verification ([#289](https://github.com/mihkels/open-messaging-benchmark/issues/289)) ([0c6d897](https://github.com/mihkels/open-messaging-benchmark/commit/0c6d89715d7b0d99ec19217e2736d2e353f25048))
* refresh driver semantics and latency guidance ([#323](https://github.com/mihkels/open-messaging-benchmark/issues/323)) ([3c49f65](https://github.com/mihkels/open-messaging-benchmark/commit/3c49f652b6a9585eb77ea2cd08e856ac9e92293e))


### Dependencies

* Bump com.rabbitmq:amqp-client from 5.37.0 to 5.37.1 ([#330](https://github.com/mihkels/open-messaging-benchmark/issues/330)) ([fe22a56](https://github.com/mihkels/open-messaging-benchmark/commit/fe22a56061a5fc14d5ce725f410745cbaaad9895))
* Bump org.codehaus.mojo:flatten-maven-plugin from 1.8.0 to 1.9.0 ([#329](https://github.com/mihkels/open-messaging-benchmark/issues/329)) ([7313e5e](https://github.com/mihkels/open-messaging-benchmark/commit/7313e5e6f38c63afafd48415369dee786ea8406c))


### CI

* publish Docker images only on release tags ([#242](https://github.com/mihkels/open-messaging-benchmark/issues/242)) ([c8877df](https://github.com/mihkels/open-messaging-benchmark/commit/c8877df140a1355a4627ac81c73eccf1da600452))

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
