# Changelog

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
