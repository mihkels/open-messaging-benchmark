# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Image builds for local work (mise tasks) and CI. Examples:
#
#   docker buildx bake                                  # host-arch image, every driver
#   DRIVERS=kafka docker buildx bake                    # only the Kafka plugin
#   docker buildx bake multiarch-amd64 multiarch-arm64  # both architectures, separate local tags

variable "IMAGE" {
  default = "openmessaging-benchmark:local"
}

# "all" or a comma-separated list of names from docker/drivers.txt
variable "DRIVERS" {
  default = "all"
}

variable "VERSION" {
  default = "dev"
}

variable "REVISION" {
  default = "unknown"
}

group "default" {
  targets = ["local"]
}

target "app" {
  context    = "."
  dockerfile = "docker/Dockerfile"
  target     = "runtime"
  args = {
    DRIVERS  = DRIVERS
    VERSION  = VERSION
    REVISION = REVISION
  }
}

target "local" {
  inherits = ["app"]
  tags     = ["${IMAGE}"]
  output   = ["type=docker"]
}

# Docker Desktop's default builder cannot load a multi-platform index, so each architecture gets
# its own local tag.
target "multiarch-amd64" {
  inherits  = ["app"]
  platforms = ["linux/amd64"]
  tags      = ["${IMAGE}-amd64"]
  output    = ["type=docker"]
}

target "multiarch-arm64" {
  inherits  = ["app"]
  platforms = ["linux/arm64"]
  tags      = ["${IMAGE}-arm64"]
  output    = ["type=docker"]
}

# Tags and labels come from docker/metadata-action in CI; empty for local use.
target "docker-metadata-action" {}

# CI: published image. The distribution is built once on the runner and passed as the prebuilt
# context, so every driver variant reuses the same tarball.
target "release" {
  inherits  = ["app", "docker-metadata-action"]
  platforms = ["linux/amd64", "linux/arm64"]
  args = {
    DIST_SOURCE = "prebuilt"
  }
  contexts = {
    prebuilt = "dist"
  }
  attest = [
    "type=provenance,mode=max",
    "type=sbom",
  ]
}

# CI: single-architecture image for smoke tests and scanning, loaded into the runner's Docker.
target "check" {
  inherits = ["app"]
  tags     = ["${IMAGE}"]
  output   = ["type=docker"]
  args = {
    DIST_SOURCE = "prebuilt"
  }
  contexts = {
    prebuilt = "dist"
  }
}
