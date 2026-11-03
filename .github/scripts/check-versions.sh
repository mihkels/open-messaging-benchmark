#!/usr/bin/env bash
#
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
#

# Fails unless the project version (pom.xml <revision>, version.txt), the Helm chart version and
# appVersion and the version the example plugin compiles against are equal. On a release tag (GITHUB_REF
# refs/tags/vX.Y.Z) the tag must match as well. release-please keeps them in sync.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
chart="$root/deployment/kubernetes/helm/open-messaging-benchmark-chart/Chart.yaml"

pom=$(sed -n 's|.*<revision>\(.*\)</revision>.*|\1|p' "$root/pom.xml" | head -n 1)
example=$(sed -n 's|.*<omb.version>\(.*\)</omb.version>.*|\1|p' "$root/examples/external-driver-plugin/pom.xml" | head -n 1)
chart_version=$(sed -n 's/^version:[[:space:]]*\([^[:space:]#]*\).*/\1/p' "$chart" | tr -d '"' | head -n 1)
app_version=$(sed -n 's/^appVersion:[[:space:]]*\([^[:space:]#]*\).*/\1/p' "$chart" | tr -d '"' | head -n 1)

version_file=$(tr -d '[:space:]' < "$root/version.txt")

printf 'version.txt:             %s\n' "$version_file"
printf 'pom.xml revision:        %s\n' "$pom"
printf 'example omb.version:     %s\n' "$example"
printf 'Chart.yaml version:      %s\n' "$chart_version"
printf 'Chart.yaml appVersion:   %s\n' "$app_version"

status=0
for value in "$version_file" "$example" "$chart_version" "$app_version"; do
    if [[ "$value" != "$pom" ]]; then
        status=1
    fi
done

ref=${GITHUB_REF:-}
if [[ "$ref" == refs/tags/* ]]; then
    tag=${ref#refs/tags/}
    printf 'tag:                     %s\n' "$tag"
    if [[ ! "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "Release tags must look like vX.Y.Z" >&2
        exit 1
    fi
    if [[ "${tag#v}" != "$pom" ]]; then
        status=1
    fi
fi

if [[ $status -ne 0 ]]; then
    echo "Versions differ. Only release-please changes them; see docs/development/releasing.md" >&2
fi
exit $status
