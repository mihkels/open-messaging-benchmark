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

# Driver selection for docker/Dockerfile.
#
#   select-drivers.sh maven-args <drivers>   prints the Maven arguments that build only <drivers>
#   select-drivers.sh prune <drivers> <dir>  removes the plugins and configurations of every other
#                                            driver from the unpacked distribution in <dir>
#
# <drivers> is "all" or a comma-separated list of names from docker/drivers.txt.
set -euo pipefail

script_dir=$(cd "$(dirname "$0")" && pwd)
known=$(grep -v -E '^[[:space:]]*(#|$)' "$script_dir/drivers.txt")

selected() {
    local list=$1 name
    if [[ "$list" == "all" ]]; then
        printf '%s\n' "$known"
        return
    fi
    IFS=',' read -r -a names <<< "$list"
    if [[ ${#names[@]} -eq 0 ]]; then
        echo "DRIVERS is empty; use \"all\" or a comma-separated list" >&2
        exit 1
    fi
    for name in "${names[@]}"; do
        if ! grep -qx -- "$name" <<< "$known"; then
            echo "Unknown driver '$name'. Known drivers: all, $(paste -s -d, - <<< "$known")" >&2
            exit 1
        fi
        printf '%s\n' "$name"
    done
}

case "${1:-}" in
    maven-args)
        [[ $# -eq 2 ]] || { echo "usage: $0 maven-args <drivers>" >&2; exit 2; }
        if [[ "$2" == "all" ]]; then
            exit 0
        fi
        profiles=$(selected "$2" | sed 's/^/driver-/' | paste -s -d, -)
        printf -- '-Domb.selectDrivers -P%s\n' "$profiles"
        ;;
    prune)
        [[ $# -eq 3 ]] || { echo "usage: $0 prune <drivers> <dir>" >&2; exit 2; }
        keep=$(selected "$2")
        while read -r name; do
            if ! grep -qx -- "$name" <<< "$keep"; then
                rm -rf -- "$3/drivers/driver-$name" "$3/driver-$name"
            fi
        done <<< "$known"
        ;;
    *)
        echo "usage: $0 maven-args <drivers> | prune <drivers> <dir>" >&2
        exit 2
        ;;
esac
