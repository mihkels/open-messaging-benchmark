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

prepare_distribution() {
    local script_dir target_dir
    script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
    target_dir=$(cd "$script_dir/../package/target" && pwd)
    shopt -s nullglob
    local archives=("$target_dir"/openmessaging-benchmark-*-bin.tar.gz)
    shopt -u nullglob
    if [ "${#archives[@]}" -ne 1 ]; then
        echo "Expected exactly one distribution tarball in $target_dir; found ${#archives[@]}" >&2
        return 1
    fi

    TARBALL=${archives[0]}
    DIST_NAME=$(basename "$TARBALL" -bin.tar.gz)
    PACKAGE_WORK_DIR=$(mktemp -d "$target_dir/.omb-jms.XXXXXXXX")
    trap 'rm -rf "$PACKAGE_WORK_DIR"' EXIT
    tar -xzf "$TARBALL" -C "$PACKAGE_WORK_DIR"
    if [ ! -d "$PACKAGE_WORK_DIR/$DIST_NAME" ]; then
        echo "Distribution tarball does not contain $DIST_NAME" >&2
        return 1
    fi
    EXT_DIR="$PACKAGE_WORK_DIR/$DIST_NAME/drivers/driver-jms/ext"
    mkdir -p "$EXT_DIR"
}

replace_distribution() {
    tar -czf "$PACKAGE_WORK_DIR/repacked.tar.gz" -C "$PACKAGE_WORK_DIR" "$DIST_NAME"
    mv -f "$PACKAGE_WORK_DIR/repacked.tar.gz" "$TARBALL"
}

verify_sha256() {
    local file=$1 expected=$2 actual
    if command -v sha256sum > /dev/null 2>&1; then
        actual=$(sha256sum "$file")
    elif command -v shasum > /dev/null 2>&1; then
        actual=$(shasum -a 256 "$file")
    else
        echo "A sha256sum or shasum executable is required" >&2
        return 1
    fi
    actual=${actual%% *}
    if [ "$actual" != "$expected" ]; then
        echo "SHA-256 mismatch for $file" >&2
        return 1
    fi
}
