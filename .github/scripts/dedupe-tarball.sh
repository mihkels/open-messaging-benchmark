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

# Rewrites the distribution tarball in place so byte-identical jars are stored once, as hard links.
# Every driver bundle keeps its own lib/ paths and classloader; only the bytes on disk are shared.
# Needs GNU tar (--sort, --owner); the release runs it on ubuntu-latest.
#
#   dedupe-tarball.sh <dist.tar.gz>
set -euo pipefail

tarball=$1
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tar -xpzf "$tarball" -C "$work"
entries=("$work"/*)
if [[ ${#entries[@]} -ne 1 || ! -d ${entries[0]} ]]; then
    echo "Expected one top-level directory in $tarball" >&2
    exit 1
fi
top=${entries[0]##*/}

linked=0
prev_hash=
first=
while read -r hash path; do
    if [[ $hash == "$prev_hash" ]]; then
        ln -f "$first" "$path"
        linked=$((linked + 1))
    else
        prev_hash=$hash
        first=$path
    fi
done < <(find "$work" -type f -name '*.jar' -exec sha256sum {} + | sort)

# Linking touched the lib/ directories, and the assembly has no entries for some directories;
# give them all the build's timestamp
find "$work" -type d -exec touch -r "$work/$top/LICENSE" {} +

tar -C "$work" --sort=name --owner=root:0 --group=root:0 -cf - "$top" | gzip -n -9 > "$work/out.tar.gz"

# The rewritten archive must hold exactly the same files; it may add directory entries the
# assembly plugin leaves out
files() { tar -tzf "$1" | grep -v '/$' | sort; }
if ! diff <(files "$tarball") <(files "$work/out.tar.gz") > /dev/null; then
    echo "Deduplicated tarball lists different files than $tarball" >&2
    exit 1
fi

before=$(stat -c %s "$tarball")
mv "$work/out.tar.gz" "$tarball"
after=$(stat -c %s "$tarball")
echo "Hard-linked $linked duplicate jars: $((before / 1048576)) MB -> $((after / 1048576)) MB"
