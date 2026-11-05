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

# Removes the distribution tarball and SHA256SUMS from every published release except the newest
# KEEP (default 1) by version. Release notes, tags and any other assets stay. DRY_RUN=1 prints
# what would be removed without deleting anything.
set -euo pipefail

keep=${KEEP:-1}
dry_run=${DRY_RUN:-0}

summary() {
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        echo "$1" >> "$GITHUB_STEP_SUMMARY"
    fi
}

old_tags=$(gh release list --exclude-drafts --exclude-pre-releases --limit 1000 \
    --json tagName --jq '.[].tagName' | sort -rV | tail -n +$((keep + 1)))

for tag in $old_tags; do
    for asset in $(gh release view "$tag" --json assets --jq '.assets[].name'); do
        if [[ "$asset" == openmessaging-benchmark-*-bin.tar.gz || "$asset" == SHA256SUMS ]]; then
            if [[ "$dry_run" == 1 ]]; then
                echo "Would remove $asset from $tag"
            else
                gh release delete-asset "$tag" "$asset" --yes
                echo "Removed $asset from $tag"
                summary "Removed $asset from release $tag"
            fi
        fi
    done
done
