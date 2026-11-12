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

# Keeps the Actions cache under the 10 GB per-repository limit. Deletes:
#   - caches on refs/pull/<n>/head and /merge once pull request <n> is merged or closed
#   - older duplicates on refs/heads/main, keeping the newest entry per key prefix (Maven, CodeQL
#     dependencies and overlay databases, mise JVM)
#   - caches on refs/heads/refs/tags/* (left by tag builds)
#
# Environment: GH_TOKEN, GH_REPO (owner/name). DRY_RUN=1 prints what would be deleted without
# deleting anything. CACHES_JSON=<file> reads the cache list from a file instead of the API (for
# testing; pull request states are still read from the API).
set -euo pipefail

dry_run=${DRY_RUN:-0}

summary() {
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        echo "$1" >> "$GITHUB_STEP_SUMMARY"
    fi
}

if [[ -n "${CACHES_JSON:-}" ]]; then
    caches=$(cat "$CACHES_JSON")
else
    caches=$(gh cache list --limit 10000 --json id,key,ref,sizeInBytes,createdAt)
fi

# Pull requests that are no longer open
closed_prs=$(for pr in $(jq -r '.[].ref | capture("^refs/pull/(?<n>[0-9]+)/(head|merge)$").n' <<<"$caches" | sort -un); do
    if [[ "$(gh pr view "$pr" --json state --jq .state)" != OPEN ]]; then
        echo "$pr"
    fi
done | jq -R 'tonumber' | jq -s .)

# One "id<TAB>size<TAB>ref<TAB>key<TAB>reason" line per cache to delete
plan=$(jq -r --argjson closed "$closed_prs" '
    def group:
        .key | capture("^(?<g>maven-Linux-|codeql-dependencies-1-Linux-java-|mise-v1-jvm-)").g
            // (capture("^codeql-overlay-base-database-1-[0-9a-f]+-(?<lang>java|python)-")
                | "codeql-overlay-base-database-1-*-\(.lang)-");
    [
        (.[] | select(.ref | test("^refs/pull/[0-9]+/(head|merge)$"))
            | select((.ref | capture("^refs/pull/(?<n>[0-9]+)/").n | tonumber) as $n | $closed | index($n))
            | . + {reason: "pull request closed"}),
        (.[] | select(.ref | startswith("refs/heads/refs/tags/")) | . + {reason: "tag ref"}),
        ([.[] | select(.ref == "refs/heads/main") | select(group != null) | . + {g: group}]
            | group_by(.g)[] | sort_by(.createdAt) | reverse | .[1:][]
            | . + {reason: "older duplicate of \(.g)"})
    ] | .[] | [.id, .sizeInBytes, .ref, .key, .reason] | @tsv
' <<<"$caches")

total=$(jq '[.[].sizeInBytes] | add // 0' <<<"$caches")
freed=0
count=0
verb=$([[ "$dry_run" == 1 ]] && echo "Would delete" || echo "Deleted")

summary "| Cache | Ref | MB | Reason |"
summary "|---|---|---:|---|"
while IFS=$'\t' read -r id size ref key reason; do
    [[ -z "$id" ]] && continue
    if [[ "$dry_run" != 1 ]]; then
        gh cache delete "$id"
    fi
    echo "$verb $key ($ref, $((size / 1048576)) MB): $reason"
    summary "| \`$key\` | \`$ref\` | $((size / 1048576)) | $reason |"
    freed=$((freed + size))
    count=$((count + 1))
done <<<"$plan"

line="$verb $count caches, $((freed / 1048576)) MB of $((total / 1048576)) MB"
echo "$line"
summary ""
summary "$line"
