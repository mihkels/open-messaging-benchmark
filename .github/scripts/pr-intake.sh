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

# Closes a pull request from an outside contributor unless it closes an open issue in this
# repository that a maintainer labelled `accepted`. A PR closed this way gets the
# `no-accepted-issue` label; once its description links an accepted issue, the next `edited` event
# reopens it. Maintainers (OWNER, MEMBER, COLLABORATOR) and GitHub Apps installed on the repository
# (Dependabot, release-please, Copilot) are skipped.
#
# Reads the pull request only through the API and never runs its code.
#
# Environment: GH_TOKEN, REPO (owner/name), PR_NUMBER. DRY_RUN=1 prints the decision without
# changing anything.
set -euo pipefail

: "${REPO:?}" "${PR_NUMBER:?}"
[[ "$PR_NUMBER" =~ ^[0-9]+$ ]] || { echo "PR_NUMBER must be numeric" >&2; exit 1; }

ACCEPTED_LABEL=accepted
CLOSED_LABEL=no-accepted-issue
MARKER='<!-- pr-intake -->'
CONTRIBUTING="https://github.com/$REPO/blob/main/CONTRIBUTING.md#before-you-start"

run() {
  if [[ "${DRY_RUN:-}" == 1 ]]; then
    echo "dry run: $*" >&2
  else
    "$@"
  fi
}

pr=$(gh api "repos/$REPO/pulls/$PR_NUMBER")
association=$(jq -r .author_association <<<"$pr")
author_type=$(jq -r .user.type <<<"$pr")
state=$(jq -r .state <<<"$pr")
has_closed_label=$(jq --arg l "$CLOSED_LABEL" '[.labels[].name] | index($l) != null' <<<"$pr")

case "$association" in
  OWNER | MEMBER | COLLABORATOR)
    echo "Skipping: author is a $association"
    exit 0
    ;;
esac
if [[ "$author_type" == Bot ]]; then
  echo "Skipping: author is an app installed on the repository"
  exit 0
fi

owner=${REPO%%/*}
name=${REPO#*/}
# shellcheck disable=SC2016 # GraphQL variables, not shell expansions
accepted=$(gh api graphql -F owner="$owner" -F name="$name" -F number="$PR_NUMBER" -f query='
  query($owner: String!, $name: String!, $number: Int!) {
    repository(owner: $owner, name: $name) {
      pullRequest(number: $number) {
        closingIssuesReferences(first: 25) {
          nodes { number state repository { nameWithOwner } labels(first: 50) { nodes { name } } }
        }
      }
    }
  }' | jq -r --arg repo "$REPO" --arg label "$ACCEPTED_LABEL" '
    [.data.repository.pullRequest.closingIssuesReferences.nodes[]
      | select(.repository.nameWithOwner == $repo and .state == "OPEN")
      | select([.labels.nodes[].name] | index($label) != null)
      | .number] | map("#\(.)") | join(", ")')

if [[ -n "$accepted" ]]; then
  echo "Allowed: closes accepted issue(s) $accepted"
  if [[ "$state" == closed && "$has_closed_label" == true ]]; then
    echo "Reopening: it was closed by this check"
    run gh api -X PATCH "repos/$REPO/pulls/$PR_NUMBER" -f state=open >/dev/null
    run gh api -X DELETE "repos/$REPO/issues/$PR_NUMBER/labels/$CLOSED_LABEL" >/dev/null
  fi
  exit 0
fi

if [[ "$state" != open ]]; then
  echo "No accepted issue linked; pull request is already closed"
  exit 0
fi

echo "Closing: no open issue labelled '$ACCEPTED_LABEL' is linked"
already_commented=$(gh api --paginate "repos/$REPO/issues/$PR_NUMBER/comments" \
  --jq ".[] | select(.body | startswith(\"$MARKER\")) | .id" | head -n1)
if [[ -z "$already_commented" ]]; then
  body="$MARKER
Thanks for the pull request. Pull requests from outside contributors must close an open issue that a maintainer has labelled \`$ACCEPTED_LABEL\`, so this one was closed automatically without review.

- If an accepted issue exists, add \`Closes #<issue>\` to the description. The pull request reopens on its own.
- If not, open a bug report, or start a discussion for features and new drivers, and wait for a maintainer to accept it.

See [Before you start]($CONTRIBUTING)."
  run gh api "repos/$REPO/issues/$PR_NUMBER/comments" -f body="$body" >/dev/null
fi
run gh api "repos/$REPO/issues/$PR_NUMBER/labels" -f "labels[]=$CLOSED_LABEL" >/dev/null
run gh api -X PATCH "repos/$REPO/pulls/$PR_NUMBER" -f state=closed >/dev/null
