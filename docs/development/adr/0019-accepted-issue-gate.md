<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# ADR 0019: Close outside pull requests that do not link an accepted issue

- **Status:** Accepted, implemented
- **Date:** 2026-11-08
- **Issue:** [#209](https://github.com/mihkels/open-messaging-benchmark/issues/209), implemented in
  [#222](https://github.com/mihkels/open-messaging-benchmark/pull/222)
- **Scope:** `.github/workflows/pr-intake.yml`, `.github/scripts/pr-intake.sh`, contributor workflow

## Context

Anyone can open a pull request. The only control was a temporary interaction limit that expired on
2026-11-14. Unsolicited pull requests, such as typo-only changes, drive-by refactors, bulk or
AI-generated changes and features nobody agreed to, cost maintainer time. `CONTRIBUTING.md` already
asked for an issue first, but nothing enforced it.

## Decision drivers

- Spend review time only on agreed work.
- Never check out or run code from an outside pull request.

## Options considered

- **Label rejected pull requests `invalid`.** Rejected for a dedicated `no-accepted-issue` label, so
  reopening never touches pull requests a maintainer closed for other reasons.
- **Run on `pull_request`.** Rejected: a fork's token is read-only there, so the workflow uses
  `pull_request_target` and only calls the API.
- **List bots by name.** Rejected for skipping authors of type Bot.

## Decision

A pull request from anyone who is not an owner, member, collaborator or installed app must close an
open issue labelled `accepted`. Otherwise it gets one comment and the `no-accepted-issue` label and
is closed. Editing the description to add `Closes #<accepted issue>` reopens it.

## Consequences

- Outside pull requests without an agreed issue are closed without review.
- The workflow runs with minimal permissions and an inline justification for the
  `pull_request_target` trigger.
- **Enforced by:** `.github/workflows/pr-intake.yml` and `.github/scripts/pr-intake.sh`. The rule is
  under "Before you start" in `CONTRIBUTING.md`.

