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

# ADR 0003: Build the documentation site with Zensical

- **Status:** Accepted, implemented
- **Date:** 2026-11-12
- **Issue:** [#369](https://github.com/mihkels/open-messaging-benchmark/issues/369), implemented in [#370](https://github.com/mihkels/open-messaging-benchmark/issues/370); follow-up [#566](https://github.com/mihkels/open-messaging-benchmark/issues/566)
- **Scope:** `docs`, `overrides`, `mkdocs.yml`, `.github/workflows/docs.yml`

## Context

The site started on MkDocs with Material for MkDocs in October 2026. Material for MkDocs is in
maintenance mode and does not work with MkDocs 2.0, which the docs workflow had to silence with
`NO_MKDOCS_2_WARNING`. The project also wanted a blog and a landing page.

## Decision drivers

- A maintained theme and build tool for the long term.
- Keep the existing `mkdocs.yml`, Markdown and navigation without a rewrite.
- Native blog, RSS and redirect support.

## Options considered

- **Stay on MkDocs and Material for MkDocs.** No migration cost, but the theme is in maintenance mode
  and does not move to MkDocs 2.0.
- **Zensical.** Built by the Material for MkDocs team as its successor and reads the existing
  `mkdocs.yml`. A trial build with Zensical 0.1.1 passed in strict mode; the only regression was an
  undefined `page.is_homepage`, fixed in `overrides/main.html`.

## Decision

Build the site with Zensical, configured through `mkdocs.yml`, and pin the Zensical version exactly
while it is in beta.

## Consequences

- Zensical checks only broken links and anchors. A page missing from the `nav` no longer fails the
  build, so the [documentation guide](../documentation.md) says to check the `nav` by hand.
- Zensical 0.1.x cannot run MkDocs plugins. `llms.txt` is generated after the build with
  `llmstxt-standalone` instead of `mkdocs-llmstxt` ([#566](https://github.com/mihkels/open-messaging-benchmark/issues/566)).
- Upgrades are deliberate: the exact pin means each Zensical release is reviewed, and the analytics
  configuration may need changes on a later upgrade.
- **Enforced by:** the hash-pinned `docs/requirements.txt` and `zensical build --clean --strict` in
  `.github/workflows/docs.yml`.

