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

# Documentation

This site is built with [MkDocs](https://www.mkdocs.org/) and the Material theme from the `docs/`
directory and
[`mkdocs.yml`](https://github.com/mihkels/open-messaging-benchmark/blob/main/mkdocs.yml) at the repository
root. This page explains how to preview it, the conventions pages follow, and how to add a driver page.

## Local preview

The Python dependencies are listed in
[`docs/requirements.in`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docs/requirements.in)
and locked, with every transitive package pinned and hashed, in
[`docs/requirements.txt`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docs/requirements.txt).
Install with `--require-hashes --only-binary :all:` so pip rejects unhashed packages and never runs a
source distribution's build script. Dependabot keeps both files up to date.
`mise install` provides Python (see [Building the project](building-the-project.md#tools)). CI builds
the site with Python 3.13 (`docs.yml`); any Python 3 that installs `docs/requirements.txt` works
locally. To preview or validate docs, use a virtual environment outside the checkout and install
dependencies only when missing.

From the repository root, set `DOCS_VENV` to that environment's absolute path:

```bash
DOCS_VENV="/absolute/path/outside-checkout/docs-venv"
```

If the documentation dependencies are missing:

```bash
python3 -m venv "$DOCS_VENV"
"$DOCS_VENV/bin/pip" install --require-hashes --only-binary :all: -r docs/requirements.txt
```

To preview:

```bash
"$DOCS_VENV/bin/mkdocs" serve
```

`mkdocs serve` serves the site at `http://127.0.0.1:8000/` and reloads it when files change.

Before opening a pull request, build the site the way CI does:

```bash
"$DOCS_VENV/bin/mkdocs" build --strict
```

`mkdocs.yml` also sets `strict: true`, so any warning fails the build: a missing page, a broken relative
link, or a page in `docs/` that is not in the navigation. The output goes to `site/`.

To add or bump a docs dependency, edit `docs/requirements.in`, then regenerate the lock file with
Python 3.13 (the version CI uses) and commit both files:

```bash
cd docs && uvx --python 3.13 --from pip-tools pip-compile --generate-hashes --allow-unsafe --strip-extras requirements.in
```

## Navigation

The navigation is defined explicitly in the `nav` section of `mkdocs.yml`. A new page appears only after
it is added there:

```yaml
nav:
  - Driver development:
      - Architecture: development/architecture.md
      - Building the project: development/building-the-project.md
```

Pages are grouped by audience:

|     Directory      |                            Contents                             |
|--------------------|-----------------------------------------------------------------|
| `getting-started/` | First runs, from a single host to distributed workers           |
| `guides/`          | Tasks: workloads, payloads, results, monitoring, containers     |
| `drivers/`         | One page per driver, plus an overview                           |
| `methodology/`     | How to design and compare benchmarks                            |
| `reference/`       | CLI, workload schema, runtime settings, results schema          |
| `development/`     | Framework architecture, building, driver development, this page |

## Page conventions

The Maven build checks Markdown files like source files, so pages must satisfy the same checks.

**License header.** The license-maven-plugin requires the Apache license header on every `.md` file, as
an HTML comment at the top of the file:

```text
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
```

`mvn license:format` adds it to new files.

**Formatting.** Spotless formats every Markdown file in the repository with flexmark (`mvn spotless:apply`),
and `mvn verify` fails on unformatted files. Keep pages to plain CommonMark with tables and fenced code
blocks:

- Start with the license header, then one `# Title`. Use `##` and `###` below it.
- Give every fenced code block a language, such as `bash`, `yaml`, `json`, `java` or `text`.
- Do not use MkDocs admonitions (`!!! note`), content tabs, or other syntax that depends on indentation,
  because the formatter can break it. Write notes as a blockquote: `> **Note:** ...`.
- Do not use Mermaid. Draw diagrams as text in a `text` code block.

**Links.**

- Link to other pages of this site with relative links to the `.md` file, for example
  `[Workloads](../guides/workloads.md)` or `[CLI](../reference/cli.md#binbenchmark)`. Strict mode checks
  that the target exists.
- Link to repository files with absolute GitHub URLs:
  `https://github.com/mihkels/open-messaging-benchmark/blob/main/<path>` for files and
  `https://github.com/mihkels/open-messaging-benchmark/tree/main/<path>` for directories. Relative links
  that leave `docs/` are not part of the site and do not work there.

**Content.**

- Check every statement against the source. Do not describe features that the code does not implement.
- Do not publish performance numbers or comparisons between systems.
- State whether paths are relative to the repository root (source checkout) or to the distribution
  directory (`/benchmark` in the container images). Driver YAML files are at `driver-*/*.yaml` in both;
  the `drivers/` directory of the distribution contains only plugin bundles.
- Each driver page includes **Local Docker Compose** and **Local kind cluster** quickstarts using
  registered contract cases, with prerequisites, results, cleanup and platform restrictions.
  Link both from the driver's README and the [driver index](../drivers/index.md#local-quickstarts).
  Reuse the existing matrix and shared setup rather than maintaining separate example stacks.
- The landing page (`overrides/home.html`) shows output and charts from one recorded Quickstart run,
  generated by `docs/demo/render_demo.py` and labelled as example output. This is the only place
  numbers appear, and they must not be presented as a measurement of any system.

## Landing page

The home page does not use the normal page template.
[`overrides/main.html`](https://github.com/mihkels/open-messaging-benchmark/blob/main/overrides/main.html)
selects
[`overrides/home.html`](https://github.com/mihkels/open-messaging-benchmark/blob/main/overrides/home.html)
for the home page, because YAML front matter (`template: home.html`) cannot precede the license header.

- The driver grid is
  [`overrides/partials/omb-drivers.html`](https://github.com/mihkels/open-messaging-benchmark/blob/main/overrides/partials/omb-drivers.html).
  Its logos are from [Simple Icons](https://simpleicons.org/) (CC0).
- To refresh the demo, rerun the [Quickstart](../getting-started/quickstart.md), save the coordinator
  output to `docs/demo/quickstart-run.log` and the result file to `docs/demo/quickstart-result.json`,
  then run `python3 docs/demo/render_demo.py` from the repository root. It rewrites
  `overrides/partials/omb-demo-terminal.html` and `overrides/partials/omb-demo-charts.html`.
- Theme colors are defined in
  [`docs/assets/stylesheets/extra.css`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docs/assets/stylesheets/extra.css).

## Analytics

The site uses Google Analytics 4 through the Material theme. The `extra.analytics` and `extra.consent`
sections of `mkdocs.yml` hold the measurement ID, the "Was this page helpful?" widget and the cookie
banner. Google Analytics loads only after a visitor accepts analytics cookies; the "Change cookie
settings" link in the footer reopens the banner. Pages served by `mkdocs serve` load it too once you
accept, so reject the banner while previewing.

Material sends page views, `search` (the search term) and `feedback` (`page` and `data`, 1 for helpful
and 0 for not helpful). Custom events are sent from
[`docs/assets/javascripts/site.js`](https://github.com/mihkels/open-messaging-benchmark/blob/main/docs/assets/javascripts/site.js):

|      Event      |                Sent when                |            Parameters            |
|-----------------|-----------------------------------------|----------------------------------|
| `code_copy`     | A code block copy button is clicked     | `snippet`: first copied line     |
| `cta_click`     | A landing page hero button is clicked   | `target`: `quickstart`, `github` |
| `driver_select` | A landing page driver card is clicked   | `driver`: driver page name       |
| `demo_replay`   | The landing page demo Replay is clicked | none                             |

To track another element, add `data-omb-event="<event name>"` and optionally `data-omb-target` or
`data-omb-driver` to it. Parameters show in Google Analytics reports only after they are registered as
event-scoped custom dimensions (**Admin > Custom definitions**).

## Adding a driver page

Each driver has a page in `docs/drivers/`. Every driver page uses the same sections, in this order:

|                   Section                   |                                           Contents                                            |
|---------------------------------------------|-----------------------------------------------------------------------------------------------|
| Summary table                               | Key facts at a glance: module, plugin id, driver class, configuration files                   |
| Prerequisites and broker setup              | What must be running and configured on the broker side                                        |
| Minimal configuration and benchmark command | The smallest working driver YAML and the `bin/benchmark` command that uses it                 |
| Configuration reference                     | Every key the driver reads, with its default and meaning                                      |
| Semantics                                   | How topics, subscriptions, keys, acknowledgements and the publish timestamp map to the broker |
| Tuning examples                             | Settings that change the measurement, and what the shipped YAML variants change               |
| Troubleshooting and verification            | Common errors, and how to confirm the run behaved as configured                               |

Adding a driver page takes four changes:

1. Add the page, `docs/drivers/<name>.md`, with the sections above.
2. Add a `nav` entry under `Drivers` in `mkdocs.yml`.
3. Update the module README, `driver-<name>/README.md`, to link to the page on the published site, for
   example `https://mihkels.github.io/open-messaging-benchmark/drivers/<name>/`.
4. Add a row for the driver to the overview table in `docs/drivers/index.md`.

For the driver itself, see [Implementing a driver](implementing-a-driver.md).

## Publishing

The site is published to <https://mihkels.github.io/open-messaging-benchmark/> by the GitHub Actions
workflow `.github/workflows/docs.yml`:

- Pushes to `main` that change `docs/`, `overrides/`, `mkdocs.yml` or the workflow build the site and deploy it to
  GitHub Pages. The workflow can also be started manually from the Actions tab; only runs on `main`
  deploy the site.
- Pull requests to `main` that change those paths only build the site, so broken links and
  navigation errors are reported before a merge, but nothing is deployed.

`site_url`, `repo_url` and `edit_uri` in `mkdocs.yml` point at the fork, so the "Edit this page" action
opens the page on the `main` branch.

Deployment requires GitHub Pages to be enabled once for the repository with **Settings > Pages >
Build and deployment > Source** set to **GitHub Actions**.
