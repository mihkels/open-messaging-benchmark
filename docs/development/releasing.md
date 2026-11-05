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

# Releasing

Releases are automated. The project version, the Helm chart version and the chart `appVersion` are
always the same number, and only [release-please](https://github.com/googleapis/release-please)
changes it.

## How a release happens

1. Pull requests are squash-merged, and their titles are
   [conventional commits](https://www.conventionalcommits.org/) (`feat: ...`, `fix: ...`); the
   `Pull request title` workflow checks this.
2. On every push to `main`, the `Release` workflow updates an open **Release PR**. It sets the next
   version in `pom.xml` (`revision`), `version.txt`, the example plugin's `omb.version` and the
   chart's `Chart.yaml` (`version` and `appVersion`), and adds the changes to `CHANGELOG.md`.
   Before 1.0, `feat` raises the minor version and `fix` the patch version; breaking changes
   (`feat!:` or a `BREAKING CHANGE:` footer) raise the minor version.
3. Merging the Release PR tags `vX.Y.Z` and creates a GitHub release.
4. The tag triggers publishing:
   - `Docker image`: full verification, then multi-arch images `X.Y.Z`, `X.Y`, `latest` and their
     `-<driver>` variants, signed with cosign; the same verified
     `openmessaging-benchmark-X.Y.Z-bin.tar.gz` and a `SHA256SUMS` file are attached to the GitHub
     release. Only the newest 10 releases keep these two assets; older releases keep their notes
     and tag (`.github/scripts/prune-release-assets.sh`);
   - `Helm chart`: the chart `X.Y.Z` pushed to `oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart`
     and signed.

Every push to `main` also publishes the `edge` and `sha-<commit>` images. Pull requests build, test
and scan the image but never publish, and never receive registry credentials.

`.github/scripts/check-versions.sh` fails the build when the version files disagree, or when a
release tag does not match them. Do not edit the version lines marked `x-release-please-version` by
hand.

## Supply-chain safeguards

- Workflows default to `permissions: {}`; each job requests only what it needs.
- Third-party actions are pinned to full commit SHAs; Dependabot proposes updates. Checkouts do
  not persist credentials.
- Registry credentials live in the `dockerhub` environment, which only `main` and `v*` tags can
  use. Only the publish jobs reference it, and they never run for pull requests.
- Publishing workflows use no caches, so a pull request cannot poison what is published.
- Images and charts are signed keylessly with cosign through GitHub's OIDC token: no signing keys
  are stored. Images carry SBOM and SLSA provenance attestations.
- Trivy scans every image and sends all findings to code scanning. Fixable critical and high
  vulnerabilities in the base image packages and the benchmark framework (`/benchmark/lib`) fail
  the build. Findings in the client libraries bundled with driver plugins (`/benchmark/drivers`)
  are reported but do not block, since they follow each broker's client releases; Dependabot
  proposes Maven upgrades for them.
- `Workflow lint` runs actionlint and zizmor, and checks that images are published only to
  Docker Hub.

## One-time repository setup

1. **Docker Hub**: create the repositories `mihkels/open-messaging-benchmark` and
   `mihkels/open-messaging-benchmark-chart`, and an access token with **Read & Write** permission
   (not Delete).
2. **Environment `dockerhub`** (Settings → Environments): variable `DOCKERHUB_USERNAME`, secret
   `DOCKERHUB_TOKEN`; deployment branches and tags limited to `main` and `v*`. A required reviewer
   is optional.
3. **GitHub App for release-please**: create an app with repository permissions *Contents*,
   *Pull requests* and *Issues* set to read and write, install it on this repository only, and
   create environment `release` (limited to `main`) with variable `RELEASE_APP_ID` (the app's App ID or Client ID) and secret
   `RELEASE_APP_PRIVATE_KEY`. The app token is needed because pull requests and tags created with
   `GITHUB_TOKEN` do not trigger workflows.
4. **Rulesets**: protect `main` (pull requests, required checks) and `v*` tags (only maintainers
   and the release app may create them).
5. **Actions settings**: workflow permissions read-only by default, and approval required for
   workflows from fork pull requests.
6. **Security**: enable code scanning and private vulnerability reporting.

## Verifying images and charts

Signatures:

```bash
cosign verify docker.io/mihkels/open-messaging-benchmark:0.1.0 \
  --certificate-identity-regexp '^https://github.com/mihkels/open-messaging-benchmark/\.github/workflows/docker\.yml@refs/(heads/main|tags/v.*)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

cosign verify docker.io/mihkels/open-messaging-benchmark-chart:0.1.0 \
  --certificate-identity-regexp '^https://github.com/mihkels/open-messaging-benchmark/\.github/workflows/helm\.yml@refs/tags/v.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

SBOM and provenance:

```bash
docker buildx imagetools inspect docker.io/mihkels/open-messaging-benchmark:0.1.0 \
  --format '{{ json .SBOM }}'
docker buildx imagetools inspect docker.io/mihkels/open-messaging-benchmark:0.1.0 \
  --format '{{ json .Provenance }}'
```

For production clusters, pin the image digest (`image.digest` in the chart) rather than a tag.
