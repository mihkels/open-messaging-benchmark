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
     release as soon as the distribution is built, even if the image scan fails. Only the latest
     release keeps these two assets; older releases keep their notes and tag
     (`.github/scripts/prune-release-assets.sh`, `KEEP=1`). This is deliberate: users get older
     versions from their image or by building the tag, see
     [Older versions](building-the-project.md#older-versions). The note at the top of
     `CHANGELOG.md` says so too. The "Source code" archives on every release are added by GitHub
     and cannot be removed;
   - `Helm chart`: the chart `X.Y.Z` pushed to `oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart`
     and signed;
   - `Maven Central`: the parent POM and `driver-api` as `io.github.mihkels.omb`, only when
     `driver-api/` changed since the previous tag, see [Maven Central](#maven-central).

Images are published from release tags only; pushes to `main` publish nothing. Pull requests build,
test and scan the image but never publish, and never receive registry credentials. A weekly
scheduled `Docker image` run (also startable by hand) scans the published `latest` image so code
scanning picks up new advisories between releases.

`.github/scripts/check-versions.sh` fails the build when the version files disagree, or when a
release tag does not match them. Do not edit the version lines marked `x-release-please-version` by
hand.

## Supply-chain safeguards

- Workflows default to `permissions: {}`; each job requests only what it needs.
- Third-party actions are pinned to full commit SHAs; Dependabot proposes updates. Checkouts do
  not persist credentials.
- Registry credentials live in the `dockerhub` environment, which only `main` and `v*` tags can
  use. Only the publish jobs reference it, and they never run for pull requests. The Maven
  Central token and signing key live in the `maven-central` environment under the same rules.
- Only pushes to `main` write caches, and release tags use none, so a pull request cannot poison
  what is published.
- Images and charts are signed keylessly with cosign through GitHub's OIDC token: no signing keys
  are stored. Images carry cosign SBOM (SPDX) and SLSA provenance attestations; the chart and the
  release tarball carry SLSA provenance as GitHub artifact attestations.
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
7. **Maven Central**: see [Maven Central setup](#maven-central-setup).

## Maven Central

External driver authors compile against `io.github.mihkels.omb:driver-api` with `provided` scope.
The `Maven Central` workflow (`.github/workflows/maven-central.yml`) publishes it, with the parent
POM `io.github.mihkels.omb:messaging-benchmark` that its POM references. No other module goes to
Central: drivers, the tool and the distribution ship in the tarball and the Docker image.

Central's free tier limits file count, size and release count per month; only the release count
is within reach (about 7 a month). A release adds about 20 files and less than 1 MB. To save
releases, a tag publishes only when `driver-api/` changed since the previous `v*` tag. Dependency
upgrades in the root `pom.xml` alone do not trigger a publish. Run the workflow by hand with
`force` to publish a tag anyway.

The workflow does nothing until the repository variable `MAVEN_CENTRAL_ENABLED` is `true`. An upload
stops at **VALIDATED** in the Central Portal, where a maintainer checks it and presses Publish,
unless `MAVEN_CENTRAL_AUTO_PUBLISH` is `true`. Published versions cannot be deleted or replaced.

The `central` Maven profile adds the sources and javadoc jars and the signatures, and uploads through
`central-publishing-maven-plugin`. To check the bundle locally without uploading:

```bash
export MAVEN_GPG_KEY="$(gpg --armor --export-secret-keys <FINGERPRINT>)" MAVEN_GPG_PASSPHRASE=...
mvn -Pcentral -pl .,driver-api -Dcentral.skipPublishing=true deploy
```

The plugin still reads the `central` server from `settings.xml`, so it needs one, even with dummy
credentials.

### Maven Central setup

1. **Namespace**: sign in to [central.sonatype.com](https://central.sonatype.com) with the `mihkels`
   GitHub account. **Namespaces** lists `io.github.mihkels` as verified; if not, add it and follow
   the verification steps. It covers `io.github.mihkels.omb`.
2. **Portal token**: Account → **Generate User Token**. Keep the username and password pair.
3. **Signing key**: create a key without a personal email in its user ID and publish the public
   key:

   ```bash
   gpg --quick-gen-key "Open Messaging Benchmark Release (https://github.com/mihkels/open-messaging-benchmark)" ed25519 sign 2y
   gpg --keyserver keyserver.ubuntu.com --send-keys <FINGERPRINT>
   gpg --armor --export-secret-keys <FINGERPRINT>    # value of MAVEN_GPG_KEY
   ```

   `keys.openpgp.org` shows user IDs only for verified emails, so use `keyserver.ubuntu.com`. Keep a
   backup of the key and its revocation certificate offline.

4. **Environment `maven-central`** (Settings → Environments): deployment branches and tags limited to
   `main` and `v*` (`main` for manual runs). Secrets: `MAVEN_GPG_KEY`, `MAVEN_GPG_PASSPHRASE`,
   `CENTRAL_TOKEN_USERNAME`, `CENTRAL_TOKEN_PASSWORD`. A required reviewer is optional.

5. **Repository variables**: `MAVEN_CENTRAL_ENABLED=true`. Leave `MAVEN_CENTRAL_AUTO_PUBLISH` unset
   for the first release.

6. **First release**: Actions → `Maven Central` → Run workflow with the first tag that uses the
   `io.github.mihkels.omb` groupId and `force` checked. In the Portal under **Deployments**, check
   the files, then **Publish** or **Drop**.

7. **Usage**: in the [Usage Center](https://central.sonatype.com/publishing/usage), check the file,
   size and release counts, including whether the parent POM counts as a release of its own. Then
   set `MAVEN_CENTRAL_AUTO_PUBLISH=true` if uploads should publish without a manual step.

8. The artifacts appear under
   `https://repo1.maven.org/maven2/io/github/mihkels/omb/driver-api/` some minutes after publishing.

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

SBOM and provenance attestations on images, one of each per platform. The SPDX document name
ends with the platform (`linux/amd64`, `linux/arm64`). The provenance predicate is SLSA v1, so
the type is `slsaprovenance1` (`slsaprovenance` means SLSA v0.2):

```bash
cosign verify-attestation docker.io/mihkels/open-messaging-benchmark:0.1.0 --type spdxjson \
  --certificate-identity-regexp '^https://github.com/mihkels/open-messaging-benchmark/\.github/workflows/docker\.yml@refs/tags/v.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  | jq -r '.payload | @base64d | fromjson | .predicate.name'

cosign verify-attestation docker.io/mihkels/open-messaging-benchmark:0.1.0 --type slsaprovenance1 \
  --certificate-identity-regexp '^https://github.com/mihkels/open-messaging-benchmark/\.github/workflows/docker\.yml@refs/tags/v.*$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com > /dev/null
```

The same SBOM and provenance are also readable unsigned with
`docker buildx imagetools inspect <image> --format '{{ json .SBOM }}'` (or `.Provenance`).

SLSA provenance for the Helm chart and the release tarball is stored as a GitHub artifact
attestation:

```bash
gh attestation verify oci://docker.io/mihkels/open-messaging-benchmark-chart:0.1.0 \
  --repo mihkels/open-messaging-benchmark \
  --signer-workflow mihkels/open-messaging-benchmark/.github/workflows/helm.yml

gh release download v0.1.0 --repo mihkels/open-messaging-benchmark \
  --pattern 'openmessaging-benchmark-*-bin.tar.gz'
gh attestation verify openmessaging-benchmark-0.1.0-bin.tar.gz \
  --repo mihkels/open-messaging-benchmark \
  --signer-workflow mihkels/open-messaging-benchmark/.github/workflows/docker.yml
```

Releases published before these attestations were added carry only the image signatures and the
BuildKit SBOM and provenance.

For production clusters, pin the image digest (`image.digest` in the chart) rather than a tag.
