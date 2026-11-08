# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/mihkels/open-messaging-benchmark/security/advisories/new).
Do not open a public issue. Include the affected version or image tag, the impact, and steps to
reproduce.

## Scope

The benchmark workers expose an HTTP API that loads driver configurations and connects to arbitrary
brokers. It is unauthenticated unless a shared bearer token is configured with `OMB_WORKER_TOKEN` or
`--token-file` (see [Authentication](docs/reference/cli.md#authentication)); the token is sent in
clear text over HTTP. Run workers only on networks you trust; the Helm chart restricts the worker
port to the coordinator with a NetworkPolicy by default and can enable the token with
`workers.auth.enabled`.

## Verifying published artifacts

Images (`docker.io/mihkels/open-messaging-benchmark`) and the Helm chart
(`docker.io/mihkels/open-messaging-benchmark-chart`) are signed keylessly with cosign from this
repository's GitHub Actions workflows, and images carry SBOM and provenance attestations. See
[Releasing](docs/development/releasing.md#verifying-images-and-charts) for the `cosign verify`
commands.
