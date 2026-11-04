# Helm chart

> **Note:** See [Containers and Kubernetes](https://mihkels.github.io/open-messaging-benchmark/guides/containers-and-kubernetes/) in the documentation site for a usage guide.

[`open-messaging-benchmark-chart`](open-messaging-benchmark-chart) deploys benchmark workers (a
StatefulSet with a headless Service) and a coordinator, either as a long-running pod to exec into
or as a Job that runs one driver and workload. The chart is published to Docker Hub as an OCI
artifact; its version always equals the benchmark version.

```bash
helm install omb oci://registry-1.docker.io/mihkels/open-messaging-benchmark-chart --version <version> \
  --set-file driverConfig=my-kafka.yaml \
  --set workers.replicas=4
```

From a checkout, use the chart directory instead:

```bash
helm upgrade --install omb deployment/kubernetes/helm/open-messaging-benchmark-chart \
  --set-file driverConfig=my-kafka.yaml
```

The default image is `docker.io/mihkels/open-messaging-benchmark:<chart appVersion>`. Set
`image.variant=kafka` for the single-driver image, or `image.repository`, `image.tag` or
`image.digest` for your own build.

Then start a run from the coordinator:

```bash
kubectl exec -it deploy/omb-open-messaging-benchmark-chart-coordinator -- bash
benchmark --drivers "$DRIVER" --workers "$WORKERS" -o /results/result.json "$WORKLOAD"
```

Or set `coordinator.mode=job` with `--set-file workloadConfig=my-workload.yaml`; the Job prints the
result JSON between `OMB RESULT BEGIN/END` markers in its log.

Security defaults: non-root (uid 10001), read-only root file system, all capabilities dropped,
`RuntimeDefault` seccomp profile, no service account token, and a NetworkPolicy that only lets the
coordinator reach the unauthenticated worker API. Broker addresses in the driver configuration
must be reachable from the worker pods (egress is not restricted).

See [`values.yaml`](open-messaging-benchmark-chart/values.yaml) for all values, and
[Local Kubernetes](../../../docs/guides/kubernetes-local.md) for running every driver on kind.
Remove a release with `helm uninstall omb`.
