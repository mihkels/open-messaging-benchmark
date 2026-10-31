# Helm deployment

The chart uses Helm 3. From the repository root, install it with the release name `benchmark`:

```bash
IMAGE=registry.example.com/your/openmessaging-benchmark:java25
helm upgrade --install benchmark deployment/kubernetes/helm/benchmark \
  --set image="$IMAGE" --set driverMemoryLimit=8Gi
```

Replace `IMAGE` with a reachable Java 25 image containing the current distribution and its
`drivers/` plugin bundles; the chart's default image is not maintained by this fork. The release
name matters because the current worker-address template hard-codes `benchmark-worker`. The chart
defaults to eight workers and a 4 GiB driver heap. Size the cluster and pod limits accordingly:
the default 2 GiB coordinator limit is too small for that heap plus the supervisor process.

After the pods are ready, enter the coordinator pod and run a workload:

```bash
kubectl exec -it benchmark-driver -- sh
bin/benchmark --drivers driver-pulsar/pulsar.yaml --workers "$WORKERS" \
  workloads/1-topic-16-partitions-1kb.yaml
```

Configure the driver YAML with broker addresses reachable from the workers. The chart populates
`WORKERS` with the worker URLs. Remove the deployment with:

```bash
helm uninstall benchmark
```

