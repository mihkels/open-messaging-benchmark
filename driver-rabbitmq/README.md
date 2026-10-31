# RabbitMQ benchmarks

> **Legacy AWS deployment:** the Terraform/Ansible assets below install Java 11 and refer to the
> 0.0.1 tarball and old launcher settings. Adapt them to deploy the current tarball, Java 27 and
> complete `drivers/` plugin bundles on every worker before use. For a local broker-backed check,
> use the [Compose regression suite](../e2e-tests/compose/README.md).

This folder houses all of the assets necessary to run benchmarks for [RabbitMQ](https://www.rabbitmq.com/). In order to run these benchmarks, you'll need to:

* [Create the necessary local artifacts](#creating-local-artifacts)
* [Stand up a RabbitMQ cluster](#creating-a-rabbitmq-cluster-on-amazon-web-services-aws-using-terraform-and-ansible) on Amazon Web Services (which includes a client host for running the benchmarks)
* [SSH into the client host](#sshing-into-the-client-host)
* [Run the benchmarks from the client host](#running-the-benchmarks-from-the-client-host)

## Creating local artifacts

To create local artifacts, install [Maven](https://maven.apache.org/install.html) and JDK 27, then build
the full reactor to include the driver plugins:

```bash
$ git clone https://github.com/mihkels/open-messaging-benchmark.git
$ cd open-messaging-benchmark
$ mvn clean install
```

## Creating a RabbitMQ cluster on Amazon Web Services (AWS) using Terraform and Ansible

In order to create an RabbitMQ cluster on AWS, you'll need to have the following installed:

* [Terraform](https://terraform.io)
* [The `terraform-inventory` plugin for Terraform](https://github.com/adammck/terraform-inventory)
* [Ansible](http://docs.ansible.com/ansible/latest/intro_installation.html)

In addition, you will need to:

* [Create an AWS account](https://aws.amazon.com/account/) (or use an existing account)
* [Install the `aws` CLI tool](https://aws.amazon.com/cli/)
* [Configure the `aws` CLI tool](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)

Once those conditions are in place, you'll need to create an SSH public and private key at `~/.ssh/rabbitmq_aws` (private) and `~/.ssh/rabbitmq_aws.pub` (public), respectively.

```bash
$ ssh-keygen -f ~/.ssh/rabbitmq_aws
```

When prompted to enter a passphrase, simply hit **Enter** twice. Then, make sure that the keys have been created:

```bash
$ ls ~/.ssh/rabbitmq_aws*
```

With SSH keys in place, you can create the necessary AWS resources using a single Terraform command:

```bash
$ cd driver-rabbitmq/deploy
$ terraform init
$ terraform apply
```

That will install the following [EC2](https://aws.amazon.com/ec2) instances (plus some other resources, such as a [Virtual Private Cloud](https://aws.amazon.com/vpc/) (VPC)):

| Resource            | Description                                                 | Count |
|:--------------------|:------------------------------------------------------------|:------|
| RabbitMQ instances  | The VMs on which RabbitMQ brokers will run                  | 3     |
| Client instance     | The VM from which the benchmarking suite itself will be run | 4     |
| Prometheus instance | The VM on which metrics services will be run                | 1     |

When you run `terraform apply`, you will be prompted to type `yes`. Type `yes` to continue with the installation or anything else to quit.

Once the installation is complete, you will see a confirmation message listing the resources that have been installed.

### Variables

There's a handful of configurable parameters related to the Terraform deployment that you can alter by modifying the defaults in the `terraform.tfvars` file.

| Variable          | Description                                                                                                              | Default                                                                     |
|:------------------|:-------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------------------|
| `region`          | The AWS region in which the RabbitMQ cluster will be deployed                                                            | `us-west-2`                                                                 |
| `az`              | The availability zone in which the RabbitMQ cluster will be deployed                                                     | `us-west-2a`                                                                |
| `public_key_path` | The path to the SSH public key that you've generated                                                                     | `~/.ssh/rabbitmq_aws.pub`                                                   |
| `ami`             | The [Amazon Machine Image](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AMIs.html) used by the cluster's machines | `ami-08970fb2e5767e3b8` (RHEL 8, us-west-2)                                 |
| `instance_types`  | The EC2 instance types used by the various components                                                                    | `i3en.6xlarge` (RabbitMQ), `m5n.8xlarge` (clients), `t2.large` (Prometheus) |

> If you modify the `public_key_path`, make sure that you point to the appropriate SSH key path when running the [Ansible playbook](#running-the-ansible-playbook).

### Running the Ansible playbook

With the appropriate infrastructure in place, run Ansible after adapting the playbook as noted above.
`TF_STATE` must point to the directory containing the Terraform state.

```bash
$ TF_STATE=. ansible-playbook \
  --user ec2-user \
  --inventory `which terraform-inventory` \
  deploy.yaml
```

> If you're using an SSH private key path different from `~/.ssh/rabbitmq_aws`, you can specify that path using the `--private-key` flag, for example `--private-key=~/.ssh/my_key`.

## SSHing into the client host

In the [output](https://www.terraform.io/intro/getting-started/outputs.html) produced by Terraform, there's a `client_ssh_host` variable that provides the IP address for the client EC2 host from which benchmarks can be run. You can SSH into that host using this command:

```bash
$ ssh -i ~/.ssh/rabbitmq_aws ec2-user@$(terraform output client_ssh_host)
```

## Running the benchmarks from the client host

Once you've successfully SSHed into the client host, you can run all [available benchmark workloads](../workloads/) like this:

```bash
$ cd /opt/benchmark
$ sudo bin/benchmark --drivers driver-rabbitmq/rabbitmq.yaml workloads/*.yaml
```

You can also run specific workloads in the `workloads` folder. Here's an example:

```bash
$ sudo bin/benchmark --drivers driver-rabbitmq/rabbitmq.yaml workloads/1-topic-1-partition-1kb.yaml
```

The checked-in configuration uses quorum queues; adjust its broker URI and queue type for your deployment.

## Monitoring

### Native

The `rabbitmq_management` plugin is installed, and the HTTP endpoint is **publicly** exposed on all RabbitMQ instances
on port `15672`. This allows access to the management UI which provides some basic status metrics. It should also be
possible to access the management REST API at this endpoint.

Note that the connection is authenticated but not currently encrypted and so passwords will be passed in plain text. Use
the `admin` account configured in the [Terraform](deploy/provision-rabbitmq-aws.tf) file to log in.

### Prometheus

The `rabbitmq_prometheus` plugin is installed and Prometheus is installed on a standalone instance, along with
[Node Exporter](https://github.com/prometheus/node_exporter) on all brokers to allow the collection of system metrics.
Prometheus exposes a public endpoint `http://${prometheus_host}:9090`. See
'[RabbitMQ.com — Monitoring with Prometheus & Grafana](https://www.rabbitmq.com/prometheus.html)' for more information.

### Grafana

Grafana and a set of standard dashboards are installed alongside Prometheus. These are exposed on a public endpoint
`http://${prometheus_host}:3000`. Credentials are `admin`/`admin`. Dashboards included:
* [RabbitMQ's standard dashboards](https://grafana.com/rabbitmq)
* [Node Exporter dashboard](https://grafana.com/grafana/dashboards/1860-node-exporter-full/)
