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

# Runtime and plugins

Both `bin/benchmark` and `bin/benchmark-worker` start a small supervisor JVM. When a driver is
initialized, the supervisor starts a separate driver JVM with the framework, the selected plugin
and any required delegate plugins on its classpath. Independent drivers are isolated, but bundles
that share a process and the framework still need compatible dependencies. The runtime checks known
artifact coordinates and reports conflicts; see [Packaging a plugin](../development/packaging-a-plugin.md).

## Environment variables

|        Variable        |          Default           |   Applies to   |                                                 Description                                                 |
|------------------------|----------------------------|----------------|-------------------------------------------------------------------------------------------------------------|
| `HEAP_OPTS`            | `-Xms4G -Xmx4G`            | Driver JVM     | Heap options                                                                                                |
| `JVM_OPTS`             | empty                      | Driver JVM     | Extra options appended to the defaults, for example GC logging                                              |
| `OMB_DRIVER_JVM_OPTS`  | derived                    | Driver JVM     | Replaces **all** driver JVM options, including `HEAP_OPTS`, `JVM_OPTS` and the required `--add-opens` flags |
| `SUPERVISOR_HEAP_OPTS` | `-Xmx1G`                   | Supervisor JVM | Heap options; also the place for supervisor `-D` system properties                                          |
| `OMB_HOME`             | the distribution directory | Supervisor     | Directory searched for plugins                                                                              |
| `OMB_PLUGIN_PATH`      | empty                      | Supervisor     | Extra plugin locations, separated by `:`                                                                    |

When `OMB_DRIVER_JVM_OPTS` is not set, the scripts build it as
`-server $HEAP_OPTS -XX:+UseG1GC <defaults> $JVM_OPTS`. Prefer `HEAP_OPTS` and `JVM_OPTS`; set
`OMB_DRIVER_JVM_OPTS` only when you need full control, and copy the default flags from `bin/benchmark`
into it.

## System properties

Pass these to the supervisor through `SUPERVISOR_HEAP_OPTS`, for example
`SUPERVISOR_HEAP_OPTS="-Xmx1G -Domb.driver.startupTimeoutSeconds=300"`.

|              Property              |                 Default                 |                        Description                         |
|------------------------------------|-----------------------------------------|------------------------------------------------------------|
| `omb.home`                         | `OMB_HOME`, then the scripts' directory | Plugin search home. The scripts always set it              |
| `omb.plugin.path`                  | `OMB_PLUGIN_PATH`                       | Extra plugin locations; takes precedence over the variable |
| `omb.driver.jvmOpts`               | `OMB_DRIVER_JVM_OPTS`                   | Driver JVM options; takes precedence over the variable     |
| `omb.driver.startupTimeoutSeconds` | 120                                     | How long to wait for the driver JVM to start               |

`log4j.configurationFile`, `log4j2.configurationFile` and `zone.id` are forwarded from the
supervisor to the driver JVM.

## Plugin discovery

Plugins are collected from all of these locations. Two bundles with the same `id`, or two bundles
that provide the same driver class, are an error for the affected driver classes, so do not add a
second copy of an installed plugin to the plugin path.

1. `<home>/drivers/*`, the layout of the distribution and the Docker image.
2. `<home>/*/target/omb-plugin`, the layout of a built source checkout.
3. Each entry of `omb.plugin.path` or `OMB_PLUGIN_PATH`. Relative entries are resolved against the
   home directory.

A driver configuration selects a plugin through its `driverClass`. The plugin IDs and driver
classes are listed in [Supported drivers](../drivers/index.md).

### Bundle layout

```text
drivers/driver-jms/
├── plugin.yaml
├── driver-jms-<version>.jar
├── lib/          # driver dependencies
└── ext/          # optional, user-supplied jars
```

### plugin.yaml

|           Field            | Required |                                                      Description                                                      |
|----------------------------|----------|-----------------------------------------------------------------------------------------------------------------------|
| `descriptorVersion`        | yes      | Must be `1`                                                                                                           |
| `id`                       | yes      | Bundle ID; letters, digits, `.`, `_` and `-`                                                                          |
| `version`                  | no       | Bundle version                                                                                                        |
| `driverApiVersion`         | yes      | Must match the framework's driver API version, currently `1`                                                          |
| `javaVersion`              | no       | Minimum Java feature version; the bundle is rejected on an older runtime                                              |
| `drivers`                  | yes      | Fully qualified driver classes the bundle provides                                                                    |
| `classpath`                | yes      | Jar paths or globs relative to the bundle directory                                                                   |
| `optionalClasspath`        | no       | Like `classpath`, but entries may match nothing                                                                       |
| `classReferenceProperties` | no       | Driver configuration properties whose values name classes in other bundles; those bundles are added to the driver JVM |
| `description`              | no       | Free text                                                                                                             |

Invalid bundles are reported with every problem found. See
[Packaging a plugin](../development/packaging-a-plugin.md) for building one.

### Adding provider jars

The JMS plugin loads `ext/*.jar` from its bundle. Copy a JMS provider's client jars into
`drivers/driver-jms/ext/` before running. See [JMS](../drivers/jms.md).

### --legacy-driver-name

For local runs only, `--legacy-driver-name <path>` loads the driver from a specific location instead
of the installed plugins. The path may be a plugin bundle directory, a driver module directory or a
jar. It is ignored, with a warning, when remote workers are used, because workers load drivers from
their own plugins.

## Logs

|          File          |               Written by               |
|------------------------|----------------------------------------|
| `benchmark-worker.log` | The coordinator and worker supervisors |
| `benchmark-driver.log` | The driver JVM                         |

Both are written to the current directory and roll over at 100 MB. The driver JVM's standard output
and error are also shown in the supervisor's output.

## Common errors

|                                 Message                                 |                                                  Cause                                                  |
|-------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| `No driver plugin bundle provides driver class ...`                     | No plugin with that `driverClass` was found. The message lists known classes and the searched locations |
| `Duplicate plugin id ...` or `Driver class ... is provided by both ...` | The same plugin was found in two locations                                                              |
| `Driver configuration ... must specify a driverClass`                   | The driver YAML has no `driverClass`                                                                    |
| `Driver process for ... exited during startup with code N`              | The driver JVM failed; its output is shown above the message                                            |
| `... did not start within Ns`                                           | Raise `omb.driver.startupTimeoutSeconds` or check the driver JVM output                                 |
| `Failed to initialize driver ...`                                       | The driver's `initialize` method threw, usually a connection or configuration error                     |

See [Troubleshooting](../guides/troubleshooting.md) for more.
