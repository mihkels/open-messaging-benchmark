/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.tool.report;

import io.openmessaging.benchmark.ResultMetadata;
import io.openmessaging.benchmark.TestResult;
import io.openmessaging.benchmark.Workload;
import io.openmessaging.benchmark.utils.distributor.KeyDistribution;
import io.openmessaging.benchmark.utils.distributor.KeyDistributorType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

/** The workload and environment of runs, and how they differ between runs. */
final class RunContext {

    /**
     * A setting with different values on the two sides of a comparison.
     *
     * @param name the setting, such as {@code jdk} or {@code driverConfig.acks}
     * @param baseline the distinct baseline values
     * @param candidate the distinct candidate values
     */
    record SettingDifference(String name, Set<String> baseline, Set<String> candidate) {}

    private RunContext() {}

    /**
     * Returns the workload settings that make runs incomparable, keyed by setting. Version 1 files
     * have no workload definition, so only the settings stored in the result are compared for them.
     *
     * @param run the run to describe
     * @return the setting values by setting name
     */
    static Map<String, String> workloadIdentity(Run run) {
        TestResult result = run.result();
        Map<String, String> identity = new LinkedHashMap<>();
        ResultMetadata metadata = result.metadata;
        if (metadata != null) {
            Workload workload = metadata.workload();
            identity.put("name", String.valueOf(workload.name));
            identity.put("producerRate", String.valueOf(workload.producerRate));
            identity.put("messageSize", String.valueOf(workload.messageSize));
            identity.put("partitionsPerTopic", String.valueOf(workload.partitionsPerTopic));
            // Settings missing from older files read as the Workload defaults, the same values a run had
            identity.put("topics", String.valueOf(workload.topics));
            identity.put("producersPerTopic", String.valueOf(workload.producersPerTopic));
            identity.put("subscriptionsPerTopic", String.valueOf(workload.subscriptionsPerTopic));
            identity.put("consumerPerSubscription", String.valueOf(workload.consumerPerSubscription));
            identity.put("keyDistributor", String.valueOf(workload.keyDistributor));
            if (workload.keyDistributor == KeyDistributorType.ZIPFIAN) {
                KeyDistribution keys =
                        workload.keyDistribution != null ? workload.keyDistribution : KeyDistribution.DEFAULT;
                identity.put("keyDistribution", keys.toString());
            }
            Map<String, Integer> distribution = workload.messageSizeDistribution;
            identity.put(
                    "messageSizeDistribution",
                    distribution == null || distribution.isEmpty()
                            ? "none"
                            : new TreeMap<>(distribution).toString());
        } else {
            identity.put("name", String.valueOf(result.workload));
            identity.put("messageSize", String.valueOf(result.messageSize));
            identity.put("partitionsPerTopic", String.valueOf(result.partitions));
        }
        return identity;
    }

    /**
     * Describes each workload setting that differs between any two runs, on either side. Each message
     * names the files with each value.
     *
     * @param runs the runs of both sides
     * @return one message per mismatched setting; empty when the workloads match
     */
    static List<String> workloadMismatches(List<Run> runs) {
        Map<String, Map<String, List<String>>> filesByValueBySetting = new LinkedHashMap<>();
        for (Run run : runs) {
            workloadIdentity(run)
                    .forEach((setting, value) ->
                            filesByValueBySetting
                                    .computeIfAbsent(setting, key -> new LinkedHashMap<>())
                                    .computeIfAbsent(value, key -> new ArrayList<>())
                                    .add(run.name()));
        }
        List<String> mismatches = new ArrayList<>();
        filesByValueBySetting.forEach((setting, filesByValue) -> {
            if (filesByValue.size() > 1) {
                List<String> values = new ArrayList<>();
                filesByValue.forEach(
                        (value, files) -> values.add(value + " (" + String.join(", ", files) + ")"));
                mismatches.add("Workload " + setting + " differs: " + String.join(" vs ", values));
            }
        });
        return mismatches;
    }

    /**
     * Returns the workload name to head the report with.
     *
     * @param runs the runs of both sides
     * @return the distinct workload names, comma separated, or {@code unknown} without runs
     */
    static String workloadName(List<Run> runs) {
        return runs.stream()
                .map(run -> workloadIdentity(run).get("name"))
                .distinct()
                .reduce((a, b) -> a + ", " + b)
                .orElse("unknown");
    }

    /**
     * Returns the environment settings whose values differ between the baseline and candidate.
     *
     * @param baseline the baseline runs
     * @param candidate the candidate runs
     * @return the differing settings, in the order they are first seen
     */
    static List<SettingDifference> settingDifferences(List<Run> baseline, List<Run> candidate) {
        Map<String, Set<String>> baselineSettings = settings(baseline);
        Map<String, Set<String>> candidateSettings = settings(candidate);
        Set<String> names = new LinkedHashSet<>(baselineSettings.keySet());
        names.addAll(candidateSettings.keySet());
        List<SettingDifference> differences = new ArrayList<>();
        for (String name : names) {
            Set<String> b = baselineSettings.getOrDefault(name, Set.of("(not set)"));
            Set<String> c = candidateSettings.getOrDefault(name, Set.of("(not set)"));
            if (!b.equals(c)) {
                differences.add(new SettingDifference(name, b, c));
            }
        }
        return differences;
    }

    // The distinct values of each setting across the runs of one side
    private static Map<String, Set<String>> settings(List<Run> runs) {
        Map<String, Set<String>> settings = new LinkedHashMap<>();
        for (Run run : runs) {
            settings(run)
                    .forEach(
                            (name, value) -> settings.computeIfAbsent(name, key -> new TreeSet<>()).add(value));
        }
        return settings;
    }

    private static Map<String, String> settings(Run run) {
        TestResult result = run.result();
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("driver", String.valueOf(result.driver));
        ResultMetadata metadata = result.metadata;
        if (metadata != null) {
            settings.put("workerCount", String.valueOf(metadata.workerCount()));
            settings.put("jdk", String.valueOf(metadata.jdk()));
            settings.put("ombVersion", String.valueOf(metadata.ombVersion()));
            settings.put("gitCommit", String.valueOf(metadata.gitCommit()));
            Map<String, String> flattened = new TreeMap<>();
            flatten("driverConfig", metadata.driverConfig(), flattened);
            settings.putAll(flattened);
        }
        return settings;
    }

    /**
     * Flattens nested maps into dotted keys, such as {@code driverConfig.producerConfig.acks}.
     *
     * @param prefix the key prefix of the map's entries
     * @param map the map to flatten
     * @param into receives the flattened entries
     */
    static void flatten(String prefix, Map<?, ?> map, Map<String, String> into) {
        map.forEach((key, value) -> {
            String name = prefix + "." + key;
            if (value instanceof Map<?, ?> nested) {
                flatten(name, nested, into);
            } else {
                into.put(name, String.valueOf(value));
            }
        });
    }

    /**
     * Joins the runs of both sides.
     *
     * @param baseline the baseline runs
     * @param candidate the candidate runs
     * @return all runs, baseline first
     */
    static List<Run> all(List<Run> baseline, List<Run> candidate) {
        return Stream.concat(baseline.stream(), candidate.stream()).toList();
    }
}
