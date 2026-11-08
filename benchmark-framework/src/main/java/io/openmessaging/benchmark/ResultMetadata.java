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
package io.openmessaging.benchmark;

import io.openmessaging.benchmark.driver.SecretRedaction;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record ResultMetadata(
        String runId,
        String startedAtUtc,
        String finishedAtUtc,
        String ombVersion,
        String gitCommit,
        String jdk,
        String jvmOptions,
        int workerCount,
        Workload workload,
        Map<String, Object> driverConfig) {
    public ResultMetadata {
        driverConfig = redactDriverConfig(driverConfig);
    }

    @Override
    public Map<String, Object> driverConfig() {
        return redactDriverConfig(driverConfig);
    }

    private static final String UNKNOWN = "unknown";
    private static final Pattern JVM_OPTION = Pattern.compile("^(--?D?)([^=]+)=(.*)$");
    private static final List<Path> GIT_LOCATIONS =
            List.of(
                    Path.of("/usr/bin/git"), Path.of("/usr/local/bin/git"), Path.of("/opt/homebrew/bin/git"));

    static ResultMetadata create(
            String runId,
            Instant startedAt,
            Instant finishedAt,
            int workerCount,
            Workload workload,
            Map<String, Object> driverConfig) {
        String ombVersion =
                System.getProperty(
                        "omb.version",
                        Benchmark.class.getPackage().getImplementationVersion() == null
                                ? UNKNOWN
                                : Benchmark.class.getPackage().getImplementationVersion());
        String jvmOptions =
                String.join(
                        " ",
                        ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                                .map(ResultMetadata::redactJvmOption)
                                .toList());
        return new ResultMetadata(
                runId,
                startedAt.toString(),
                finishedAt.toString(),
                ombVersion,
                resolveGitCommit(),
                System.getProperty("java.version"),
                jvmOptions,
                workerCount,
                workload,
                redactDriverConfig(driverConfig));
    }

    static Map<String, Object> redactDriverConfig(Map<String, Object> driverConfig) {
        return SecretRedaction.redactMap(driverConfig);
    }

    /**
     * Whether a configuration key names a credential; delegates to {@link
     * SecretRedaction#isSecretKey(String)}.
     */
    static boolean isSecretKey(String key) {
        return SecretRedaction.isSecretKey(key);
    }

    private static String resolveGitCommit() {
        String configured =
                System.getProperty(
                        "omb.gitCommit",
                        System.getenv()
                                .getOrDefault("GIT_COMMIT", System.getenv().getOrDefault("GITHUB_SHA", "")));
        if (!configured.isBlank()) {
            return configured;
        }
        return gitRevision(GIT_LOCATIONS);
    }

    /**
     * Runs {@code git rev-parse HEAD} with the first executable of the given candidates.
     *
     * @return the revision, or {@code "unknown"} if no candidate exists or git fails
     */
    static String gitRevision(List<Path> candidates) {
        Optional<Path> git = findExecutable(candidates);
        if (git.isEmpty()) {
            return UNKNOWN;
        }
        try {
            Process process =
                    new ProcessBuilder(git.get().toString(), "rev-parse", "HEAD")
                            .redirectErrorStream(true)
                            .start();
            if (!process.waitFor(2, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                return UNKNOWN;
            }
            String revision =
                    new String(process.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim();
            return process.exitValue() == 0 ? revision : UNKNOWN;
        } catch (IOException _) {
            return UNKNOWN;
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return UNKNOWN;
        }
    }

    /**
     * Returns the first of the given absolute paths that is an executable regular file. Using fixed
     * locations instead of a {@code PATH} lookup keeps a writable {@code PATH} entry from
     * substituting another binary.
     */
    static Optional<Path> findExecutable(List<Path> candidates) {
        return candidates.stream()
                .filter(Path::isAbsolute)
                .filter(Files::isRegularFile)
                .filter(Files::isExecutable)
                .findFirst();
    }

    static String redactJvmOption(String option) {
        Matcher matcher = JVM_OPTION.matcher(option);
        if (!matcher.matches() || !isSecretKey(matcher.group(2))) {
            return option;
        }
        return matcher.group(1) + matcher.group(2) + "=" + SecretRedaction.REDACTED;
    }
}
