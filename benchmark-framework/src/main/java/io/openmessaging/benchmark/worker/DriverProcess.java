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
package io.openmessaging.benchmark.worker;

import io.openmessaging.benchmark.driver.Failures;
import io.openmessaging.benchmark.utils.SecureTempFiles;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One driver JVM started by {@link ProcessWorker}, together with its work directory, HTTP endpoint
 * and client, which all share its lifetime. Its fields are volatile because the JVM shutdown hook
 * reads them without the worker's lock while {@link #start} may still be running.
 */
final class DriverProcess {
    private static final Logger log = LoggerFactory.getLogger(DriverProcess.class);

    private static final List<String> FORWARDED_PROPERTIES =
            List.of("log4j.configurationFile", "log4j2.configurationFile", "zone.id");
    private static final String LOG_NAME_PROPERTY = "omb.log.name";
    private static final String DRIVER_PROCESS = "Driver process ";
    private static final String DRIVER_LOG_NAME = "benchmark-driver";

    // Places the driver log next to the supervisor log, so a supervisor started with a log directory
    // (OMB_LOG_DIR) does not make the driver write into a read-only working directory.
    static String driverLogName(String supervisorLogName) {
        if (supervisorLogName == null) {
            return DRIVER_LOG_NAME;
        }
        Path directory = Path.of(supervisorLogName).getParent();
        return directory == null ? DRIVER_LOG_NAME : directory.resolve(DRIVER_LOG_NAME).toString();
    }

    private final String description;
    private volatile Path workDir;
    // Never cleared once set, so that a call failing during a concurrent stop can still report the
    // exit; processStopped records that the process no longer needs stopping.
    private volatile Process process;
    private volatile boolean processStopped;
    private volatile String endpoint;
    private volatile HttpWorkerClient client;
    private volatile boolean crashReported;

    DriverProcess(String description) {
        this.description = description;
    }

    DriverProcess(String description, Process process) {
        this(description);
        this.process = process;
    }

    // The process, unless it has been stopped
    private Process owned() {
        return processStopped ? null : process;
    }

    String description() {
        return description;
    }

    /**
     * Starts the JVM and waits until it serves the worker API.
     *
     * @param classpath the driver process classpath
     * @param closed whether the owning worker was closed meanwhile, in which case the process is told
     *     to exit
     * @throws IOException if the work directory or argument file cannot be written
     * @throws InterruptedException if interrupted while waiting for the process
     */
    void start(List<Path> classpath, BooleanSupplier closed)
            throws IOException, InterruptedException {
        workDir = SecureTempFiles.createDirectory("omb-driver-");
        Path portFile = workDir.resolve("port");
        Path argFile = workDir.resolve("java.args");

        List<String> jvmArgs = new ArrayList<>();
        for (String property : FORWARDED_PROPERTIES) {
            String value = System.getProperty(property);
            if (value != null) {
                jvmArgs.add("-D" + property + "=" + value);
            }
        }
        jvmArgs.addAll(JvmOptions.configured());
        jvmArgs.add(
                "-D" + LOG_NAME_PROPERTY + "=" + driverLogName(System.getProperty(LOG_NAME_PROPERTY)));
        jvmArgs.add("-cp");
        jvmArgs.add(
                classpath.stream().map(Path::toString).collect(Collectors.joining(File.pathSeparator)));
        jvmArgs.add(DriverProcessMain.class.getName());
        jvmArgs.add("--port-file");
        jvmArgs.add(portFile.toString());
        // An argument file keeps long classpaths clear of command line length limits
        Files.write(
                argFile,
                jvmArgs.stream().map(JvmOptions::quoteForArgumentFile).toList(),
                StandardCharsets.UTF_8);

        Path java = Path.of(System.getProperty("java.home"), "bin", "java");
        ProcessBuilder builder =
                new ProcessBuilder(java.toString(), "@" + argFile)
                        .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                        .redirectError(ProcessBuilder.Redirect.INHERIT)
                        .redirectInput(ProcessBuilder.Redirect.PIPE);
        builder.environment().remove("CLASSPATH");
        log.debug("Driver process classpath: {}", classpath);
        Process started = builder.start();
        process = started;
        if (closed.getAsBoolean()) {
            started.getOutputStream().close();
            throw new IllegalStateException("Cannot initialize a closed ProcessWorker");
        }

        long timeoutSeconds = startupTimeoutSeconds();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (!Files.isRegularFile(portFile)) {
            if (!started.isAlive()) {
                throw new IllegalStateException(
                        "Driver process for "
                                + description
                                + " exited during startup with code "
                                + started.exitValue()
                                + "; see its output above");
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(
                        "Driver process for " + description + " did not start within " + timeoutSeconds + "s");
            }
            Thread.sleep(50);
        }
        int port = Integer.parseInt(Files.readString(portFile, StandardCharsets.US_ASCII).trim());
        endpoint = "http://127.0.0.1:" + port;
        client = new HttpWorkerClient(endpoint);
        log.info("Driver process pid {} is serving {} on {}", started.pid(), description, endpoint);
    }

    private static long startupTimeoutSeconds() {
        return Long.getLong(ProcessWorker.STARTUP_TIMEOUT_PROPERTY, 120);
    }

    HttpWorkerClient client() {
        return client;
    }

    String endpoint() {
        return endpoint;
    }

    Long pid() {
        Process current = owned();
        return current == null ? null : current.pid();
    }

    boolean isRunning() {
        Process current = owned();
        return client != null && current != null && current.isAlive();
    }

    boolean hasExited() {
        Process current = process;
        return current != null && !current.isAlive();
    }

    /**
     * Waits briefly for the process to exit, since a failed call is often the first sign of a crash.
     *
     * @param timeout how long to wait
     * @return true if the process has exited
     */
    boolean awaitExit(Duration timeout) {
        Process current = process;
        try {
            return current.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return !current.isAlive();
        }
    }

    /**
     * Describes a process that has exited, remembering that its crash has been reported.
     *
     * @param cause the failure that revealed the crash, or {@code null}
     * @return the failure to throw
     */
    IllegalStateException crashed(Throwable cause) {
        crashReported = true;
        return new IllegalStateException(
                "Driver process for " + description + " exited with code " + process.exitValue(), cause);
    }

    boolean crashReported() {
        return crashReported;
    }

    String describeFailure(Throwable e) {
        Throwable cause = e;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage() == null ? cause.toString() : cause.getMessage();
        if (hasExited()) {
            message += " (driver process exited with code " + process.exitValue() + ")";
        }
        return message;
    }

    /**
     * Closes the process's standard input, which tells the driver process to exit.
     *
     * @param failures collects a failure to close the input
     */
    void closeInput(Failures failures) {
        Process current = owned();
        if (current != null) {
            failures.close(current.getOutputStream());
        }
    }

    /**
     * Closes the client, stops the process and deletes the work directory. Whatever cannot be
     * released is kept, so that a later call can retry it.
     *
     * @param failures collects the failures of every step
     */
    void stop(Failures failures) {
        HttpWorkerClient closingClient = client;
        if (closingClient != null) {
            client = null;
            // The client bounds its own close; clear the interrupt it may restore so the steps below run
            failures.close(closingClient);
            failures.deferInterrupt("Interrupted while closing the driver HTTP client");
        }
        Process closing = owned();
        if (closing != null) {
            stopProcess(closing, failures);
        }
        endpoint = null;
        if (workDir != null && owned() == null) {
            try {
                deleteWorkDir(workDir);
                workDir = null;
            } catch (IOException e) {
                failures.add(e);
            }
        }
    }

    private void stopProcess(Process closing, Failures failures) {
        boolean wasAlive = closing.isAlive();
        failures.close(closing.getOutputStream());
        try {
            if (!closing.waitFor(10, TimeUnit.SECONDS)) {
                failures.add(new TimeoutException(DRIVER_PROCESS + closing.pid() + " did not stop"));
                closing.destroy();
                if (!closing.waitFor(5, TimeUnit.SECONDS)) {
                    closing.destroyForcibly();
                    if (!closing.waitFor(5, TimeUnit.SECONDS)) {
                        failures.add(
                                new TimeoutException(
                                        DRIVER_PROCESS + closing.pid() + " did not exit after being killed"));
                    }
                }
            }
        } catch (InterruptedException e) {
            failures.addInterruption(e);
            closing.destroyForcibly();
            try {
                if (!closing.waitFor(5, TimeUnit.SECONDS)) {
                    failures.add(
                            new TimeoutException("Interrupted driver process did not exit after being killed"));
                }
            } catch (InterruptedException second) {
                failures.addInterruption(second);
            }
        }
        if (!closing.isAlive()) {
            if (wasAlive && closing.exitValue() != 0) {
                failures.add(
                        new IllegalStateException(
                                DRIVER_PROCESS + closing.pid() + " exited with code " + closing.exitValue()));
            }
            processStopped = true;
        }
    }

    boolean isStopped() {
        return owned() == null && client == null && workDir == null;
    }

    /** Stops the process on JVM shutdown without throwing, since nobody could handle the failure. */
    void stopOnShutdown() {
        Process current = owned();
        Path directory = workDir;
        if (current == null) {
            if (directory != null) {
                deleteWorkDirQuietly(directory);
            }
            return;
        }
        try {
            current.getOutputStream().close();
        } catch (IOException e) {
            log.debug("Failed to close driver process input", e);
        }
        try {
            if (!current.waitFor(5, TimeUnit.SECONDS)) {
                log.warn("Driver process {} did not stop during shutdown, killing it", current.pid());
                current.destroyForcibly();
                if (!current.waitFor(5, TimeUnit.SECONDS)) {
                    log.error("Driver process {} did not exit after being killed", current.pid());
                }
            }
        } catch (InterruptedException e) {
            current.destroyForcibly();
            Thread.currentThread().interrupt();
        }
        if (!current.isAlive() && directory != null) {
            deleteWorkDirQuietly(directory);
        }
    }

    private static void deleteWorkDir(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        IOException failure = null;
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path path : files.sorted(Comparator.reverseOrder()).toList()) {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void deleteWorkDirQuietly(Path directory) {
        try {
            deleteWorkDir(directory);
        } catch (IOException e) {
            log.warn("Failed to delete driver work directory {}", directory, e);
        }
    }
}
