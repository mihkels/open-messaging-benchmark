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

import java.time.Duration;
import org.jspecify.annotations.Nullable;

/**
 * How long a worker HTTP request may take. Both sides read the same settings: the client gives up
 * after the timeout, and the worker's HTTP server keeps a busy request open at least that long.
 */
public final class WorkerTimeouts {
    public static final String REQUEST_PROPERTY = "omb.worker.requestTimeoutSeconds";
    public static final String REQUEST_ENV = "OMB_WORKER_REQUEST_TIMEOUT_SECONDS";
    public static final String STOP_PROPERTY = "omb.worker.stopTimeoutSeconds";
    public static final String STOP_ENV = "OMB_WORKER_STOP_TIMEOUT_SECONDS";

    static final Duration DEFAULT_REQUEST = Duration.ofMinutes(10);
    static final Duration DEFAULT_STOP = Duration.ofMinutes(3);

    private WorkerTimeouts() {}

    /**
     * The timeout of every worker request except {@code /stop-all}.
     *
     * @return the {@value #REQUEST_PROPERTY} property, else the {@value #REQUEST_ENV} variable, else
     *     10 minutes
     * @throws IllegalArgumentException if the configured value is not a positive whole number
     */
    public static Duration request() {
        return configured(REQUEST_PROPERTY, REQUEST_ENV, DEFAULT_REQUEST);
    }

    /**
     * The timeout of {@code /stop-all}, which closes every producer and consumer.
     *
     * @return the {@value #STOP_PROPERTY} property, else the {@value #STOP_ENV} variable, else 3
     *     minutes
     * @throws IllegalArgumentException if the configured value is not a positive whole number
     */
    public static Duration stop() {
        return configured(STOP_PROPERTY, STOP_ENV, DEFAULT_STOP);
    }

    /**
     * The idle timeout the worker's HTTP server needs so that it never cuts a request the client is
     * still waiting for.
     *
     * @return the longer of {@link #request()} and {@link #stop()}
     */
    static Duration serverIdle() {
        Duration request = request();
        Duration stop = stop();
        return request.compareTo(stop) >= 0 ? request : stop;
    }

    private static Duration configured(String property, String env, Duration defaultValue) {
        String value = System.getProperty(property);
        return value != null
                ? parse(property, value, defaultValue)
                : parse(env, System.getenv(env), defaultValue);
    }

    static Duration parse(String name, @Nullable String value, Duration defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        long seconds;
        try {
            seconds = Long.parseLong(value.strip());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number of seconds: " + value, e);
        }
        if (seconds <= 0) {
            throw new IllegalArgumentException(name + " must be positive: " + value);
        }
        return Duration.ofSeconds(seconds);
    }
}
