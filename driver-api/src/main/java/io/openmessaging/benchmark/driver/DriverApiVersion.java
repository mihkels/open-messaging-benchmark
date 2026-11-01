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
package io.openmessaging.benchmark.driver;

/**
 * Binary compatibility contract of the driver plugin SPI ({@link BenchmarkDriver}, {@link
 * BenchmarkProducer}, {@link BenchmarkConsumer} and {@link ConsumerCallback}).
 *
 * <p>Driver plugin bundles declare the contract they were built against in the {@code
 * driverApiVersion} field of their {@code plugin.yaml}. The benchmark host only launches bundles
 * declaring the contract it implements. Incompatible SPI changes normally increment the value,
 * independently of the project version.
 *
 * <p>As a one-time exception, the change from {@code PrometheusMeterRegistry} to {@code
 * MeterRegistry} in {@link BenchmarkDriver#initialize} keeps version {@code 1}. Older version-1
 * bundles are binary-incompatible even though their descriptor still matches, so this check cannot
 * detect them. Rebuild the host and all plugin bundles together against the changed driver API.
 */
public final class DriverApiVersion {

    /** The driver SPI contract implemented by this version of driver-api. */
    public static final int CURRENT = 1;

    private DriverApiVersion() {}
}
