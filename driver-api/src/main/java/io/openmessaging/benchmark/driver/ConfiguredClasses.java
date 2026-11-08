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
 * Loads classes named in a driver configuration.
 *
 * <p>Driver configurations reach a worker over its HTTP API, so a class name in one is untrusted
 * input. A class is loaded without being initialized and is checked against the expected type
 * before the caller may instantiate it, so no static initializer or constructor of an unrelated
 * class on the classpath runs.
 */
public final class ConfiguredClasses {
    private ConfiguredClasses() {}

    /**
     * Loads {@code className} without initializing it and checks that it is a subtype of {@code
     * expected}.
     *
     * @param className the fully qualified class name from the configuration
     * @param expected the type the class must extend or implement
     * @param loader the class loader to load the class with
     * @return the loaded class, not yet initialized
     * @param <T> the expected type
     * @throws ClassNotFoundException if the class cannot be found
     * @throws IllegalArgumentException if the class is not a subtype of {@code expected}
     */
    public static <T> Class<? extends T> load(String className, Class<T> expected, ClassLoader loader)
            throws ClassNotFoundException {
        Class<?> loaded = Class.forName(className, false, loader);
        if (!expected.isAssignableFrom(loaded)) {
            throw new IllegalArgumentException(
                    "Class "
                            + className
                            + " configured as a "
                            + expected.getName()
                            + " does not implement it");
        }
        return loaded.asSubclass(expected);
    }
}
