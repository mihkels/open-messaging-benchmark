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
package io.openmessaging.benchmark.utils;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Child-first classloader that isolates driver dependencies from the main classpath. Use this to
 * load drivers with conflicting transitive dependencies.
 */
public final class IsolatedDriverLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(IsolatedDriverLoader.class);

    private IsolatedDriverLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        // Always delegate java.*, javax.*, and our driver-api to parent
        if (name.startsWith("java.")
                || name.startsWith("javax.")
                || name.startsWith("io.openmessaging.benchmark.driver.")) {
            return super.loadClass(name, resolve);
        }

        // Child-first for everything else
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    c = super.loadClass(name, resolve);
                }
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

    /**
     * Create an isolated classloader for the given driver home.
     *
     * @param driverName The driver name that will be loaded by the isolated classloader.
     * @return The classloader for the driver.
     * @throws Exception If the driver name is invalid.
     */
    public static ClassLoader forDriverFolder(Path driverName) throws Exception {
        List<URL> urls = new ArrayList<>();

        Path fileName = driverName.getFileName();
        if (Files.isRegularFile(driverName)
                && fileName != null
                && fileName.toString().endsWith(".jar")) {
            // Single jar
            urls.add(driverName.toUri().toURL());
        } else if (Files.isDirectory(driverName)) {
            // Maven module layout: target/classes or a distribution folder
            Path classesDir = driverName.resolve("target/classes");
            if (Files.exists(classesDir)) {
                urls.add(classesDir.toUri().toURL());
            }

            // Add all jars in root
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(driverName, "*.jar")) {
                for (Path p : stream) {
                    urls.add(p.toUri().toURL());
                }
            }

            // Add all jars in lib/
            Path libDir = driverName.resolve("lib");
            if (Files.exists(libDir) && Files.isDirectory(libDir)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(libDir, "*.jar")) {
                    for (Path p : stream) {
                        urls.add(p.toUri().toURL());
                    }
                }
            }
        }

        if (urls.isEmpty()) {
            throw new IllegalArgumentException(
                    "No jars found in driver home: " + driverName.toAbsolutePath());
        }

        log.info("Creating isolated classloader with {} URLs for {}", urls.size(), driverName);
        return new IsolatedDriverLoader(
                urls.toArray(new URL[0]), IsolatedDriverLoader.class.getClassLoader());
    }

    /**
     * Reflectively instantiate a class from an isolated classloader.
     *
     * @param cl The classloader to use.
     * @param className The class name to instantiate of the isolated benchmark driver.
     * @param <T> The type of the class to instantiate.
     * @return The instantiated class.
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(ClassLoader cl, String className) throws Exception {
        Class<?> cls = Class.forName(className, true, cl);
        Constructor<?> ctor = cls.getDeclaredConstructor();
        ctor.setAccessible(true);
        return (T) ctor.newInstance();
    }
}
