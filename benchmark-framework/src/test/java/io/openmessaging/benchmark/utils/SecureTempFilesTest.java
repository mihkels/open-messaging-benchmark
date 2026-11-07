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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import org.junit.jupiter.api.Test;

class SecureTempFilesTest {

    @Test
    void createFileIsOwnerOnly() throws IOException {
        Path file = SecureTempFiles.createFile("secure-temp-test", ".yaml");
        try {
            assertThat(file).isRegularFile();
            assertThat(file.getFileName().toString()).startsWith("secure-temp-test").endsWith(".yaml");
            assumePosix();
            assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(file)))
                    .isEqualTo("rw-------");
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    void createDirectoryIsOwnerOnly() throws IOException {
        Path directory = SecureTempFiles.createDirectory("secure-temp-test-");
        try {
            assertThat(directory).isDirectory();
            assertThat(directory.getFileName().toString()).startsWith("secure-temp-test-");
            assumePosix();
            assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(directory)))
                    .isEqualTo("rwx------");
        } finally {
            Files.deleteIfExists(directory);
        }
    }

    private static void assumePosix() {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    }
}
