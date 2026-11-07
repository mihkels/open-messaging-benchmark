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

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * Creates temporary files and directories that only the current user can access.
 *
 * <p>On POSIX file systems the owner-only permissions are applied atomically at creation. Elsewhere
 * the entry is created first and then restricted to its owner.
 */
public final class SecureTempFiles {
    private static final boolean POSIX =
            FileSystems.getDefault().supportedFileAttributeViews().contains("posix");

    private SecureTempFiles() {}

    /**
     * Creates a temporary file readable and writable only by its owner ({@code rw-------}).
     *
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @return the created file
     * @throws IOException if the file cannot be created or restricted
     */
    public static Path createFile(String prefix, String suffix) throws IOException {
        if (POSIX) {
            return Files.createTempFile(
                    prefix,
                    suffix,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
        }
        File file = File.createTempFile(prefix, suffix);
        restrictToOwner(file, false);
        return file.toPath();
    }

    /**
     * Creates a temporary directory accessible only by its owner ({@code rwx------}).
     *
     * @param prefix the directory name prefix
     * @return the created directory
     * @throws IOException if the directory cannot be created or restricted
     */
    public static Path createDirectory(String prefix) throws IOException {
        if (POSIX) {
            return Files.createTempDirectory(
                    prefix,
                    PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        }
        File directory = Files.createTempDirectory(prefix).toFile();
        restrictToOwner(directory, true);
        return directory.toPath();
    }

    private static void restrictToOwner(File file, boolean executable) throws IOException {
        boolean restricted =
                file.setReadable(true, true)
                        && file.setWritable(true, true)
                        && file.setExecutable(executable, true);
        if (!restricted) {
            Files.deleteIfExists(file.toPath());
            throw new IOException("Cannot restrict permissions of " + file);
        }
    }
}
