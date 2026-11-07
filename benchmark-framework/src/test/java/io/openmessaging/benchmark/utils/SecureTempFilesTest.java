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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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

    @Test
    void posixFileSystemGetsOwnerOnlyPermissions() throws IOException {
        FileSystem fileSystem = fileSystemWithViews(Set.of("basic", "posix"));

        assertThat(SecureTempFiles.ownerOnly(fileSystem, false).value())
                .isEqualTo(PosixFilePermissions.fromString("rw-------"));
        assertThat(SecureTempFiles.ownerOnly(fileSystem, true).value())
                .isEqualTo(PosixFilePermissions.fromString("rwx------"));
        assertThat(SecureTempFiles.ownerOnly(fileSystem, true).name()).isEqualTo("posix:permissions");
    }

    @Test
    void aclFileSystemGrantsAccessToOwnerOnly() throws IOException {
        FileSystem fileSystem = fileSystemWithViews(Set.of("basic", "acl", "owner"));
        UserPrincipalLookupService lookup = mock(UserPrincipalLookupService.class);
        UserPrincipal owner = mock(UserPrincipal.class);
        when(fileSystem.getUserPrincipalLookupService()).thenReturn(lookup);
        when(lookup.lookupPrincipalByName(System.getProperty("user.name"))).thenReturn(owner);

        FileAttribute<?> attribute = SecureTempFiles.ownerOnly(fileSystem, false);

        assertThat(attribute.name()).isEqualTo("acl:acl");
        assertThat(attribute.value())
                .isEqualTo(
                        List.of(
                                AclEntry.newBuilder()
                                        .setType(AclEntryType.ALLOW)
                                        .setPrincipal(owner)
                                        .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                                        .build()));
    }

    @Test
    void unsupportedFileSystemIsRejected() {
        FileSystem fileSystem = fileSystemWithViews(Set.of("basic"));

        assertThatThrownBy(() -> SecureTempFiles.ownerOnly(fileSystem, false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("neither POSIX permissions nor ACLs");
    }

    private static FileSystem fileSystemWithViews(Set<String> views) {
        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.supportedFileAttributeViews()).thenReturn(views);
        return fileSystem;
    }

    private static void assumePosix() {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"));
    }
}
