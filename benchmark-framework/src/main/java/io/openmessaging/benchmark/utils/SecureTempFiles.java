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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Creates temporary files and directories that only the current user can access.
 *
 * <p>The owner-only permissions are applied atomically at creation: as POSIX permissions where the
 * default file system supports them, otherwise as an ACL granting access to the owner alone (e.g.
 * on Windows).
 */
public final class SecureTempFiles {
    private SecureTempFiles() {}

    /**
     * Creates a temporary file readable and writable only by its owner.
     *
     * @param prefix the file name prefix
     * @param suffix the file name suffix
     * @return the created file
     * @throws IOException if the file cannot be created, or the file system supports neither POSIX
     *     permissions nor ACLs
     */
    public static Path createFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(prefix, suffix, ownerOnly(FileSystems.getDefault(), false));
    }

    /**
     * Creates a temporary directory accessible only by its owner.
     *
     * @param prefix the directory name prefix
     * @return the created directory
     * @throws IOException if the directory cannot be created, or the file system supports neither
     *     POSIX permissions nor ACLs
     */
    public static Path createDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix, ownerOnly(FileSystems.getDefault(), true));
    }

    // Returns either a POSIX or an ACL attribute; the wildcard is inherent
    @SuppressWarnings("java:S1452")
    static FileAttribute<?> ownerOnly(FileSystem fileSystem, boolean directory) throws IOException {
        Set<String> views = fileSystem.supportedFileAttributeViews();
        if (views.contains("posix")) {
            return PosixFilePermissions.asFileAttribute(
                    PosixFilePermissions.fromString(directory ? "rwx------" : "rw-------"));
        }
        if (views.contains("acl")) {
            UserPrincipal owner =
                    fileSystem
                            .getUserPrincipalLookupService()
                            .lookupPrincipalByName(System.getProperty("user.name"));
            return ownerOnlyAcl(owner);
        }
        throw new IOException(
                "Cannot create owner-only temporary files: the file system supports neither POSIX"
                        + " permissions nor ACLs");
    }

    static FileAttribute<List<AclEntry>> ownerOnlyAcl(UserPrincipal owner) {
        List<AclEntry> acl =
                List.of(
                        AclEntry.newBuilder()
                                .setType(AclEntryType.ALLOW)
                                .setPrincipal(owner)
                                .setPermissions(EnumSet.allOf(AclEntryPermission.class))
                                .build());
        return new FileAttribute<>() {
            @Override
            public String name() {
                return "acl:acl";
            }

            @Override
            public List<AclEntry> value() {
                return acl;
            }
        };
    }
}
