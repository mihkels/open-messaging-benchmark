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
package io.openmessaging.benchmark.e2e;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;

final class ArtifactResolver {
    private ArtifactResolver() {}

    static synchronized Path resolve(String name) throws Exception {
        Path root = Path.of(System.getProperty("omb.home"));
        Path manifest = root.resolve("e2e-tests/compose/artifacts.txt");
        List<String> rows = Files.readAllLines(manifest);
        for (String row : rows) {
            if (row.isBlank() || row.stripLeading().startsWith("#")) {
                continue;
            }
            String[] fields = row.split("\\|", -1);
            if (fields.length != 3) {
                throw new IllegalStateException("Invalid artifact manifest row: " + row);
            }
            if (fields[0].trim().equals(name)) {
                URI uri = URI.create(fields[1].trim());
                String checksum = fields[2].trim();
                String filename = Path.of(uri.getPath()).getFileName().toString();
                Path cache = root.resolve("e2e-tests/target/testcontainers-artifacts");
                Files.createDirectories(cache);
                Path artifact = cache.resolve(checksum + "-" + filename);
                if (!Files.isRegularFile(artifact)) {
                    download(uri, artifact, checksum);
                }
                verify(artifact, checksum);
                return artifact;
            }
        }
        throw new IllegalArgumentException("Artifact is not listed in " + manifest + ": " + name);
    }

    private static void download(URI uri, Path target, String checksum) throws Exception {
        Path temporary = Files.createTempFile(target.getParent(), "artifact-", ".download");
        try {
            try (HttpClient client =
                    HttpClient.newBuilder()
                            .followRedirects(HttpClient.Redirect.NORMAL)
                            .connectTimeout(Duration.ofSeconds(30))
                            .build()) {
                HttpRequest request =
                        HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(5)).GET().build();
                HttpResponse<InputStream> response =
                        client.send(request, HttpResponse.BodyHandlers.ofInputStream());
                try (InputStream input = response.body()) {
                    if (response.statusCode() != 200) {
                        throw new IllegalStateException(
                                "Downloading " + uri + " failed with HTTP " + response.statusCode());
                    }
                    Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            verify(temporary, checksum);
            Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    static void verify(Path artifact, String expected) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = new DigestInputStream(Files.newInputStream(artifact), digest)) {
            input.transferTo(OutputStream.nullOutputStream());
        }
        String actual = HexFormat.of().formatHex(digest.digest());
        if (!actual.equalsIgnoreCase(expected)) {
            throw new IllegalStateException(
                    "SHA-256 mismatch for " + artifact + ": expected " + expected + ", got " + actual);
        }
    }
}
