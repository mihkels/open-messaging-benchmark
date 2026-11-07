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
package io.openmessaging.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResultMetadataGitLookupTest {
    @TempDir Path directory;

    @Test
    void returnsFirstExecutableCandidate() throws IOException {
        Path missing = directory.resolve("missing");
        Path notExecutable = Files.createFile(directory.resolve("plain"));
        Path executable = Files.createFile(directory.resolve("git"));
        assumeTrue(executable.toFile().setExecutable(true, true));
        assumeTrue(!Files.isExecutable(notExecutable));

        assertThat(ResultMetadata.findExecutable(List.of(missing, notExecutable, executable)))
                .contains(executable);
    }

    @Test
    void gitRevisionIsUnknownWithoutCandidates() {
        assertThat(ResultMetadata.gitRevision(List.of(directory.resolve("missing"))))
                .isEqualTo("unknown");
    }

    @Test
    void gitRevisionReadsCandidateOutput() throws IOException {
        Path git = fakeGit("echo abc123", "fake-git");

        assertThat(ResultMetadata.gitRevision(List.of(git))).isEqualTo("abc123");
    }

    @Test
    void gitRevisionIsUnknownWhenGitFails() throws IOException {
        Path git = fakeGit("echo 'not a git repository'; exit 128", "failing-git");

        assertThat(ResultMetadata.gitRevision(List.of(git))).isEqualTo("unknown");
    }

    @Test
    void gitRevisionIsUnknownWhenGitCannotStart() throws IOException {
        Path git = directory.resolve("unstartable-git");
        Files.writeString(git, "#!" + directory.resolve("missing-interpreter") + "\n");
        assumeTrue(git.toFile().setExecutable(true, true));

        assertThat(ResultMetadata.gitRevision(List.of(git))).isEqualTo("unknown");
    }

    private Path fakeGit(String body, String name) throws IOException {
        Path shell = Path.of("/bin/sh");
        assumeTrue(Files.isExecutable(shell));
        Path git = directory.resolve(name);
        Files.writeString(git, "#!/bin/sh\n" + body + "\n");
        assumeTrue(git.toFile().setExecutable(true, true));
        return git;
    }

    @Test
    void ignoresDirectoriesAndRelativePaths() {
        assertThat(ResultMetadata.findExecutable(List.of(directory, Path.of("git")))).isEmpty();
    }
}
