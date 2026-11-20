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
package io.github.mihkels.omb.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

// Sets the driver JVM options property that concurrent ProcessWorkerTest methods read
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class JvmOptionsTest {

    @Test
    void launcherPassesEveryDefaultToTheDriverJvm() throws Exception {
        // The launcher always sets OMB_DRIVER_JVM_OPTS, so DEFAULTS apply only without it
        String launcher =
                Files.readString(Path.of("../bin/omb-launch.sh"), StandardCharsets.UTF_8)
                        .replace("--add-opens ", "--add-opens=");
        assertThat(launcher).contains(JvmOptions.DEFAULTS);
    }

    @Test
    void parsesQuotedAndEscapedOptions() {
        assertThat(
                        JvmOptions.parse(
                                "-Xmx256m -Da='two words' -Db=three\\ words -Dc=\"say \\\"hi\\\"\""
                                        + " -Dd=C:\\data\\file"))
                .containsExactly(
                        "-Xmx256m", "-Da=two words", "-Db=three words", "-Dc=say \"hi\"", "-Dd=C:\\data\\file");
    }

    @Test
    void rejectsAnUnterminatedQuote() {
        assertThatThrownBy(() -> JvmOptions.parse("-Da=\"unterminated"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unterminated quote");
    }

    @Test
    void readsTheSystemPropertyOrFallsBackToTheDefaults() {
        String old = System.getProperty(JvmOptions.PROPERTY);
        try {
            System.setProperty(JvmOptions.PROPERTY, " ");
            assertThat(JvmOptions.configured()).isEqualTo(JvmOptions.DEFAULTS);

            System.setProperty(JvmOptions.PROPERTY, "-Xmx256m -Dfixture=1");
            assertThat(JvmOptions.configured()).containsExactly("-Xmx256m", "-Dfixture=1");
        } finally {
            if (old == null) {
                System.clearProperty(JvmOptions.PROPERTY);
            } else {
                System.setProperty(JvmOptions.PROPERTY, old);
            }
        }
    }

    @Test
    void quotesArgumentsForAnArgumentFile() {
        assertThat(JvmOptions.quoteForArgumentFile("C:\\dir\\a \"b\""))
                .isEqualTo("\"C:\\\\dir\\\\a \\\"b\\\"\"");
    }

    @Test
    void escapesLineBreaksSoTheLauncherKeepsOneArgument(@TempDir Path dir) throws Exception {
        String forged = "driver=kafka\n--token-file\r\n/etc/passwd";
        assertThat(JvmOptions.quoteForArgumentFile(forged)).doesNotContain("\n", "\r");

        // Run a real launcher over the argfile: the program must see exactly the original argument
        Path echo = dir.resolve("Echo.java");
        Files.writeString(
                echo,
                "public class Echo { public static void main(String[] a) {"
                        + " System.out.print(a.length + \":\" + String.join(\"|\", a)); } }");
        Path argFile = dir.resolve("java.args");
        Files.write(
                argFile,
                List.of(
                        JvmOptions.quoteForArgumentFile(echo.toString()),
                        JvmOptions.quoteForArgumentFile(forged)));
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process process = new ProcessBuilder(java, "@" + argFile).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(process.waitFor()).as(output).isZero();
        assertThat(output).isEqualTo("1:" + forged);
    }

    @Test
    void handlesEscapesInsideAndOutsideQuotes() {
        assertThat(JvmOptions.parse("  -Da='x\\y'  -Db=\"p\\\\q\\r\" -Dc=\\'s\\\" -Dd=end\\"))
                .containsExactly("-Da=x\\y", "-Db=p\\q\\r", "-Dc='s\"", "-Dd=end\\");
        assertThat(JvmOptions.parse(" '' ")).containsExactly("");
        assertThat(JvmOptions.parse("   ")).isEmpty();
    }
}
