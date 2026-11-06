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
package io.openmessaging.benchmark.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class JvmOptionsTest {

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
    void handlesEscapesInsideAndOutsideQuotes() {
        assertThat(JvmOptions.parse("  -Da='x\\y'  -Db=\"p\\\\q\\r\" -Dc=\\'s\\\" -Dd=end\\"))
                .containsExactly("-Da=x\\y", "-Db=p\\q\\r", "-Dc='s\"", "-Dd=end\\");
        assertThat(JvmOptions.parse(" '' ")).containsExactly("");
        assertThat(JvmOptions.parse("   ")).isEmpty();
    }
}
