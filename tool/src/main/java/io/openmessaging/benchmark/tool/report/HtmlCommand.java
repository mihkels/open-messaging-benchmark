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
package io.openmessaging.benchmark.tool.report;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import io.openmessaging.benchmark.HtmlReport;
import io.openmessaging.benchmark.TestResult;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;

/** {@code benchmark-report html}: renders result files as one HTML page with charts. */
@Parameters(
        commandNames = "html",
        commandDescription = "Render one or more result files as an HTML report with charts")
final class HtmlCommand {
    @Parameter(description = "RESULT...")
    List<Path> files = new ArrayList<>();

    @Parameter(
            names = {"-o", "--output"},
            description = "Write the report to this file instead of stdout")
    @Nullable Path output;

    @Parameter(
            names = {"-h", "--help"},
            description = "Help message",
            help = true)
    boolean help;

    /**
     * Renders the report.
     *
     * @param out receives the report unless --output is given
     * @param err receives errors
     * @return 0 on success, 1 on bad input or a failed write
     * @throws ParameterException if no result file is given
     */
    int execute(PrintStream out, PrintStream err) {
        if (files.isEmpty()) {
            throw new ParameterException("Expected at least one result file");
        }
        Map<String, TestResult> runs = new LinkedHashMap<>();
        try {
            for (Path file : files) {
                Run run = Run.read(file);
                // Same file name from different directories: label by path instead
                runs.put(runs.containsKey(run.name()) ? file.toString() : run.name(), run.result());
            }
        } catch (JacksonException e) {
            err.println("Cannot read result file: " + e.getOriginalMessage());
            return 1;
        }
        String rendered = HtmlReport.render(runs);
        Path target = output;
        if (target == null) {
            out.print(rendered);
            return 0;
        }
        try {
            Files.writeString(target, rendered, StandardCharsets.UTF_8);
            return 0;
        } catch (IOException e) {
            err.println("Cannot write " + target + ": " + e.getMessage());
            return 1;
        }
    }
}
