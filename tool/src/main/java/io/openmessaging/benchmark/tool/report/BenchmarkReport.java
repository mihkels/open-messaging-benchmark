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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.PrintStream;

/** Entry point of {@code bin/benchmark-report}, which reports on benchmark result files. */
public final class BenchmarkReport {
    static final String PROGRAM = "benchmark-report";

    private BenchmarkReport() {}

    static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Parses the arguments and runs the chosen command.
     *
     * @param args the command-line arguments
     * @param out the standard output
     * @param err the standard error
     * @return the process exit code
     */
    static int run(String[] args, PrintStream out, PrintStream err) {
        Options options = new Options();
        DiffCommand diff = new DiffCommand();
        JCommander jc =
                JCommander.newBuilder().programName(PROGRAM).addObject(options).addCommand(diff).build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            err.println("Error: " + e.getMessage());
            err.print(usage(jc));
            return 1;
        }

        String command = jc.getParsedCommand();
        if (options.help || command == null) {
            (options.help ? out : err).print(usage(jc));
            return options.help ? 0 : 1;
        }
        if (diff.help) {
            out.print(usage(jc, command));
            return 0;
        }
        try {
            return diff.execute(out, err);
        } catch (ParameterException e) {
            err.println("Error: " + e.getMessage());
            err.print(usage(jc, command));
            return 1;
        }
    }

    private static String usage(JCommander jc) {
        StringBuilder usage = new StringBuilder();
        jc.getUsageFormatter().usage(usage);
        return usage.toString();
    }

    private static String usage(JCommander jc, String command) {
        StringBuilder usage = new StringBuilder();
        jc.getUsageFormatter().usage(command, usage);
        return usage.toString();
    }

    static final class Options {
        @Parameter(
                names = {"-h", "--help"},
                description = "Help message",
                help = true)
        boolean help;
    }
}
