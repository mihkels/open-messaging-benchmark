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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResultsToCsvTest {

    @TempDir Path tempDir;

    @Test
    void writesSortedResultsFromJsonFiles() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("results"));
        String golden = resource("golden/test-result.json");
        Files.writeString(results.resolve("test-result.json"), withRates(golden, 100, 200, 80, 120));
        Files.writeString(
                results.resolve("smaller-result.json"),
                withRates(golden.replace("\"messageSize\" : 1024", "\"messageSize\" : 512"), 10, 20, 8, 12));
        Files.writeString(results.resolve("ignored.txt"), "not JSON");

        Path csv = tempDir.resolve("results.csv");
        new ResultsToCsv().writeAllResultFiles(results.toString(), csv.toFile());

        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(3);
        assertThat(lines.get(0))
                .isEqualTo(
                        "topics,partitions,message-size,producers-per-topic,consumers-per-topic,"
                                + "prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,"
                                + "con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max,");
        assertThat(lines.get(1)).startsWith("2,3,512,1,2,");
        assertThat(lines.get(1).split(",")).hasSize(13);
        assertThat(lines.get(2)).startsWith("2,3,1024,1,2,");
        assertThat(lines.get(2).split(",")).hasSize(13);
    }

    @Test
    void writesHeaderForEmptyDirectory() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("empty"));
        Path csv = tempDir.resolve("empty.csv");

        new ResultsToCsv().writeAllResultFiles(results.toString(), csv.toFile());

        var lines = Files.readAllLines(csv);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).startsWith("topics,partitions,");
    }

    @Test
    void rejectsMalformedInputJsonWithoutWritingCsv() throws Exception {
        Path results = Files.createDirectory(tempDir.resolve("malformed"));
        Files.writeString(results.resolve("bad.json"), "{");
        Path csv = tempDir.resolve("malformed.csv");

        assertThatThrownBy(() -> new ResultsToCsv().writeAllResultFiles(results.toString(), csv.toFile()))
                .isInstanceOf(RuntimeException.class);

        assertThat(csv).doesNotExist();
    }

    private static String resource(String name) throws Exception {
        try (var stream = ResultsToCsvTest.class.getClassLoader().getResourceAsStream(name)) {
            return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    private static String withRates(
            String json, double publishRate1, double publishRate2, double consumeRate1, double consumeRate2) {
        int closingBrace = json.lastIndexOf('}');
        return json.substring(0, closingBrace)
                + ",\"publishRate\":["
                + publishRate1
                + ","
                + publishRate2
                + "],\"consumeRate\":["
                + consumeRate1
                + ","
                + consumeRate2
                + "]"
                + json.substring(closingBrace);
    }
}
