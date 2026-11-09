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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.openmessaging.benchmark.worker.jackson.ObjectMappers;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.ObjectMapper;

public class ResultsToCsv {

    private static final Logger log = LoggerFactory.getLogger(ResultsToCsv.class);
    private static final ObjectMapper MAPPER = ObjectMappers.DEFAULT.mapper();

    public void writeAllResultFiles(String directory) {
        String fileName = "results-" + Instant.now().getEpochSecond() + ".csv";
        String resultsDirectory = System.getenv(Benchmark.RESULTS_DIR_ENV);
        File outputFile =
                resultsDirectory == null || resultsDirectory.isEmpty()
                        ? new File(fileName)
                        : Path.of(resultsDirectory, fileName).toFile();
        writeAllResultFiles(directory, outputFile);
    }

    void writeAllResultFiles(String directory, File outputFile) {
        try {
            File dir = new File(directory);
            File[] directoryListing = dir.listFiles();
            if (directoryListing == null) {
                throw new IllegalArgumentException("Not a directory: " + directory);
            }
            Arrays.sort(directoryListing);

            List<String> lines = new ArrayList<>();
            lines.add(
                    "topics,partitions,message-size,producers-per-topic,consumers-per-topic,"
                            + "prod-rate-min,prod-rate-avg,prod-rate-std-dev,prod-rate-max,"
                            + "con-rate-min,con-rate-avg,con-rate-std-dev,con-rate-max");

            List<TestResult> results = new ArrayList<>();
            for (File file : directoryListing) {
                if (file.isFile() && file.getAbsolutePath().endsWith(".json")) {
                    TestResult tr = MAPPER.readValue(new File(file.getAbsolutePath()), TestResult.class);
                    results.add(tr);
                }
            }

            List<TestResult> sortedResults =
                    results.stream()
                            .sorted(
                                    Comparator.comparingLong(TestResult::getMessageSize)
                                            .thenComparingInt(TestResult::getTopics)
                                            .thenComparingInt(TestResult::getPartitions))
                            .toList();
            for (TestResult tr : sortedResults) {
                lines.add(extractResults(tr));
            }

            try (Writer writer = Files.newBufferedWriter(outputFile.toPath(), UTF_8)) {
                for (String str : lines) {
                    writer.write(str + System.lineSeparator());
                }
                log.info("Results extracted into CSV {}", outputFile);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed creating csv file " + outputFile, e);
        }
    }

    public String extractResults(TestResult tr) {
        return MessageFormat.format(
                        "{0,number,#},{1,number,#},{2,number,#},{3,number,#},{4,number,#},",
                        tr.topics,
                        tr.partitions,
                        tr.messageSize,
                        tr.producersPerTopic,
                        tr.consumersPerTopic)
                + rateStatistics(tr.publishRate)
                + ","
                + rateStatistics(tr.consumeRate);
    }

    // min,avg,std-dev,max of the period rates; empty fields when there are no periods
    private static String rateStatistics(List<Double> rates) {
        DoubleSummaryStatistics stats =
                rates.stream().mapToDouble(Double::doubleValue).summaryStatistics();
        if (stats.getCount() == 0) {
            return ",,,";
        }
        double mean = stats.getAverage();
        double squaredDeviations =
                rates.stream().mapToDouble(rate -> (rate - mean) * (rate - mean)).sum();
        return MessageFormat.format(
                "{0,number,#},{1,number,#},{2,number,#.##},{3,number,#}",
                stats.getMin(),
                mean,
                Math.sqrt(squaredDeviations / stats.getCount()),
                stats.getMax());
    }
}
