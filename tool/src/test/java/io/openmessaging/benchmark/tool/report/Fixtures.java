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

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.HdrHistogram.Histogram;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

/** The result files under {@code src/test/resources/report}. */
final class Fixtures {
    static final ObjectMapper MAPPER = new ObjectMapper();

    private Fixtures() {}

    static Path path(String name) {
        URL url = Fixtures.class.getResource("/report/" + name);
        if (url == null) {
            throw new IllegalArgumentException("No fixture " + name);
        }
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    static Run run(String name) {
        return Run.read(path(name));
    }

    static List<Run> runs(String prefix) {
        return IntStream.rangeClosed(1, 3).mapToObj(i -> run(prefix + "-" + i + ".json")).toList();
    }

    static String[] files(String prefix) {
        return IntStream.rangeClosed(1, 3)
                .mapToObj(i -> path(prefix + "-" + i + ".json").toString())
                .toArray(String[]::new);
    }

    /**
     * Writes a copy of a fixture with raw histograms whose values, in microseconds, are the given
     * values scaled by {@code factor}.
     *
     * @param name the fixture to copy
     * @param dir the directory to write the copy to
     * @param factor the scale of the recorded values
     * @return the written copy
     */
    static Path withHistograms(String name, Path dir, double factor) {
        ObjectNode tree = (ObjectNode) MAPPER.readTree(path(name).toFile());
        ObjectNode histograms = tree.putObject("histograms");
        for (String key : MetricExtractor.HISTOGRAMS) {
            Histogram histogram = new Histogram(3);
            for (int value = 1; value <= 1000; value++) {
                histogram.recordValue(Math.round(value * 10 * factor));
            }
            histograms.put(key, encode(histogram));
        }
        Path file = dir.resolve("histograms-" + name);
        MAPPER.writeValue(file.toFile(), tree);
        return file;
    }

    /**
     * Writes a copy of a fixture with the given histogram strings.
     *
     * @param name the fixture to copy
     * @param dir the directory to write the copy to
     * @param encoded the histogram strings by key
     * @return the written copy
     */
    static Path withHistogramStrings(String name, Path dir, Map<String, String> encoded) {
        ObjectNode tree = (ObjectNode) MAPPER.readTree(path(name).toFile());
        ObjectNode histograms = tree.putObject("histograms");
        encoded.forEach(histograms::put);
        Path file = dir.resolve("custom-" + name);
        MAPPER.writeValue(file.toFile(), tree);
        return file;
    }

    static String encode(Histogram histogram) {
        ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        histogram.encodeIntoCompressedByteBuffer(buffer);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }
}
