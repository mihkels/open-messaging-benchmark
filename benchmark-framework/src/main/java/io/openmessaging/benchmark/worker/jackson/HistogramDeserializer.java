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
package io.openmessaging.benchmark.worker.jackson;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.deser.std.StdDeserializer;

public class HistogramDeserializer extends StdDeserializer<Histogram> {

    public HistogramDeserializer() {
        super(Histogram.class);
    }

    @Override
    public Histogram deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws JacksonException {
        byte[] encodedHistogram = jsonParser.getBinaryValue();
        ByteBuffer buffer = ByteBuffer.wrap(encodedHistogram);
        try {
            // Long.MIN_VALUE used so that Histogram will defer to the value encoded in the histogram
            // value. This assumes that it is acceptable for the deserialized value we create to
            // share the same parameters of the source histogram that was serialized.
            return Histogram.decodeFromCompressedByteBuffer(buffer, Long.MIN_VALUE);
        } catch (Exception e) {
            byte[] bytes = new byte[Math.min(encodedHistogram.length, 4096)];
            ByteBuffer content = ByteBuffer.wrap(encodedHistogram);
            content.get(bytes);
            log.error(
                    "Failed to decode publish delay latency ({} bytes): {}",
                    encodedHistogram.length,
                    HexFormat.ofDelimiter(" ").formatHex(bytes));
            throw new RuntimeException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HistogramDeserializer.class);
}
