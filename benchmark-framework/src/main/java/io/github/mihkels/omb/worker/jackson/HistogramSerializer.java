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
package io.github.mihkels.omb.worker.jackson;

import java.nio.ByteBuffer;
import org.HdrHistogram.Histogram;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ser.std.StdSerializer;

public class HistogramSerializer extends StdSerializer<Histogram> {
    // Compressed histograms are a few KB; serializeHistogram doubles the buffer when one is larger
    private static final int INITIAL_BUFFER_SIZE = 64 * 1024;

    public HistogramSerializer() {
        super(Histogram.class);
    }

    /**
     * Encodes a histogram in the compressed form the JSON results and worker API carry.
     *
     * @param histogram the histogram
     * @param initialBufferSize the first buffer size to try; larger histograms double it
     * @return the compressed bytes
     */
    public static byte[] encode(Histogram histogram, int initialBufferSize) {
        return toByteArray(serializeHistogram(histogram, ByteBuffer.allocate(initialBufferSize)));
    }

    static byte[] toByteArray(ByteBuffer buffer) {
        byte[] encodedBuffer = new byte[buffer.remaining()];
        buffer.get(encodedBuffer);
        return encodedBuffer;
    }

    static ByteBuffer serializeHistogram(Histogram histo, ByteBuffer buffer) {
        buffer.clear();
        while (true) {
            final int outBytes = histo.encodeIntoCompressedByteBuffer(buffer);
            if (outBytes != buffer.position()) {
                throw new IllegalArgumentException(
                        "HdrHistogram encoding produced inconsistent results: outBytes="
                                + outBytes
                                + ", buffer.position()="
                                + buffer.position());
            }

            final int capacity = buffer.capacity();
            if (outBytes < capacity) {
                // encoding successful
                break;
            }
            // We filled the entire buffer, an indication that the buffer was not
            // large enough, so we double the buffer and try again.
            // See: https://github.com/HdrHistogram/HdrHistogram/issues/201
            buffer = ByteBuffer.allocate(capacity * 2);
        }
        buffer.flip();
        return buffer;
    }

    @Override
    public void serialize(Histogram histogram, JsonGenerator jsonGenerator, SerializationContext ctxt)
            throws JacksonException {
        ByteBuffer buffer = serializeHistogram(histogram, ByteBuffer.allocate(INITIAL_BUFFER_SIZE));
        jsonGenerator.writeBinary(buffer.array(), buffer.arrayOffset(), buffer.limit());
    }
}
