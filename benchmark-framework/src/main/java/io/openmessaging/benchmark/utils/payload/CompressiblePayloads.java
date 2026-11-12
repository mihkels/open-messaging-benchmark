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
package io.openmessaging.benchmark.utils.payload;

import com.github.luben.zstd.Zstd;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates payloads that compress to a target ratio.
 *
 * <p>Each payload is a run of segments that are either fresh random bytes or a copy of an earlier
 * segment of the same payload. Codecs cannot shrink the random segments and turn the copies into
 * short back-references, so the share of random segments sets the ratio for zstd, lz4 and snappy
 * alike. Payloads share no content, so batching them does not compress better than each one alone.
 */
public final class CompressiblePayloads {
    private static final Logger log = LoggerFactory.getLogger(CompressiblePayloads.class);

    /** The zstd level payloads are calibrated and measured with. */
    public static final int ZSTD_LEVEL = 3;

    // shortcut: a fixed segment length floors the ratio at a few percent; vary it if lower
    // targets are needed
    private static final int SEGMENT = 64;
    // Copies stay within deflate's 32 KiB window, the smallest of the common codecs
    private static final int WINDOW = 32 * 1024;
    private static final int SAMPLE_BYTES = 1 << 20;
    private static final int CALIBRATION_STEPS = 16;
    private static final double TOLERANCE = 0.05;

    private CompressiblePayloads() {}

    /**
     * Generates distinct payloads of one size whose zstd ratio is close to the target.
     *
     * @param size the payload length in bytes
     * @param targetRatio compressed size divided by original size, in (0, 1]
     * @param poolSize the number of payloads
     * @param random the source of the payloads' seed
     * @return the payloads
     */
    public static List<byte[]> pool(int size, double targetRatio, int poolSize, Random random) {
        long seed = random.nextLong();
        if (size == 0) {
            return generate(0, 1, poolSize, seed);
        }
        int samples = Math.clamp(SAMPLE_BYTES / size, 1, poolSize);
        double low = 0;
        double high = 1;
        double bestShare = 1;
        double bestRatio = Double.NaN;
        for (int step = 0; step < CALIBRATION_STEPS; step++) {
            double share = (low + high) / 2;
            double achieved = ratio(generate(size, share, samples, seed), null);
            if (Double.isNaN(bestRatio)
                    || Math.abs(achieved - targetRatio) < Math.abs(bestRatio - targetRatio)) {
                bestShare = share;
                bestRatio = achieved;
            }
            if (achieved < targetRatio) {
                low = share;
            } else {
                high = share;
            }
        }
        if (Math.abs(bestRatio - targetRatio) > TOLERANCE * targetRatio) {
            log.warn(
                    "Payloads of {} bytes compress to {} with zstd, not the target {}; codec overhead"
                            + " keeps small payloads from reaching low ratios",
                    size,
                    bestRatio,
                    targetRatio);
        }
        // Same seed, so the calibrated sample is the start of the pool
        return generate(size, bestShare, poolSize, seed);
    }

    /**
     * Measures how well payloads compress with zstd, weighting each by how often it is sent.
     *
     * @param payloads the payloads and their optional weights
     * @return the total compressed size divided by the total original size, or 1 if there is nothing
     *     to compress
     */
    public static double ratio(PayloadTemplates payloads) {
        return ratio(payloads.templates(), payloads.weights());
    }

    private static double ratio(List<byte[]> payloads, int @Nullable [] weights) {
        double compressed = 0;
        double original = 0;
        for (int i = 0; i < payloads.size(); i++) {
            byte[] payload = payloads.get(i);
            if (payload.length > 0) {
                double weight = weights == null ? 1 : weights[i];
                compressed += weight * Zstd.compress(payload, ZSTD_LEVEL).length;
                original += weight * payload.length;
            }
        }
        return original == 0 ? 1 : compressed / original;
    }

    // Separate streams make the literal-or-copy choices independent of the share, so the ratio
    // rises with it
    private static List<byte[]> generate(int size, double literalShare, int count, long seed) {
        Random choices = new Random(seed);
        Random content = new Random(~seed);
        List<byte[]> payloads = new ArrayList<>(count);
        byte[] literal = new byte[SEGMENT];
        for (int i = 0; i < count; i++) {
            byte[] payload = new byte[size];
            for (int offset = 0; offset < size; offset += SEGMENT) {
                int length = Math.min(SEGMENT, size - offset);
                int earlierSegments = Math.min(offset, WINDOW) / SEGMENT;
                if (earlierSegments == 0 || choices.nextDouble() < literalShare) {
                    content.nextBytes(literal);
                    System.arraycopy(literal, 0, payload, offset, length);
                } else {
                    int source = offset - SEGMENT * (1 + content.nextInt(earlierSegments));
                    System.arraycopy(payload, source, payload, offset, length);
                }
            }
            payloads.add(payload);
        }
        return payloads;
    }
}
