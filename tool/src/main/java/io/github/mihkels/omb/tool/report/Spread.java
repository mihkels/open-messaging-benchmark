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
package io.github.mihkels.omb.tool.report;

import io.github.mihkels.omb.RepeatSummary;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.List;

/**
 * The spread of one metric across the repeated runs of one side.
 *
 * @param min the smallest value
 * @param median the median value, which deltas are computed from
 * @param max the largest value
 * @param runs the number of runs
 */
record Spread(double min, double median, double max, int runs) {

    /** Summarizes a non-empty list of per-run values. */
    static Spread of(List<Double> values) {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("No values");
        }
        double[] array = values.stream().mapToDouble(Double::doubleValue).toArray();
        DoubleSummaryStatistics stats = Arrays.stream(array).summaryStatistics();
        return new Spread(stats.getMin(), RepeatSummary.median(array), stats.getMax(), array.length);
    }

    /** Whether the [min, max] ranges of both spreads share at least one value. */
    boolean overlaps(Spread other) {
        return min <= other.max && other.min <= max;
    }
}
