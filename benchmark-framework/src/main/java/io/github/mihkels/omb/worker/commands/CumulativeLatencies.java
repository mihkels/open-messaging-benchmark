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
package io.github.mihkels.omb.worker.commands;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.List;
import org.HdrHistogram.Histogram;

public class CumulativeLatencies {

    public Histogram publishLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram publishDelayLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram publishScheduleLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram endToEndLatency = new Histogram(HOURS.toMicros(12), 4);

    public CumulativeLatencies plus(CumulativeLatencies toAdd) {
        return aggregate(List.of(this, toAdd));
    }

    /**
     * Sums the latencies into one new instance, without an intermediate copy per step.
     *
     * @param latencies the latencies to sum
     * @return the sum
     */
    public static CumulativeLatencies aggregate(List<CumulativeLatencies> latencies) {
        CumulativeLatencies result = new CumulativeLatencies();
        for (CumulativeLatencies latency : latencies) {
            result.publishLatency.add(latency.publishLatency);
            result.publishDelayLatency.add(latency.publishDelayLatency);
            result.publishScheduleLatency.add(latency.publishScheduleLatency);
            result.endToEndLatency.add(latency.endToEndLatency);
        }
        return result;
    }
}
