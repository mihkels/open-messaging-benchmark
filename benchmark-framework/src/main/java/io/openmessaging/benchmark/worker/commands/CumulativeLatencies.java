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
package io.openmessaging.benchmark.worker.commands;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.HdrHistogram.Histogram;

public class CumulativeLatencies {

    public Histogram publishLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram publishDelayLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram publishScheduleLatency = new Histogram(SECONDS.toMicros(60), 4);
    public Histogram endToEndLatency = new Histogram(HOURS.toMicros(12), 4);

    public CumulativeLatencies plus(CumulativeLatencies toAdd) {
        CumulativeLatencies result = new CumulativeLatencies();

        result.publishLatency.add(this.publishLatency);
        result.publishDelayLatency.add(this.publishDelayLatency);
        result.publishScheduleLatency.add(this.publishScheduleLatency);
        result.endToEndLatency.add(this.endToEndLatency);

        result.publishLatency.add(toAdd.publishLatency);
        result.publishDelayLatency.add(toAdd.publishDelayLatency);
        result.publishScheduleLatency.add(toAdd.publishScheduleLatency);
        result.endToEndLatency.add(toAdd.endToEndLatency);

        return result;
    }
}
