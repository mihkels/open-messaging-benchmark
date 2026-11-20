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
package io.github.mihkels.omb.driver;

/**
 * What a completed send reports through {@link BenchmarkProducer#sendAsyncWithResult}.
 *
 * @param partition the partition the message was written to, or {@link
 *     ConsumerCallback#UNKNOWN_PARTITION}
 */
public record SendResult(int partition) {

    /** Result of a send whose partition the driver does not know. */
    public static final SendResult UNKNOWN = new SendResult(ConsumerCallback.UNKNOWN_PARTITION);
}
