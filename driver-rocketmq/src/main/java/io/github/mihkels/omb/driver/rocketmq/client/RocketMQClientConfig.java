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
package io.github.mihkels.omb.driver.rocketmq.client;

import org.jspecify.annotations.Nullable;

public class RocketMQClientConfig {
    // The RocketMQ client falls back to the NAMESRV_ADDR environment variable when unset.
    public @Nullable String namesrvAddr;
    // Required; the driver checks it in initialize.
    public @Nullable String clusterName;
    // Unset options keep the RocketMQ client defaults. The client reads this one from a system
    // property, so it stays unset rather than copying a default.
    public @Nullable Boolean vipChannelEnabled;
    // The defaults of the pinned RocketMQ client 5.5.1 (DefaultMQProducer)
    public int maxMessageSize = 4 * 1024 * 1024;
    public int compressMsgBodyOverHowmuch = 4 * 1024;
    public boolean batchCQ;
    public boolean autoBatch;
    // Unset batch limits defer to the client's batch accumulator, which has its own defaults
    public @Nullable Integer batchMaxBytes;
    public @Nullable Integer batchMaxDelayMs;
    public @Nullable Integer totalBatchMaxBytes;

    public boolean enableBackpressure;
    // Setting it resizes the client's send semaphore, so unset leaves the client untouched
    public @Nullable Integer backpressureConcurrency;
    // Blank keys disable ACL.
    public String accessKey = "";
    public String secretKey = "";
}
