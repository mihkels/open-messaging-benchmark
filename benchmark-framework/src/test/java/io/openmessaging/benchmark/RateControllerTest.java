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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.junit.jupiter.api.Test;

class RateControllerTest {
    private final RateController rateController = new RateController(1);
    private double rate = 10_000;
    private long periodNanos = SECONDS.toNanos(1);

    @Test
    void receiveBacklog() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 10_000);
        assertThat(rate).isEqualTo(20_000);
        assertThat(rateController.decision()).isEqualTo("rampUp");
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // receive backlog
        rate = rateController.nextRate(rate, periodNanos, 20_000, 15_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.decision()).isEqualTo("receiveBacklog");
        assertThat(rateController.receiveBacklog()).isEqualTo(5_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);
    }

    @Test
    void publishBacklog() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 10_000);
        assertThat(rate).isEqualTo(20_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // publish backlog
        rate = rateController.nextRate(rate, periodNanos, 15_000, 20_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.decision()).isEqualTo("publishBacklog");
        assertThat(rateController.publishBacklog()).isEqualTo(15_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);
    }

    @Test
    void rampUp() {
        assertThat(rateController.getRampingFactor()).isEqualTo(1);

        // receive backlog
        rate = rateController.nextRate(rate, periodNanos, 10_000, 5_000);
        assertThat(rate).isEqualTo(5_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(0.5);

        // no backlog
        rate = rateController.nextRate(rate, periodNanos, 20_000, 20_000);
        assertThat(rate).isEqualTo(10_000);
        assertThat(rateController.getRampingFactor()).isEqualTo(1);
    }

    @Test
    void recoversFromABacklogLargerThanOnePeriod() {
        // 100k msg/s for 3 s expects 300k; a consumer stall leaves 400k unreceived
        long period = SECONDS.toNanos(3);
        double next = rateController.nextRate(100_000, period, 400_000, 0);
        assertThat(rateController.decision()).isEqualTo("receiveBacklog");
        assertThat(next).isEqualTo(1);

        // consumers catch up, so the rate climbs again
        next = rateController.nextRate(next, period, 400_003, 400_003);
        assertThat(rateController.decision()).isEqualTo("rampUp");
        assertThat(next).isGreaterThan(1);
    }

    @Test
    void receiveBacklogCountsOneCopyPerSubscription() {
        RateController controller = new RateController(2);
        // 300k sent and 500k received over 2 subscriptions: 100k copies still to receive
        double next = controller.nextRate(100_000, SECONDS.toNanos(3), 300_000, 500_000);
        assertThat(controller.decision()).isEqualTo("receiveBacklog");
        assertThat(controller.receiveBacklog()).isEqualTo(100_000);
        // 250k messages per subscription received; 300k expected less 50k behind per subscription
        assertThat(next).isCloseTo(250_000 / 3.0, within(0.001));
    }

    @Test
    void producerOnlyWorkloadHasNoReceiveBacklog() {
        RateController controller = new RateController(0);
        double next = controller.nextRate(10_000, periodNanos, 10_000, 0);
        assertThat(controller.decision()).isEqualTo("rampUp");
        assertThat(controller.receiveBacklog()).isZero();
        assertThat(next).isEqualTo(20_000);
    }
}
