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
package io.openmessaging.benchmark.worker;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Future cancellation only signals a task. This separate latch tracks when its body really exits,
 * including cancellation before the executor starts it.
 *
 * @param <T> task result type
 */
final class TrackedTask<T> extends FutureTask<T> {
    private final String description;
    private final AtomicInteger state = new AtomicInteger();
    private final CountDownLatch exited = new CountDownLatch(1);

    TrackedTask(String description, Callable<T> operation) {
        super(operation);
        this.description = description;
    }

    String description() {
        return description;
    }

    @Override
    public void run() {
        if (state.compareAndSet(0, 1)) {
            try {
                super.run();
            } finally {
                state.set(2);
                exited.countDown();
            }
        }
    }

    @Override
    public boolean cancel(boolean interrupt) {
        boolean cancelled = super.cancel(interrupt);
        if (state.compareAndSet(0, 2)) {
            exited.countDown();
        }
        return cancelled;
    }

    boolean awaitExit(long deadlineNanos) throws InterruptedException {
        long remaining = deadlineNanos - System.nanoTime();
        return exited.getCount() == 0
                || (remaining > 0 && exited.await(remaining, TimeUnit.NANOSECONDS));
    }
}
