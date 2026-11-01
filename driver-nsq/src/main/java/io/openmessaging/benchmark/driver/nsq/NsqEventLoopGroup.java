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
package io.openmessaging.benchmark.driver.nsq;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.AbstractEventExecutorGroup;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tracks one consumer's channels on a driver-owned event loop. This also captures channels created
 * inside the NSQ connection constructor, before that constructor can return a Connection.
 */
final class NsqEventLoopGroup extends AbstractEventExecutorGroup implements EventLoopGroup {
    private final EventLoopGroup delegate;
    private final Object lock = new Object();
    private final Map<Channel, ChannelFuture> channels = new HashMap<>();
    private final Map<Channel, CloseRequest> closing = new HashMap<>();
    private final Promise<Void> termination = ImmediateEventExecutor.INSTANCE.newPromise();
    private CompletableFuture<Void> pendingCloses = CompletableFuture.completedFuture(null);
    private Throwable cleanupFailure;
    private Set<Channel> attempt;
    private boolean accepting = true;

    NsqEventLoopGroup(EventLoopGroup delegate) {
        this.delegate = delegate;
    }

    void beginAttempt() {
        synchronized (lock) {
            if (!accepting) {
                throw new RejectedExecutionException("NSQ consumer is closed");
            }
            if (attempt != null) {
                throw new IllegalStateException("NSQ connection attempt already in progress");
            }
            attempt = new HashSet<>();
        }
    }

    List<Channel> endAttempt(boolean succeeded) {
        List<Channel> attempted;
        synchronized (lock) {
            attempted = attempt == null ? List.of() : new ArrayList<>(attempt);
            attempt = null;
        }
        if (!succeeded) {
            closeAttempt(attempted);
        }
        return attempted;
    }

    void closeAttempt(List<Channel> channelsToClose) {
        channelsToClose.forEach(this::requestClose);
    }

    CompletableFuture<Void> pendingCloses() {
        synchronized (lock) {
            return pendingCloses.copy();
        }
    }

    Throwable cleanupFailure() {
        synchronized (lock) {
            return cleanupFailure;
        }
    }

    void stopConnecting() {
        List<Channel> toClose;
        synchronized (lock) {
            accepting = false;
            toClose = attempt == null ? List.of() : new ArrayList<>(attempt);
        }
        closeAttempt(toClose);
        completeTerminationIfClosed();
    }

    void closeChannels() {
        List<Channel> toClose;
        synchronized (lock) {
            accepting = false;
            toClose = new ArrayList<>(channels.keySet());
        }
        closeAttempt(toClose);
        completeTerminationIfClosed();
    }

    @Override
    public ChannelFuture register(Channel channel) {
        return registerChannel(channel, null);
    }

    @Override
    public ChannelFuture register(ChannelPromise promise) {
        return registerChannel(promise.channel(), promise);
    }

    @Override
    @SuppressWarnings("deprecation")
    public ChannelFuture register(Channel channel, ChannelPromise promise) {
        if (promise.channel() != channel) {
            throw new IllegalArgumentException("Promise belongs to a different channel");
        }
        return registerChannel(channel, promise);
    }

    private ChannelFuture registerChannel(Channel channel, ChannelPromise promise) {
        ChannelFuture registration;
        Throwable registrationFailure = null;
        boolean rejected;
        synchronized (lock) {
            rejected = !accepting;
            if (rejected) {
                ChannelPromise rejectedPromise =
                        promise == null
                                ? new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE)
                                : promise;
                registration =
                        rejectedPromise.setFailure(new RejectedExecutionException("NSQ consumer is closed"));
            } else {
                try {
                    registration = promise == null ? delegate.register(channel) : delegate.register(promise);
                } catch (RuntimeException | Error failure) {
                    ChannelPromise completed =
                            new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
                    registration =
                            channel.isRegistered() ? completed.setSuccess() : completed.setFailure(failure);
                    registrationFailure = failure;
                }
            }
            channels.put(channel, registration);
            if (attempt != null) {
                attempt.add(channel);
            }
        }
        try {
            channel.closeFuture().addListener(ignored -> channelClosed(channel));
        } catch (RuntimeException | Error failure) {
            recordCleanupFailure(failure);
            if (registrationFailure == null) {
                registrationFailure = failure;
            }
        }
        if (rejected || registrationFailure != null) {
            requestClose(channel);
            if (registrationFailure == null) {
                return registration;
            }
            Throwable closeFailure = cleanupFailure();
            if (closeFailure != null && closeFailure != registrationFailure) {
                registrationFailure.addSuppressed(closeFailure);
            }
            if (registrationFailure instanceof RuntimeException exception) {
                throw exception;
            }
            throw (Error) registrationFailure;
        }
        try {
            registration.addListener(
                    result -> {
                        if (!result.isSuccess()) {
                            requestClose(channel);
                        }
                    });
        } catch (RuntimeException | Error failure) {
            recordCleanupFailure(failure);
            requestClose(channel);
            throw failure;
        }
        return registration;
    }

    private void channelClosed(Channel channel) {
        CompletableFuture<Void> idle;
        synchronized (lock) {
            channels.remove(channel);
            if (attempt != null) {
                attempt.remove(channel);
            }
            CloseRequest request = closing.get(channel);
            if (request != null) {
                request.closed = true;
            }
            idle = finishCloseRequest(channel, request);
        }
        if (idle != null) {
            idle.complete(null);
        }
        completeTerminationIfClosed();
    }

    private void completeTerminationIfClosed() {
        Throwable failure;
        synchronized (lock) {
            if (accepting || !channels.isEmpty() || !closing.isEmpty()) {
                return;
            }
            failure = cleanupFailure;
        }
        if (failure == null) {
            termination.trySuccess(null);
        } else {
            termination.tryFailure(failure);
        }
    }

    private void requestClose(Channel channel) {
        ChannelFuture registration;
        CloseRequest request;
        synchronized (lock) {
            registration = channels.get(channel);
            if (registration == null || closing.containsKey(channel)) {
                return;
            }
            if (closing.isEmpty()) {
                pendingCloses = new CompletableFuture<>();
            }
            request = new CloseRequest();
            closing.put(channel, request);
        }
        try {
            registration.addListener(
                    result -> {
                        if (!result.isSuccess()) {
                            forceClose(channel, request, null);
                            return;
                        }
                        try {
                            channel
                                    .close()
                                    .addListener(
                                            closed ->
                                                    finishClose(
                                                            channel,
                                                            request,
                                                            closed.isSuccess() ? null : closeFailure(closed.cause())));
                        } catch (RuntimeException | Error failure) {
                            forceClose(channel, request, failure);
                        }
                    });
        } catch (RuntimeException | Error failure) {
            forceClose(channel, request, failure);
        }
    }

    private void forceClose(Channel channel, CloseRequest request, Throwable failure) {
        if (failure != null) {
            recordCleanupFailure(failure);
        }
        try {
            channel.unsafe().closeForcibly();
        } catch (RuntimeException | Error closeFailure) {
            recordCleanupFailure(closeFailure);
        }
        finishClose(channel, request, null);
    }

    private void finishClose(Channel channel, CloseRequest request, Throwable failure) {
        if (failure != null) {
            forceClose(channel, request, failure);
            return;
        }
        boolean closed = !channel.isOpen();
        CompletableFuture<Void> idle;
        synchronized (lock) {
            request.settled = true;
            if (closed) {
                request.closed = true;
                channels.remove(channel);
                if (attempt != null) {
                    attempt.remove(channel);
                }
            }
            idle = finishCloseRequest(channel, request);
        }
        if (idle != null) {
            idle.complete(null);
        }
        completeTerminationIfClosed();
    }

    private CompletableFuture<Void> finishCloseRequest(Channel channel, CloseRequest request) {
        if (request != null && request.closed && request.settled) {
            closing.remove(channel);
            if (closing.isEmpty()) {
                return pendingCloses;
            }
        }
        return null;
    }

    private void recordCleanupFailure(Throwable failure) {
        synchronized (lock) {
            if (cleanupFailure == null) {
                cleanupFailure = failure;
            } else if (cleanupFailure != failure) {
                cleanupFailure.addSuppressed(failure);
            }
        }
    }

    private static Throwable closeFailure(Throwable cause) {
        return cause == null ? new IllegalStateException("NSQ channel close failed") : cause;
    }

    private Future<Void> terminationView() {
        Promise<Void> view = ImmediateEventExecutor.INSTANCE.newPromise();
        view.setUncancellable();
        termination.addListener(
                finished -> {
                    if (finished.isSuccess()) {
                        view.trySuccess(null);
                    } else {
                        view.tryFailure(closeFailure(finished.cause()));
                    }
                });
        return view;
    }

    @Override
    public EventLoop next() {
        return delegate.next();
    }

    @Override
    public Iterator<EventExecutor> iterator() {
        return delegate.iterator();
    }

    @Override
    public boolean isShuttingDown() {
        synchronized (lock) {
            return !accepting;
        }
    }

    @Override
    public Future<?> shutdownGracefully(long quietPeriod, long timeout, TimeUnit unit) {
        closeChannels();
        return terminationView();
    }

    @Override
    public Future<?> terminationFuture() {
        return terminationView();
    }

    @Override
    @SuppressWarnings("deprecation")
    public void shutdown() {
        closeChannels();
    }

    @Override
    public boolean isShutdown() {
        return isShuttingDown();
    }

    @Override
    public boolean isTerminated() {
        return termination.isDone();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return termination.await(timeout, unit);
    }

    private static final class CloseRequest {
        private boolean closed;
        private boolean settled;
    }
}
