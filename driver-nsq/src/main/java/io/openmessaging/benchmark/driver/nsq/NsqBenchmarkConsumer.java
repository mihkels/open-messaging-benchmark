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

import com.github.brainlag.nsq.Connection;
import com.github.brainlag.nsq.NSQCommand;
import com.github.brainlag.nsq.NSQConfig;
import com.github.brainlag.nsq.NSQConsumer;
import com.github.brainlag.nsq.NSQMessage;
import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.NSQFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import com.github.brainlag.nsq.lookup.NSQLookup;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.openmessaging.benchmark.driver.AbandonableCreation;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import io.openmessaging.benchmark.driver.ResourceCreationCompletion;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NsqBenchmarkConsumer implements BenchmarkConsumer {
    private static final Logger log = LoggerFactory.getLogger(NsqBenchmarkConsumer.class);
    private static final long DEFAULT_READINESS_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long DEFAULT_LOOKUP_PERIOD_MILLIS = TimeUnit.MINUTES.toMillis(1);
    private static final long DEFAULT_RETRY_MILLIS = 250;
    private static final long DEFAULT_CLOSE_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(30);
    private static final long CALLBACK_DRAIN_SECONDS = 15;
    private static final int MESSAGES_PER_BATCH = 200;

    @FunctionalInterface
    interface ConnectionFactory {
        Connection connect(ServerAddress address, NSQConfig config) throws Exception;
    }

    record Settings(
            long readinessMillis,
            long lookupPeriodMillis,
            long retryMillis,
            ConnectionFactory connectionFactory,
            long closeWaitMillis) {
        Settings(
                long readinessMillis,
                long lookupPeriodMillis,
                long retryMillis,
                ConnectionFactory connectionFactory) {
            this(
                    readinessMillis,
                    lookupPeriodMillis,
                    retryMillis,
                    connectionFactory,
                    DEFAULT_CLOSE_WAIT_MILLIS);
        }
    }

    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final ConsumerCallback callback;
    private final Predicate<Set<ServerAddress>> publisherPresent;
    private final long readinessMillis;
    private final long lookupPeriodMillis;
    private final long retryMillis;
    private final long closeWaitMillis;
    private final ConnectionFactory connectionFactory;
    private final AtomicReference<Thread> connectorThread = new AtomicReference<>();
    private final AtomicReference<Thread> schedulerThread = new AtomicReference<>();
    private final ThreadLocal<Boolean> inCallback = new ThreadLocal<>();
    private final ScheduledExecutorService scheduler;
    private final ExecutorService connector;
    private final ExecutorService callbacks;
    private final NsqEventLoopGroup eventLoopGroup;
    private final AtomicBoolean refreshing = new AtomicBoolean();
    private final AtomicBoolean abandonmentRequested = new AtomicBoolean();
    private final Object lock = new Object();
    private final Map<ServerAddress, Connection> connections = new HashMap<>();
    private final Set<Connection> connecting = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Object cleanupLock = new Object();
    private final List<Throwable> cleanupFailures = new ArrayList<>();
    private final Set<Throwable> recordedFailures =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private final CompletableFuture<Void> initializationFinished = new CompletableFuture<>();
    private final CompletableFuture<Void> resultCallbacksFinished = new CompletableFuture<>();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();
    private final CompletableFuture<Void> closedFinished = new CompletableFuture<>();
    private final CompletableFuture<Void> closeResult = new CompletableFuture<>();
    private final CompletableFuture<Void> quiescence = new CompletableFuture<>();
    private final CreationFuture creation = new CreationFuture();
    private volatile boolean closed;
    private boolean started;
    private boolean ready;
    private Thread closingThread;
    private volatile Throwable lastConnectionFailure;
    private volatile Throwable startupFailure;
    private NSQConfig connectionConfig;
    private NSQConsumer dispatcher;
    private ScheduledFuture<?> deadline;
    private ScheduledFuture<?> refresh;

    NsqBenchmarkConsumer(
            NSQLookup lookup,
            String topic,
            String channel,
            ConsumerCallback callback,
            Predicate<Set<ServerAddress>> publisherPresent,
            NsqEventLoopGroup eventLoopGroup) {
        this(
                lookup,
                topic,
                channel,
                callback,
                publisherPresent,
                eventLoopGroup,
                new Settings(
                        DEFAULT_READINESS_MILLIS,
                        DEFAULT_LOOKUP_PERIOD_MILLIS,
                        DEFAULT_RETRY_MILLIS,
                        Connection::new));
    }

    NsqBenchmarkConsumer(
            NSQLookup lookup,
            String topic,
            String channel,
            ConsumerCallback callback,
            Predicate<Set<ServerAddress>> publisherPresent,
            NsqEventLoopGroup eventLoopGroup,
            Settings settings) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.callback = callback;
        this.publisherPresent = publisherPresent;
        this.eventLoopGroup = eventLoopGroup;
        this.readinessMillis = settings.readinessMillis();
        this.lookupPeriodMillis = settings.lookupPeriodMillis();
        this.retryMillis = settings.retryMillis();
        this.closeWaitMillis = settings.closeWaitMillis();
        this.connectionFactory = settings.connectionFactory();
        ScheduledThreadPoolExecutor scheduled =
                new ScheduledThreadPoolExecutor(
                        1, daemonFactory("nsq-consumer-scheduler", schedulerThread));
        scheduled.setRemoveOnCancelPolicy(true);
        scheduled.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduled.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler = scheduled;
        connector =
                new ThreadPoolExecutor(
                        0,
                        1,
                        5,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        daemonFactory("nsq-consumer-connect", connectorThread));
        callbacks = Executors.newCachedThreadPool(daemonFactory("nsq-consumer-callback", null));
        termination.whenComplete(
                (ignored, failure) -> {
                    if (failure != null) {
                        closeResult.completeExceptionally(failure);
                    } else {
                        closedFinished.whenComplete(
                                (closed, closeFailure) -> complete(closeResult, closeFailure));
                    }
                });
        CompletableFuture.allOf(initializationFinished, resultCallbacksFinished)
                .thenRun(this::finishCreationQuiescence);
    }

    CompletableFuture<BenchmarkConsumer> startAsync() {
        synchronized (lock) {
            if (started) {
                return creation;
            }
            started = true;
        }
        boolean submitted = false;
        RuntimeException failure = null;
        try {
            synchronized (lock) {
                if (!closed) {
                    deadline =
                            scheduler.schedule(this::readinessTimedOut, readinessMillis, TimeUnit.MILLISECONDS);
                    connector.execute(
                            () -> {
                                try {
                                    initializeAndConnect();
                                } finally {
                                    initializationFinished.complete(null);
                                }
                            });
                    submitted = true;
                }
            }
        } catch (RuntimeException error) {
            failure = error;
        }
        if (!submitted) {
            initializationFinished.complete(null);
        }
        if (failure != null) {
            failStartup(failure);
        }
        return creation;
    }

    CompletableFuture<BenchmarkConsumer> failBeforeStart(Throwable failure) {
        creation.completeExceptionally(failure);
        return creation;
    }

    private void readinessTimedOut() {
        TimeoutException timeout =
                new TimeoutException(
                        "NSQ consumer subscription was not ready within " + readinessMillis + " ms");
        Throwable lastFailure = lastConnectionFailure;
        if (lastFailure != null) {
            timeout.initCause(lastFailure);
        }
        failStartup(timeout);
    }

    private void initializeAndConnect() {
        try {
            NSQConfig config = new NSQConfig();
            if (closed) {
                return;
            }
            config.setEventLoopGroup(eventLoopGroup);
            NSQConsumer consumer = new NSQConsumer(lookup, topic, channel, this::deliver, config);
            consumer.setExecutor(callbacks);
            consumer.setScheduledExecutor(scheduler);
            synchronized (lock) {
                if (closed) {
                    return;
                }
                connectionConfig = config;
                dispatcher = consumer;
            }
            while (!closed) {
                try {
                    if (refreshConnections(true)) {
                        awaitStartupCleanup();
                        completeCreation();
                        return;
                    }
                } catch (SubscriptionRejected failure) {
                    for (Throwable cleanupFailure : failure.getSuppressed()) {
                        failure.getCause().addSuppressed(cleanupFailure);
                    }
                    failStartup(failure.getCause());
                    return;
                }
                Thread.sleep(retryMillis);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            failStartup(interrupted);
        } catch (Throwable failure) {
            failStartup(failure);
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private void awaitStartupCleanup() throws Exception {
        eventLoopGroup.pendingCloses().get();
        recordScopeFailure();
        Throwable failure = cleanupOutcome();
        if (failure != null) {
            throw new IOException("Unable to clean up an NSQ connection attempt", failure);
        }
    }

    private boolean refreshConnections(boolean initial) throws Exception {
        Set<ServerAddress> discovered = new HashSet<>(lookup.lookup(topic));
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("NSQ lookup interrupted");
        }
        List<Connection> removed = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                return false;
            }
            Iterator<Map.Entry<ServerAddress, Connection>> iterator = connections.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ServerAddress, Connection> entry = iterator.next();
                Connection connection = entry.getValue();
                if (!connection.isConnected()
                        || !connection.isHeartbeatStatusOK()
                        || (!discovered.isEmpty() && !discovered.contains(entry.getKey()))) {
                    iterator.remove();
                    removed.add(connection);
                }
            }
        }
        closeConnections(removed, null);
        if (initial && cleanupOutcome() != null) {
            throw new IOException("Unable to close an NSQ connection", cleanupOutcome());
        }
        for (ServerAddress address : discovered) {
            synchronized (lock) {
                if (closed) {
                    return false;
                }
                if (connections.containsKey(address)) {
                    continue;
                }
            }
            try {
                connect(address);
            } catch (SubscriptionRejected failure) {
                if (initial) {
                    throw failure;
                }
                log.warn("NSQ subscription rejected by {}", address, failure.getCause());
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception failure) {
                if (!closed) {
                    if (initial && cleanupOutcome() != null) {
                        throw failure;
                    }
                    if (initial) {
                        lastConnectionFailure = failure;
                        log.debug("Unable to connect NSQ consumer to {}", address, failure);
                    } else {
                        log.warn("Unable to connect NSQ consumer to {}", address, failure);
                    }
                }
            }
        }
        if (!allConnected(discovered) || (initial && !publisherPresent.test(discovered))) {
            return false;
        }
        return allConnected(discovered);
    }

    private boolean allConnected(Set<ServerAddress> discovered) {
        synchronized (lock) {
            return !closed
                    && !discovered.isEmpty()
                    && discovered.stream()
                            .allMatch(
                                    address ->
                                            connections.containsKey(address) && connections.get(address).isConnected());
        }
    }

    private void connect(ServerAddress address) throws Exception {
        NSQConfig config;
        NSQConsumer consumer;
        synchronized (lock) {
            if (closed) {
                return;
            }
            config = connectionConfig;
            consumer = dispatcher;
        }
        Connection connection;
        List<Channel> attemptChannels;
        boolean constructed = false;
        eventLoopGroup.beginAttempt();
        try {
            connection = connectionFactory.connect(address, config);
            constructed = true;
        } finally {
            attemptChannels = eventLoopGroup.endAttempt(constructed);
        }
        boolean accepted;
        synchronized (lock) {
            accepted = !closed;
            if (accepted) {
                connecting.add(connection);
            }
        }
        if (!accepted) {
            closeConnections(List.of(connection), null);
            cleanup(() -> eventLoopGroup.closeAttempt(attemptChannels), null);
            return;
        }
        Throwable failure = null;
        try {
            if (!connection.isConnected()) {
                throw new IOException("NSQ connection is not active: " + address);
            }
            connection.setConsumer(consumer);
            NSQFrame response = connection.commandAndWait(NSQCommand.subscribe(topic, channel));
            if (response == null) {
                Thread.currentThread().interrupt();
                throw new InterruptedException("NSQ subscription interrupted");
            }
            if (response instanceof ErrorFrame error) {
                throw new SubscriptionRejected(NSQException.of(error));
            }
            if (!(response instanceof ResponseFrame ack) || !"OK".equals(ack.getMessage())) {
                throw new SubscriptionRejected(
                        new NSQException("Unexpected NSQ SUB response from " + address + ": " + response));
            }
            ChannelFuture readyWrite;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                readyWrite = connection.command(NSQCommand.ready(MESSAGES_PER_BATCH));
            }
            if (!readyWrite.await(15, TimeUnit.SECONDS)) {
                throw new TimeoutException("NSQ RDY command timed out: " + address);
            }
            if (!readyWrite.isSuccess()) {
                throw new IOException("NSQ RDY command failed: " + address, readyWrite.cause());
            }
            if (!connection.isConnected()) {
                throw new IOException("NSQ connection closed after SUB: " + address);
            }
            synchronized (lock) {
                if (!closed && connecting.remove(connection)) {
                    connections.put(address, connection);
                }
            }
        } catch (Exception | Error cause) {
            failure = cause;
            throw cause;
        } finally {
            boolean stillOwned;
            synchronized (lock) {
                stillOwned = connecting.remove(connection);
            }
            if (stillOwned) {
                try {
                    connection.close();
                } catch (RuntimeException | Error closeFailure) {
                    recordCleanupFailure(closeFailure, failure);
                    if (failure == null) {
                        throw closeFailure;
                    }
                } finally {
                    cleanup(() -> eventLoopGroup.closeAttempt(attemptChannels), failure);
                }
            }
        }
    }

    private void completeCreation() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            refresh =
                    scheduler.scheduleAtFixedRate(
                            this::scheduleRefresh, lookupPeriodMillis, lookupPeriodMillis, TimeUnit.MILLISECONDS);
            ready = true;
            deadline.cancel(false);
        }
        creation.complete(this);
    }

    private void scheduleRefresh() {
        if (closed || !refreshing.compareAndSet(false, true)) {
            return;
        }
        try {
            connector.execute(
                    () -> {
                        try {
                            refreshConnections(false);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        } catch (Exception failure) {
                            log.warn("Unable to refresh NSQ consumer connections", failure);
                        } finally {
                            refreshing.set(false);
                        }
                    });
        } catch (RejectedExecutionException ignored) {
            refreshing.set(false);
        }
    }

    private void deliver(NSQMessage message) {
        Boolean previous = inCallback.get();
        inCallback.set(true);
        try {
            callback.messageReceived(message.getMessage(), message.getTimestamp().getTime());
            message.finished();
        } finally {
            if (previous == null) {
                inCallback.remove();
            } else {
                inCallback.set(previous);
            }
        }
    }

    private void failStartup(Throwable failure) {
        Closing closing = beginClose(true, failure);
        if (closing != null) {
            startClosing(closing, failure, true, Thread.currentThread(), false);
            creation.completeExceptionally(failure);
        }
    }

    private void abandonStartup() {
        if (!abandonmentRequested.compareAndSet(false, true)) {
            return;
        }
        CancellationException failure =
                new CancellationException("NSQ consumer creation was abandoned");
        Thread initiator = Thread.currentThread();
        daemonFactory("nsq-consumer-abandon", null)
                .newThread(
                        () -> {
                            Closing closing = beginClose(true, failure);
                            if (closing != null) {
                                startClosing(closing, failure, true, initiator, true);
                            }
                        })
                .start();
    }

    private Closing beginClose(boolean onlyStarting, Throwable failure) {
        synchronized (lock) {
            if (closed || (onlyStarting && ready)) {
                return null;
            }
            closed = true;
            startupFailure = failure;
            if (deadline != null) {
                deadline.cancel(false);
            }
            if (refresh != null) {
                refresh.cancel(false);
            }
            Closing closing =
                    new Closing(
                            new ArrayList<>(connections.values()), new ArrayList<>(connecting), ready, !started);
            connections.clear();
            connecting.clear();
            return closing;
        }
    }

    private void startClosing(
            Closing closing,
            Throwable failure,
            boolean abort,
            Thread initiator,
            boolean completeCreation) {
        if (closing.neverStarted()) {
            initializationFinished.complete(null);
        }
        daemonFactory("nsq-consumer-cleanup", null)
                .newThread(() -> closeResources(closing, failure, abort, initiator))
                .start();
        if (completeCreation) {
            daemonFactory("nsq-consumer-creation-result", null)
                    .newThread(() -> creation.completeExceptionally(failure))
                    .start();
        }
    }

    private void closeResources(Closing closing, Throwable failure, boolean abort, Thread initiator) {
        synchronized (lock) {
            closingThread = Thread.currentThread();
        }
        try {
            if (abort) {
                abortResources(closing, failure, initiator);
            } else {
                closeReady(closing, initiator);
            }
        } catch (Throwable closeFailure) {
            recordCleanupFailure(closeFailure, failure);
        } finally {
            recordScopeFailure();
            complete(termination, cleanupOutcome());
        }
        awaitResourceTermination();
        recordScopeFailure();
        complete(closedFinished, cleanupOutcome());
    }

    private void abortResources(Closing closing, Throwable failure, Thread initiator) {
        cleanup(eventLoopGroup::closeChannels, failure);
        cleanup(() -> stopConnector(initiator), failure);
        cleanup(callbacks::shutdownNow, failure);
        closeConnections(closing.connecting(), failure);
        closeConnections(closing.active(), failure);
        cleanup(() -> stopScheduler(initiator), failure);
    }

    private void awaitResourceTermination() {
        boolean interrupted = false;
        for (ExecutorService executor : List.of(connector, callbacks, scheduler)) {
            boolean terminated = false;
            while (!terminated) {
                try {
                    terminated = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
        }
        while (true) {
            try {
                eventLoopGroup.pendingCloses().get();
                break;
            } catch (InterruptedException ignored) {
                interrupted = true;
            } catch (ExecutionException failure) {
                recordCleanupFailure(failure.getCause(), startupFailure);
                break;
            }
        }
        io.netty.util.concurrent.Future<?> channels = eventLoopGroup.terminationFuture();
        channels.awaitUninterruptibly();
        if (!channels.isSuccess()) {
            recordCleanupFailure(channels.cause(), startupFailure);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void finishCreationQuiescence() {
        boolean closing;
        synchronized (lock) {
            closing = closed;
        }
        if (closing) {
            closedFinished.whenComplete((ignored, failure) -> complete(quiescence, failure));
        } else {
            recordScopeFailure();
            complete(quiescence, cleanupOutcome());
        }
    }

    @Override
    public void close() throws Exception {
        if (Boolean.TRUE.equals(inCallback.get())) {
            throw new IllegalStateException(
                    "Cannot close an NSQ consumer from its own callback; retry from another thread");
        }
        IllegalStateException failure = new IllegalStateException("NSQ consumer is closed");
        Closing closing = beginClose(false, failure);
        if (closing == null) {
            synchronized (lock) {
                if (closingThread == Thread.currentThread()) {
                    return;
                }
            }
        } else {
            startClosing(closing, failure, !closing.ready(), Thread.currentThread(), !closing.ready());
        }
        CompletableFuture<Void> outcome =
                Thread.currentThread() == connectorThread.get()
                                || Thread.currentThread() == schedulerThread.get()
                        ? termination
                        : closeResult;
        try {
            outcome.get(closeWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failed) {
            throwFailure(failed.getCause());
        } catch (TimeoutException timeout) {
            TimeoutException failureToClose =
                    new TimeoutException(
                            "NSQ consumer close did not finish within " + closeWaitMillis + " ms");
            failureToClose.initCause(timeout);
            throw failureToClose;
        }
    }

    private void closeReady(Closing closing, Thread initiator) throws Exception {
        Throwable failure = null;
        try {
            eventLoopGroup.stopConnecting();
        } catch (Throwable closeFailure) {
            failure = merge(failure, closeFailure);
        }
        try {
            stopConnector(initiator);
        } catch (Throwable closeFailure) {
            failure = merge(failure, closeFailure);
        }
        failure = closeConnectionsCollecting(closing.connecting(), failure);
        try {
            closeGracefully(closing, initiator);
        } catch (Throwable closeFailure) {
            failure = merge(failure, closeFailure);
        }
        throwFailure(failure);
    }

    private void closeGracefully(Closing closing, Thread initiator) throws Exception {
        Throwable failure = null;
        try {
            for (Connection connection : closing.active()) {
                try {
                    if (!connection.isConnected()) {
                        continue;
                    }
                    NSQFrame response = connection.commandAndWait(NSQCommand.startClose());
                    if (response instanceof ErrorFrame error
                            && error.getErrorMessage().startsWith("E_INVALID")) {
                        throw new IllegalStateException(error.getErrorMessage());
                    }
                    if (response == null) {
                        Thread.currentThread().interrupt();
                        throw new InterruptedException("NSQ consumer close interrupted");
                    }
                } catch (Throwable closeFailure) {
                    failure = merge(failure, closeFailure);
                    if (closeFailure instanceof InterruptedException) {
                        break;
                    }
                }
            }
        } finally {
            try {
                callbacks.shutdown();
                if (!callbacks.awaitTermination(CALLBACK_DRAIN_SECONDS, TimeUnit.SECONDS)) {
                    callbacks.shutdownNow();
                    failure =
                            merge(
                                    failure,
                                    new TimeoutException("NSQ consumer callbacks did not finish during close"));
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                callbacks.shutdownNow();
                failure = merge(failure, interrupted);
            } catch (Throwable closeFailure) {
                failure = merge(failure, closeFailure);
            } finally {
                failure = finishClose(closing, failure, initiator);
            }
        }
        throwFailure(failure);
    }

    private Throwable finishClose(Closing closing, Throwable failure, Thread initiator) {
        failure = closeConnectionsCollecting(closing.active(), failure);
        try {
            eventLoopGroup.closeChannels();
        } catch (Throwable closeFailure) {
            failure = merge(failure, closeFailure);
        }
        try {
            stopScheduler(initiator);
        } catch (Throwable closeFailure) {
            failure = merge(failure, closeFailure);
        }
        return failure;
    }

    private void stopConnector(Thread initiator) {
        if (initiator == connectorThread.get()) {
            connector.shutdown();
        } else {
            connector.shutdownNow();
        }
    }

    private void stopScheduler(Thread initiator) {
        if (initiator == schedulerThread.get()) {
            scheduler.shutdown();
        } else {
            scheduler.shutdownNow();
        }
    }

    private void closeConnections(List<Connection> connections, Throwable failure) {
        for (Connection connection : connections) {
            cleanup(connection::close, failure);
        }
    }

    private static Throwable closeConnectionsCollecting(
            List<Connection> connections, Throwable failure) {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Throwable closeFailure) {
                failure = merge(failure, closeFailure);
            }
        }
        return failure;
    }

    private static Throwable merge(Throwable failure, Throwable additional) {
        if (failure == null) {
            return additional;
        }
        suppress(failure, additional);
        return failure;
    }

    private void cleanup(Runnable action, Throwable failure) {
        try {
            action.run();
        } catch (Throwable closeFailure) {
            recordCleanupFailure(closeFailure, failure);
        }
    }

    private void recordScopeFailure() {
        Throwable failure = eventLoopGroup.cleanupFailure();
        if (failure != null) {
            recordCleanupFailure(failure, startupFailure);
        }
    }

    private void recordCleanupFailure(Throwable failure, Throwable primary) {
        if (failure == null) {
            return;
        }
        synchronized (cleanupLock) {
            if (recordedFailures.add(failure)) {
                cleanupFailures.add(failure);
            }
            suppress(primary, failure);
            if (startupFailure != primary) {
                suppress(startupFailure, failure);
            }
        }
    }

    private Throwable cleanupOutcome() {
        synchronized (cleanupLock) {
            if (cleanupFailures.isEmpty()) {
                return null;
            }
            Throwable first = cleanupFailures.getFirst();
            for (int i = 1; i < cleanupFailures.size(); i++) {
                suppress(first, cleanupFailures.get(i));
            }
            return first;
        }
    }

    private static void suppress(Throwable primary, Throwable additional) {
        if (primary == null || primary == additional) {
            return;
        }
        for (Throwable suppressed : primary.getSuppressed()) {
            if (suppressed == additional) {
                return;
            }
        }
        primary.addSuppressed(additional);
    }

    private static void complete(CompletableFuture<Void> future, Throwable failure) {
        if (failure == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(failure);
        }
    }

    private static void throwFailure(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("NSQ consumer close failed", failure);
        }
    }

    private static ThreadFactory daemonFactory(String name, AtomicReference<Thread> reference) {
        return task -> {
            Thread thread = new Thread(task, name);
            thread.setDaemon(true);
            if (reference != null) {
                reference.set(thread);
            }
            return thread;
        };
    }

    private final class CreationFuture extends CompletableFuture<BenchmarkConsumer>
            implements AbandonableCreation, ResourceCreationCompletion {
        private final AtomicBoolean completionClaimed = new AtomicBoolean();

        @Override
        public boolean complete(BenchmarkConsumer consumer) {
            if (!completionClaimed.compareAndSet(false, true)) {
                return false;
            }
            try {
                return super.complete(consumer);
            } finally {
                resultCallbacksFinished.complete(null);
            }
        }

        @Override
        public boolean completeExceptionally(Throwable failure) {
            if (failure == null) {
                throw new NullPointerException("failure");
            }
            if (!completionClaimed.compareAndSet(false, true)) {
                return false;
            }
            try {
                Closing closing = beginClose(false, failure);
                if (closing != null) {
                    startClosing(closing, failure, true, Thread.currentThread(), false);
                }
                return super.completeExceptionally(failure);
            } finally {
                resultCallbacksFinished.complete(null);
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return isCancelled()
                    || completeExceptionally(
                            new CancellationException("NSQ consumer creation was cancelled"));
        }

        @Override
        public void abandonCreation() {
            abandonStartup();
        }

        @Override
        public CompletableFuture<Void> quiescence() {
            return quiescence.copy();
        }
    }

    private record Closing(
            List<Connection> active, List<Connection> connecting, boolean ready, boolean neverStarted) {}

    private static final class SubscriptionRejected extends Exception {
        private SubscriptionRejected(Throwable cause) {
            super(cause);
        }
    }
}
