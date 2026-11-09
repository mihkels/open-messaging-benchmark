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
import io.openmessaging.benchmark.driver.Failures;
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
import java.util.concurrent.CompletionException;
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
import java.util.function.Consumer;
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
        @SuppressWarnings("java:S112") // Test seam over the NSQ client connect, which throws Exception
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
    private final Settings settings;
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
        this.settings = settings;
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
        termination.whenComplete((ignored, failure) -> {
            if (failure != null) {
                closeResult.completeExceptionally(failure);
            } else {
                closedFinished.whenComplete((_, closeFailure) -> complete(closeResult, closeFailure));
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
                            scheduler.schedule(
                                    this::readinessTimedOut, settings.readinessMillis(), TimeUnit.MILLISECONDS);
                    connector.execute(() -> {
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
                        "NSQ consumer subscription was not ready within " + settings.readinessMillis() + " ms");
        Throwable lastFailure = lastConnectionFailure;
        if (lastFailure != null) {
            timeout.initCause(lastFailure);
        }
        failStartup(timeout);
    }

    // An NSQConsumer holds no connections or threads until start(), and its close() would shut down
    // the scheduler shared with this consumer, so an instance dropped on the early return is not
    // closed
    @SuppressWarnings("java:S2095")
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
                if (attemptStartup()) {
                    return;
                }
                Thread.sleep(settings.retryMillis());
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

    // Returns true once startup has finished, either ready or failed by a rejected subscription.
    private boolean attemptStartup() throws Exception {
        try {
            if (refreshConnections(true)) {
                awaitStartupCleanup();
                completeCreation();
                return true;
            }
            return false;
        } catch (SubscriptionRejected failure) {
            for (Throwable cleanupFailure : failure.getSuppressed()) {
                failure.getCause().addSuppressed(cleanupFailure);
            }
            failStartup(failure.getCause());
            return true;
        }
    }

    private void awaitStartupCleanup() throws IOException, InterruptedException, ExecutionException {
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
        if (!removeStaleConnections(discovered, removed)) {
            return false;
        }
        closeConnections(removed, closeFailure -> recordCleanupFailure(closeFailure, null));
        Throwable cleanupFailure = initial ? cleanupOutcome() : null;
        if (cleanupFailure != null) {
            throw new IOException("Unable to close an NSQ connection", cleanupFailure);
        }
        if (!connectDiscovered(discovered, initial)) {
            return false;
        }
        if (!allConnected(discovered)) {
            return false;
        }
        // A connection can drop while the publisher check runs, so readiness checks them again
        // after it. A refresh ignores the result and needs neither.
        return !initial || (publisherPresent.test(discovered) && allConnected(discovered));
    }

    // Moves dead or no longer discovered connections into removed; returns false once closed.
    private boolean removeStaleConnections(Set<ServerAddress> discovered, List<Connection> removed) {
        synchronized (lock) {
            if (closed) {
                return false;
            }
            Iterator<Map.Entry<ServerAddress, Connection>> iterator = connections.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<ServerAddress, Connection> entry = iterator.next();
                if (isStale(entry.getKey(), entry.getValue(), discovered)) {
                    iterator.remove();
                    removed.add(entry.getValue());
                }
            }
            return true;
        }
    }

    private static boolean isStale(
            ServerAddress address, Connection connection, Set<ServerAddress> discovered) {
        return !connection.isConnected()
                || !connection.isHeartbeatStatusOK()
                || (!discovered.isEmpty() && !discovered.contains(address));
    }

    // Connects to every discovered address without a connection; returns false once closed.
    private boolean connectDiscovered(Set<ServerAddress> discovered, boolean initial)
            throws Exception {
        for (ServerAddress address : discovered) {
            synchronized (lock) {
                if (closed) {
                    return false;
                }
                if (connections.containsKey(address)) {
                    continue;
                }
            }
            connectOrReport(address, initial);
        }
        return true;
    }

    private void connectOrReport(ServerAddress address, boolean initial) throws Exception {
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
            if (reportConnectFailure(address, initial, failure)) {
                throw failure;
            }
        }
    }

    // Returns true when the failure must abort startup instead of being retried.
    private boolean reportConnectFailure(ServerAddress address, boolean initial, Exception failure) {
        if (closed) {
            return false;
        }
        if (initial && cleanupOutcome() != null) {
            return true;
        }
        if (initial) {
            lastConnectionFailure = failure;
            log.debug("Unable to connect NSQ consumer to {}", address, failure);
        } else {
            log.warn("Unable to connect NSQ consumer to {}", address, failure);
        }
        return false;
    }

    private boolean allConnected(Set<ServerAddress> discovered) {
        synchronized (lock) {
            return !closed
                    && !discovered.isEmpty()
                    && discovered.stream()
                            .allMatch(address -> {
                                Connection connection = connections.get(address);
                                return connection != null && connection.isConnected();
                            });
        }
    }

    // The subscribe failure, including an interruption, is rethrown unchanged after cleanup
    @SuppressWarnings("InterruptedExceptionSwallowed")
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
            connection = settings.connectionFactory().connect(address, config);
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
            closeConnections(
                    List.of(connection), closeFailure -> recordCleanupFailure(closeFailure, null));
            cleanup(() -> eventLoopGroup.closeAttempt(attemptChannels), null);
            return;
        }
        try {
            subscribe(address, connection, consumer);
        } catch (Exception | Error cause) {
            releaseAttempt(connection, attemptChannels, cause);
            throw cause;
        }
        releaseAttempt(connection, attemptChannels, null);
    }

    private void subscribe(ServerAddress address, Connection connection, NSQConsumer consumer)
            throws IOException, InterruptedException, TimeoutException, SubscriptionRejected {
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
    }

    // Closes a connection that subscribe(...) did not hand over to connections, then releases the
    // attempt's channels. A close failure is rethrown only when there is no primary failure to
    // attach it to, and never before the channels are released.
    private void releaseAttempt(
            Connection connection, List<Channel> attemptChannels, Throwable failure) {
        boolean stillOwned;
        synchronized (lock) {
            stillOwned = connecting.remove(connection);
        }
        if (!stillOwned) {
            return;
        }
        Throwable unreported = null;
        try {
            connection.close();
        } catch (RuntimeException | Error closeFailure) {
            recordCleanupFailure(closeFailure, failure);
            if (failure == null) {
                unreported = closeFailure;
            }
        }
        cleanup(() -> eventLoopGroup.closeAttempt(attemptChannels), failure);
        if (unreported instanceof Error error) {
            throw error;
        }
        if (unreported instanceof RuntimeException runtime) {
            throw runtime;
        }
    }

    private void completeCreation() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            refresh =
                    scheduler.scheduleAtFixedRate(
                            this::scheduleRefresh,
                            settings.lookupPeriodMillis(),
                            settings.lookupPeriodMillis(),
                            TimeUnit.MILLISECONDS);
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
            connector.execute(() -> {
                try {
                    refreshConnections(false);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                } catch (Exception failure) {
                    log.warn("Unable to refresh NSQ consumer connections", failure);
                } finally {
                    refreshing.set(false);
                }
            });
        } catch (RejectedExecutionException _) {
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
        closeConnections(
                closing.connecting(), closeFailure -> recordCleanupFailure(closeFailure, failure));
        closeConnections(closing.active(), closeFailure -> recordCleanupFailure(closeFailure, failure));
        cleanup(() -> stopScheduler(initiator), failure);
    }

    // The interrupt is deferred until every executor has terminated, then restored
    @SuppressWarnings("java:S2142")
    private void awaitResourceTermination() {
        boolean interrupted = false;
        for (ExecutorService executor : List.of(connector, callbacks, scheduler)) {
            boolean terminated = false;
            while (!terminated) {
                try {
                    terminated = executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        awaitPendingCloses();
        io.netty.util.concurrent.Future<?> channels = eventLoopGroup.terminationFuture();
        channels.awaitUninterruptibly();
        if (!channels.isSuccess()) {
            recordCleanupFailure(channels.cause(), startupFailure);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    // Waits for pending channel closes without being interruptible, recording a close failure.
    // join() leaves the thread's interrupt status untouched, so a pending interrupt is preserved.
    private void awaitPendingCloses() {
        try {
            eventLoopGroup.pendingCloses().join();
        } catch (CompletionException failure) {
            recordCleanupFailure(failure.getCause(), startupFailure);
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
            outcome.get(settings.closeWaitMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw interrupted;
        } catch (ExecutionException failed) {
            Failures.rethrow(failed.getCause());
        } catch (TimeoutException timeout) {
            TimeoutException failureToClose =
                    new TimeoutException(
                            "NSQ consumer close did not finish within " + settings.closeWaitMillis() + " ms");
            failureToClose.initCause(timeout);
            throw failureToClose;
        }
    }

    private void closeReady(Closing closing, Thread initiator) throws Exception {
        Failures failures = new Failures();
        failures.close(eventLoopGroup::stopConnecting);
        failures.close(() -> stopConnector(initiator));
        closeConnections(closing.connecting(), failures::add);
        closeGracefully(closing, initiator, failures);
        Failures.rethrow(failures.failure());
    }

    private void closeGracefully(Closing closing, Thread initiator, Failures failures) {
        requestStartClose(closing.active(), failures);
        drainCallbacks(failures);
        finishClose(closing, failures, initiator);
    }

    // Asks each connected connection to stop delivering; stops at the first interruption.
    private static void requestStartClose(List<Connection> active, Failures failures) {
        for (Connection connection : active) {
            Throwable closeFailure = startClose(connection);
            failures.add(closeFailure);
            if (closeFailure instanceof InterruptedException) {
                break;
            }
        }
    }

    private static Throwable startClose(Connection connection) {
        try {
            if (!connection.isConnected()) {
                return null;
            }
            return startCloseFailure(connection.commandAndWait(NSQCommand.startClose()));
        } catch (Throwable closeFailure) {
            return closeFailure;
        }
    }

    private static Throwable startCloseFailure(NSQFrame response) {
        if (response instanceof ErrorFrame error && error.getErrorMessage().startsWith("E_INVALID")) {
            return new IllegalStateException(error.getErrorMessage());
        }
        if (response == null) {
            Thread.currentThread().interrupt();
            return new InterruptedException("NSQ consumer close interrupted");
        }
        return null;
    }

    private void drainCallbacks(Failures failures) {
        try {
            callbacks.shutdown();
            if (!callbacks.awaitTermination(CALLBACK_DRAIN_SECONDS, TimeUnit.SECONDS)) {
                callbacks.shutdownNow();
                failures.add(new TimeoutException("NSQ consumer callbacks did not finish during close"));
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            callbacks.shutdownNow();
            failures.add(interrupted);
        } catch (Throwable closeFailure) {
            failures.add(closeFailure);
        }
    }

    private void finishClose(Closing closing, Failures failures, Thread initiator) {
        closeConnections(closing.active(), failures::add);
        failures.close(eventLoopGroup::closeChannels);
        failures.close(() -> stopScheduler(initiator));
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

    // Closes every connection, handing each close failure to onFailure.
    private static void closeConnections(
            List<Connection> connections, Consumer<Throwable> onFailure) {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (Throwable closeFailure) {
                onFailure.accept(closeFailure);
            }
        }
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
            Failures.suppress(primary, failure);
            if (startupFailure != primary) {
                Failures.suppress(startupFailure, failure);
            }
        }
    }

    private Throwable cleanupOutcome() {
        synchronized (cleanupLock) {
            Failures outcome = new Failures();
            cleanupFailures.forEach(outcome::add);
            return outcome.failure();
        }
    }

    private static void complete(CompletableFuture<Void> future, Throwable failure) {
        if (failure == null) {
            future.complete(null);
        } else {
            future.completeExceptionally(failure);
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
            if (!abandonmentRequested.compareAndSet(false, true)) {
                return;
            }
            CancellationException failure =
                    new CancellationException("NSQ consumer creation was abandoned");
            Thread initiator = Thread.currentThread();
            daemonFactory("nsq-consumer-abandon", null)
                    .newThread(() -> {
                        Closing closing = beginClose(true, failure);
                        if (closing != null) {
                            startClosing(closing, failure, true, initiator, true);
                        }
                    })
                    .start();
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
