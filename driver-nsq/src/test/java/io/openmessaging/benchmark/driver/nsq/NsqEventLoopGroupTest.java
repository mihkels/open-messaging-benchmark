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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class NsqEventLoopGroupTest {
    @Test
    void registrationFailureClosesTheChannelAndRetainsTheOriginalCause() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        when(channel.unsafe()).thenReturn(unsafe);
        RuntimeException failure = new RuntimeException("registration failed");
        RuntimeException cleanupFailure = new RuntimeException("close failed");
        when(delegate.register(channel)).thenThrow(failure);
        doThrow(cleanupFailure).when(unsafe).closeForcibly();
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        assertThatThrownBy(() -> scope.register(channel)).isSameAs(failure);
        assertThat(failure.getSuppressed()).containsExactly(cleanupFailure);
        verify(unsafe).closeForcibly();
        scope.closeChannels();
        assertThat(scope.isTerminated()).isFalse();
        assertThat(scope.pendingCloses()).isNotDone();
        open.set(false);
        channelClosed.setSuccess();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
        assertThat(scope.isTerminated()).isTrue();
        assertThat(scope.terminationFuture().cause()).isSameAs(cleanupFailure);
    }

    @Test
    void terminationViewsCannotCompleteTheOwnedPromise() {
        NsqEventLoopGroup scope = new NsqEventLoopGroup(mock(EventLoopGroup.class));
        io.netty.util.concurrent.Future<?> view = scope.terminationFuture();
        assertThat(view.cancel(false)).isFalse();
        assertThat(((Promise<?>) view).trySuccess(null)).isTrue();

        assertThat(scope.isTerminated()).isFalse();
        assertThat(scope.terminationFuture().isDone()).isFalse();
        scope.closeChannels();
        assertThat(scope.terminationFuture().isSuccess()).isTrue();
    }

    @Test
    void terminationWaitsForBothTheCloseRequestAndTheActualChannelClose() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        ChannelPromise registration =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise closeRequest =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        when(delegate.register(channel)).thenReturn(registration);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.close()).thenReturn(closeRequest);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);
        scope.register(channel);
        registration.setSuccess();

        scope.closeChannels();
        assertThat(scope.pendingCloses()).isNotDone();
        assertThat(scope.terminationFuture().isDone()).isFalse();
        closeRequest.setSuccess();
        assertThat(scope.pendingCloses()).isNotDone();
        assertThat(scope.terminationFuture().isDone()).isFalse();

        open.set(false);
        channelClosed.setSuccess();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(scope.terminationFuture().isSuccess()).isTrue();
    }

    @Test
    void registrationExceptionTracksAnAlreadyRegisteredChannelsAsynchronousClose() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        ChannelPromise closeRequest =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        RuntimeException failure = new RuntimeException("registration failed");
        when(delegate.register(channel)).thenThrow(failure);
        when(channel.isRegistered()).thenReturn(true);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.close()).thenReturn(closeRequest);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);
        scope.beginAttempt();

        assertThatThrownBy(() -> scope.register(channel)).isSameAs(failure);
        scope.endAttempt(false);
        scope.closeChannels();
        closeRequest.setSuccess();
        assertThat(scope.pendingCloses()).isNotDone();
        assertThat(scope.terminationFuture().isDone()).isFalse();
        open.set(false);
        channelClosed.setSuccess();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        verify(channel).close();
    }

    @Test
    void rejectedLateRegistrationTracksChannelEvenAfterScopeTermination() throws Exception {
        NsqEventLoopGroup scope = new NsqEventLoopGroup(mock(EventLoopGroup.class));
        scope.closeChannels();
        assertThat(scope.terminationFuture().isSuccess()).isTrue();
        Channel channel = mock(Channel.class);
        Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        when(channel.unsafe()).thenReturn(unsafe);
        RuntimeException failure = new RuntimeException("force close failed");
        doThrow(failure).when(unsafe).closeForcibly();

        assertThat(scope.register(channel).cause()).isInstanceOf(RejectedExecutionException.class);
        assertThat(scope.cleanupFailure()).isSameAs(failure);
        assertThat(scope.pendingCloses()).isNotDone();

        open.set(false);
        channelClosed.setSuccess();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
    }

    @Test
    void failureToObserveChannelCloseStillClosesTheChannel() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        ChannelPromise registration =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
        ChannelPromise closeRequest =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
        RuntimeException failure = new RuntimeException("cannot observe channel close");
        when(delegate.register(channel)).thenReturn(registration);
        when(channel.closeFuture()).thenThrow(failure);
        when(channel.close()).thenReturn(closeRequest);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        assertThatThrownBy(() -> scope.register(channel)).isSameAs(failure);
        scope.closeChannels();
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(scope.terminationFuture().cause()).isSameAs(failure);
        verify(channel).close();
    }

    @Test
    void registrationAndCloseObservationFailuresAreBothPreserved() {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        RuntimeException registrationFailure = new RuntimeException("registration failed");
        RuntimeException observationFailure = new RuntimeException("cannot observe channel close");
        when(delegate.register(channel)).thenThrow(registrationFailure);
        when(channel.closeFuture()).thenThrow(observationFailure);
        when(channel.unsafe()).thenReturn(unsafe);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        assertThatThrownBy(() -> scope.register(channel)).isSameAs(registrationFailure);
        assertThat(registrationFailure.getSuppressed()).containsExactly(observationFailure);
        verify(unsafe).closeForcibly();
        scope.closeChannels();
        assertThat(scope.terminationFuture().cause()).isSameAs(observationFailure);
    }

    @Test
    void failedChannelCloseIsReportedAfterForcedCleanup() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Channel channel = mock(Channel.class);
        Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        ChannelPromise registration =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise closeRequest =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        AtomicBoolean open = new AtomicBoolean(true);
        when(delegate.register(channel)).thenReturn(registration);
        when(channel.closeFuture()).thenReturn(channelClosed);
        when(channel.close()).thenReturn(closeRequest);
        when(channel.unsafe()).thenReturn(unsafe);
        when(channel.isOpen()).thenAnswer(invocation -> open.get());
        org.mockito.Mockito.doAnswer(
                        invocation -> {
                            open.set(false);
                            return null;
                        })
                .when(unsafe)
                .closeForcibly();
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);
        scope.register(channel);
        registration.setSuccess();
        scope.closeChannels();
        RuntimeException failure = new RuntimeException("asynchronous channel close failed");
        closeRequest.setFailure(failure);

        scope.pendingCloses().get(5, TimeUnit.SECONDS);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(scope.terminationFuture().cause()).isSameAs(failure);
        verify(unsafe).closeForcibly();
    }

    @Test
    void attemptsAreRejectedAfterCloseAndCannotOverlap() {
        NsqEventLoopGroup scope = new NsqEventLoopGroup(mock(EventLoopGroup.class));
        assertThat(scope.endAttempt(true)).isEmpty();
        scope.beginAttempt();
        assertThatThrownBy(scope::beginAttempt).isInstanceOf(IllegalStateException.class);
        assertThat(scope.endAttempt(true)).isEmpty();

        assertThat(scope.isShuttingDown()).isFalse();
        scope.stopConnecting();
        assertThat(scope.isShuttingDown()).isTrue();
        assertThat(scope.isShutdown()).isTrue();
        assertThatThrownBy(scope::beginAttempt).isInstanceOf(RejectedExecutionException.class);
    }

    @Test
    void shutdownMethodsCloseChannelsAndReportTermination() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        scope.shutdown();
        assertThat(scope.awaitTermination(1, TimeUnit.SECONDS)).isTrue();
        assertThat(scope.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).isSuccess()).isTrue();
        scope.next();
        var _ = scope.iterator();
        verify(delegate).next();
        verify(delegate).iterator();
    }

    @Test
    void promiseForAnotherChannelIsRejected() {
        NsqEventLoopGroup scope = new NsqEventLoopGroup(mock(EventLoopGroup.class));
        Channel channel = mock(Channel.class);
        ChannelPromise other =
                new DefaultChannelPromise(mock(Channel.class), ImmediateEventExecutor.INSTANCE);

        assertThatThrownBy(() -> scope.register(channel, other))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void promiseRegistrationIsDelegatedAndRejectedAfterClose() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Fixture first = new Fixture();
        ChannelPromise promise =
                new DefaultChannelPromise(first.channel, ImmediateEventExecutor.INSTANCE);
        when(delegate.register(promise)).thenReturn(promise);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        assertThat(scope.register(first.channel, promise)).isSameAs(promise);
        promise.setSuccess();
        scope.closeChannels();
        first.closeRequest.setSuccess();
        first.closed();

        Fixture late = new Fixture();
        ChannelPromise latePromise =
                new DefaultChannelPromise(late.channel, ImmediateEventExecutor.INSTANCE);
        assertThat(scope.register(latePromise)).isSameAs(latePromise);
        assertThat(latePromise.cause()).isInstanceOf(RejectedExecutionException.class);
        late.closed();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
    }

    @Test
    void registrationErrorIsRethrown() {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Fixture fixture = new Fixture();
        AssertionError failure = new AssertionError("registration error");
        when(delegate.register(fixture.channel)).thenThrow(failure);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);

        assertThatThrownBy(() -> scope.register(fixture.channel)).isSameAs(failure);
        verify(fixture.unsafe).closeForcibly();
    }

    @Test
    void asynchronousRegistrationFailureForcesTheChannelClosed() throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Fixture fixture = new Fixture();
        when(delegate.register(fixture.channel)).thenReturn(fixture.registration);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);
        scope.register(fixture.channel);

        fixture.registration.setFailure(new RuntimeException("registration failed"));

        verify(fixture.unsafe).closeForcibly();
        fixture.closed();
        scope.pendingCloses().get(5, TimeUnit.SECONDS);
    }

    @Test
    void severalChannelsCloseTogetherAndAccumulateDistinctFailures() throws Exception {
        assertSeveralChannelFailures(false);
        assertSeveralChannelFailures(true);
    }

    private static void assertSeveralChannelFailures(boolean sameFailure) throws Exception {
        EventLoopGroup delegate = mock(EventLoopGroup.class);
        Fixture first = new Fixture();
        Fixture second = new Fixture();
        RuntimeException firstFailure = new RuntimeException("first");
        RuntimeException secondFailure = sameFailure ? firstFailure : new RuntimeException("second");
        doThrow(firstFailure).when(first.unsafe).closeForcibly();
        doThrow(secondFailure).when(second.unsafe).closeForcibly();
        when(delegate.register(first.channel)).thenReturn(first.registration);
        when(delegate.register(second.channel)).thenReturn(second.registration);
        NsqEventLoopGroup scope = new NsqEventLoopGroup(delegate);
        scope.register(first.channel);
        scope.register(second.channel);
        first.registration.setSuccess();
        second.registration.setSuccess();

        scope.closeChannels();
        first.closeRequest.setFailure(firstFailure);
        first.closed();
        assertThat(scope.pendingCloses()).isNotDone();
        second.closeRequest.setFailure(secondFailure);
        second.closed();

        scope.pendingCloses().get(5, TimeUnit.SECONDS);
        assertThat(scope.terminationFuture().await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(scope.terminationFuture().cause()).isSameAs(firstFailure);
        if (sameFailure) {
            assertThat(firstFailure.getSuppressed()).isEmpty();
        } else {
            assertThat(firstFailure.getSuppressed()).containsOnly(secondFailure);
        }
    }

    private static final class Fixture {
        private final Channel channel = mock(Channel.class);
        private final Channel.Unsafe unsafe = mock(Channel.Unsafe.class);
        private final ChannelPromise registration =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        private final ChannelPromise closeRequest =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        private final ChannelPromise channelClosed =
                new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        private final AtomicBoolean open = new AtomicBoolean(true);

        private Fixture() {
            when(channel.closeFuture()).thenReturn(channelClosed);
            when(channel.close()).thenReturn(closeRequest);
            when(channel.unsafe()).thenReturn(unsafe);
            when(channel.isOpen()).thenAnswer(invocation -> open.get());
        }

        private void closed() {
            open.set(false);
            channelClosed.trySuccess();
        }
    }
}
