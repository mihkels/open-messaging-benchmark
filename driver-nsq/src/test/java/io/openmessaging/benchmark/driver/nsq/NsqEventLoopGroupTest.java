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
}
