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
package io.openmessaging.benchmark.driver.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RabbitMqBenchmarkConsumer extends DefaultConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqBenchmarkConsumer.class);

    // Stream queues do not support automatic acknowledgements and require a prefetch limit
    static final int STREAM_PREFETCH = 1000;

    private final Channel channel;
    private final ConsumerCallback callback;
    private final boolean manualAck;

    /**
     * Private constructor for the RabbitMQ consumer.
     *
     * @param channel The RabbitMQ channel.
     * @param callback The callback to invoke on message receipt.
     * @param manualAck Whether deliveries are acknowledged explicitly instead of automatically.
     */
    RabbitMqBenchmarkConsumer(Channel channel, ConsumerCallback callback, boolean manualAck) {
        super(channel);
        this.channel = channel;
        this.callback = callback;
        this.manualAck = manualAck;
    }

    /**
     * Safely creates and starts a new RabbitMQ benchmark consumer.
     *
     * @param channel The RabbitMQ channel to use.
     * @param queueName The name of the queue to consume from.
     * @param callback The callback for handling received messages.
     * @param manualAck Whether to acknowledge deliveries explicitly, as required by stream queues.
     * @return A new, initialized {@link RabbitMqBenchmarkConsumer}.
     * @throws IOException if the consumer cannot be started.
     */
    public static RabbitMqBenchmarkConsumer create(
            Channel channel, String queueName, ConsumerCallback callback, boolean manualAck)
            throws IOException {
        RabbitMqBenchmarkConsumer consumer =
                new RabbitMqBenchmarkConsumer(channel, callback, manualAck);
        if (manualAck) {
            channel.basicQos(STREAM_PREFETCH);
        }
        channel.basicConsume(queueName, !manualAck, consumer);
        return consumer;
    }

    @Override
    public void handleDelivery(
            String consumerTag, Envelope envelope, BasicProperties properties, byte[] body)
            throws IOException {
        callback.messageReceived(body, properties.getTimestamp().getTime());
        if (manualAck) {
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            channel.close();
        } catch (AlreadyClosedException e) {
            log.warn("Channel already closed", e);
        }
    }
}
