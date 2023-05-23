/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streamnative.pulsar.handlers.amqp;

import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

/**
 * Logic of exchange.
 */
@Slf4j
public class ExchangeServiceImpl implements ExchangeService {
    private ExchangeContainer exchangeContainer;
    private String bindAddr;
    private ConcurrentHashMap<String, String> exMap = new ConcurrentHashMap<>();
    private Map<String, CompletableFuture<Pair<String, Integer>>> exFutureMap = new ConcurrentHashMap<>();
//    Semaphore semaphore = new Semaphore(1);

    public ExchangeServiceImpl(ExchangeContainer exchangeContainer, String bindAddr) {
        this.exchangeContainer = exchangeContainer;
        this.bindAddr = bindAddr;
//        this.exFutureMap = exchangeContainer.getExFutureMap();
    }

    private void handleDefaultExchangeInExchangeDeclare(AmqpChannel channel, AMQShortString exchange) {
        if (isDefaultExchange(exchange)) {
            StringBuffer sb = new StringBuffer();
            sb.append("Attempt to redeclare default exchange: of type ")
                .append(ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
            channel.closeChannel(ErrorCodes.ACCESS_REFUSED, sb.toString());
        }
    }

    private String formatString(String s) {
        return s.replaceAll("\r", "").
            replaceAll("\n", "").trim();
    }

    @SneakyThrows
    @Override
    public void exchangeDeclare(AmqpChannel channel, AMQShortString exchange, AMQShortString type,
                                boolean passive, boolean durable, boolean autoDelete,
                                boolean internal, boolean nowait, FieldTable arguments) {
//        semaphore.acquire();
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDeclare[ exchange: {},"
                    + " type: {}, passive: {}, durable: {}, autoDelete: {}, internal: {}, "
                    + "nowait: {}, arguments: {} ]", channelId, exchange,
                type, passive, durable, autoDelete, internal, nowait, arguments);
        }
        if (isDefaultExchange(exchange)) {
            handleDefaultExchangeInExchangeDeclare(channel, exchange);
            return;
        }
        log.info("[{}] exchangeDeclare exchange={}", connection.ctx.name(), exchange);
        String exchangeName = formatString(exchange.toString());
        boolean createIfMissing = !passive;
        String exchangeType = type.toString();
        if (channel.isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = channel.getExchangeType(exchange.toString());
        }
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
            exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName,
                createIfMissing, exchangeType);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            // log.info("amqpExchange-------------"+amqpExchange);
            if (throwable != null) {
                log.error("Failed to get topic from exchange container", throwable);
                connection.sendConnectionClose(ErrorCodes.NOT_FOUND, "Unknown exchange: " + exchangeName, channelId);
            } else {
                if (null == amqpExchange) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange:" + exchangeName);
                } else {
                    if (!(type == null || type.length() == 0)
                        && !amqpExchange.getType().toString().equalsIgnoreCase(type.toString())) {
                        connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED,
                            "Attempt to redeclare exchange: '"
                                + exchangeName + "' of type " + amqpExchange.getType()
                                + " to " + type + ".", channelId);
                    } else if (!nowait) {
                        AMQMethodBody declareOkBody = connection.getMethodRegistry().createExchangeDeclareOkBody();
                        log.info("[{}] declareOkBody:[{}], remoteAddress:[{}]", connection.ctx.name(), declareOkBody, connection.ctx.channel().remoteAddress());
                        connection.writeFrame(declareOkBody.generateFrame(channelId));
                    }
                }
            }
        });
//        semaphore.release();
    }

    @Override
    public void exchangeDelete(AmqpChannel channel, AMQShortString exchange, boolean ifUnused, boolean nowait) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                nowait);
        }
        if (isDefaultExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Default Exchange cannot be deleted. ",
                channelId);
        } else if (isBuildInExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "BuildIn Exchange cannot be deleted. ",
                channelId);
        } else {
            String exchangeName = formatString(exchange.toString());
            CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName, false, null);
            amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to get topic from exchange container", throwable);
                } else {
                    if (null == amqpExchange) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + exchangeName + "'");
                    } else {
                        PersistentTopic topic = (PersistentTopic) amqpExchange.getTopic();

//                        List<Consumer> a = topic.getSubscriptions().get("a").getConsumers();

                        if (ifUnused && topic.getSubscriptions().isEmpty()) {
                            channel.closeChannel(ErrorCodes.IN_USE, "Exchange has bindings. ");
                        } else {
                            try {
                                exchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                                topic.delete().get();
                                ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().
                                    createExchangeDeleteOkBody();
                                connection.writeFrame(responseBody.generateFrame(channelId));
                            } catch (Exception e) {
                                connection.sendConnectionClose(INTERNAL_ERROR,
                                    "Catch a PulsarAdminException: " + e.getMessage()
                                        + ". channelId: ", channelId);
                            }
                        }
                    }
                }
            });
        }
    }

    @Override
    public void exchangeBound(AmqpChannel channel, AMQShortString exchange, AMQShortString routingKey,
                              AMQShortString queueName) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                routingKey, queueName);
        }

        String exchangeName = formatString(exchange.toString());
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
            exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName, false, null);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from exchange container", throwable);
                connection.sendConnectionClose(ErrorCodes.NOT_FOUND, "Unknown exchange: " + exchangeName, channelId);
            } else {
                int replyCode;
                StringBuilder replyText = new StringBuilder();
                if (null == amqpExchange) {
                    replyCode = ExchangeBoundOkBody.EXCHANGE_NOT_FOUND;
                    replyText = replyText.insert(0, "Exchange '").append(exchange).append("' not found");
                } else {
                    List<String> subs = amqpExchange.getTopic().getSubscriptions().keys();
                    if (null == subs || subs.isEmpty()) {
                        replyCode = ExchangeBoundOkBody.QUEUE_NOT_FOUND;
                        replyText = replyText.insert(0, "Queue '").append(queueName).append("' not found");
                    } else {
                        replyCode = ExchangeBoundOkBody.OK;
                        replyText = null;
                    }
                }
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                    .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(replyText));
                connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
            }
        });
    }

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    private boolean isBuildInExchange(final AMQShortString exchangeName) {
        if (exchangeName.toString().equals(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
            || (exchangeName.toString().equals(ExchangeDefaults.FANOUT_EXCHANGE_NAME))
            || (exchangeName.toString().equals(ExchangeDefaults.TOPIC_EXCHANGE_NAME))) {
            return true;
        } else {
            return false;
        }
    }

}
