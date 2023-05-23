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

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

/**
 * Logic of queue.
 */
@Slf4j
public class QueueServiceImpl implements QueueService {
    private ExchangeContainer exchangeContainer;
    private QueueContainer queueContainer;
    private Map<String, String> exToRealExMap = new ConcurrentHashMap<>();
    private Map<String, CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>>> futureMap;
    private static Object lock = new ReentrantLock();

    public QueueServiceImpl(ExchangeContainer exchangeContainer,
                            QueueContainer queueContainer) {
        this.exchangeContainer = exchangeContainer;
        this.queueContainer = queueContainer;
        futureMap = exchangeContainer.getExFutureMap();
    }

    @SneakyThrows
    @Override
    public void queueDeclare(AmqpChannel channel, AMQShortString queue, boolean passive, boolean durable,
                             boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
        log.info("amqp channel queueDeclare, queueName=" + queue.toString());
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        log.info("RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
            channelId, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        if ((queue == null) || (queue.length() == 0)) {
            queue = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
        }
        AMQShortString finalQueue = queue;
        // log.info"asyncGetQueue------------------"+connection.getNamespaceName()+"-------------"+finalQueue.toString()+"------------"+!passive
//        +"------exclusive"+exclusive);
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
            queueContainer.asyncGetQueue(connection.getNamespaceName(), finalQueue.toString(), !passive);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            // log.info"amqpQueue---------------"+amqpQueue);
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
            } else {
                if (null == amqpQueue) {
                    // log.info"finalQueue------------------"+finalQueue);
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + finalQueue);
                } else {
                    channel.checkExclusiveQueue(amqpQueue);
                    channel.setDefaultQueue(amqpQueue);
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(finalQueue, 0, 0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            }
        });
    }

    @Override
    public void queueDelete(AmqpChannel channel, AMQShortString queue, boolean ifUnused,
                            boolean ifEmpty, boolean nowait) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                ifUnused, ifEmpty, nowait);
        }
        if ((queue == null) || (queue.length() == 0)) {
            //get the default queue on the channel:
            AmqpQueue amqpQueue = channel.getDefaultQueue();
            delete(channel, amqpQueue);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                log.info("amqpQueue------------" + amqpQueue);
                if (throwable != null) {
                    log.error("Failed to get topic from queue container", throwable);
                    channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
                } else {
                    if (null == amqpQueue) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queue.toString());
                    } else {
                        delete(channel, amqpQueue);
                    }
                }
            });
        }
    }

    @SneakyThrows
    @Override
    public void queueBind(AmqpChannel channel, AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                          boolean nowait, FieldTable argumentsTable) {
        // log.info"amqp connection queueBind");
        if (Objects.isNull(exchange)) {
            exchange = AMQShortString.createAMQShortString(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
        }
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        PulsarClientImpl pulsarClient = (PulsarClientImpl) connection.getPulsarService().getClient();
        log.info("connection.getPulsarService().getBrokerServiceUrl()-----------" + connection.getPulsarService().getBrokerServiceUrl());
        LookupService lookupService = pulsarClient.getLookup();
        String url = pulsarClient.getConfiguration().getServiceUrl();
        log.info("url----------" + url);
        log.info("queueBind--------------" + exchange + "-------------queueName------------" + queue);
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        Map<String, Object> arguments = FieldTable.convertToMap(argumentsTable);
        //现在要先找到需要bind的exchange了
        int count = 0;
        String bindAddr = url.split("//")[1];
        log.info("exToRealExMap" + exToRealExMap.toString());
//        if(exToRealExMap.get(exchange.toString())!=null){
//            log.info("exToRealExMap.get(exchange)-------------"+exToRealExMap.get(exchange.toString()));
//            exchange = AMQShortString.valueOf(exToRealExMap.get(exchange.toString()));
//        }else {
////            synchronized (lock) {
//                while (true) {
//                    String topic = TopicName.get(TopicDomain.persistent.value(),
//                            connection.getNamespaceName(), PersistentExchange.TOPIC_PREFIX + exchange + "_" + count).toString();
//                    log.info("QueueServiceImpl futureMap--------------------" + futureMap.get(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count));
//                    futureMap.putIfAbsent(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count, lookupService.getBroker(TopicName.get(topic)));
//                    CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookupData = futureMap.get(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count);
//                    Pair<InetSocketAddress, InetSocketAddress> pair = lookupData.get();
//                    log.info("pair-----" + pair.getKey() + "--------bindAddr--------" + bindAddr);
//                    if (bindAddr.equals(pair.getKey().toString())) {
//                        exToRealExMap.put(exchange.toString(), exchange + "_" + count);
//                        exchange = AMQShortString.valueOf(exchange + "_" + count);
//                        break;
//                    }
//                    count++;
//                }
////            }
//        }

        if (queue == null || StringUtils.isEmpty(queue.toString())) {
            AmqpQueue amqpQueue = channel.getDefaultQueue();
            if (amqpQueue != null && bindingKey == null) {
                bindingKey = AMQShortString.valueOf(amqpQueue.getName());
            }
            // log.info"exchange:"+exchange);
            bind(channel, exchange, amqpQueue, bindingKey.toString(), arguments);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            AMQShortString finalBindingKey = bindingKey;
            AMQShortString finalExchange = exchange;
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                log.info("amqpQueue---------------------" + amqpQueue);
                if (throwable != null) {
                    log.error("Failed to get topic from queue container", throwable);
                    channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
                } else {
                    if (amqpQueue == null) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                        return;
                    }
                    channel.checkExclusiveQueue(amqpQueue);
                    if (null == finalBindingKey) {
                        // log.info"exchange:"+finalExchange);
                        bind(channel, finalExchange, amqpQueue, amqpQueue.getName(), arguments);
                    } else {
                        // log.info"exchange:"+finalExchange);
                        bind(channel, finalExchange, amqpQueue, finalBindingKey.toString(), arguments);
                    }
                }
            });
        }
    }

    @Override
    public void queueUnbind(AmqpChannel channel, AMQShortString queue, AMQShortString exchange,
                            AMQShortString bindingKey, FieldTable arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                exchange, bindingKey, arguments);
        }
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
            queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                    return;
                }
                channel.checkExclusiveQueue(amqpQueue);
                String exchangeName;
                if (channel.isDefaultExchange(exchange)) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = exchange.toString();
                }
                CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                    exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName, false, null);
                amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable1) -> {
                    if (throwable1 != null) {
                        log.error("Failed to get topic from exchange container", throwable1);
//                        connection.close();
                        channel.closeChannel(INTERNAL_ERROR, "Failed to get exchange: " + throwable1.getMessage());
                    } else {
                        try {
                            amqpQueue.unbindExchange(amqpExchange);
                            if (amqpExchange.getAutoDelete() && (amqpExchange.getQueueSize() == 0)) {
                                exchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                                amqpExchange.getTopic().delete().get();
                            }
                            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
                            connection.writeFrame(responseBody.generateFrame(channelId));
                        } catch (Exception e) {
                            connection.sendConnectionClose(INTERNAL_ERROR,
                                "unbind failed:" + e.getMessage(), channelId);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void queuePurge(AmqpChannel channel, AMQShortString queue, boolean nowait) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueuePurge[ queue: {}, nowait:{} ]", channelId, queue, nowait);
        }
        // TODO queue purge process

        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(0);
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    private void delete(AmqpChannel channel, AmqpQueue amqpQueue) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (amqpQueue == null) {
            channel.closeChannel(ErrorCodes.NOT_FOUND, "Queue does not exist.");
        } else {
            channel.checkExclusiveQueue(amqpQueue);
            queueContainer.deleteQueue(connection.getNamespaceName(), amqpQueue.getName());
            // TODO delete the binding with the default exchange and delete the topic in pulsar.

            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        }
    }

    private void bind(AmqpChannel channel, AMQShortString exchange, AmqpQueue amqpQueue,
                      String bindingKey, Map<String, Object> arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        String exchangeName = channel.isDefaultExchange(exchange)
            ? AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE : exchange.toString();
        if (exchangeName.equals(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)) {
            channel.closeChannel(ErrorCodes.ACCESS_REFUSED, "Can not bind to default exchange ");
        }
        String exchangeType = null;
        boolean createIfMissing = false;
        if (channel.isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = channel.getExchangeType(exchange.toString());
        }
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
            exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName,
                createIfMissing, exchangeType);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from exchange container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get exchange: " + throwable.getMessage());
            } else {
                AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(amqpExchange.getType());
                if (messageRouter == null) {
                    connection.sendConnectionClose(INTERNAL_ERROR, "Unsupported router type!", channelId);
                    return;
                }
                try {
                    amqpQueue.bindExchange(amqpExchange, messageRouter,
                        bindingKey, arguments);
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                    log.info("[{}] amqp queueBind remoteAddress: [{}] localAddress: [{}]", connection.ctx.name(), connection.ctx.channel().remoteAddress(), connection.ctx.channel().localAddress());
                    connection.writeFrame(responseBody.generateFrame(channelId));
                } catch (Exception e) {
                    log.warn("Failed to bind queue[{}] with exchange[{}].", amqpQueue.getName(), exchange, e);
                    connection.sendConnectionClose(INTERNAL_ERROR,
                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
                }
            }
        });
    }

}
