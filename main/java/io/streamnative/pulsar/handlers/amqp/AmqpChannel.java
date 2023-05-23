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
import static org.apache.qpid.server.transport.util.Functions.hex;
import static org.apache.qpid.server.transport.util.Functions.str;

import com.google.common.annotations.VisibleForTesting;

import io.kubernetes.client.openapi.StringUtil;
import io.streamnative.pulsar.handlers.amqp.flow.AmqpFlowCreditManager;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.ConsumerTagInUseException;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.*;
import org.apache.qpid.server.txn.AsyncCommand;
import org.apache.qpid.server.txn.ServerTransaction;

/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    private final int channelId;
    private final AmqpConnection connection;
    private final AtomicBoolean blocking = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final Queue<AsyncCommand> unfinishedCommandsQueue = new ConcurrentLinkedQueue<>();
    private long confirmedMessageCounter;
    private volatile ServerTransaction transaction;
    private boolean confirmOnPublish;
    /**
     * A channel has a default queue (the last declared) that is used when no queue name is explicitly set.
     */
    private volatile AmqpQueue defaultQueue;

    private final UnacknowledgedMessageMap unacknowledgedMessageMap;

    /**
     * Maps from consumer tag to consumers instance.
     */
    private final Map<String, Consumer> tag2ConsumersMap = new ConcurrentHashMap<>();

    private final Map<String, AmqpConsumer> fetchConsumerMap = new ConcurrentHashMap<>();

    /**
     * The current message - which may be partial in the sense that not all frames have been received yet - which has
     * been received by this channel. As the frames are received the message gets updated and once all frames have been
     * received the message can then be routed.
     */
    private IncomingMessage currentMessage;
    private List<IncomingMessage> currentMessageList = new ArrayList<>();

    private final String defaultSubscription = "defaultSubscription";
    public static final AMQShortString EMPTY_STRING = AMQShortString.createAMQShortString((String) null);
    /**
     * This tag is unique per subscription to a queue. The server returns this in response to a basic.consume request.
     */
    private volatile int consumerTag;

    /**
     * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver frame so that
     * value of this represents the <b>last</b> tag sent out.
     */
    private volatile long deliveryTag = 0;
    private final AmqpFlowCreditManager creditManager;
    private final AtomicBoolean blockedOnCredit = new AtomicBoolean(false);
    public static final int DEFAULT_CONSUMER_PERMIT = 1000;
    private ExchangeService exchangeService;
    private QueueService queueService;
    private ExchangeContainer exchangeContainer;
    private QueueContainer queueContainer;
    private ConcurrentHashMap<String, String> exMap;
    private Map<String, CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>>> exFutureMap = new ConcurrentHashMap<>();

    private SocketAddress remoteAddress;
    private SocketAddress localAddress;

    private long msgId;
    private String uuId;

    public AmqpChannel(int channelId, AmqpConnection connection, AmqpBrokerService amqpBrokerService) {
        log.info("AmqpChannel initialized------------");
        this.channelId = channelId;
        this.connection = connection;
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap(this);
        this.creditManager = new AmqpFlowCreditManager(0, 0);
        this.exchangeService = amqpBrokerService.getExchangeService();
        this.queueService = amqpBrokerService.getQueueService();
        this.exchangeContainer = amqpBrokerService.getExchangeContainer();
        this.queueContainer = amqpBrokerService.getQueueContainer();
        this.exMap = amqpBrokerService.getExMap();
        this.remoteAddress = connection.getRemoteAddress();
        this.localAddress = connection.getLocalAddress();
        this.cnxName = connection.ctx.name();
    }

    private String cnxName;


    private AtomicBoolean cnt = new AtomicBoolean(true);

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active,
                                     boolean write, boolean read) {
        log.info("[{}] RECV[{}] AccessRequest[ realm: {}, exclusive: {}, passive: {}, active: {}, write: {}, read: {}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, channelId, realm, exclusive, passive, active, write, read, remoteAddress, localAddress);
        MethodRegistry methodRegistry = connection.getMethodRegistry();

        // We don't implement access control class, but to keep clients happy that expect it always use the "0" ticket.
        AccessRequestOkBody response = methodRegistry.createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(channelId));

    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable,
                                       boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        log.info("[{}] amqp receiveExchangeDeclare exchange: [{}] type: [{}] passive: [{}] durable: [{}] autoDelete: [{}]  remoteAddress: [{}] localAddress: [{}]",
            cnxName, exchange, type, passive, durable, autoDelete, remoteAddress, localAddress);
        // if (cnt.get()){
        //     connection.close();
        //     cnt.compareAndSet(true, false);
        // }
        this.exchangeService.exchangeDeclare(this, exchange, type, passive, durable, autoDelete, internal, nowait,
            arguments);
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        log.info("[{}] amqp receiveExchangeDelete exchange: [{}] isUnused: [{}]  remoteAddress: [{}] localAddress: [{}]",
            cnxName, exchange, ifUnused, remoteAddress, localAddress);
        exchangeService.exchangeDelete(this, exchange, ifUnused, nowait);
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queueName) {
        log.info("[{}] amqp receiveExchangeBound exchange: [{}] routingKey: [{}] queueName: [{}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, exchange, routingKey, queueName, remoteAddress, localAddress);
        exchangeService.exchangeBound(this, exchange, routingKey, queueName);
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                                    boolean autoDelete, boolean nowait, FieldTable arguments) {
        log.info("[{}] amqp receiveExchangeBound queue: [{}] passive: [{}] durable: [{}] exclusive: [{}] autoDelete: [{}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, queue, passive, durable, exclusive, autoDelete, remoteAddress, localAddress);
        queueService.queueDeclare(this, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable argumentsTable) {
        log.info("[{}] amqp receiveQueueBind queue: [{}] exchange: [{}] bindKey: [{}] remoteAddress: [{}] localAddress: [{}]",
            connection.ctx.name(), queue, exchange, bindingKey, remoteAddress, localAddress);
        queueService.queueBind(this, queue, exchange, bindingKey, nowait, argumentsTable);
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        log.info("[{}] amqp receiveQueuePurge queue: [{}] remoteAddress: [{}] localAddress: [{}]",
            connection.ctx.name(), queue, remoteAddress, localAddress);
        queueService.queuePurge(this, queue, nowait);
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        log.info("[{}] amqp receiveQueueDelete queue: [{}] remoteAddress: [{}] localAddress: [{}]",
            connection.ctx.name(), queue, remoteAddress, localAddress);
        queueService.queueDelete(this, queue, ifUnused, ifEmpty, nowait);
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                   FieldTable arguments) {
        log.info("[{}] amqp receiveQueueUnbind queue: [{}] exchange: [{}] bindKey: [{}] remoteAddress: [{}] localAddress: [{}]",
            connection.ctx.name(), queue, exchange, bindingKey, remoteAddress, localAddress);
        queueService.queueUnbind(this, queue, exchange, bindingKey, arguments);
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        log.debug("[{}] amqp receiveBasicQos [prefetchSize: {} prefetchCount: {} global: {}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, prefetchSize, prefetchCount, global, remoteAddress, localAddress);
        if (prefetchSize > 0) {
            closeChannel(ErrorCodes.NOT_IMPLEMENTED, "prefetchSize not supported ");
        }
        creditManager.setCreditLimits(0, prefetchCount);
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
//        MethodRegistry methodRegistry = connection.getMethodRegistry();
//        AMQMethodBody responseBody = methodRegistry.createBasicQosOkBody();
//        connection.writeFrame(responseBody.generateFrame(getChannelId()));
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag,
                                    boolean noLocal, boolean noAck, boolean exclusive,
                                    boolean nowait, FieldTable arguments) {

        log.info("[{}] amqp receiveBasicConsume queue: [{}] consumerTag: [{}] remoteAddress: [{}] localAddress: [{}]",
            cnxName,
            queue,
            consumerTag,
            remoteAddress,
            localAddress);

        final String consumerTag1;
        if (consumerTag == null) {
            consumerTag1 = "consumerTag_" + getNextConsumerTag();
        } else {
            consumerTag1 = consumerTag.toString();
        }
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
            queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get the queue from the queue container", throwable);
                closeChannel(INTERNAL_ERROR, "Internal error: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                } else {
                    PersistentTopic indexTopic = ((PersistentQueue) amqpQueue).getIndexTopic();
                    subscribe(consumerTag1, queue.toString(), indexTopic, noAck, exclusive, nowait);
                }
            }
        });
    }

    private void subscribe(String consumerTag, String queueName, Topic topic,
                           boolean ack, boolean exclusive, boolean nowait) {
        if (consumerTag == null) {
            consumerTag = "consumerTag_" + getNextConsumerTag();
        }
        final String finalConsumerTag = consumerTag;

        CompletableFuture<Void> exceptionFuture = new CompletableFuture<>();
        exceptionFuture.whenComplete((ignored, e) -> {
            if (e != null) {
                closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
                log.error("BasicConsume error queue:{} consumerTag:{} ex {}",
                    queueName, finalConsumerTag, e.getMessage());
            }
        });

        if (tag2ConsumersMap.containsKey(consumerTag)) {
            exceptionFuture.completeExceptionally(
                new ConsumerTagInUseException("Consumer already exists with same consumerTag: " + consumerTag));
        }

        CompletableFuture<Subscription> subscriptionFuture = topic.createSubscription(
            defaultSubscription, CommandSubscribe.InitialPosition.Earliest, false);
        //这里是为了满足云平台的需求
//        topic.truncate().join();
        subscriptionFuture.thenAccept(subscription -> {
            AmqpConsumer consumer = null;
            try {
                consumer = new AmqpConsumer(queueContainer, subscription, exclusive
                    ? CommandSubscribe.SubType.Exclusive :
                    CommandSubscribe.SubType.Shared, topic.getName(), 0, 0,
                    finalConsumerTag, 0, connection.getServerCnx(), "", null,
                    false, CommandSubscribe.InitialPosition.Latest,
                    null, this, finalConsumerTag, queueName, ack);
                // log.info("consumer created---290:" + consumer.getAvailablePermits());
            } catch (BrokerServiceException e) {
                exceptionFuture.completeExceptionally(e);
                return;
            }
            subscription.addConsumer(consumer);
            // log.info("consumer created:---296" + consumer.getAvailablePermits());
            consumer.handleFlow(DEFAULT_CONSUMER_PERMIT);
            // log.info("DEFAULT_CONSUMER_PERMIT:---298" + DEFAULT_CONSUMER_PERMIT);
            // log.info("consumer created:---299" + consumer.getAvailablePermits());
            log.info("[{}] consumer create ----- queue: [{}]", connection.ctx.name(), queueName);
            tag2ConsumersMap.put(finalConsumerTag, consumer);
//            // log.info"consume----------"+channelId);
//            MethodRegistry methodRegistry = connection.getMethodRegistry();
//            AMQMethodBody responseBody = methodRegistry.
//                    createBasicConsumeOkBody(AMQShortString.
//                            createAMQShortString(consumer.getConsumerTag()));
//            connection.writeFrame(responseBody.generateFrame(channelId));

            if (!nowait) {
                // log.info"consume----------"+channelId);
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.
                    createBasicConsumeOkBody(AMQShortString.
                        createAMQShortString(consumer.getConsumerTag()));
                connection.writeFrame(responseBody.generateFrame(channelId));
            }
            exceptionFuture.complete(null);
        });
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        log.info("[{}] amqp receiveBasicCancel consumerTag: [{}] remoteAddress: [{}] localAddress: [{}]",
            cnxName,
            consumerTag,
            remoteAddress,
            localAddress);
        unsubscribeConsumer(AMQShortString.toString(consumerTag));
        if (!noWait) {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            BasicCancelOkBody cancelOkBody = methodRegistry.createBasicCancelOkBody(consumerTag);
            connection.writeFrame(cancelOkBody.generateFrame(channelId));
        }

    }

    @SneakyThrows
    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
                                    boolean immediate) {
        log.info("[{}] amqp receiveBasicPublish[exchange: {} routingKey: {} mandatory: {} immediate: {}] remoteAddress: [{}] localAddress: [{}]",
                cnxName, exchange, routingKey, mandatory, immediate, remoteAddress, localAddress);
        if (isDefaultExchange(exchange)) {
            exchange = AMQShortString.createAMQShortString(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
            List<AMQShortString> rightExV1 = findRightExV1(exchange);
            String queue = PersistentQueue.TOPIC_PREFIX + routingKey;
            Bundle queueBundle = getBundleByTopic(queue);
//            log.info("queue:{}---queueBundle----{}", queue, queueBundle);
            rightExV1.forEach((realEx) -> {
                Bundle exBundle = getBundleByTopic(PersistentExchange.TOPIC_PREFIX + realEx.toString());
                AbstractAmqpExchange.mapForDefaultEx.put(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE, realEx.toString());
                CompletableFuture<AmqpExchange> completableFuture = exchangeContainer.
                        asyncGetExchange(connection.getNamespaceName(), String.valueOf(realEx),
                                true, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
                AMQShortString finalExchange = realEx;
                completableFuture.whenComplete((amqpExchange, throwable) -> {
                    if (null != throwable) {
                        log.error("Get exchange failed. exchange name:{}", AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
                        closeChannel(ErrorCodes.INTERNAL_ERROR, "Get exchange failed. ");
                    } else {
                        String queueName = routingKey.toString();
                        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                                queueContainer.asyncGetQueue(connection.getNamespaceName(), queueName, false);
                        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable1) -> {
                            if (throwable1 != null) {
                                log.error("Get Topic error:{}", throwable1.getMessage());
                                closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable1.getMessage());
                            } else {
                                if (amqpQueue == null) {
//                                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue:No such queue: " + queueName);
                                    log.info(" No such queue:No such queue: " + queueName);
//                                    return;
                                } else {
                                    // bind to default exchange.
//                                    log.info("queue--405-: " + amqpQueue.getName());
//                                    amqpQueue.getRouter(finalExchange.toString()) == null &&
                                    if (queueBundle.equals(exBundle)) {
                                        log.info("queue bind with default ex: {}---routingKey:{}", finalExchange.toString(), routingKey);
                                        amqpQueue.bindExchange(amqpExchange,
                                                AbstractAmqpMessageRouter.generateRouter(AmqpExchange.Type.Direct),
                                                routingKey.toString(), null);
                                        MessagePublishInfo info = new MessagePublishInfo(realEx, immediate,
                                                mandatory, routingKey);
                                        addPublishFrame(info, null);
                                    }
                                }
//                                MessagePublishInfo info = new MessagePublishInfo(realEx, immediate,
//                                        mandatory, routingKey);
//                                addPublishFrame(info, null);
                            }
                        }).join();
                    }
                }).join();
            });
        } else {
            Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = ProxyService.getBundleBrokerBindMap();
            PulsarClientImpl pulsarClient = (PulsarClientImpl) connection.getPulsarService().getClient();
            Map<String, Bundle> topicBundleMap = connection.getTopicBundleMap();
            String url = pulsarClient.getConfiguration().getServiceUrl();
            String bindAddr = url.split("//")[1].split(":")[0]; //bindAddr 10.155.193.185:6650
            AMQShortString finalExchange1 = exchange;
            currentMessageList.clear();
            bundleBrokerBindMap.forEach((k, v) -> {
                if (bindAddr.equals(v.getKey())) {
                    int suffix = 0;
                    while (true) {
                        String topic = PersistentExchange.TOPIC_PREFIX + finalExchange1 + "_" + suffix;
                        Bundle temp;
                        if (topicBundleMap.containsKey(topic)) {
                            temp = topicBundleMap.get(topic);
                        } else {
                            temp = getBundleByTopic(topic);
                            topicBundleMap.put(topic, temp);
                        }
                        if (temp.equals(k)) {
                            String realEx = finalExchange1 + "_" + suffix;
                            MessagePublishInfo info = new MessagePublishInfo(AMQShortString.createAMQShortString(realEx), immediate,
                                    mandatory, routingKey);
                            addPublishFrame(info, null);
                            return;
                        } else {
                            suffix++;
                        }
                    }
                }
            });
        }
    }

    public Bundle getBundleByTopic(String topic) {
        // log.info("topic---------" + TopicName.get(TopicDomain.persistent.value(),
        //     NamespaceName.get("public", "vhost1"), topic));
        NamespaceBundle namespaceBundle = connection.getPulsarService().getNamespaceService().getBundle(TopicName.get(TopicDomain.persistent.value(),
            NamespaceName.get("public", "vhost1"), topic));
        String range = namespaceBundle.getBundleRange();
        String[] boundaries = range.split("_");
        Bundle bundle = new Bundle(boundaries[0], boundaries[1]);
//        topicBundleMap.putIfAbsent(topic, bundle);
        return bundle;
    }

    @SneakyThrows
    private AMQShortString findRightEx(AMQShortString exchange) {
        PulsarClientImpl pulsarClient = (PulsarClientImpl) connection.getPulsarService().getClient();
        LookupService lookupService = pulsarClient.getLookup();
        String url = pulsarClient.getConfiguration().getServiceUrl();
        //现在要先找到需要bind的exchange了
        int count = 0;
        String bindAddr = url.split("//")[1];
        if (exMap.get(exchange.toString()) == null) {
            while (true) {
                String topic = TopicName.get(TopicDomain.persistent.value(),
                    connection.getNamespaceName(), PersistentExchange.TOPIC_PREFIX + exchange + "_" + count).toString();
                CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookupData = null;
                if (exFutureMap.get(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count) != null) {
                    lookupData = exFutureMap.get(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count);
                } else {
                    lookupData = lookupService.getBroker(TopicName.get(topic));
                    exFutureMap.put(PersistentExchange.TOPIC_PREFIX + exchange + "_" + count, lookupData);
                }
                Pair<InetSocketAddress, InetSocketAddress> pair = lookupData.get();
                if (bindAddr.equals(pair.getKey().toString())) {
                    exMap.put(exchange.toString(), AMQShortString.valueOf(exchange + "_" + count).toString());
                    return AMQShortString.valueOf(exchange + "_" + count);
                }
                count++;
            }
        } else {
            return AMQShortString.valueOf(exMap.get(exchange.toString()));
        }
    }


    @SneakyThrows
    private List<AMQShortString> findRightExV1(AMQShortString exchange) {
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = ProxyService.getBundleBrokerBindMap();
        PulsarClientImpl pulsarClient = (PulsarClientImpl) connection.getPulsarService().getClient();
        Map<String, Bundle> topicBundleMap = connection.getTopicBundleMap();
        String url = pulsarClient.getConfiguration().getServiceUrl();
        String bindAddr = url.split("//")[1].split(":")[0]; //bindAddr 10.155.193.185:6650
        AMQShortString finalExchange1 = exchange;
        List<AMQShortString> list = new ArrayList<>();
        currentMessageList.clear();
        bundleBrokerBindMap.forEach((k, v) -> {
            if (bindAddr.equals(v.getKey())) {
                int suffix = 0;
                while (true) {
                    String topic = PersistentExchange.TOPIC_PREFIX + finalExchange1 + "_" + suffix;
                    Bundle temp;
                    if (topicBundleMap.containsKey(topic)) {
                        temp = topicBundleMap.get(topic);
                    } else {
                        temp = getBundleByTopic(topic);
                        topicBundleMap.put(topic, temp);
                    }
                    if (temp.equals(k)) {
                        String realEx = finalExchange1 + "_" + suffix;
                        list.add(AMQShortString.createAMQShortString(realEx));
                        break;
                    } else {
                        suffix++;
                    }
                }
            }
        });
        return list;
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
    }

    //pub
    private void addPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        IncomingMessage currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
        currentMessageList.add(currentMessage);
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {

        log.info("[{}] amqp receiveBasicGet[queue: {}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, queue, remoteAddress, localAddress);

        String queueName = AMQShortString.toString(queue);
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
            queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queueName);
                    return;
                } else {
                    Topic topic = amqpQueue.getTopic();
                    AmqpConsumer amqpConsumer = fetchConsumerMap.computeIfAbsent(queueName, value -> {

                        Subscription subscription = topic.getSubscription(defaultSubscription);
                        AmqpConsumer consumer;
                        try {
                            if (subscription == null) {
                                subscription = topic.createSubscription(defaultSubscription,
                                    CommandSubscribe.InitialPosition.Earliest, false).get();
                            }
                            consumer = new AmqpPullConsumer(queueContainer, subscription,
                                CommandSubscribe.SubType.Shared,
                                topic.getName(), 0, 0, "", 0,
                                connection.getServerCnx(), "", null, false,
                                CommandSubscribe.InitialPosition.Latest, null, this,
                                "", queueName, noAck);
                            subscription.addConsumer(consumer);
                            consumer.handleFlow(DEFAULT_CONSUMER_PERMIT);
                            return consumer;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
                    MessageFetchContext.handleFetch(this, amqpConsumer, noAck);
                }
            }
        });
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        log.info("[{}] amqp receiveChannelFlow[active: {}], remoteAddress: [{}] localAddress: [{}]", cnxName, active, remoteAddress, localAddress);
        // TODO channelFlow process
        ChannelFlowOkBody body = connection.getMethodRegistry().createChannelFlowOkBody(true);
        connection.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        log.info("[{}] amqp receiveChannelFlowOk[active: {}] remoteAddress: [{}] localAddress: [{}]", cnxName, active, remoteAddress, localAddress);
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        log.info("[{}] receiveChannelClose[replyCode: {} replyText: {} classId: {} methodId: {}] remoteAddress: [{}] localAddress: [{}]",
            cnxName, replyCode, replyText, classId, methodId, remoteAddress, localAddress);
        // TODO Process outstanding client requests
        processAsync();
        connection.closeChannel(this);
    }

    @Override
    public void receiveChannelCloseOk() {
        log.info("[{}] amqp receiveChannelCloseOk remoteAddress: [{}] localAddress: [{}]", cnxName, remoteAddress, localAddress);
        connection.closeChannelOk(getChannelId());
    }

    private boolean hasCurrentMessage() {
        return currentMessage != null;
    }

    //pub
    private boolean hasCurrentMessageList() {
        return !currentMessageList.isEmpty();
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        log.info("[{}] amqp receiveMessageContent data: [{}] remoteAddress: [{}] localAddress: [{}] msgId:[{}] uuId:[{}]"
                , cnxName, remoteAddress, localAddress, str(data), msgId, uuId);
        if (log.isDebugEnabled()) {
            int binaryDataLimit = 2000;
            log.debug("RECV[{}] MessageContent[data:{}]", channelId, hex(data, binaryDataLimit));
        }
        if (hasCurrentMessageList()) {
            confirmedMessageCounter++;
            currentMessageList.forEach(currentMessage -> publishContentBody(new ContentBody(data), currentMessage));
        } else {
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentBody(ContentBody contentBody) {
        if (log.isDebugEnabled()) {
            log.debug("content body received on channel {}", channelId);
        }

        try {
            long currentSize = currentMessage.addContentBodyFrame(contentBody);
            if (currentSize > currentMessage.getSize()) {
                connection.sendConnectionClose(ErrorCodes.FRAME_ERROR,
                    "More message data received than content header defined", channelId);
            } else {
                deliverCurrentMessageIfComplete();
            }
        } catch (RuntimeException e) {
            currentMessage = null;
            throw e;
        }
    }

    private void publishContentBody(ContentBody contentBody, IncomingMessage currentMessage) {
        if (log.isDebugEnabled()) {
            log.debug("content body received on channel {}", channelId);
        }

        try {
            long currentSize = currentMessage.addContentBodyFrame(contentBody);
            if (currentSize > currentMessage.getSize()) {
                connection.sendConnectionClose(ErrorCodes.FRAME_ERROR,
                    "More message data received than content header defined", channelId);
            } else {
                deliverCurrentMessageIfComplete(currentMessage);
            }
        } catch (RuntimeException e) {
            // we want to make sure we don't keep a reference to the message in the
            // event of an error
            currentMessage = null;
            throw e;
        }
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        long transferTime = System.currentTimeMillis() - properties.getTimestamp();
        log.info("[{}] amqp receiveMessageHeader[properties: {{}} bodySize: {}] remoteAddress: [{}] localAddress: [{}] transferTime: [{}]ms",
            cnxName, properties, bodySize, remoteAddress, localAddress, transferTime);
        if (transferTime > 500){
            log.info("[{}] transferTime too long!!! transferTime: [{}]", cnxName, transferTime);
        }

        // TODO - maxMessageSize ?
        long maxMessageSize = 1024 * 1024 * 10;
        if (hasCurrentMessageList()) {
            if (bodySize > maxMessageSize) {
                properties.dispose();
                closeChannel(ErrorCodes.MESSAGE_TOO_LARGE,
                    "Message size of " + bodySize + " greater than allowed maximum of " + maxMessageSize);
            } else {
                currentMessageList.forEach(currentMessage -> publishContentHeader(new ContentHeaderBody(properties, bodySize), currentMessage));
//                publishContentHeader(new ContentHeaderBody(properties, bodySize));
            }
        } else {
            properties.dispose();
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentHeader(ContentHeaderBody contentHeaderBody) {
        if (log.isDebugEnabled()) {
            log.debug("Content header received on channel {}", channelId);
        }

        currentMessage.setContentHeaderBody(contentHeaderBody);

        deliverCurrentMessageIfComplete();
    }

    private void publishContentHeader(ContentHeaderBody contentHeaderBody, IncomingMessage currentMessage) {
        if (log.isDebugEnabled()) {
            log.debug("Content header received on channel {}", channelId);
        }
        msgId = Long.parseLong(contentHeaderBody.getProperties().getMessageId().toString());
        uuId = contentHeaderBody.getProperties().getAppIdAsString();
        currentMessage.setContentHeaderBody(contentHeaderBody);
        deliverCurrentMessageIfComplete(currentMessage);
    }

    private void deliverCurrentMessageIfComplete() {
        if (currentMessage.allContentReceived()) {
            MessagePublishInfo info = currentMessage.getMessagePublishInfo();
            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                connection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.", channelId);
                return;
            }
            boolean createIfMissing = false;
            String exchangeType = null;
            if (isDefaultExchange(AMQShortString.valueOf(exchangeName))
                || isBuildInExchange(AMQShortString.valueOf(exchangeName))) {
                // Auto create default and buildIn exchanges if use.
                createIfMissing = true;
                exchangeType = getExchangeType(exchangeName);
            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = String.valueOf(findRightEx(AMQShortString.createAMQShortString(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)));
            }
            CompletableFuture<AmqpExchange> completableFuture = exchangeContainer.
                asyncGetExchange(connection.getNamespaceName(), exchangeName, createIfMissing, exchangeType);
            completableFuture.thenApply(amqpExchange -> amqpExchange.writeMessageAsync(message, routingKey).
                thenApply(position -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Publish message success, position {}", position.toString());
                    }
                    if (confirmOnPublish) {
                        confirmedMessageCounter++;
                        BasicAckBody body = connection.getMethodRegistry().
                            createBasicAckBody(confirmedMessageCounter, false);
                        // log.info"deliverCurrentMessageIfComplete-------"+body);
                        connection.writeFrame(body.generateFrame(channelId));
                    }
                    return position;
                })).exceptionally(throwable -> {
                log.error("Failed to write message to exchange", throwable);
                return null;
            });
        }
    }

    private void deliverCurrentMessageIfComplete(IncomingMessage currentMessage) {
        if (currentMessage.allContentReceived()) {
            MessagePublishInfo info = currentMessage.getMessagePublishInfo();
            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                connection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.", channelId);
                return;
            }
            boolean createIfMissing = false;
            String exchangeType = null;
            if (isDefaultExchange(AMQShortString.valueOf(exchangeName))
                || isBuildInExchange(AMQShortString.valueOf(exchangeName))) {
                // Auto create default and buildIn exchanges if use.
                createIfMissing = true;
                exchangeType = getExchangeType(exchangeName);
            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = String.valueOf(findRightEx(AMQShortString.createAMQShortString(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)));
            }
            CompletableFuture<AmqpExchange> completableFuture = exchangeContainer.
                asyncGetExchange(connection.getNamespaceName(), exchangeName, createIfMissing, exchangeType);
            completableFuture.thenApply(amqpExchange -> amqpExchange.writeMessageAsync(message, routingKey).
                thenApply(position -> {
                    if (log.isDebugEnabled()) {
                        log.debug("Publish message success, position {}", position.toString());
                    }
                    if (confirmOnPublish) {
                        BasicAckBody body = connection.getMethodRegistry().
                            createBasicAckBody(msgId, false);
                        connection.writeFrame(body.generateFrame(channelId));
                    }
                    return position;
                })).exceptionally(throwable -> {
                log.error("Failed to write message to exchange", throwable);
                return null;
            });
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        // log.info("[{}] amqp receiveBasicNAck[deliveryTag: {} multiple: {} requeue: {}] remoteAddress: [{}] localAddress: [{}]",
            // cnxName, deliveryTag, multiple, requeue, remoteAddress, localAddress);
        messageNAck(deliveryTag, multiple, requeue);
    }

    public void messageNAck(long deliveryTag, boolean multiple, boolean requeue) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            requeue(ackedMessages);
        } else {
//            closeChannel(ErrorCodes.IN_USE, "deliveryTag not found");
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        log.info("[{}] amqp receiveBasicRecover requeue: [{}] remoteAddress: [{}] localAddress: [{}]", cnxName, requeue, remoteAddress, localAddress);
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledgeAll();
        if (!ackedMessages.isEmpty()) {
            requeue(ackedMessages);
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
    }

    private void requeue(Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> messages) {
        Map<AmqpConsumer, List<PositionImpl>> positionMap = new HashMap<>();
        messages.stream().forEach(association -> {
            AmqpConsumer consumer = association.getConsumer();
            List<PositionImpl> positions = positionMap.computeIfAbsent(consumer,
                list -> new ArrayList<>());
            positions.add((PositionImpl) association.getPosition());
        });
        positionMap.entrySet().stream().forEach(entry -> {
            entry.getKey().redeliverAmqpMessages(entry.getValue());
        });
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        // log.info("[{}] amqp receiveBasicAck deliveryTag: {} multiple: {} remoteAddress: [{}] localAddress: [{}]",
            // cnxName, deliveryTag, multiple, remoteAddress, localAddress);
        messageAck(deliveryTag, multiple);
    }

    private void messageAck(long deliveryTag, boolean multiple) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            ackedMessages.stream().forEach(entry -> {
                entry.getConsumer().messagesAck(entry.getPosition());
            });
        } else {
//            closeChannel(ErrorCodes.IN_USE, "deliveryTag not found");
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }

    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        messageNAck(deliveryTag, false, requeue);
    }

    @Override
    public void receiveTxSelect() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxSelect", channelId);
        }
        // TODO txSelect process
        TxSelectOkBody txSelectOkBody = connection.getMethodRegistry().createTxSelectOkBody();
        connection.writeFrame(txSelectOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveTxCommit() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxCommit", channelId);
        }
        // TODO txCommit process
        TxCommitOkBody txCommitOkBody = connection.getMethodRegistry().createTxCommitOkBody();
        connection.writeFrame(txCommitOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveTxRollback() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxRollback", channelId);
        }
        // TODO txRollback process
        TxRollbackOkBody txRollbackBody = connection.getMethodRegistry().createTxRollbackOkBody();
        connection.writeFrame(txRollbackBody.generateFrame(channelId));
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        log.info("[{}] amqp receiveConfirmSelect nowait: [{}] remoteAddress: [{}] localAddress: [{}]", cnxName, nowait, remoteAddress, localAddress);
        confirmOnPublish = true;

        if (!nowait) {
//            connection.writeFrame(new AMQFrame(channelId, ConfirmSelectOkBody.INSTANCE));
        }
    }

    public void receivedComplete() {
        processAsync();
    }

    private void sendChannelClose(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public void processAsync() {
        // TODO
    }

    public void close() {
        // TODO
        unsubscribeConsumerAll();
        // TODO need to delete exclusive queues in this channel.
        setDefaultQueue(null);
    }

    public synchronized void block() {
        // TODO
    }

    public synchronized void unblock() {
        // TODO
    }

    public int getChannelId() {
        return channelId;
    }

    public boolean isClosing() {
        return closing.get() || connection.isClosing();
    }

    public boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    public boolean isBuildInExchange(final AMQShortString exchangeName) {
        if (exchangeName.toString().equals(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
            || (exchangeName.toString().equals(ExchangeDefaults.FANOUT_EXCHANGE_NAME))
            || (exchangeName.toString().equals(ExchangeDefaults.TOPIC_EXCHANGE_NAME))) {
            return true;
        } else {
            return false;
        }
    }

    public void closeChannel(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public long getNextDeliveryTag() {
        return ++deliveryTag;
    }

    private int getNextConsumerTag() {
        return ++consumerTag;
    }

    public AmqpConnection getConnection() {
        return connection;
    }

    public UnacknowledgedMessageMap getUnacknowledgedMessageMap() {
        return unacknowledgedMessageMap;
    }

    private boolean unsubscribeConsumer(String consumerTag) {
        if (log.isDebugEnabled()) {
            log.debug("Unsubscribing consumer '{}' on channel {}", consumerTag, this);
        }

        Consumer consumer = tag2ConsumersMap.remove(consumerTag);
        if (consumer != null) {
            try {
                consumer.close();
                return true;
            } catch (BrokerServiceException e) {
                log.error(e.getMessage());
            }

        } else {
            log.warn("Attempt to unsubscribe consumer with tag  {} which is not registered.", consumerTag);
        }
        return false;
    }

    private void unsubscribeConsumerAll() {
        if (log.isDebugEnabled()) {
            if (!tag2ConsumersMap.isEmpty()) {
                log.debug("Unsubscribing all consumers on channel  {}", channelId);
            } else {
                log.debug("No consumers to unsubscribe on channel {}", channelId);
            }
        }
        try {
            tag2ConsumersMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }

            });
            tag2ConsumersMap.clear();
            fetchConsumerMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }

            });
            fetchConsumerMap.clear();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    protected void setDefaultQueue(AmqpQueue queue) {
        defaultQueue = queue;
    }

    protected AmqpQueue getDefaultQueue() {
        return defaultQueue;
    }

    public void checkExclusiveQueue(AmqpQueue amqpQueue) {
        if (amqpQueue != null && amqpQueue.isExclusive()
            && (amqpQueue.getConnectionId() != connection.getConnectionId())) {
            closeChannel(ErrorCodes.ALREADY_EXISTS,
                "Exclusive queue can not be used form other connection, queueName: '" + amqpQueue.getName() + "'");
        }
    }

    @VisibleForTesting
    public Map<String, Consumer> getTag2ConsumersMap() {
        return tag2ConsumersMap;
    }

    public void restoreCredit(final int count, final long size) {
        creditManager.restoreCredit(count, size);
    }

    public boolean setBlockedOnCredit() {
        return this.blockedOnCredit.compareAndSet(false, true);
    }

    public boolean isBlockedOnCredit() {
        if (log.isDebugEnabled()) {
            log.debug("isBlockedOnCredit {}", blockedOnCredit.get());
        }
        return this.blockedOnCredit.get();
    }

    public void unBlockedOnCredit() {
        if (this.blockedOnCredit.compareAndSet(true, false)) {
            notifyAllConsumers();
        }
    }

    private void notifyAllConsumers() {
        tag2ConsumersMap.values().stream().forEach(consumer -> {
            ((AmqpConsumer) consumer).handleFlow(1);
        });
    }

    public AmqpFlowCreditManager getCreditManager() {
        return creditManager;
    }

    public String getExchangeType(String exchangeName) {
        if (null == exchangeName) {
            exchangeName = "";
        }
        switch (exchangeName) {
            case "":
            case ExchangeDefaults.DIRECT_EXCHANGE_NAME:
                return ExchangeDefaults.DIRECT_EXCHANGE_CLASS;
            case ExchangeDefaults.FANOUT_EXCHANGE_NAME:
                return ExchangeDefaults.FANOUT_EXCHANGE_CLASS;
            case ExchangeDefaults.TOPIC_EXCHANGE_NAME:
                return ExchangeDefaults.TOPIC_EXCHANGE_CLASS;
            default:
                return "";
        }
    }
}
