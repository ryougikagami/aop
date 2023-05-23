package io.streamnative.pulsar.handlers.amqp.proxy;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.Bundle;
import io.streamnative.pulsar.handlers.amqp.collection.SyncConcurrentHashMap;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;

import java.net.SocketAddress;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.qpid.server.transport.util.Functions.str;

@Slf4j
public class ProxyChannel implements ServerChannelMethodProcessor {

    private final int channelId;
    private final ProxyConnection connection;

    private SocketAddress remoteAddress;
    private SocketAddress localAddress;

    public ProxyChannel(int channelId, ProxyConnection connection) {
        this.channelId = channelId;
        this.connection = connection;
        this.remoteAddress = connection.getCnx().channel().remoteAddress();
        this.localAddress = connection.getCnx().channel().localAddress();
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        log.info("[{}] receiveAccessRequest realm: [{}] exclusive: [{}] passive: [{}] active: [{}] write: [{}] read: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), realm, exclusive, passive, active, write, read, remoteAddress, localAddress);
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AccessRequestOkBody response = methodRegistry.createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(channelId));
    }

    @SneakyThrows
    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        log.info("[{}] receiveExchangeDeclare exchange: [{}] type: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), exchange, type, remoteAddress, localAddress);
        ProxyService proxyService = connection.getProxyService();
        Map<String, Set<String>> newMapForEx = proxyService.getNewMapForEx();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        Map<String, Bundle> topicBundleMap = proxyService.getTopicBundleMap();
        List<Bundle> bundleList = proxyService.getBundleList();
        int ticket = getTicket();
        //先扫描mapForEx,如果有数据则可以直接知道topic名字 需要保证map里对应的exchangeList是全的
        // TODO 这里要读配置文件
        if (newMapForEx.containsKey(exchange.toString()) && newMapForEx.get(exchange.toString()).size() == 3) {
            Set<String> exSet = newMapForEx.get(exchange.toString());
            log.info("[{}] receiveExchangeDeclare exList.size={}", connection.getCnx().name(), exSet.size());
            exSet.forEach((realEx) -> {
                String topic = PersistentExchange.TOPIC_PREFIX + realEx;
                Bundle bundle = topicBundleMap.get(topic);
                BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                ExchangeDeclareBody exchangeDeclareBody = methodRegistry.createExchangeDeclareBody(ticket, realEx, type.toString(), passive, durable, autoDelete, internal, nowait, FieldTable.convertToMap(arguments));
                connection.writeAndFlushMsg(brokerConf, new AMQFrame(channelId, exchangeDeclareBody));
            });
        } else {
            // TODO 多线程处理
            for (Bundle bundle : bundleList) { //循环bundleList,确保每个bundle上都有topic
                Pair<String, Integer> pair = bundleBrokerBindMap.get(bundle);
                BrokerConf brokerConf = new BrokerConf(pair);
                // log.info("[{}] ----- receiveExchangeDeclare ----- brokerConf: [{}]", connection.getCnx().name(), brokerConf);
                // ProxyHandler proxyHandler = proxyHandlerMap.get(brokerConf); //知道bundle就可以拿到对应的proxyHandler
                int suffix = 0;
                while (true) {
                    String topic = PersistentExchange.TOPIC_PREFIX + exchange + "_" + suffix;
                    Bundle temp = proxyService.getBundleByTopic(topic);
                    if (temp.equals(bundle)) {
                        newMapForEx.putIfAbsent(exchange.toString(), new HashSet<>());
                        Set<String> set = newMapForEx.get(exchange.toString());
                        String realEx = exchange + "_" + suffix;
                        set.add(realEx);
                        MethodRegistry methodRegistry = connection.getMethodRegistry();
                        ExchangeDeclareBody exchangeDeclareBody = methodRegistry.createExchangeDeclareBody(ticket, realEx, type.toString(), passive, durable, autoDelete, internal, nowait, FieldTable.convertToMap(arguments));
                        connection.writeAndFlushMsg(brokerConf, new AMQFrame(channelId, exchangeDeclareBody));
                        break;
                    } else {
                        suffix++;
                    }
                }
            }
        }
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        log.info("[{}] receiveExchangeDelete exchange: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), exchange, remoteAddress, localAddress);
        int ticket = getTicket();
        ExchangeDeleteBody exchangeDeleteBody = new ExchangeDeleteBody(ticket,exchange,ifUnused,nowait);
        proxyToBundles(exchangeDeleteBody);
    }


    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        log.info("[unexpected branch] receiveExchangeBound, connection={} remoteAddress: [{}] localAddress: [{}]",
            connection.toString(), remoteAddress, localAddress);
    }

    @SneakyThrows
    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                                    boolean autoDelete, boolean nowait, FieldTable arguments) {
        log.info("[{}] receiveQueueDeclare queue: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), queue, remoteAddress, localAddress);

        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeclareBody queueDeclareBody = methodRegistry.createQueueDeclareBody(ticket,
                queue.toString(),
                passive,
                durable,
                exclusive,
                autoDelete,
                nowait,
                FieldTable.convertToMap(arguments));

        findBrokerByQueue(queue,new AMQFrame(channelId,queueDeclareBody));
    }

    @SneakyThrows
    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable arguments) {
        log.info("[{}] receiveQueueBind queue: [{}] exchange: [{}] bindKey: [{}] remoteAddress: [{}] localAddress: [{}]",
            connection.getCnx().name(), queue, exchange, bindingKey, remoteAddress, localAddress);
        Map<String, List<Object>> queueConsumerMap = connection.getQueueConsumerMap();
        queueConsumerMap.putIfAbsent(PersistentQueue.TOPIC_PREFIX + queue.toString(), new ArrayList<>());
        List<Object> consumerList = queueConsumerMap.get(PersistentQueue.TOPIC_PREFIX + queue);
        ProxyService proxyService = connection.getProxyService();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        Map<String, Bundle> topicBundleMap = proxyService.getTopicBundleMap();
        Map<String, Set<String>> newMapForEx = proxyService.getNewMapForEx();
        int ticket = getTicket();

        //根据queue找到bundle
        String topic = PersistentQueue.TOPIC_PREFIX + queue;
        Bundle bundle = topicBundleMap.containsKey(topic) ? topicBundleMap.get(topic) : proxyService.getBundleByTopic(topic);
        Set<String> exSet = newMapForEx.get(exchange.toString());
        exSet.forEach((ex) -> {
            String realEx = PersistentExchange.TOPIC_PREFIX + ex;
            Bundle temp = topicBundleMap.get(realEx);
            if (temp.equals(bundle)) {
                log.info("[{}] proxy queueBind realEx: [{}] queue: [{}] remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), realEx, queue, connection.getCnx().channel().remoteAddress(), connection.getCnx().channel().localAddress());
                BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                QueueBindBody queueBindBody = methodRegistry.createQueueBindBody(ticket, queue.toString(), ex, bindingKey == null ? null : bindingKey.toString(), nowait, FieldTable.convertToMap(arguments));
                consumerList.add(new AMQFrame(channelId, queueBindBody));
                connection.writeAndFlushMsg(brokerConf, new AMQFrame(channelId, queueBindBody));
            }
        });
    }

    @SneakyThrows
    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        log.info("[{}] [unexpected branch] receiveQueuePurge queue: [{}] nowait: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), queue.toString(), nowait, remoteAddress, localAddress);
        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueuePurgeBody queuePurgeBody = methodRegistry.createQueuePurgeBody(ticket,
                queue,
                nowait);
        findBrokerByQueue(queue,new AMQFrame(channelId,queuePurgeBody));
    }

    @SneakyThrows
    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        log.info("[{}] receiveQueueDelete queue: [{}] isUnused: [{}] ifEmpty: [{}] nowait: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), queue, ifUnused, ifEmpty, nowait, remoteAddress, localAddress);
        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeleteBody queueDeleteBody = methodRegistry.createQueueDeleteBody(ticket,
                queue.toString(),
                ifUnused,
                ifEmpty,
                nowait);
        findBrokerByQueue(queue,new AMQFrame(channelId,queueDeleteBody));
    }


    @SneakyThrows
    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString
            bindingKey, FieldTable arguments) {
        log.info("[{}] receiveQueueUnbind queue: [{}] exchange: [{}] bindKey: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), queue, exchange, bindingKey, remoteAddress, localAddress);
        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueUnbindBody queueUnbindBody = methodRegistry.createQueueUnbindBody(ticket,
                queue,
                exchange,
                bindingKey,
                arguments);
        findBrokerByQueue(queue,new AMQFrame(channelId,queueUnbindBody));
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        log.info("[{}] receiveBasicRecover requeue: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), requeue, remoteAddress, localAddress);
        if (sync) {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            AMQMethodBody recoverOk = methodRegistry.createBasicRecoverSyncOkBody();
            connection.writeFrame(recoverOk.generateFrame(channelId));
        }
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicRecoverBody basicRecoverBody = methodRegistry.createBasicRecoverBody(requeue);
        proxyToBundles(basicRecoverBody);
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        log.info("[{}] receiveBasicQos prefetchSize: [{}],  prefetchCount: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), prefetchSize, prefetchCount, remoteAddress, localAddress);
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicQosBody basicQosBody = methodRegistry.createBasicQosBody(prefetchSize,prefetchCount,global);
        proxyToBundles(basicQosBody);
        AMQMethodBody responseBody = methodRegistry.createBasicQosOkBody();
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @SneakyThrows
    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal,
                                    boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {
        log.info("[{}] receiveBasicConsume queue: [{}] remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), queue, connection.getCnx().channel().remoteAddress(), connection.getCnx().channel().localAddress());
        Map<String, List<Object>> queueConsumerMap = connection.getQueueConsumerMap();
        queueConsumerMap.putIfAbsent(PersistentQueue.TOPIC_PREFIX + queue.toString(), new ArrayList<>());
        List<Object> consumerList = queueConsumerMap.get(PersistentQueue.TOPIC_PREFIX + queue);
        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicConsumeBody basicConsumeBody = methodRegistry.createBasicConsumeBody(ticket,
                queue.toString(),
                consumerTag.toString(),
                noLocal,
                noAck,
                exclusive,
                nowait,
                FieldTable.convertToMap(arguments));
        consumerList.add(new AMQFrame(channelId,basicConsumeBody));
        findBrokerByQueue(queue,new AMQFrame(channelId,basicConsumeBody));
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        log.info("[{}] receiveBasicCancel consumerTag: [{}] remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), consumerTag, remoteAddress, localAddress);
    }

    @SneakyThrows
    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
                                         boolean immediate) {
        log.info("[{}] receiveBasicPublish exchange: [{}] routingKey: [{}] remoteAddress: [{}] localAddress: [{}]"
                , connection.getCnx().name(), exchange == null ? null : exchange.toString(), routingKey, remoteAddress, localAddress);
        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(connection.getBb().nioBuffer());
        int ticket = qpidByteBuffer.getUnsignedShort();
        connection.setDefaultEx(false);
        BasicPublishBody basicPublishBody = connection.getMethodRegistry().createBasicPublishBody(ticket, AMQShortString.toString(exchange), AMQShortString.toString(routingKey), mandatory, immediate);
        if (Objects.isNull(exchange)) {
            findBrokerByQueue(routingKey, basicPublishBody.generateFrame(channelId));
            connection.setAckCount(1);
            connection.setDefaultEx(true);
            connection.setRoutingKey(routingKey);
        } else {
            writeMsgToBundles(basicPublishBody.generateFrame(channelId));
        }

    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {

        log.info("[{}}] receiveMessageHeader properties: [{}] bodySize: [{}] confirmSelect: [{}] ackMapSize: [{}] remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), properties, bodySize, connection.isConfirmSelect(), connection.getAckMap().size(), remoteAddress, localAddress);
        //设置当时时间，求生产到消费所需要的总时间
        properties.setTimestamp(System.currentTimeMillis());
        long msgId;
        String uuid = String.valueOf(UUID.randomUUID());
        connection.setUUID(uuid);
        properties.setAppId(uuid);
        if (properties.getMessageIdAsString() == null) {
            msgId = connection.getMsgId();
            properties.setMessageId(String.valueOf(msgId));
            connection.setMsgId(msgId + 1);
        } else {
            msgId = Long.parseLong(properties.getMessageIdAsString());
        }
        if (connection.isConfirmSelect()){
            connection.getAckMap().putIfAbsent(msgId, new AtomicInteger(connection.getAckCount()));
        }
        connection.setAckCount(connection.getProxyService().getBundleList().size());
        AMQFrame amqFrame = ContentHeaderBody.createAMQFrame(channelId, properties, bodySize);
        if (connection.isDefaultEx()) {
            findBrokerByQueue(connection.getRoutingKey(), amqFrame);
        } else {
            writeMsgToBundles(amqFrame);
        }
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        log.info("[{}] receiveMessageContent remoteAddress: [{}] localAddress: [{}] data:[{}] msgId:[{}] uuid:[{}]"
                , connection.getCnx().name(), remoteAddress, localAddress, str(data), connection.getMsgId() - 1, connection.getUUID());
        AMQFrame amqFrame = ContentBody.createAMQFrame(channelId, new ContentBody(data));
        if (connection.isDefaultEx()) {
            findBrokerByQueue(connection.getRoutingKey(), amqFrame);
        } else {
            writeMsgToBundles(amqFrame);
        }
    }

    @SneakyThrows
    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        log.info("[{}] receiveBasicGet queue: [{}] remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), queue, remoteAddress, localAddress);
        int ticket = getTicket();
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicGetBody basicGetBody = methodRegistry.createBasicGetBody(ticket, queue, noAck);
        findBrokerByQueue(queue,new AMQFrame(channelId,basicGetBody));
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        log.info("[{}] [unexpected branch] receiveChannelFlow remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
        ChannelFlowOkBody body = connection.getMethodRegistry().createChannelFlowOkBody(true);
        connection.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        log.info("[{}] [unexpected branch] receiveChannelFlowOk remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        // TODO 客户端发来的channelClose需要关闭 (与ConnectionCLose不同)
        log.info("[{}] receiveChannelClose remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
        connection.writeFrame(new AMQFrame(channelId, connection.getMethodRegistry().createChannelCloseOkBody()));
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        ChannelCloseBody channelCloseBody = methodRegistry.createChannelCloseBody(replyCode,replyText,classId,methodId);
        proxyToBundles(channelCloseBody);
    }


    @Override
    public void receiveChannelCloseOk() {
        log.info("[{}] [unexpected branch] receiveChannelCloseOk remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        log.info("[{}] [unexpected branch] receiveBasicNack remoteAddress: [{}] localAddress: [{}]", remoteAddress, localAddress);
        BasicNackBody basicNackBody = new BasicNackBody(deliveryTag,multiple,requeue);
        proxyToBundles(basicNackBody);
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        log.info("[{}] receiveBasicAck remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicAckBody basicAckBody = methodRegistry.createBasicAckBody(deliveryTag, multiple);
        proxyToBundles(basicAckBody);
    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        log.info("[{}] [unexpected branch] receiveBasicReject, remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicRejectBody basicRejectBody = methodRegistry.createBasicRejectBody(deliveryTag, requeue);
        proxyToBundles(basicRejectBody);
    }

    @Override
    public void receiveTxSelect() {
        log.info("[{}] [unexpected branch] receiveTxSelect remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
//        proxyToBundles();
    }

    @Override
    public void receiveTxCommit() {
        log.info("[{}] [unexpected branch] receiveTxCommit remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
//        proxyToBundles();
    }

    @Override
    public void receiveTxRollback() {
        log.info("[{}] [unexpected branch] receiveTxRollback remoteAddress: [{}] localAddress: [{}]", connection.getCnx().name(), remoteAddress, localAddress);
//        proxyToBundles();
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        log.info("[{}] [unexpected branch] receiveConfirmSelect nowait: [{}] remoteAddress: [{}] localAddress: [{}]", nowait, connection.getCnx().name(), remoteAddress, localAddress);

        connection.setConfirmSelect(true);

        ConfirmSelectBody confirmSelectBody = new ConfirmSelectBody(nowait);
        proxyToBundles(confirmSelectBody);
        if (!nowait) {
            connection.writeFrame(new AMQFrame(channelId, ConfirmSelectOkBody.INSTANCE));
        }
    }


    private void writeMsgToBundles(AMQFrame amqFrame) {
        SyncConcurrentHashMap<Bundle, Pair<String, Integer>> bundleBrokerBindMap = connection.getProxyService().getBundleBrokerBindMap();
        Set<BrokerConf> brokerConfSet = new HashSet<>();
        bundleBrokerBindMap.forEach((k, v) -> {
            brokerConfSet.add(new BrokerConf(v));
        });
        brokerConfSet.forEach(brokerConf -> {
            connection.writeAndFlushMsg(brokerConf, amqFrame);
        });
//        if (connection.getBb().refCnt() != 0) {
//            connection.getBb().release();
//        }
    }

    private void proxyToBundles(AMQBody amqBody) {
        ProxyService proxyService = connection.getProxyService();
        List<Bundle> bundleList = proxyService.getBundleList();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        bundleList.forEach(bundle -> {
            BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
            connection.writeAndFlushMsg(brokerConf, new AMQFrame(channelId,amqBody));
        });
    }

    private void findBrokerByQueue(AMQShortString queue) {
        log.info("[{}] findBrokerByQueue queue: [{}] remoteAddress: [{}] localAddress: [{}]",
                connection.getCnx().name(), queue, remoteAddress, localAddress);
        ProxyService proxyService = connection.getProxyService();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        Map<String, Bundle> topicBundleMap = proxyService.getTopicBundleMap();
        //根据queue找到bundle
        String topic = PersistentQueue.TOPIC_PREFIX + queue;
        Bundle bundle = topicBundleMap.containsKey(topic) ? topicBundleMap.get(topic) : proxyService.getBundleByTopic(topic);
        // log.info("[{}] ----- findBrokerByQueue ----- bundle: [{}]", connection.getCnx().name(), bundle);
        BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
        connection.writeAndFlushMsg(brokerConf, connection.getBb());
    }

    private void findBrokerByQueue(AMQShortString queue, AMQFrame amqFrame) {
        ProxyService proxyService = connection.getProxyService();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        Map<String, Bundle> topicBundleMap = proxyService.getTopicBundleMap();
        //根据queue找到bundle
        String topic = PersistentQueue.TOPIC_PREFIX + queue;
        Bundle bundle = topicBundleMap.containsKey(topic) ? topicBundleMap.get(topic) : proxyService.getBundleByTopic(topic);
        BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
        log.info("[{}] findBrokerByQueue queue: [{}] remoteAddress: [{}] localAddress: [{}] brokerConf: [{}]",
                connection.getCnx().name(), queue, remoteAddress, localAddress, brokerConf);
        connection.writeAndFlushMsg(brokerConf, amqFrame);
    }


    private int getTicket() {
        ByteBuf buf = connection.getBb();
        ByteBuffer byteBuffer = buf.nioBuffer();
        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(byteBuffer);
        return qpidByteBuffer.getUnsignedShort();
    }

    public void receivedComplete() {
        processAsync();
    }


    public void processAsync() {
        // TODO
    }

}
