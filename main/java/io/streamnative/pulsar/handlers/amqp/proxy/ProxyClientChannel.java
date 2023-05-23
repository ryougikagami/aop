package io.streamnative.pulsar.handlers.amqp.proxy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import java.util.Set;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.PulsarService;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.qpid.server.protocol.ErrorCodes.REPLY_SUCCESS;

@Slf4j
public class ProxyClientChannel implements ClientChannelMethodProcessor {
    private final int channelId;
    private final ProxyConnection connection;
    private Object msgRec;
    private Channel clientChannel;
    private Channel brokerChannel;
    private ProxyHandler proxyHandler;
    private CompletableFuture<Boolean> cf;
    //    private final ProxyConnection amqpConnection;
    public ProxyClientChannel(int channelId, ProxyConnection proxyConnection, Channel clientChannel, Channel brokerChannel, ProxyHandler proxyHandler, CompletableFuture<Boolean> cf) {
        this.channelId = channelId;
        this.connection = proxyConnection;
        this.clientChannel = clientChannel;
        this.brokerChannel = brokerChannel;
        this.proxyHandler = proxyHandler;
        this.cf = cf;
    }

    private void flush(Object m) {
        clientChannel.writeAndFlush(m);
    }

    @SneakyThrows
    @Override
    public void receiveChannelOpenOk() {
        log.info("[{}] proxyClientChannel receiveChannelOpenOK", this);
        AtomicInteger a = connection.getOpen();
        a.decrementAndGet();
        proxyHandler.setState(ProxyHandler.State.Connected);
        cf.complete(true);
        if (a.get() == 0) {
            ChannelOpenOkBody response = connection.getMethodRegistry().createChannelOpenOkBody();
            connection.writeFrame(response.generateFrame(channelId));
            a.set(connection.getProxyHandlerMap().size());
        }

    }

    @Override
    public void receiveChannelAlert(int replyCode, AMQShortString replyText, FieldTable details) {
        log.info("[unexpected branch] receiveChannelAlert, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveAccessRequestOk(int ticket) {
        log.info("[unexpected branch] receiveAccessRequestOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @SneakyThrows
    @Override
    public void receiveExchangeDeclareOk() {
        AtomicInteger a = connection.getDeclareOk();
        a.decrementAndGet();

        log.info("[{}] receiveExchangeDeclareOk a={}, exchangeDeclareCount={},remoteAddress:[{}],localAddress:[{}]"
            , connection.getCnx().name(), connection.getDeclareOk().get(), connection.getProxyService().getExchangeDeclareCount(), brokerChannel.remoteAddress(), brokerChannel.localAddress());
        if (a.get() == 0) {
            final AMQMethodBody declareOkBody = connection.getMethodRegistry().createExchangeDeclareOkBody();
            //一定记得process完以后要变更状态就要写
            connection.writeFrame(declareOkBody.generateFrame(channelId));
            log.info("proxyHandlerMap----" + connection.getProxyHandlerMap().size());
            a.set(connection.getProxyHandlerMap().size());
        }
    }

    @Override
    public void receiveExchangeDeleteOk() {
        AtomicInteger a = connection.getExDeleteOk();
        a.decrementAndGet();
        if (a.get() == 0) {
            final AMQMethodBody declareOkBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
            //一定记得process完以后要变更状态就要写
            connection.writeFrame(declareOkBody.generateFrame(channelId));
            changeToConnected();
            a.set(connection.getProxyHandlerMap().size());
        }
    }

    @Override
    public void receiveExchangeBoundOk(int replyCode, AMQShortString replyText) {
        log.info("[unexpected branch] receiveExchangeBoundOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveQueueBindOk() {
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
        connection.writeFrame(responseBody.generateFrame(channelId));
        changeToConnected();
    }

    private void changeToConnected() {
        ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = connection.getProxyHandlerMap();
        Iterator<Map.Entry<BrokerConf, ProxyHandler>> iterator = proxyHandlerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<BrokerConf, ProxyHandler> entry = iterator.next();
            ProxyHandler proxyHandler = entry.getValue();
            proxyHandler.setState(ProxyHandler.State.Connected);
        }
    }

    @Override
    public void receiveQueueUnbindOk() {
        log.info("[unexpected branch] receiveQueueUnbindOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveQueueDeclareOk(AMQShortString queue, long messageCount, long consumerCount) {
        log.info("[{}] receiveQueueDeclareOk remoteAddress:[{}] localAddress:[{}] queue:[{}]",
                this.connection.getCnx().name(),brokerChannel.remoteAddress(),brokerChannel.localAddress(),queue);
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueuePurgeOk(long messageCount) {
        log.info("[unexpected branch] receiveQueuePurgeOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveQueueDeleteOk(long messageCount) {
        log.info("[unexpected branch] receiveQueueDeleteOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveBasicRecoverSyncOk() {
        log.info("[unexpected branch] receiveBasicRecoverSyncOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveBasicQosOk() {
        log.info("[unexpected branch] receiveBasicQosOk, connection={}", connection.toString());
    }

    @Override
    public void receiveBasicConsumeOk(AMQShortString consumerTag) {
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.
            createBasicConsumeOkBody(consumerTag);
        connection.writeFrame(responseBody.generateFrame(channelId));
        // TODO 这里是否可以去掉了
        changeToConnected();
    }

    @Override
    public void receiveBasicCancelOk(AMQShortString consumerTag) {
        log.info("[unexpected branch] receiveBasicCancelOk, connection={}", connection.toString());
    }

    @Override
    public void receiveBasicReturn(int replyCode, AMQShortString replyText, AMQShortString exchange, AMQShortString routingKey) {
        log.info("[unexpected branch] receiveBasicReturn, connection={}", connection.toString());
    }

    @Override
    public void receiveBasicDeliver(AMQShortString consumerTag, long deliveryTag, boolean redelivered, AMQShortString exchange, AMQShortString routingKey) {
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicDeliverBody basicDeliverBody = methodRegistry.createBasicDeliverBody(consumerTag, deliveryTag, redelivered, exchange, routingKey);
        connection.writeFrame(basicDeliverBody.generateFrame(channelId));
        log.info("[unexpected branch] receiveBasicDeliver, connection={}", connection.toString());
    }

    @Override
    public void receiveBasicGetOk(long deliveryTag, boolean redelivered, AMQShortString exchange, AMQShortString routingKey, long messageCount) {
        log.info("[unexpected branch] receiveBasicGetOk, connection={}", connection.toString());
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        BasicGetOkBody basicGetOkBody = methodRegistry.createBasicGetOkBody(deliveryTag, redelivered, exchange, routingKey, messageCount);
        connection.writeFrame(basicGetOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveBasicGetEmpty() {
        log.info("[unexpected branch] receiveBasicGetEmpty, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveTxSelectOk() {
        log.info("[unexpected branch] receiveTxSelectOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveTxCommitOk() {
        log.info("[unexpected branch] receiveTxCommitOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveTxRollbackOk() {
        log.info("[unexpected branch] receiveTxRollbackOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveConfirmSelectOk() {
        log.info("[unexpected branch] receiveConfirmSelectOk, connection={}", connection.toString());
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        log.info("[unexpected branch] receiveChannelFlow, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        log.info("[unexpected branch] receiveChannelFlowOk, connection={}", connection.toString());
        flush(msgRec);
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        // FIXME 故障
        log.info("[unexpected branch] receiveChannelClose, connection={}", connection.toString());
        ChannelCloseBody channelCloseBody = new ChannelCloseBody(REPLY_SUCCESS, AMQShortString.createAMQShortString("OK"), classId, methodId);
        ChannelCloseBody channelCloseBody1 = new ChannelCloseBody(replyCode, replyText, classId, methodId);
        connection.writeFrame(new AMQFrame(channelId, channelCloseBody1));

        ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = connection.getProxyHandlerMap();
        for (Map.Entry<BrokerConf, ProxyHandler> brokerConfProxyHandlerEntry : proxyHandlerMap.entrySet()) {
            ProxyHandler proxyHandler1 = brokerConfProxyHandlerEntry.getValue();
            if (!proxyHandler1.equals(proxyHandler)) {
                proxyHandler1.getBrokerChannel().writeAndFlush(new AMQFrame(channelId, channelCloseBody));
            }
        }
        Set<ProxyConnection> proxyConnectionSet = connection.getProxyService().getProxyConnectionSet();
        proxyConnectionSet.remove(connection);
        clientChannel.close();
    }

    @Override
    public void receiveChannelCloseOk() {
        log.info("[unexpected branch] receiveChannelCloseOk, connection={}", connection.toString());
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        ChannelCloseOkBody channelCloseOkBody = methodRegistry.createChannelCloseOkBody();
        connection.writeFrame(channelCloseOkBody.generateFrame(channelId));
        ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = connection.getProxyHandlerMap();
        Iterator<Map.Entry<BrokerConf, ProxyHandler>> iterator = proxyHandlerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            ProxyHandler proxyHandler1 = iterator.next().getValue();
            proxyHandler1.getBrokerChannel().close();
            proxyHandler1.setState(ProxyHandler.State.Closed);
        }
    }

    @SneakyThrows
    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        log.info("[unexpected branch] receiveMessageContent, connection={}", connection.toString());
        ContentBody contentBody = new ContentBody(data);
        AMQFrame contentBodyFrame = ContentBody.createAMQFrame(1, contentBody);
        connection.writeFrame(contentBodyFrame);
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        log.info("[unexpected branch] receiveMessageHeader, connection={}", connection.toString());
//        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQFrame contentHeaderBody = ContentHeaderBody.createAMQFrame(channelId, properties, bodySize);
//        clientChannel.writeAndFlush(contentHeaderBody);
        connection.writeFrame(contentHeaderBody);
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        log.info("[unexpected branch] receiveBasicNack, connection={}", connection.toString());
//        AtomicInteger a = connection.getNAck();
//        a.decrementAndGet();
//        if (a.get() == 0) {
//            // log.info"receiveBasicNack:" + a.get());
//            BasicAckBody body = connection.getMethodRegistry().
//                createBasicAckBody(deliveryTag, false);
//            connection.writeFrame(body.generateFrame(channelId));
//            a.set(connection.getProxyHandlerMap().size());
//        }
        BasicAckBody body = connection.getMethodRegistry().
                createBasicAckBody(deliveryTag, false);
        connection.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        Map<Long, AtomicInteger> ackMap = connection.getAckMap();
        log.info("[{}] proxyClient receiveBasicAck remoteAddress: [{}] localAddress: [{}] deliveryTag: [{}] ackMap.size: [{}] ",
                this.connection.getCnx().name(), brokerChannel.remoteAddress(), brokerChannel.localAddress(), deliveryTag, ackMap.size());

        AtomicInteger count = ackMap.get(deliveryTag);
        count.decrementAndGet();
        if (count.get() == 0) {
            // log.info"receiveBasicAck:"+a.get());
            BasicAckBody body = connection.getMethodRegistry().
                    createBasicAckBody(deliveryTag, false);
            connection.writeFrame(body.generateFrame(channelId));
            ackMap.remove(deliveryTag);
        }
    }

}
