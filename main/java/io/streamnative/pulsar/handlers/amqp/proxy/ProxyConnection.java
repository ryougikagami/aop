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
package io.streamnative.pulsar.handlers.amqp.proxy;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.streamnative.pulsar.handlers.amqp.Bundle;
import io.streamnative.pulsar.handlers.amqp.collection.SyncConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;
import sun.nio.ch.DirectBuffer;

/**
 * Proxy connection.
 */
@Slf4j
public class ProxyConnection extends ChannelInboundHandlerAdapter implements ServerMethodProcessor<ServerChannelMethodProcessor>, FutureListener<Void> {
    @Getter
    private ProxyService proxyService;
    @Getter
    private ProxyConfiguration proxyConfig;
    @Getter
    private ChannelHandlerContext cnx;
    @Getter
    @Setter
    private State state;
    @Getter
    @Setter
    private ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private AtomicInteger count = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicInteger close = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicInteger open = new AtomicInteger(3);
    @Getter
    @Setter
    private Map<Long, AtomicInteger> ackMap = new HashMap<>();
    @Getter
    @Setter
    private AtomicInteger nAck = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicInteger declareOk = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicInteger exDeleteOk = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicInteger confirmSelectOk = new AtomicInteger(3);
    @Getter
    @Setter
    private AtomicBoolean ticketUsed = new AtomicBoolean(false);
    @Getter
    @Setter
    private AtomicLong exchangeDeclareLatencyMills = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicLong queueDeclareLatencyMills = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicLong queueBindLatencyMills = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicLong consumeLatencyMills = new AtomicLong(0);
    @Getter
    protected AmqpBrokerDecoder brokerDecoder;
    @Getter
    private MethodRegistry methodRegistry;
    @Getter
    private ProtocolVersion protocolVersion;
    @Getter
    private LookupHandler lookupHandler;
    @Getter
    private AMQShortString virtualHost;
    @Getter
    private String vhost;
    @Getter
    private List<Object> connectMsgList = new ArrayList<>();
    @Getter
    private ByteBuf bb;
    @Getter
    @Setter
    private Boolean flag = false;
    @Getter
    private Map<String, List<Object>> queueConsumerMap = new HashMap<>();
    @Getter
    @Setter
    private String UUID;
    @Getter
    @Setter
    private int ackCount;
    @Getter
    @Setter
    private boolean defaultEx;
    @Getter
    @Setter
    private AMQShortString routingKey;

    @Getter
    @Setter
    private boolean confirmSelect = false;

    private int heartTimes = 0;

    public static final int MAX_HEART_TIMES = 20;

    @Getter
    private Boolean isChannelFinished = false;

    @Getter
    private int channelId;

    @Getter
    @Setter
    private long msgId = 0;

    private final ConcurrentLongHashMap<ProxyChannel> proxyChannelHandlers;

    @Getter
    public enum State {
        Init, RedirectLookup, RedirectToBroker, Closed
    }

    public ProxyConnection(ProxyService proxyService) throws PulsarClientException {
        log.info("ProxyConnection init ...");
        this.proxyService = proxyService;
        this.proxyConfig = proxyService.getProxyConfig();
        brokerDecoder = new AmqpBrokerDecoder(this);
        protocolVersion = ProtocolVersion.v0_91;
        methodRegistry = new MethodRegistry(protocolVersion);
        lookupHandler = proxyService.getLookupHandler();
        state = State.Init;
        ackCount = proxyService.getBundleList().size();
        this.proxyChannelHandlers = new ConcurrentLongHashMap<>();
    }

    @Override
    public void channelActive(ChannelHandlerContext cnx) throws Exception {
        super.channelActive(cnx);
        this.cnx = cnx;
        proxyService.getProxyConnectionSet().add(this);
        printLogMessage("[{}] ProxyConnection [channelActive] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
    }

    public void printLogMessage(String message, Object... var1) {
        log.info(message, var1);
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        log.info("[{}] ----- proxyService - [proxyConnectionSet] ------- size: [{}]", this.cnx.name(), proxyConnectionSet.size());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Iterator<Map.Entry<BrokerConf, ProxyHandler>> iterator = proxyHandlerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            ProxyHandler proxyHandler = iterator.next().getValue();
            if (proxyHandler != null) {
                proxyHandler.close();
                iterator.remove();
            }
        }
        proxyHandlerMap.clear();
        proxyChannelHandlers.clear();
        brokerDecoder.close();
        proxyService.getProxyConnectionSet().remove(this);
        connectMsgList.forEach(msg->{
            ByteBuf byteBuf = (ByteBuf) msg;
            byteBuf.release();
        });
        state = State.Closed;
        printLogMessage("[{}] ProxyConnection [channelInactive] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
        if (cnx != null) {
            cnx.channel().close();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void operationComplete(Future future) throws Exception {
        if (future.isSuccess()) {
            cnx.read();
        } else {
            log.warn("Error in writing to inbound channel. Closing", future.cause());
            close();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (state) {
            case Init:
            case RedirectLookup:
                ByteBuf buffer = (ByteBuf) msg;
                if (!isChannelFinished) {
                    buffer.retain();
                    connectMsgList.add(msg);
                }

//                log.info("channelRead msg: [{}]", buffer.nioBuffer().toString());

                // log.info("proxy bufferSize: [{}]", buffer.capacity());
                // ByteBuffer byteBuffer1 = buffer.nioBuffer(0, 11);
                // log.info("proxy channelRead type: [{}] classId: [{}] methodId: [{}]", byteBuffer1.get(0), byteBuffer1.get(8), byteBuffer1.get(10));
                // log.info("reset bufferMsg: [{}]", buffer);

                bb = (ByteBuf) msg;


                try {
                    brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
                } catch (Throwable e) {
                    log.error("error while handle command:", e);
                    e.printStackTrace();
                    close();
                } finally {
//                    ((DirectBuffer)buffer.nioBuffer()).cleaner().clean();
                    buffer.release();
                }

                break;
            case RedirectToBroker:
                break;
            case Closed:
                break;
            default:
                log.error("ProxyConnection [channelRead] - invalid state");
                break;
        }
    }

    // step 1
    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        log.info("[{}] ProxyConnection [receiveProtocolHeader] Protocol Header: [{}] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                protocolInitiation,
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());

        // 只有所有的broker启动后才能建立链接, 只有在RUNNING状态才能继续建立连接
        if (ProxyService.BrokerState.Init.equals(proxyService.getState())) {
            proxyService.getBrokerInitLock().lock();
            log.info("[{}] acquire the lock ", cnx.name());
            try {
                if (ProxyService.BrokerState.Init.equals(proxyService.getState())) {
                    proxyService.bundleInit();
                    proxyService.getBundleBrokerBindMap().forEach((bundle, broker) -> {
                        log.info("bindResult bundle: [{}], broker: [{}]", bundle, broker);
                    });
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            } finally {
                proxyService.getBrokerInitLock().unlock();
            }
        }
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            // TODO serverProperties mechanis
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody((short) protocolVersion.getMajorVersion(), (short) pv.getActualMinorVersion(), null,
                    // TODO temporary modification
                    "PLAIN".getBytes(US_ASCII), "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (Exception e) {
            log.error("Received unsupported protocol initiation for protocol version: [{}] ", getProtocolVersion(), e);
        }
    }

    // step 2
    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response, AMQShortString locale) {
        log.info("[{}] ProxyConnection [receiveConnectionStartOk] clientProperties: [{}], mechanism: [{}], locale: [{}], remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                clientProperties,
                mechanism,
                locale,
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
        // TODO AUTH
        ConnectionTuneBody tuneBody = methodRegistry.createConnectionTuneBody(proxyConfig.getAmqpMaxNoOfChannels(), proxyConfig.getAmqpMaxFrameSize(), proxyConfig.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    // step 3
    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        log.info("[{}] ProxyConnection [receiveConnectionSecureOk] response: [{}], remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                new String(response, UTF_8),
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
        ConnectionTuneBody tuneBody = methodRegistry.createConnectionTuneBody(proxyConfig.getAmqpMaxNoOfChannels(), proxyConfig.getAmqpMaxFrameSize(), proxyConfig.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    // step 4
    @Override
    public void receiveConnectionTuneOk(int i, long l, int i1) {

        //FIXME 商议heartBeat无用，客户端发送heartBeat始终为60
        log.info("[{}] ProxyConnection [receiveConnectionTuneOk], channelMax: [{}] frameMax: [{}] heartbeat: [{}] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                i, l, i1,
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
        brokerDecoder.setMaxFrameSize((int) l);
    }

    // step 5
    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        log.info("[{}] ProxyConnection [receiveConnectionOpen] virtualHost: [{}] capabilities: [{}] insist: [{}], remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                virtualHost,
                capabilities,
                insist,
                this.cnx.channel().remoteAddress(),
                this.cnx.channel().localAddress());
        this.virtualHost = virtualHost;
        state = State.RedirectLookup;
        String virtualHostStr = AMQShortString.toString(virtualHost);
        if (virtualHostStr.equals("/")) {
            virtualHostStr = "/vhost1";
        }
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/') {
            virtualHostStr = virtualHostStr.substring(1);
            if (org.apache.commons.lang.StringUtils.isEmpty(virtualHostStr)) {
                virtualHostStr = AmqpConnection.DEFAULT_NAMESPACE;
            }
        }
        vhost = virtualHostStr;
        AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHost);
        writeFrame(responseBody.generateFrame(0));
    }

    @SneakyThrows
    @Override
    public void receiveChannelOpen(int channelId) {
        log.info("[{}] ProxyConnection [receiveChannelOpen], channelID: [{}], remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(),
                channelId,
                this.cnx.channel().localAddress(),
                this.cnx.channel().remoteAddress());
        this.channelId = channelId;
        isChannelFinished = true;
        List<Bundle> bundleList = proxyService.getBundleList();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        for (Bundle bundle : bundleList) {
            Pair<String, Integer> pair = bundleBrokerBindMap.get(bundle);
            String aopBrokerHost = pair.getKey();
            Integer aopBrokerPort = pair.getValue();
            // TODO
            aopBrokerPort = 5682;
            BrokerConf brokerConf = new BrokerConf(aopBrokerHost, aopBrokerPort);
            // 判断是否已经建立过连接
            if (!proxyHandlerMap.containsKey(brokerConf)) {
                createProxyHandler(brokerConf);
            }
        }
        open = new AtomicInteger(proxyHandlerMap.size());
        int count = proxyService.getBundleList().size();
        declareOk = new AtomicInteger(count);
        exDeleteOk = new AtomicInteger(count);
        close = new AtomicInteger(proxyHandlerMap.size());
    }

    public ProxyHandler createProxyHandler(BrokerConf brokerConf) {
        AMQMethodBody responseBody = this.getMethodRegistry().createChannelOpenOkBody();
        ProxyHandler proxyHandler = null;
        try {
            proxyHandler = new ProxyHandler(this.getVhost(), this.getProxyService(), this, brokerConf.getAopBrokerHost(), brokerConf.getAopBrokerPort(), this.getConnectMsgList(), responseBody);
            proxyHandlerMap.put(brokerConf, proxyHandler);
            log.info("[{}] ProxyConnection createProxyHandler [{}] BrokerConf----[{}]", this.cnx.name(), proxyHandler, brokerConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return proxyHandler;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        log.info("[{}] ProxyConnection [getProtocolVersion] version: [{}] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(), protocolVersion, this.cnx.channel().remoteAddress(), this.cnx.channel().localAddress());
        return protocolVersion;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        // TODO 每次decode都new ProxyChannel()
        proxyChannelHandlers.putIfAbsent(channelId,new ProxyChannel(channelId, this));
        return proxyChannelHandlers.get(channelId);
    }

    @Override
    public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {
        proxyService.getProxyConnectionSet().remove(this);
        log.info("[{}] ProxyConnection [receiveConnectionClose] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(), this.cnx.channel().remoteAddress(), this.cnx.channel().localAddress());
        ConnectionCloseBody connectionCloseBody = methodRegistry.createConnectionCloseBody(i,amqShortString,i1,i2);
        for (Map.Entry<BrokerConf, ProxyHandler> brokerConfProxyHandlerEntry : proxyHandlerMap.entrySet()) {
            ProxyHandler proxyHandler = brokerConfProxyHandlerEntry.getValue();
            proxyHandler.getBrokerChannel().writeAndFlush(new AMQFrame(channelId,connectionCloseBody));
        }
    }


    @Override
    public void receiveConnectionCloseOk() {
        log.info("[{}] ProxyConnection [receiveConnectionCloseOk] remoteAddress: [{}] localAddress: [{}]",
                this.cnx.name(), this.cnx.channel().remoteAddress(), this.cnx.channel().localAddress());
    }

    @Override
    public void receiveHeartbeat() {
//        log.info("[{}] ProxyConnection [receiveHeartbeat] remoteAddress: [{}] localAddress: [{}]",
//                this.cnx.name(), this.cnx.channel().remoteAddress(), this.cnx.channel().localAddress());
        if (++heartTimes == MAX_HEART_TIMES) {
            SyncConcurrentHashMap<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
            Set<BrokerConf> brokerConfSet = new HashSet<>();
            bundleBrokerBindMap.forEach((k, v) -> {
                brokerConfSet.add(new BrokerConf(v));
            });
            brokerConfSet.forEach(brokerConf -> {
                HeartbeatBody heartbeatBody = new HeartbeatBody();
                writeAndFlushMsg(brokerConf, new AMQFrame(channelId, heartbeatBody));
            });
            heartTimes = 0;
        }
    }


    @Override
    public void setCurrentMethod(int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [setCurrentMethod] classId: {}, methodId: {}", classId, methodId);
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [ignoreAllButCloseOk]");
        }
        return false;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("send: " + frame);
        }
        cnx.writeAndFlush(frame);
    }

    public void close() {
        log.info("[{}] ProxyConnection close remoteAddress: [{}] localAddress: [{}]",
                cnx.name(), cnx.channel().remoteAddress(), cnx.channel().localAddress());
        Iterator<Map.Entry<BrokerConf, ProxyHandler>> iterator = proxyHandlerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            ProxyHandler proxyHandler = iterator.next().getValue();
            proxyHandler.close();
            iterator.remove();
        }
        proxyHandlerMap.clear();
        proxyService.getProxyConnectionSet().remove(this);
        if (cnx != null) {
            cnx.channel().close();
        }
        state = State.Closed;
    }

    public void writeAndFlushMsg(BrokerConf brokerConf, Object msg){
//        log.info("[{}] writeAndFlushMsg brokerConf={}", connection.getCnx().name(), brokerConf);
        //所有的handle都失去连接
        if (proxyHandlerMap.size() == 0) {
            close();
            return;
        }
//        log.info("[{}] writeAndFlushMsg proxyHandlerMap.size={}", connection.getCnx().name(), proxyHandlerMap.size());
        ProxyHandler proxyHandler = proxyHandlerMap.get(brokerConf);
//        log.info("[{}] writeAndFlushMsg proxyHandler={}", connection.getCnx().name(), proxyHandler);
        if (Objects.isNull(proxyHandler)) {
            proxyHandler = createProxyHandler(brokerConf);
            log.info("proxyHandle is reCreate because of Null!!! proxyHandler={}", proxyHandler);
        }

//        ProxyHandler finalProxyHandler = proxyHandler;
//        proxyHandler.getFlagFuture().whenCompleteAsync((flag, throwable)->{
        proxyHandler.getBrokerChannel().writeAndFlush(msg);
//        });
    }

}
