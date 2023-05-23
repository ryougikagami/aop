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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.streamnative.pulsar.handlers.amqp.Bundle;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.*;
import org.checkerframework.checker.units.qual.A;


/**
 * Proxy handler is the bridge between client and broker.
 */
@Slf4j
public class ProxyHandler {

    private String vhost;
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    @Getter
    private Channel clientChannel;
    @Getter
    private Channel brokerChannel;
    @Getter
    @Setter
    private State state;

    private List<Object> connectMsgList;

    private static AtomicInteger tag = new AtomicInteger();

    @Getter
    private CompletableFuture<Boolean> flagFuture = new CompletableFuture<>();


    class ProxyHandlerChannelInitializer extends ChannelInitializer<SocketChannel> {

        private ProxyHandler proxyHandler;

        public ProxyHandlerChannelInitializer(ProxyHandler proxyHandler) {
            this.proxyHandler = proxyHandler;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
            ch.pipeline().addLast("processor" + tag.getAndIncrement(), new ProxyBackendHandler(proxyHandler, clientChannel, flagFuture));
        }
    }


    public ProxyHandler(String vhost, ProxyService proxyService, ProxyConnection proxyConnection,
                        String amqpBrokerHost, int amqpBrokerPort, List<Object> connectMsgList,
                        AMQMethodBody responseBody) throws Exception {
        this.proxyService = proxyService;
        this.proxyConnection = proxyConnection;
        clientChannel = this.proxyConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;
        this.vhost = vhost;
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ProxyHandlerChannelInitializer(this));
        ChannelFuture channelFuture = bootstrap.connect(amqpBrokerHost, amqpBrokerPort);

        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                clientChannel.close();
            }else{
                brokerChannel = channelFuture.channel();
            }
        });
        state = State.Init;
    }


    public class ProxyBackendHandler extends ChannelInboundHandlerAdapter implements
            ClientMethodProcessor<ClientChannelMethodProcessor>, FutureListener<Void> {

        @Getter
        private ChannelHandlerContext cnx;
        private AMQMethodBody connectResponseBody;
        private AmqpClientDecoder clientDecoder;
        private ProxyConnection proxyConnection;
        private Channel clientChannel;
        private Object msg;

        @Getter
        @Setter
        private ByteBuf msgRec;
        private AtomicBoolean msgState = new AtomicBoolean(true);
        @Getter
        @Setter
        private ConcurrentLinkedQueue<ByteBuf> msgQueue = new ConcurrentLinkedQueue<>();
        private ProxyHandler proxyHandler;
        private CompletableFuture<Boolean> cf;
        private final ConcurrentLongHashMap<ProxyClientChannel> proxyClientChannelHandlers;

        ProxyBackendHandler(ProxyHandler proxyHandler, Channel clientChannel, CompletableFuture<Boolean> cf) {
            this.proxyHandler = proxyHandler;
            this.proxyConnection = proxyHandler.proxyConnection;
            this.clientChannel = clientChannel;
            this.cf = cf;
            clientDecoder = new AmqpClientDecoder(this);
            this.proxyClientChannelHandlers = new ConcurrentLongHashMap<>();
        }

        @Override
        public void operationComplete(Future future) throws Exception {
            if (future.isSuccess()) {
                brokerChannel.read();
            } else {
                log.warn("[{}] [{}] Failed to write on proxy connection. Closing both connections.", clientChannel,
                        brokerChannel, future.cause());
                clientChannel.close();
            }

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.cnx = ctx;
            log.info("[{}] ProxyBackendHandler [channelActive] vhost: [{}] proxyHandlerAddress:[{}] proxyHandler:[{}]"
                    , cnx.name(), vhost, cnx.channel().localAddress(), proxyHandler);
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                ((ByteBuf) msg).retain();
                brokerChannel.writeAndFlush(msg).syncUninterruptibly();
            }
            brokerChannel.read();
            // state = State.Connected;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msgRec) throws Exception {
            // log.info"[{}] ProxyBackendHandler [channelRead]", vhost);
//            log.info("[{}] proxyHandler channelRead state={}", cnx.name(), state);
            switch (state) {
                case Init:
                    ByteBuf buffer1 = (ByteBuf) msgRec;
//                        this.msgRec = buffer1.copy();
                    Channel nettyChannel1 = ctx.channel();
                    checkState(nettyChannel1.equals(this.cnx.channel()));
                    try {
                        clientDecoder.decodeBuffer(buffer1.nioBuffer());
                    } catch (Throwable e) {
                        log.error("error while handle command:", e);
                        e.printStackTrace();
                        close();
                    } finally {
                        buffer1.release();
                    }
                    break;
                case Failed:
                    // Get a buffer that contains the full frame
                    ByteBuf buffer = (ByteBuf) msgRec;
                    this.msg = msgRec;
                    Channel nettyChannel = ctx.channel();
                    checkState(nettyChannel.equals(this.cnx.channel()));

                    try {
                        clientDecoder.decodeBuffer(buffer.nioBuffer());
                    } catch (Throwable e) {
                        log.error("error while handle command:", e);
                        close();
                    } finally {
                        buffer.release();
                    }
                    break;
                case Connected:
                    ByteBuf buf = (ByteBuf) msgRec;
                    ByteBuffer byteBuffer = buf.nioBuffer(0, 1);
                    byte b = byteBuffer.get(0);
//                    log.info("[{}] ProxyHandler channelRead type={}", cnx.name(), b);
                    if (b == 1) {
                        ByteBuffer byteBuffer1 = buf.nioBuffer(0, 11);
                        if (
                            // (byteBuffer1.get(8)==20&&byteBuffer1.get(10)==40)    // Channel.Close
                            // ||(byteBuffer1.get(8)==20&&byteBuffer1.get(10)==11) // Channel.OpenOk
                            // ||(byteBuffer1.get(8)==40&&byteBuffer1.get(10)==11) // Exchange.DeclareOk
                            // || (byteBuffer1.get(8)==50&&byteBuffer1.get(10)==11) // queue.DeclareOk
                            // || (byteBuffer1.get(8)==50&&byteBuffer1.get(10)==21)  // queue.BindOk
                            // || (byteBuffer1.get(8)==60&&byteBuffer1.get(10)==21)  // Basic.ConsumeOk

                                (byteBuffer1.get(8) == 20 && byteBuffer1.get(10) == 40)      //Channel.close
                                        || (byteBuffer1.get(8) == 60 && byteBuffer1.get(10) == 80)    //Basic.Ack
                                        || (byteBuffer1.get(8) == 20 && byteBuffer1.get(10) == 11)    //Channel.OpenOk
                                        || (byteBuffer1.get(8) == 40 && byteBuffer1.get(10) == 11)    //Exchange.DeclareOk
                        ) {
                            try {
                                clientDecoder.decodeBuffer(buf.nioBuffer());
                            } catch (Exception e) {
                                log.info("[{}] ----- error while handle command:", cnx.name(), e);
                                e.printStackTrace();
                            } finally {
                                buf.release();
                            }
                        } else {
                            clientChannel.writeAndFlush(msgRec).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (future.isSuccess()) {
                                        if (buf.refCnt() != 0) {
                                            buf.release();
                                        }
                                    }
                                }
                            });
                        }
                    } else {
//                        log.info("[{}] proxyHandler channelRead type={}", cnx.name(), b);
                        clientChannel.writeAndFlush(msgRec).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                if (future.isSuccess()) {
                                    if (buf.refCnt() != 0) {
                                        buf.release();
                                    }
                                }
                            }
                        });
                    }
                    break;
                case Closed:
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            log.error("[{}] ProxyBackendHandler [exceptionCaught] vhost: [{}] msg: [{}] cause: [{}] remoteHost: [{}] localHost: [{}] pcrHost: [{}] pclHost: [{}]", cnx.name(), vhost, cause.getMessage(), cause, proxyHandler.brokerChannel.remoteAddress(), proxyHandler.brokerChannel.localAddress(), proxyHandler.proxyConnection.getCnx().channel().remoteAddress(), proxyHandler.proxyConnection.getCnx().channel().localAddress());
//            proxyConnection.getCnx().channel().close();
//            proxyConnection.close();
//            state = State.Failed;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            // TODO 故障
            super.channelInactive(ctx);
            proxyClientChannelHandlers.clear();
            ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = proxyConnection.getProxyHandlerMap();
            Iterator<Map.Entry<BrokerConf, ProxyHandler>> iterator = proxyHandlerMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<BrokerConf, ProxyHandler> next = iterator.next();
                if (next.getValue().equals(proxyHandler)) {
                    // proxyHandlerMap.remove(next.getKey());
                    log.info("[{}] proxyHandle removed remoteHost: [{}] localHost: [{}] connectionRemoteAddress: [{}] connectionLocalAddress: [{}]",
                            cnx.name(), proxyHandler.brokerChannel.remoteAddress(), proxyHandler.brokerChannel.localAddress(), proxyHandler.proxyConnection.getCnx().channel().remoteAddress(), proxyHandler.proxyConnection.getCnx().channel().localAddress());
                    iterator.remove();
                    break;
                }
            }
            log.warn("[{}] ProxyBackendHandler [channelInactive] vhost: [{}] remoteHost: [{}] localHost: [{}]", cnx.name(), vhost, proxyHandler.brokerChannel.remoteAddress(), proxyHandler.brokerChannel.localAddress());
        }

        @Override
        public void receiveConnectionStart(short i, short i1, FieldTable fieldTable, byte[] bytes, byte[] bytes1) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionStart]", cnx.name());
        }

        @Override
        public void receiveConnectionSecure(byte[] bytes) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionSecure]", cnx.name());
        }

        @Override
        public void receiveConnectionRedirect(AMQShortString amqShortString, AMQShortString amqShortString1) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionRedirect] amqShortString: [{}]", cnx.name(), amqShortString.toString());
        }

        @Override
        public void receiveConnectionTune(int i, long l, int i1) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionTune]", cnx.name());
        }

        @Override
        public void receiveConnectionOpenOk(AMQShortString amqShortString) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionOpenOk] amqShortString: [{}]", cnx.name(), amqShortString.toString());
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            msgState.set(true);
            log.info("[{}] ProxyBackendHandler [getProtocolVersion]", cnx.name());
            return ProtocolVersion.v0_91;
        }

        @Override
        public ClientChannelMethodProcessor getChannelMethodProcessor(int i) {
            log.info("[{}] ProxyBackendHandler [ClientChannelMethodProcessor]", cnx.name());
            proxyClientChannelHandlers.putIfAbsent(i, new ProxyClientChannel(i, proxyConnection, clientChannel, brokerChannel, proxyHandler, cf));
            return proxyClientChannelHandlers.get(i);
        }

        @Override
        public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {
            log.info("[{}] ProxyBackendHandler [receiveConnectionClose]", cnx.name());
            close();
        }

        @Override
        public void receiveConnectionCloseOk() {
            log.info("[{}] ProxyBackendHandler [receiveConnectionCloseOK]", cnx.name());
        }

        @Override
        public void receiveHeartbeat() {
//            log.info("[{}] ProxyBackendHandler [receiveHeartbeat]", cnx.name());
        }

        @Override
        public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
            log.info("[{}] ProxyBackendHandler [receiveProtocolHeader] protocolInitiation=[{}]", cnx.name(), protocolInitiation);
        }

        @Override
        public void setCurrentMethod(int i, int i1) {
        }

        @Override
        public boolean ignoreAllButCloseOk() {
            msgState.set(true);
            return false;
        }
    }

    public void close() {
        // TODO
        state = State.Closed;
        if (this.brokerChannel != null && this.brokerChannel.isActive()) {
            this.brokerChannel.close();
        }
    }

    public enum State {
        Init,
        Connected,
        Failed,
        Closed
    }

}
