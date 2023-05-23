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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.US_ASCII;

import com.alibaba.fastjson.JSON;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConnection;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import io.streamnative.pulsar.handlers.amqp.utils.ZkUtils;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.apache.bookkeeper.util.collections.ConcurrentLongLongHashMap;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfigurationUtils;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQDecoder;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodDispatcher;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.hamcrest.core.IsInstanceOf;

/**
 * Amqp server level method processor.
 */
@Log4j2
public class AmqpConnection extends AmqpCommandDecoder implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    enum ConnectionState {
        INIT,
        AWAIT_START_OK,
        AWAIT_SECURE_OK,
        AWAIT_TUNE_OK,
        AWAIT_OPEN,
        OPEN
    }

    public static final String DEFAULT_NAMESPACE = "default";

    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    private long connectionId;
    private final ConcurrentLongHashMap<AmqpChannel> channels;
    private final ConcurrentLongLongHashMap closingChannelsList = new ConcurrentLongLongHashMap();
    @Getter
    private final AmqpServiceConfiguration amqpConfig;
    private ProtocolVersion protocolVersion;
    private MethodRegistry methodRegistry;
    private ByteBufferSender bufferSender;
    private volatile ConnectionState state = ConnectionState.INIT;
    private volatile int currentClassId;
    private volatile int currentMethodId;
    @Getter
    private final AtomicBoolean orderlyClose = new AtomicBoolean(false);
    private volatile int maxChannels;
    private volatile int maxFrameSize;
    private volatile int heartBeat;
    private NamespaceName namespaceName;
    private final Object channelAddRemoveLock = new Object();
    private AtomicBoolean blocked = new AtomicBoolean();
    private AmqpOutputConverter amqpOutputConverter;
    private ServerCnx pulsarServerCnx;
    private AmqpBrokerService amqpBrokerService;
    @Getter
    private PulsarServiceLookupHandler pulsarServiceLookupHandler;
    @Getter
    private static final Map<String, Bundle> topicBundleMap = Maps.newConcurrentMap();

    @Getter
    private SocketAddress remoteAddress;
    @Getter
    private SocketAddress localAddress;


    @SneakyThrows
    public AmqpConnection(AmqpServiceConfiguration amqpConfig,
                          AmqpBrokerService amqpBrokerService) {
        super(amqpBrokerService.getPulsarService(), amqpConfig);
        this.connectionId = ID_GENERATOR.incrementAndGet();
        this.channels = new ConcurrentLongHashMap<>();
        this.protocolVersion = ProtocolVersion.v0_91;
        this.methodRegistry = new MethodRegistry(this.protocolVersion);
        this.bufferSender = new AmqpByteBufferSenderImpl(this);
        this.amqpConfig = amqpConfig;
        this.maxChannels = amqpConfig.getAmqpMaxNoOfChannels();
        this.maxFrameSize = amqpConfig.getAmqpMaxFrameSize();
        this.heartBeat = amqpConfig.getAmqpHeartBeat();
        this.amqpOutputConverter = new AmqpOutputConverter(this);
        this.amqpBrokerService = amqpBrokerService;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        this.remoteAddress = this.ctx.channel().remoteAddress();
        this.localAddress = this.ctx.channel().localAddress();
        log.info("[{}] AmqpConnection channelActive remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        isActive.set(true);
        this.brokerDecoder = new AmqpBrokerDecoder(this);
        this.pulsarServerCnx = new AmqpPulsarServerCnx(getPulsarService(), ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.warn("[{}] AmqpConnection channelInactive remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        super.channelInactive(ctx);
        completeAndCloseAllChannels();
        amqpBrokerService.getConnectionContainer().removeConnection(namespaceName, this);
        this.brokerDecoder.close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;

        Channel nettyChannel = ctx.channel();
        checkState(nettyChannel.equals(this.ctx.channel()));

        // log.info("amqp bufferSize, capacity:{}", buffer.capacity());
        // ByteBuffer byteBuffer1 = buffer.nioBuffer(0, 11);
        // log.info("amqp channelRead type: [{}] classId: [{}] methodId: [{}]", byteBuffer1.get(0), byteBuffer1.get(8), byteBuffer1.get(10));
        // log.info("amqp bufferMsg: [{}]", buffer);

        try {
            brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
            receivedCompleteAllChannels();
        } catch (Throwable e) {
            log.error("[{}] ----- error while handle command:", ctx.name(), e);
            e.printStackTrace();
            close();
        }
        finally {
            buffer.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {} cause: [{}] remoteAddress: [{}] localAddress: [{}]", ctx.name(), cause.getMessage(), cause, remoteAddress, localAddress);
        close();
    }

    @Override
    protected void close() {
        if (isActive.getAndSet(false)) {
            // log.info"close netty channel {}", ctx.channel());
            ctx.close();
        }
    }

    @SneakyThrows
    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
                                         AMQShortString locale) {
        log.info("[{}] receiveConnectionStartOk remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        assertState(ConnectionState.AWAIT_START_OK);
        // TODO clientProperties
//        try {
//            int index = 0;
//            for (int i = 1; i < response.length; i++) {
//                if(response[i]==0){
//                    index = i;
//                    break;
//                }
//            }
//            byte[] accByte = Arrays.copyOfRange(response, 1, index);
//            byte[] pwdByte = Arrays.copyOfRange(response, index + 1, response.length);
//            String account = new String(accByte);
//            String pwd = new String(pwdByte);
//            PulsarService pulsarService = this.getPulsarService();
//            MetadataStore metadataStore = pulsarService.getConfigurationMetadataStore();
//            CompletableFuture<Optional<GetResult>> re = metadataStore.get("/" + account);
//            Optional<GetResult> getResult = re.join();
//            GetResult temp = getResult.get();
//            byte[] v = temp.getValue();
//            String data = new String(v);
//            pwd = ZkUtils.getSHA256Str(pwd);
//            if (!pwd.equals(data)) {
//                throw new AMQFrameDecodingException("用户名或密码错误");
//            }
//        }catch (Exception e){
//            throw new AMQFrameDecodingException("用户名或密码错误");
//        }
        // TODO security process
//        AMQMethodBody responseBody = this.methodRegistry.createConnectionSecureBody(new byte[0]);
//        writeFrame(responseBody.generateFrame(0));
//        state = ConnectionState.AWAIT_SECURE_OK;

        ConnectionTuneBody tuneBody =
            methodRegistry.createConnectionTuneBody(maxChannels,
                maxFrameSize,
                heartBeat);

        writeFrame(tuneBody.generateFrame(0));
        state = ConnectionState.AWAIT_TUNE_OK;
    }

    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        log.info("[{}] RECV ConnectionSecureOk remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        assertState(ConnectionState.AWAIT_SECURE_OK);
        // TODO AUTH
        ConnectionTuneBody tuneBody =
            methodRegistry.createConnectionTuneBody(maxChannels,
                maxFrameSize,
                heartBeat);
        writeFrame(tuneBody.generateFrame(0));
        state = ConnectionState.AWAIT_TUNE_OK;
    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
        log.info("[{}] amqp receiveConnectionTuneOk channelMax: [{}] frameMax: [{}] heartbeat: [{}] remoteAddress: [{}] localAddress: [{}]",
            ctx.name(), channelMax, frameMax, heartbeat, remoteAddress, localAddress);
        assertState(ConnectionState.AWAIT_TUNE_OK);

        if (heartbeat > 0) {
            this.heartBeat = heartbeat;
            long writerDelay = 1000L * heartbeat;
            long readerDelay = 1000L * 2 * heartbeat * ProxyConnection.MAX_HEART_TIMES;
//            log.info("writeDelay={} readerDelay={}", writerDelay, readerDelay);
            initHeartBeatHandler(writerDelay, readerDelay);
        }
        int brokerFrameMax = maxFrameSize;
        if (brokerFrameMax <= 0) {
            brokerFrameMax = Integer.MAX_VALUE;
        }

        if (frameMax > (long) brokerFrameMax) {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                "Attempt to set max frame size to " + frameMax
                    + " greater than the broker will allow: "
                    + brokerFrameMax, 0);
        } else if (frameMax > 0 && frameMax < AMQDecoder.FRAME_MIN_SIZE) {
            sendConnectionClose(ErrorCodes.SYNTAX_ERROR,
                "Attempt to set max frame size to " + frameMax
                    + " which is smaller than the specification defined minimum: "
                    + AMQFrame.getFrameOverhead(), 0);
        } else {
            int calculatedFrameMax = frameMax == 0 ? brokerFrameMax : (int) frameMax;
            setMaxFrameSize(calculatedFrameMax);

            //0 means no implied limit, except that forced by protocol limitations (0xFFFF)
            int value = ((channelMax == 0) || (channelMax > 0xFFFF))
                ? 0xFFFF
                : channelMax;
            maxChannels = value;
        }
        state = ConnectionState.AWAIT_OPEN;

    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        log.info("[{}] amqp receiveConnectionOpen remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        assertState(ConnectionState.AWAIT_OPEN);

        boolean isDefaultNamespace = false;
        String virtualHostStr = AMQShortString.toString(virtualHost);
        if (virtualHostStr.equals("/")) {
            virtualHostStr = "vhost1";
        }
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/') {
            virtualHostStr = virtualHostStr.substring(1);
            if (StringUtils.isEmpty(virtualHostStr)) {
                virtualHostStr = DEFAULT_NAMESPACE;
                isDefaultNamespace = true;
            }
        }


        NamespaceName namespaceName = NamespaceName.get(amqpConfig.getAmqpTenant(), virtualHostStr);

        // log.info"yqtest"+namespaceName.toString()+JSON.toJSONString(namespaceName));
        if (isDefaultNamespace) {
            // avoid the namespace public/default is not owned in standalone mode
            TopicName topic = TopicName.get(TopicDomain.persistent.value(),
                namespaceName, "__lookup__");
            LookupOptions lookupOptions = LookupOptions.builder().authoritative(true).build();
            getPulsarService().getNamespaceService().getBrokerServiceUrlAsync(topic, lookupOptions);
        }
        // Policies policies = getPolicies(namespaceName);
//        if (policies != null) {
        this.namespaceName = namespaceName;

        MethodRegistry methodRegistry = getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHost);
        writeFrame(responseBody.generateFrame(0));
        state = ConnectionState.OPEN;
        amqpBrokerService.getConnectionContainer().addConnection(namespaceName, this);
//        } else {
//            sendConnectionClose(ErrorCodes.NOT_FOUND,
//                "Unknown virtual host: '" + virtualHostStr + "'", 0);
//        }
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText,
                                       int classId, int methodId) {
        log.info("[{}] amqp receiveConnectionClose remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        try {
            if (orderlyClose.compareAndSet(false, true)) {
                completeAndCloseAllChannels();
            }

            MethodRegistry methodRegistry = getMethodRegistry();
            ConnectionCloseOkBody responseBody = methodRegistry.createConnectionCloseOkBody();
            writeFrame(responseBody.generateFrame(0));
        } catch (Exception e) {
            log.error("[{}] ----- Error closing connection for " + ctx.name(), this.remoteAddress.toString(), e);
        }
        finally {
            close();
        }
    }

    @Override
    public void receiveConnectionCloseOk() {
        log.info("[{}] amqp receiveConnectionCloseOK remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        close();
    }

    public void sendConnectionClose(int errorCode, String message, int channelId) {
        sendConnectionClose(channelId, new AMQFrame(0, new ConnectionCloseBody(getProtocolVersion(),
            errorCode, AMQShortString.validValueOf(message), currentClassId, currentMethodId)));
    }

    private void sendConnectionClose(int channelId, AMQFrame frame) {
        if (orderlyClose.compareAndSet(false, true)) {
            try {
                markChannelAwaitingCloseOk(channelId);
                completeAndCloseAllChannels();
            }
            finally {
                writeFrame(frame);
            }
        }
    }

    @Override
    public void receiveChannelOpen(int channelId) {
        log.info("[{}] amqp receiveChannelOpen channelId: [{}] remoteAddress: [{}] localAddress: [{}]", ctx.name(), channelId, remoteAddress, localAddress);
        assertState(ConnectionState.OPEN);

        if (this.namespaceName == null) {
            sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                "Virtualhost has not yet been set. ConnectionOpen has not been called.", channelId);
        } else if (channels.get(channelId) != null || channelAwaitingClosure(channelId)) {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR, "Channel " + channelId + " already exists", channelId);
        } else if (channelId > maxChannels) {
            sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                "Channel " + channelId + " cannot be created as the max allowed channel id is "
                    + maxChannels,
                channelId);
        } else {
            final AmqpChannel channel = new AmqpChannel(channelId, this, amqpBrokerService);
            addChannel(channel);
            ChannelOpenOkBody response = getMethodRegistry().createChannelOpenOkBody();
            writeFrame(response.generateFrame(channelId));
        }
    }

    private void addChannel(AmqpChannel channel) {
//        log.info("[{}] addChannel remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        synchronized (channelAddRemoveLock) {
            channels.put(channel.getChannelId(), channel);
            if (blocked.get()) {
                channel.block();
            }
        }
    }

    @Override
    public void receiveHeartbeat() {
//        log.info("[{}] amqp receiveHeartbeat remoteAddress: [{}] localAddress: [{}]", ctx.name(), remoteAddress, localAddress);
        // noop
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation pi) {
        log.info("[{}] receiveProtocolHeader ProtocolInitiation: [{}] remoteAddress: [{}] localAddress: [{}]", ctx.name(), pi, remoteAddress, localAddress);
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = pi.checkVersion(); // Fails if not correct
            // TODO serverProperties mechanis
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                (short) protocolVersion.getMajorVersion(),
                (short) pv.getActualMinorVersion(),
                null,
                // TODO temporary modification
                "PLAIN".getBytes(US_ASCII),
                "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
            state = ConnectionState.AWAIT_START_OK;
        } catch (Exception e) {
            log.error("[{}] ----- Received unsupported protocol initiation for protocol version: {} ", ctx.name(), getProtocolVersion(), e);
            writeFrame(new ProtocolInitiation(ProtocolVersion.v0_91));
            throw new RuntimeException(e);
        }
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return this.protocolVersion;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        assertState(ConnectionState.OPEN);
        ServerChannelMethodProcessor channelMethodProcessor = getChannel(channelId);
        if (channelMethodProcessor == null) {
            channelMethodProcessor =
                (ServerChannelMethodProcessor) Proxy.newProxyInstance(ServerMethodDispatcher.class.getClassLoader(),
                    new Class[] {ServerChannelMethodProcessor.class}, new InvocationHandler() {
                        @Override
                        public Object invoke(final Object proxy, final Method method, final Object[] args)
                            throws Throwable {
                            if (method.getName().equals("receiveChannelCloseOk") && channelAwaitingClosure(channelId)) {
                                closeChannelOk(channelId);
                            } else if (method.getName().startsWith("receive")) {
                                sendConnectionClose(ErrorCodes.CHANNEL_ERROR,
                                    "Unknown channel id: " + channelId, channelId);
                            } else if (method.getName().equals("ignoreAllButCloseOk")) {
                                return channelAwaitingClosure(channelId);
                            }
                            return null;
                        }
                    });
        }
        return channelMethodProcessor;
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        currentClassId = classId;
        currentMethodId = methodId;
    }

    void assertState(final ConnectionState requiredState) {
        if (state != requiredState) {
            String replyText = "Command Invalid, expected " + requiredState + " but was " + state;
            sendConnectionClose(ErrorCodes.COMMAND_INVALID, replyText, 0);
            throw new RuntimeException(replyText);
        }
    }


    public boolean channelAwaitingClosure(int channelId) {
        return ignoreAllButCloseOk() || (!closingChannelsList.isEmpty()
            && closingChannelsList.containsKey(channelId));
    }

    public void completeAndCloseAllChannels() {
        try {
            receivedCompleteAllChannels();
        }
        finally {
            closeAllChannels();
        }
    }

    private void receivedCompleteAllChannels() {
        RuntimeException exception = null;

        for (AmqpChannel channel : channels.values()) {
            try {
                channel.receivedComplete();
            } catch (RuntimeException exceptionForThisChannel) {
                if (exception == null) {
                    exception = exceptionForThisChannel;
                }
                log.error("[{}] ----- error informing channel that receiving is complete. Channel: [{}]", ctx.name(), channel,
                    exceptionForThisChannel);
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("send: " + frame);
        }
        getCtx().writeAndFlush(frame);
    }

    public MethodRegistry getMethodRegistry() {
        return methodRegistry;
    }

    @VisibleForTesting
    public void setBufferSender(ByteBufferSender sender) {
        this.bufferSender = sender;
    }

    @VisibleForTesting
    public AmqpServiceConfiguration getAmqpConfig() {
        return amqpConfig;
    }

    @VisibleForTesting
    public void setMaxChannels(int maxChannels) {
        this.maxChannels = maxChannels;
    }

    @VisibleForTesting
    public void setHeartBeat(int heartBeat) {
        this.heartBeat = heartBeat;
    }

    public void initHeartBeatHandler(long writerIdle, long readerIdle) {
        log.info("[{}] amqp initHeartBeatHandler writeIdle: [{}] readerIdle: [{}] remoteAddress: [{}] localAddress: [{}]",
                ctx.name(), writerIdle, readerIdle, remoteAddress, localAddress);
        this.ctx.pipeline().addFirst("idleStateHandler", new IdleStateHandler(readerIdle, writerIdle, 0,
            TimeUnit.MILLISECONDS));
        this.ctx.pipeline().addLast("connectionIdleHandler", new ConnectionIdleHandler());
    }

    class ConnectionIdleHandler extends ChannelDuplexHandler {

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.READER_IDLE)) {
                    log.error("[{}] heartbeat timeout close remoteSocketAddress [{}]", ctx.name(),
                        AmqpConnection.this.remoteAddress.toString());
                    AmqpConnection.this.close();
                } else if (event.state().equals(IdleState.WRITER_IDLE)) {
//                    log.warn("[{}] heartbeat write idle [{}]", ctx.name(), AmqpConnection.this.remoteAddress.toString());
                    writeFrame(HeartbeatBody.FRAME);
                }
            }
            super.userEventTriggered(ctx, evt);
        }

    }

    public void setMaxFrameSize(int frameMax) {
        maxFrameSize = frameMax;
        brokerDecoder.setMaxFrameSize(frameMax);
    }

    public AmqpChannel getChannel(int channelId) {
        final AmqpChannel channel = channels.get(channelId);
        if ((channel == null) || channel.isClosing()) {
            return null;
        } else {
            return channel;
        }
    }

    public boolean isClosing() {
        return orderlyClose.get();
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return isClosing();
    }

    public void closeChannelOk(int channelId) {
        closingChannelsList.remove(channelId);
    }

    private void markChannelAwaitingCloseOk(int channelId) {
        closingChannelsList.put(channelId, System.currentTimeMillis());
    }

    private void removeChannel(int channelId) {
        synchronized (channelAddRemoveLock) {
            channels.remove(channelId);
        }
    }

    public void closeChannel(AmqpChannel channel) {
        closeChannel(channel, false);
    }

    public void closeChannelAndWriteFrame(AmqpChannel channel, int cause, String message) {
        writeFrame(new AMQFrame(channel.getChannelId(),
            getMethodRegistry().createChannelCloseBody(cause,
                AMQShortString.validValueOf(message),
                currentClassId,
                currentMethodId)));
        closeChannel(channel, true);
    }

    void closeChannel(AmqpChannel channel, boolean mark) {
        int channelId = channel.getChannelId();
        try {
            channel.close();
            if (mark) {
                markChannelAwaitingCloseOk(channelId);
            }
        }
        finally {
            removeChannel(channelId);
        }
    }

    private void closeAllChannels() {
        RuntimeException exception = null;
        try {
            for (AmqpChannel channel : channels.values()) {
                try {
                    channel.close();
                } catch (RuntimeException exceptionForThisChannel) {
                    if (exception == null) {
                        exception = exceptionForThisChannel;
                    }
                    log.error("[{}] ----- error informing channel that receiving is complete. Channel: [{}]", ctx.name(), channel,
                        exceptionForThisChannel);
                }
            }
            if (exception != null) {
                throw exception;
            }
        }
        finally {
            synchronized (channelAddRemoveLock) {
                channels.clear();
            }
        }
    }

    public void block() {
        synchronized (channelAddRemoveLock) {
            if (blocked.compareAndSet(false, true)) {
                for (AmqpChannel channel : channels.values()) {
                    channel.block();
                }
            }
        }
    }

//    public Policies getPolicies(NamespaceName namespaceName) {
//        return getPulsarService().getConfigurationCache().policiesCache()
//            .get(AdminResource.path(POLICIES, namespaceName.toString())).orElse(null);

    public int getMaxChannels() {
        return maxChannels;
    }

    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public int getHeartBeat() {
        return heartBeat;
    }

    public NamespaceName getNamespaceName() {
        return namespaceName;
    }

    @VisibleForTesting
    public void setNamespaceName(NamespaceName namespaceName) {
        this.namespaceName = namespaceName;
    }

    public boolean isCompressionSupported() {
        return true;
    }

    public int getMessageCompressionThreshold() {
        return 102400;
    }

    public AmqpOutputConverter getAmqpOutputConverter() {
        return amqpOutputConverter;
    }

    public ServerCnx getServerCnx() {
        return pulsarServerCnx;
    }

    public void setPulsarServerCnx(ServerCnx pulsarServerCnx) {
        this.pulsarServerCnx = pulsarServerCnx;
    }

    public long getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AmqpConnection that = (AmqpConnection) o;
        return connectionId == that.connectionId && Objects.equals(namespaceName, that.namespaceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionId, namespaceName);
    }

    @VisibleForTesting
    public ByteBufferSender getBufferSender() {
        return bufferSender;
    }
}
