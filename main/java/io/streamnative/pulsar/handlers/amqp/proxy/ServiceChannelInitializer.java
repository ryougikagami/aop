/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.proxy;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import io.streamnative.pulsar.handlers.amqp.utils.ssl.SslOneWayContextFactory;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

//import static io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler.TLS_HANDLER;

/**
 * Proxy service channel initializer.
 */
@Slf4j
public class ServiceChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static AtomicInteger cnt = new AtomicInteger();

    public static final int MAX_FRAME_LENGTH = 4 * 1024 * 1024;
    private ProxyService proxyService;

    private final boolean enableTls;
    @Getter
    private final SSLContext sslContext;

    public ServiceChannelInitializer(ProxyService proxyService) {
        this.proxyService = proxyService;
        ProxyConfiguration proxyConfiguration = proxyService.getProxyConfig();
        if(proxyConfiguration.isAmqpEnableTls()){
            String pkPath = proxyConfiguration.getAmqpServerKeystore();
            String keyPwd = proxyConfiguration.getAmqpServerKey();
            if(StringUtils.isEmpty(pkPath)){
                throw new Error("Failed to initialize the server-side SSLContext");
            }
            this.sslContext = SslOneWayContextFactory.getServerContext(pkPath,keyPwd);
            this.enableTls = true;
        }else {
            this.sslContext = null;
            this.enableTls = false;
        }
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        if(this.enableTls){
            SSLEngine engine  = this.sslContext.createSSLEngine();

            engine.setUseClientMode(false);
            ch.pipeline().addLast("tls",new SslHandlerImpl(engine));
        }
        ch.pipeline().addLast("frameEncoder",
                new AmqpEncoder());
        ch.pipeline().addLast("proxyPipeline" + cnt.getAndIncrement(),
                new ProxyConnection(proxyService));
    }
}
