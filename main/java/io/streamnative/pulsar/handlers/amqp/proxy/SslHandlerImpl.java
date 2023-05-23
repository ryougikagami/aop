package io.streamnative.pulsar.handlers.amqp.proxy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

/**
 * 功能描述
 *
 * @since 2022-10-19
 */
public class SslHandlerImpl extends SslHandler {

    public SslHandlerImpl(SSLEngine engine) {
        super(engine);
    }
}
