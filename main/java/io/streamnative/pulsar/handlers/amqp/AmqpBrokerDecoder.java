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
package io.streamnative.pulsar.handlers.amqp;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.PooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.ServerDecoder;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;


/**
 * Amqp broker decoder for amqp protocol requests decoding.
 */
@Slf4j
public class AmqpBrokerDecoder extends ServerDecoder {

//    private static final int bufferSize = 4 * 1024 * 1024;
    private static final int bufferSize = 4 * 1024;
    private volatile QpidByteBuffer netInputBuffer;

    /**
     * Creates a new AMQP decoder.
     *
     * @param methodProcessor method processor
     */
    public AmqpBrokerDecoder(ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor) {
        super(methodProcessor);
//        netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize);
        netInputBuffer = QpidByteBuffer.allocate(bufferSize);
    }

    @Override
    public ServerMethodProcessor<? extends ServerChannelMethodProcessor> getMethodProcessor() {
        return super.getMethodProcessor();
    }

    @Override
    public void decodeBuffer(QpidByteBuffer buf) throws AMQFrameDecodingException, AMQProtocolVersionException,
            IOException {
//        log.info("netInputBuffer.remaining()----------"+netInputBuffer.remaining());
//        if (netInputBuffer.remaining() < buf.remaining()) {
//        log.info("buf.remaining()------------"+buf.remaining());

        int messageSize = buf.remaining();
        if (netInputBuffer.remaining() < messageSize) {

//            log.info("decodeBuffer messageSize={} buf.remaining={} position={} limit={} capacity={}", messageSize, netInputBuffer.remaining(), buf.position(), buf.limit(), buf.capacity());

            QpidByteBuffer oldBuffer = netInputBuffer;
//            netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize);
            if (oldBuffer.position() != 0) {
                oldBuffer.limit(oldBuffer.position());
                oldBuffer.slice();
                oldBuffer.flip();
//                netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize + oldBuffer.remaining() + messageSize);
                netInputBuffer = QpidByteBuffer.allocate(bufferSize + oldBuffer.remaining() + messageSize);
                netInputBuffer.put(oldBuffer);
            }
            else {
//                netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize + messageSize);
                netInputBuffer = QpidByteBuffer.allocate(bufferSize + messageSize);
            }
//            log.info("oldBuffer.remaining()------------"+oldBuffer.remaining());
//            oldBuffer.clear();
            oldBuffer.close();
        }
        netInputBuffer.put(buf);
        netInputBuffer.flip();
        super.decodeBuffer(netInputBuffer);
//        log.info(netInputBuffer.capacity());
        restoreApplicationBufferForWrite();
    }

    protected void restoreApplicationBufferForWrite() {
        try (QpidByteBuffer oldNetInputBuffer = netInputBuffer) {
            int unprocessedDataLength = netInputBuffer.remaining();
            netInputBuffer.limit(netInputBuffer.capacity());
            netInputBuffer = oldNetInputBuffer.slice();
            netInputBuffer.limit(unprocessedDataLength);
        }
        if (netInputBuffer.limit() != netInputBuffer.capacity()) {
            netInputBuffer.position(netInputBuffer.limit());
            netInputBuffer.limit(netInputBuffer.capacity());
        } else {
            try (QpidByteBuffer currentBuffer = netInputBuffer) {

//                log.info("restoreApplicationBufferForWrite buf.remaining={} position={} limit={} capacity={}", currentBuffer.remaining(), currentBuffer.position(), currentBuffer.limit(), currentBuffer.capacity());

                int newBufSize;
                if (currentBuffer.capacity() < bufferSize) {
                    newBufSize = bufferSize;
                } else {
                    newBufSize = currentBuffer.capacity() + bufferSize;
                }
//                netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
                netInputBuffer = QpidByteBuffer.allocate(newBufSize);
                netInputBuffer.put(currentBuffer);
            }
        }
    }

    public void close() {
        netInputBuffer = null;
    }
}