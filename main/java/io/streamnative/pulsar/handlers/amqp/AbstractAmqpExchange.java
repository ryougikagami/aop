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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class of AMQP exchange.
 */
public abstract class AbstractAmqpExchange implements AmqpExchange {

    protected final String exchangeName;
    protected final Type exchangeType;
    protected Set<AmqpQueue> queues;

    protected Map<String, AmqpQueue> queueMap;

    protected boolean durable;
    protected boolean autoDelete;
    public static final String DEFAULT_EXCHANGE_DURABLE = "aop.direct.durable";
    public static final ConcurrentHashMap<String,String> mapForDefaultEx = new ConcurrentHashMap<>();

    protected AbstractAmqpExchange(String exchangeName, Type exchangeType,
                                   Set<AmqpQueue> queues, Map<String, AmqpQueue> queueMap, boolean durable, boolean autoDelete) {
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.queues = queues;
        this.queueMap = queueMap;
        this.durable = durable;
        this.autoDelete = autoDelete;
    }

    @Override
    public void addQueue(AmqpQueue queue) {
        queues.add(queue);
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        queues.remove(queue);
    }

    @Override
    public int getQueueSize() {
        return queues.size();
    }

    @Override
    public String getName() {
        return exchangeName;
    }

    @Override
    public boolean getDurable() {
        return durable;
    }

    @Override
    public boolean getAutoDelete() {
        return autoDelete;
    }

    @Override
    public Type getType() {
        return exchangeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractAmqpExchange that = (AbstractAmqpExchange) o;
        return exchangeName.equals(that.exchangeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeName);
    }
}
