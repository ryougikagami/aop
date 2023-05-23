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

import com.google.common.util.concurrent.MoreExecutors;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.pulsar.zookeeper.ZooKeeperClientFactory.SessionType.AllowReadOnly;

/**
 * AMQP broker related.
 */
public class AmqpBrokerService {
    @Getter
    private AmqpTopicManager amqpTopicManager;
    @Getter
    private ExchangeContainer exchangeContainer;
    @Getter
    private QueueContainer queueContainer;
    @Getter
    private ExchangeService exchangeService;
    @Getter
    private QueueService queueService;
    @Getter
    private ConnectionContainer connectionContainer;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private ConcurrentHashMap<String,String> exMap = new ConcurrentHashMap<>();


    @SneakyThrows
    public AmqpBrokerService(PulsarService pulsarService) {
//        pulsarService.getAdminClient().brokers().getActiveBrokers("");
//        pulsarService.getAdminClient().brokers().getActiveBrokers("");
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer);
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer,pulsarService.getBrokerServiceUrl());
        this.connectionContainer = new ConnectionContainer(pulsarService, exchangeContainer, queueContainer);
    }

    @SneakyThrows
    public AmqpBrokerService(PulsarService pulsarService, ConnectionContainer connectionContainer) {
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer,pulsarService.getBindAddress());
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer);
        this.connectionContainer = connectionContainer;
    }
}
