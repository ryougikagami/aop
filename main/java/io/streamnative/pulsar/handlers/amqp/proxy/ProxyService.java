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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.amqp.Bundle;
import io.streamnative.pulsar.handlers.amqp.collection.SyncConcurrentHashMap;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.streamnative.pulsar.handlers.amqp.utils.DirectMemoryReporter;
import io.streamnative.pulsar.handlers.amqp.utils.StringUtils;
import io.streamnative.pulsar.handlers.amqp.zookeeper.BrokerPathChildrenWatcher;
import io.streamnative.pulsar.handlers.amqp.zookeeper.BundleNodeWatcher;
import io.streamnative.pulsar.handlers.amqp.zookeeper.BundlePathChildrenWatcher;
import io.streamnative.pulsar.handlers.amqp.zookeeper.ZookeeperClient;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.qpid.server.model.Broker;
import org.checkerframework.checker.units.qual.A;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * This service is used for redirecting AMQP client request to proper AMQP protocol handler Broker.
 */
@Slf4j
public class ProxyService implements Closeable {
    @Getter
    private ProxyConfiguration proxyConfig;

    @Getter
    private PulsarService pulsarService;

    @Getter
    private LookupHandler lookupHandler;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;

    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("amqp-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("amqp-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors() * 16;

    @Getter
    private CountDownLatch brokerInitLatch;

    @Getter
    private BrokerState state;

    @Getter
    public enum BrokerState {
        Init, Running
    }

    @Getter
    private ZookeeperClient zkClient;

    @Getter
    private List<Bundle> bundleList;

    @Getter
    private static SyncConcurrentHashMap<Bundle, Pair<String, Integer>> bundleBrokerBindMap;

    @Getter
    private static final Map<String, Bundle> topicBundleMap = Maps.newConcurrentMap();

    @Getter
    private Set<ProxyConnection> proxyConnectionSet = Sets.newCopyOnWriteArraySet();

    @Getter
    private ReentrantLock brokerInitLock = new ReentrantLock();

    @Getter
    private Condition brokerInitCondition = brokerInitLock.newCondition();

    // FIXME 配置读取
    public static int BROKER_NUM = 3;

    public static int MAX_WAIT_TIME_IN_MILLIS = 10_000;

    private void getAndWatchBundleBrokerMap() {
    }

    public Bundle getBundleByTopic(String topic) {
        NamespaceBundle namespaceBundle = pulsarService.getNamespaceService().getBundle(TopicName.get(TopicDomain.persistent.value(), NamespaceName.get("public", "vhost1"), topic));
        String range = namespaceBundle.getBundleRange();
        String[] boundaries = range.split("_");
        Bundle bundle = new Bundle(boundaries[0], boundaries[1]);
        topicBundleMap.putIfAbsent(topic, bundle);
        return bundle;
    }

    public int getBrokerNums() {
        try {
            List<String> activeBrokers = pulsarService.getAdminClient().brokers().getActiveBrokers("pulsar-cluster-1");
            activeBrokers.forEach(broker -> {
                log.info("active Brokers: {}", broker);
            });
            return activeBrokers.size();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return 0;
    }

    public CompletableFuture<NamespaceBundle> getBundleByTopicAsync(String topic) {
        return pulsarService.getNamespaceService().getBundleAsync(TopicName.get(topic));
    }

    protected void bundleInit() {
        log.info("bundleInit ----- ");
        // 假如有多个connection同时连接，就会发多个bundle init请求
        if (BrokerState.Init.equals(state)) {
            try {
                bundleList.forEach(bundle -> {
                    if (!bundleBrokerBindMap.containsKey(bundle)) {
                        new Thread(() -> {
                            initBundleByLookUp(bundle);
                        }).start();
                    }
                });
                brokerInitLatch.await();
                state = BrokerState.Running;
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    @SneakyThrows
    public BrokerConf topicLookup(String topic) {
        topic = TopicName.get(TopicDomain.persistent.value(), NamespaceName.get("public", "vhost1"), topic).toString();
        log.info("topicLookup -------- topic: [{}]", topic);
        String brokerUrl = pulsarService.getAdminClient().lookups().lookupTopic(topic);
        BrokerConf brokerConf = StringUtils.parseUrl(brokerUrl);
        return brokerConf;
    }

    public BrokerConf initBundleByLookUp(Bundle bundle) {
        String topic = "lookUp__";
        int suffix = 0;
        Bundle temp;
        BrokerConf brokerConf;
        do {
            temp = getBundleByTopic(topic + suffix);
            suffix++;
        } while (!bundle.equals(temp));
        brokerConf = topicLookup(topic + (--suffix));
        return brokerConf;
    }

//    public BrokerConf getBrokerByBundleFromMetaData(Bundle bundle) {
//        String path = "/namespace/public/vhost1/" + bundle.getLowBoundaries() + "_" + bundle.getUpBoundaries();
//        String nodeData = zkClient.getNodeData(path);
//        JSONObject jsonObject = JSON.parseObject(nodeData);
//        String nativeUrl = (String) jsonObject.get("nativeUrl");
//        String[] urlSplit = nativeUrl.split(":");
//        return new BrokerConf(urlSplit[0].split("//")[1], Integer.parseInt(urlSplit[1]));
//    }

    private void proxyClientLoad() {
    }

    @Getter
    private static final Map<String, Pair<String, Integer>> vhostBrokerMap = Maps.newConcurrentMap();

    @Getter
    private static final Map<String, Set<ProxyConnection>> vhostConnectionMap = Maps.newConcurrentMap();

    @Getter
    private static final Map<String, CompletableFuture<Pair<String, Integer>>> futureMap = Maps.newConcurrentMap();
    @Getter
    private static final Map<String, Map<String, String>> mapForEx = Maps.newConcurrentMap();
    @Getter
    private long startTime = System.currentTimeMillis();
    @Getter
    @Setter
    private AtomicLong lastPrintTime = new AtomicLong(System.currentTimeMillis());
    @Getter
    @Setter
    private long waitStartTime = System.currentTimeMillis();
    @Getter
    @Setter
    private AtomicInteger connectionCreatingCount = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicInteger ticketCount = new AtomicInteger(0);
    @Getter
    @Setter
    private CopyOnWriteArraySet<ProxyConnection> ticketSet = new CopyOnWriteArraySet();
    @Getter
    @Setter
    private ConcurrentHashMap<ProxyConnection, Long> ticketHashMap = new ConcurrentHashMap<>();
    @Getter
    @Setter
    private AtomicInteger succeedConnection = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicBoolean isConnectionCreatingNeedWait = new AtomicBoolean(false);
    @Getter
    @Setter
    private AtomicInteger exchangeDeclareCount = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicInteger exchangeDeclareCntSum = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicLong exchangeDeclareLatency = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicInteger queueDeclareCount = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicInteger queueDeclareCntSum = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicLong queueDeclareLatency = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicInteger queueBindCount = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicInteger queueBindCntSum = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicLong queueBindLatency = new AtomicLong(0);
    @Getter
    @Setter
    private AtomicInteger basicConsumeCount = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicInteger basicConsumeCntSum = new AtomicInteger(0);
    @Getter
    @Setter
    private AtomicLong basicConsumeLatency = new AtomicLong(0);

    @Getter
    private static final Map<String, Set<String>> newMapForEx = Maps.newConcurrentMap();

    @Getter
    @Setter
    private CompletableFuture<List<String>> brokersFuture = null;

    private String tenant;

    public static final String CLUSTER_FIELD = "cluster";

    @Getter
    public String CLUSTER_NAME = "pulsar-cluster-1";

    public DirectMemoryReporter directMemoryReporter;

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        configValid(proxyConfig);

        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        this.tenant = this.proxyConfig.getAmqpTenant();
        this.zkClient = new ZookeeperClient(proxyConfig);
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, false, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, false, workerThreadFactory);
        bundleList = initBundleList();
        bundleBrokerBindMap = new SyncConcurrentHashMap<>();
        bundleBrokerBindMap.setStat(new AtomicInteger(bundleList.size()));
        brokerInitLatch = new CountDownLatch(bundleList.size());
        state = BrokerState.Init;
    }

    @SneakyThrows
    private List<Bundle> initBundleList() {
        // TODO
        // 支持bundle split做成监听
        List<Bundle> bundleList = new ArrayList<>();
        Policies policies = pulsarService.getAdminClient().namespaces().getPolicies("public/vhost1");
        BundlesData bundlesData = policies.bundles;
        List<String> bundlesDataBoundaries = bundlesData.getBoundaries();
        for (int i = 0; i < bundlesDataBoundaries.size() - 1; i++) {
            bundleList.add(new Bundle(bundlesDataBoundaries.get(i), bundlesDataBoundaries.get(i + 1)));
        }
        return bundleList;
    }

    private void configValid(ProxyConfiguration proxyConfig) {
        checkNotNull(proxyConfig);
        checkArgument(proxyConfig.getAmqpProxyPort() > 0);
        checkNotNull(proxyConfig.getAmqpTenant());
        checkNotNull(proxyConfig.getBrokerServiceURL());
    }

    private class NettyThread extends Thread {

        public ProxyService proxyService;

        public NettyThread(ProxyService proxyService) {
            this.proxyService = proxyService;
        }

        @Override
        public void run() {
            // log.info("ProxyService bundle init start...");
            // proxyService.getBrokerInitLock().lock();
            // try {
            //     Thread.sleep(3_000);
            //     while (proxyService.getBrokerNums() != ProxyService.BROKER_NUM) {
            //         log.info("proxyService await...");
            //         proxyService.getBrokerInitCondition().await(ProxyConnection.MAX_WAIT_TIME_IN_MILLIS, TimeUnit.MILLISECONDS);
            //     }
            //     log.info("start bundleInit");
            //     proxyService.bundleInit();
            //     bundleBrokerBindMap.forEach((bundle, broker) -> {
            //         log.info("bindResult bundle: [{}], broker: [{}]", bundle, broker);
            //     });
            // } catch (Exception e) {
            //     log.error(e.getMessage());
            // }
            // finally {
            //     proxyService.getBrokerInitLock().unlock();
            // }

            log.info("Netty bind thread waiting...");
            try {
                Thread.sleep(MAX_WAIT_TIME_IN_MILLIS);
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(acceptorGroup, workerGroup);
                serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
                EventLoopUtil.enableTriggeredMode(serverBootstrap);
                serverBootstrap.childHandler(new ServiceChannelInitializer(proxyService));
                serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
                serverBootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
                // serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                //     new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, proxyConfig.getAmqpMaxFrameSize()));
                serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                        new FixedRecvByteBufAllocator(16 * 1024));
                listenChannel = serverBootstrap.bind(proxyConfig.getAmqpProxyPort()).sync().channel();
                log.info("Netty Service start...");

//                new DirectMemoryReporter().startReport();

            } catch (
                    InterruptedException e) {
                try {
                    throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getAmqpProxyPort(), e);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

    }

    public void start() throws Exception {
        log.info("ProxyService start...");
        String brokerPath = "/loadbalance/brokers";
        zkClient.watchPathChange(new BrokerPathChildrenWatcher(this, brokerPath));
        String bundlePath = "/namespace/public/vhost1";
        zkClient.watchPathChange(new BundlePathChildrenWatcher(this, bundlePath));

//        pulsarService.getAdminClient().namespaces().setNamespaceMessageTTL("public/vhost1", 300);

        new NettyThread(this).start();
        pulsarService.getBrokerService().checkInactiveSubscriptions();
        this.lookupHandler = new PulsarServiceLookupHandler(proxyConfig, pulsarService);
    }

    public void cacheVhostMap(String vhost, Pair<String, Integer> lookupData) {
        this.vhostBrokerMap.put(vhost, lookupData);
    }

    public void cacheVhostMapRemove(String vhost) {
        this.vhostBrokerMap.remove(vhost);
    }

    @Override
    public void close() throws IOException {
        if (lookupHandler != null) {
            lookupHandler.close();
        }
        if (listenChannel != null) {
            listenChannel.close();
        }
    }

    private void bundleBind() {
    }
}
