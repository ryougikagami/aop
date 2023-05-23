package io.streamnative.pulsar.handlers.amqp.zookeeper;

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Objects;

@Slf4j
public class ZookeeperClient {
    private String zookeeperIp = "127.0.0.1";
    private String zookeeperPort = "2181";

    private int amqpBrokerPort;
    private String amqpListeners;

    private int SESSION_TIME = 10000;
    private int CONNECTION_TIME = 10000;

    private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

    private String LOCK_ROOT = "/locks";

    private CuratorFramework client;

    public InterProcessMutex lock;

    public CuratorFramework getClient() {
        return client;
    }

    private DefaultThreadFactory curatorThreadFactory = new DefaultThreadFactory("curator-thread");

    // FIXME curator使用多线程会出现event覆盖问题
    private ExecutorService pool = Executors.newFixedThreadPool(1, curatorThreadFactory);

    public ZookeeperClient(ProxyConfiguration proxyConfig) {
        zookeeperIp = proxyConfig.getZookeeperServers();
//        amqpListeners = proxyConfig.getAmqpListeners();
        amqpBrokerPort = proxyConfig.getAmqpBrokerPort();
        log.info("ZOOKEEPER_IP------" + zookeeperIp);
        log.info("amqpBrokerPort-------" + amqpBrokerPort);
        log.info("start zookeeperClient");
        this.client = CuratorFrameworkFactory.builder()
            .connectString(zookeeperIp)
            .sessionTimeoutMs(SESSION_TIME)
            .connectionTimeoutMs(CONNECTION_TIME)
            .retryPolicy(retryPolicy)
            .build();
        client.start();
        this.lock = new InterProcessMutex(client, LOCK_ROOT);
    }

    public int parsePort(String url) {
        log.info(url);
        String[] split = url.split(":");
        for (int i = 0; i < split.length; i++) {
            log.info(split[i]);
        }
        return Integer.parseInt(split[split.length - 1]);
    }

    public int getAmqpBrokerPort() {
        return amqpBrokerPort;
    }

    @SneakyThrows
    public List<String> getChildrenPath(String path) {
        log.info("invoke zookeeper getPath");
        return client.getChildren().forPath(path);
    }

    @SneakyThrows
    public String getNodeData(String path) {
        return new String(client.getData().forPath(path));
    }


    @SneakyThrows
    public boolean checkNodeExists(String path) {
        Stat stat = client.checkExists().forPath(path);
        return Objects.isNull(stat);
    }

    public void close() {
        client.close();
    }


    public void watchNodeChange(AbstractNodeWatcher watcher) {
        log.info("invoke zookeeper watchNodeChange");
        String path = watcher.getPath();
        watcher.setZkClient(this);
        CuratorCache curatorCache = CuratorCache.builder(client, path).build();

        NodeCacheListener nodeCacheListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("node changed");
                watcher.nodeChange();
            }
        };

        CuratorCacheListener listener = CuratorCacheListener.builder().forNodeCache(nodeCacheListener).build();
        curatorCache.listenable().addListener(listener, pool);
        curatorCache.start();
    }

    public void watchPathChange(AbstractPathChildrenWatcher watcher) {
        log.info("invoke zookeeper watchPathChange");
        watcher.setZkClient(this);
        String path = watcher.getPath();

        CuratorCache curatorCache = CuratorCache.builder(client, path).build();

        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                // FIXME 通过该方式设置event在多线程执行时会产生覆盖问题，导致监听事件对象错误
                watcher.setEvent(event);
                watcher.setCuratorClient(client);
                switch (event.getType()) {
                    case INITIALIZED:
                        log.info("node initialize");    //待测试
                        watcher.initialized();
                        break;
                    case CHILD_ADDED:
                        // log.info("node added " + "node=" + event.getData().getPath());
                        watcher.childAdded();
                        break;
                    case CHILD_UPDATED:
                        // log.info("node changed " + "node=" + event.getData().getPath());
                        watcher.childUpdate();
                        break;
                    case CHILD_REMOVED:
                        // log.info("node removed " + "node=" + event.getData().getPath());
                        watcher.childRemoved();
                        break;
                    case CONNECTION_RECONNECTED:
                        log.info("connection reconnected");
                        watcher.connectionReconnected();
                        break;
                    case CONNECTION_SUSPENDED:
                        log.info("connection suspended");
                        watcher.connectionSuspended();
                        break;
                    default:
                        break;
                }
            }
        };

        CuratorCacheListener listener = CuratorCacheListener.builder().forPathChildrenCache(path, client, pathChildrenCacheListener).build();
        //这里可以加入线程池
        curatorCache.listenable().addListener(listener, pool);
        curatorCache.start();
    }
}
