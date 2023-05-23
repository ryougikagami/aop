package io.streamnative.pulsar.handlers.amqp.zookeeper;

import io.streamnative.pulsar.handlers.amqp.proxy.BrokerConf;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

@Slf4j
public abstract class AbstractPathChildrenWatcher {

    protected ZookeeperClient zkClient;

    protected ProxyService proxyService;

    protected String path;

    protected CuratorFramework curatorClient;

    protected PathChildrenCacheEvent event;

    public static int LOCK_WAIT_TIME = 3000;


    public ZookeeperClient getZkClient() {
        return zkClient;
    }

    public ProxyService getProxyService() {
        return proxyService;
    }

    public void setZkClient(ZookeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    public CuratorFramework getCuratorClient() {
        return curatorClient;
    }

    public void setCuratorClient(CuratorFramework curatorClient) {
        this.curatorClient = curatorClient;
    }

    public PathChildrenCacheEvent getEvent() {
        return event;
    }

    public void setEvent(PathChildrenCacheEvent event) {
        this.event = event;
    }

    public AbstractPathChildrenWatcher(ProxyService proxyService, String path) {
        this.proxyService = proxyService;
        this.path = path;
    }

    public void initialized() {
        log.info("initialized");
    }

    public void childAdded() {
        log.info("childAdded");
    }

    public void childUpdate() {
        log.info("childUpdate");
    }

    public void childRemoved() {
        log.info("childRemoved");
    }

    public void connectionReconnected() {
        log.info("connectionReconnected");
    }

    public void connectionSuspended() {
        log.info("connectionSuspended");
    }

    public String getPath() {
        return path;
    }

    /**
     * @param path /loadbalance/brokers/10.155.192.113:8080
     * @return
     */
    public BrokerConf pathToBrokerConf(String path){
        String[] split = path.split("/");
        String[] conf = split[split.length - 1].split(":");
        return new BrokerConf(conf[0], proxyService.getProxyConfig().getAmqpBrokerPort());
    }

}
