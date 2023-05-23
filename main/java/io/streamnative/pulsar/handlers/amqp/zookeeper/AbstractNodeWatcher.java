package io.streamnative.pulsar.handlers.amqp.zookeeper;

import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;

public abstract class AbstractNodeWatcher {

    private ZookeeperClient zkClient;

    private ProxyService proxyService;

    private String path;

    public ZookeeperClient getZkClient() {
        return zkClient;
    }

    public void setZkClient(ZookeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    public AbstractNodeWatcher(ProxyService proxyService, String path) {
        this.proxyService = proxyService;
        this.path = path;
    }

    public void nodeChange() {

    }

    public String getPath() {
        return path;
    }

}
