package io.streamnative.pulsar.handlers.amqp.zookeeper;

import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;

public class BundleNodeWatcher extends AbstractNodeWatcher {

    public BundleNodeWatcher(ProxyService proxyService, String path) {
        super(proxyService, path);
    }


    @Override
    public void nodeChange() {
    }

}
