package io.streamnative.pulsar.handlers.amqp.proxy;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

/**
 * 功能描述
 *
 * @since 2022-10-10
 */
public class AutoDeleteManager implements Runnable{

    private ProxyService proxyService;

    public AutoDeleteManager(ProxyService proxyService) {
        this.proxyService = proxyService;
    }

    @Override
    public void run() {
        autoDelete();
    }

    private void autoDelete(){
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<Topic>>> topics = proxyService.getPulsarService().getBrokerService().getTopics();

    }

}
