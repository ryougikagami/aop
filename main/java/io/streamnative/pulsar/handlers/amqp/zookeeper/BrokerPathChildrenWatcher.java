package io.streamnative.pulsar.handlers.amqp.zookeeper;

import io.streamnative.pulsar.handlers.amqp.BundleNodeData;
import io.streamnative.pulsar.handlers.amqp.proxy.BrokerConf;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConnection;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;

import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

@Slf4j
public class BrokerPathChildrenWatcher extends AbstractPathChildrenWatcher {


    public BrokerPathChildrenWatcher(ProxyService proxyService, String path) {
        super(proxyService, path);
    }

    @Override
    public void initialized() {
        super.initialized();
    }

    @Override
    public void childAdded() {
        log.info("broker childAdded()-------------");
        proxyService.getPulsarService().getLoadManager().get().doLoadShedding();
        if (ProxyService.BrokerState.Init.equals(proxyService.getState())) {
            int brokerNums = proxyService.getBrokerNums();
            log.info("brokerNums: {}", brokerNums);
            if (brokerNums == ProxyService.BROKER_NUM) {
                log.info("getBrokerInitLock lock... 111111");
                proxyService.getBrokerInitLock().lock();
                log.info("getBrokerInitLock lock... 222222");
                try {
                    proxyService.getBrokerInitCondition().signalAll();
                    log.info("proxyService signalAll...");
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                finally {
                    proxyService.getBrokerInitLock().unlock();
                }
            }
        }
        super.childAdded();
    }

    @Override
    public void childUpdate() {
        super.childUpdate();
    }

    @Override
    public void childRemoved() {
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        String brokerPath = event.getData().getPath();
        BrokerConf brokerConf = pathToBrokerConf(brokerPath);
        proxyConnectionSet.forEach(proxyConnection -> {
            ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = proxyConnection.getProxyHandlerMap();
            if (proxyHandlerMap.containsKey(brokerConf)) {
                ProxyHandler proxyHandler = proxyHandlerMap.get(brokerConf);
                proxyHandler.close();
                proxyHandlerMap.remove(brokerConf);
            }
        });
        super.childRemoved();
    }

    @Override
    public void connectionReconnected() {
        super.connectionReconnected();
    }

    @Override
    public void connectionSuspended() {
        super.connectionSuspended();
    }

}
