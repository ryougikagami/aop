package io.streamnative.pulsar.handlers.amqp.zookeeper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.Bundle;
import io.streamnative.pulsar.handlers.amqp.BundleNodeData;
import io.streamnative.pulsar.handlers.amqp.collection.SyncConcurrentHashMap;
import io.streamnative.pulsar.handlers.amqp.proxy.BrokerConf;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConnection;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.OwnedBundle;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class BundlePathChildrenWatcher extends AbstractPathChildrenWatcher {

    private List<Bundle> bundles = new ArrayList<>();

    public BundlePathChildrenWatcher(ProxyService proxyService, String path) {
        super(proxyService, path);
    }

    @Override
    public void initialized() {
        super.initialized();
    }

    @Override
    public void childAdded() {
        log.info("bundlePathChildrenWatcher childAdded...");
        updateBundleBrokerBindMap();
        super.childAdded();
    }

    @Override
    public void childUpdate() {
        // bundle的内容发生变化，更新本地bundleBrokerMap
        // FIXME 这个函数不会执行，因为bundle在zookeeper中不会update，只会remove和add
        updateBundleBrokerBindMap();
        addBrokerProxyHandlerBindInfo();
        super.childUpdate();
    }


    @Override
    public void childRemoved() {
        SyncConcurrentHashMap<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        HashMap<Bundle, Pair<String, Integer>> oldMap = new HashMap<>();
        bundleBrokerBindMap.forEach((k,v)->{
            oldMap.put(k,v);
        });
//        log.info("oldMap ----- ", oldMap.size());


        //bundle个数变少，抢锁，进行bundle reload
        // consumer的时候可能不会触发lookup
        //这里可能存在一个broker中含有多个bundle的情况, broker故障时 bundle的删除顺序
        Bundle bundle = pathToBundle(event.getData().getPath());

        if(!bundleBrokerBindMap.isEmpty()) {
            bundles.clear();
            BrokerConf brokerConf = new BrokerConf(oldMap.get(bundle));
            oldMap.entrySet().stream().forEach(o -> {
                if (Objects.nonNull(brokerConf) && brokerConf.equals(new BrokerConf(o.getValue()))) {
                    log.info("bundles add:{}",o.getKey());
                    bundles.add(o.getKey());
                }
            });
        }


        proxyService.getBundleBrokerBindMap().remove(bundle);
        proxyService.initBundleByLookUp(bundle);  //封装一个lookup 和加锁的接口
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


    private void addBrokerProxyHandlerBindInfo() {
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        String bundleData = new String(event.getData().getData());

        BundleNodeData bundleNodeData = JSON.parseObject(bundleData, new TypeReference<BundleNodeData>() {
        });
        String nativeUrl = bundleNodeData.getNativeUrl();
        // log.info("nativeUrl-----------" + nativeUrl);
        String[] urlSplit = nativeUrl.split("//")[1].split(":");

        BrokerConf brokerConf = new BrokerConf(urlSplit[0], zkClient.getAmqpBrokerPort());
        proxyConnectionSet.forEach(proxyConnection -> {
            ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = proxyConnection.getProxyHandlerMap();
            proxyHandlerMap.forEach((k,v)->{
                log.info(k.toString()+"----166---"+v);
            });
            if (!proxyHandlerMap.containsKey(brokerConf)) {
                try {
                    AMQMethodBody responseBody = proxyConnection.getMethodRegistry().createChannelOpenOkBody();
                    ProxyHandler proxyHandler = new ProxyHandler(proxyConnection.getVhost(),
                                                                    proxyConnection.getProxyService(),
                                                                    proxyConnection,
                                                                    brokerConf.getAopBrokerHost(),
                                                                    brokerConf.getAopBrokerPort(),
                                                                    proxyConnection.getConnectMsgList(),
                                                                    responseBody);
                    proxyHandlerMap.put(brokerConf,proxyHandler);
                    // ChannelOpenBody channelOpenBody = new ChannelOpenBody();
                    // proxyHandler.getBrokerChannel().writeAndFlush(channelOpenBody);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void updateBundleBrokerBindMap() {
        Bundle bundle = pathToBundle(event.getData().getPath());
        String bundleData = new String(event.getData().getData());
        BundleNodeData bundleNodeData = JSON.parseObject(bundleData, new TypeReference<BundleNodeData>() {
        });
        String nativeUrl = bundleNodeData.getNativeUrl();
        String[] urlSplit = nativeUrl.split("//")[1].split(":");

        log.info("updateBundleBrokerBindMap ----- bundle: [{}]", bundle);
        proxyService.getBundleBrokerBindMap().put(bundle, Pair.of(urlSplit[0], zkClient.getAmqpBrokerPort()));

        switch (proxyService.getState()){
            case Init:
                proxyService.getBrokerInitLatch().countDown();
                break;
            case Running:
                reCreateProxyHandler();
                reDeclareConsume(bundle);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                sendNAck();
        }
    }

    private void reCreateProxyHandler(){
        log.info("reCreateProxyHandler----------");
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        // 获取所有有效的broker
        Set<BrokerConf> set = new HashSet<>();
        bundleBrokerBindMap.forEach(((bundle, pair) -> {
            set.add(new BrokerConf(pair));
        }));
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        // 遍历所有链接,缺少proxyHandler的要重建
        proxyConnectionSet.forEach(proxyConnection -> {

            // FIXME 初始化时不能走这一步
            if (!proxyConnection.getIsChannelFinished()){
                return;
            }

            log.info("reCreateProxyHandler ----- isChannelFinished: [{}]", proxyConnection.getIsChannelFinished());

            ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = proxyConnection.getProxyHandlerMap();
            // set中有而proxyHandlerMap中没有的链接需要重建
            set.forEach(brokerConf -> {
                // log.info("set brokerConf----------"+brokerConf.toString());
                if(!proxyHandlerMap.containsKey(brokerConf)){
                    proxyConnection.createProxyHandler(brokerConf);
                    log.info("reCreateProxyHandler ----- proxyHandlerMap.size(): [{}]", proxyHandlerMap.size());
                    // AMQMethodBody responseBody = proxyConnection.getMethodRegistry().createChannelOpenOkBody();
                    // ProxyHandler proxyHandler = null;
                    // try {
                    //     proxyHandler = new ProxyHandler(proxyConnection.getVhost(),
                    //             proxyConnection.getProxyService(),
                    //             proxyConnection,
                    //             brokerConf.getAopBrokerHost(),
                    //             brokerConf.getAopBrokerPort(),
                    //             proxyConnection.getConnectMsgList(),
                    //             responseBody);
                    // } catch (Exception e) {
                    //     e.printStackTrace();
                    // }
                    // proxyHandlerMap.put(brokerConf,proxyHandler);
                    // try {
                    //     Thread.sleep(1000);
                    // } catch (InterruptedException e) {
                    //     e.printStackTrace();
                    // }
//                    while (proxyHandler.getProxyBackendHandler().getCnx().channel().isActive()){
//                        try {
//                            Thread.sleep(10);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    ChannelOpenBody channelOpenBody = new ChannelOpenBody();
//                    proxyHandler.getBrokerChannel().writeAndFlush(channelOpenBody);
                }
            });
        });
    }

    @SneakyThrows
    private void reDeclareConsume(Bundle bundle) {
        log.info("reDeclareConsume ----- bundle: [{}]", bundle.toString());
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        Map<String, Bundle> topicBundleMap = proxyService.getTopicBundleMap();
        Map<Bundle, Pair<String, Integer>> bundleBrokerBindMap = proxyService.getBundleBrokerBindMap();
        // BrokerConf brokerConf = new BrokerConf(bundleBrokerBindMap.get(bundle));
        proxyConnectionSet.forEach(proxyConnection -> {
            Map<String, List<Object>> queueConsumerMap = proxyConnection.getQueueConsumerMap();
            ConcurrentHashMap<BrokerConf, ProxyHandler> proxyHandlerMap = proxyConnection.getProxyHandlerMap();
            // proxyHandlerMap.forEach((k, v) -> log.info(k.toString() + "---------" + v));
            // ProxyHandler proxyHandler = proxyHandlerMap.get(brokerConf);
            queueConsumerMap.forEach((queue, consumeMsgList) -> {
                // log.info("queue--281--"+queue.toString());
                Bundle queueBundle = topicBundleMap.get(queue) == null ? proxyService.getBundleByTopic(queue) : topicBundleMap.get(queue);
                // log.info("queueBundle------282----"+queueBundle.toString());
                BrokerConf queueBrokerConf = new BrokerConf(bundleBrokerBindMap.get(queueBundle));
                ProxyHandler proxyHandler = proxyHandlerMap.get(queueBrokerConf);
//                log.info("BrokerConf:{}------351----queueBrokerConf:{}",brokerConf,queueBrokerConf);
                if (bundles.contains(queueBundle)) {
                    consumeMsgList.forEach(object ->
                    {
                        if (Objects.isNull(object)||proxyHandler==null) {
                            return;
                        }
                        proxyHandler.getFlagFuture().whenCompleteAsync((flag,throwable)->{
                            if(flag) {
                                if (object instanceof ByteBuf) {
                                    ByteBuf byteBuf = (ByteBuf) object;
                                    ByteBuf temp = byteBuf.copy();
                                    proxyHandler.getBrokerChannel().writeAndFlush(temp);
                                }
                                if (object instanceof AMQFrame) {
                                    AMQFrame amqFrame = (AMQFrame) object;
                                    proxyHandler.getBrokerChannel().writeAndFlush(amqFrame);
                                }
                            }
                        });
                    });
                }
            });
        });

    }

    private void sendNAck() {
        Set<ProxyConnection> proxyConnectionSet = proxyService.getProxyConnectionSet();
        log.info("sendNAck 380");
        proxyConnectionSet.forEach(proxyConnection -> {
            Map<Long, AtomicInteger> ackMap = proxyConnection.getAckMap();
            log.info("ackMap------"+ackMap.size());
            ackMap.forEach((k,v)->{
                log.info("sendNAck------"+k+"---"+v);
            });
            if(!ackMap.isEmpty()){
                Set<Map.Entry<Long, AtomicInteger>> entries = ackMap.entrySet();
                Map.Entry<Long, AtomicInteger> longAtomicIntegerEntry = entries.stream().findFirst().get();
                Long deliveryTag = longAtomicIntegerEntry.getKey();
                BasicNackBody body = new BasicNackBody(deliveryTag,false,true);
//                log.info("writeAndFlush NAck------");
//                proxyConnection.getCnx().channel().writeAndFlush(body);
                proxyConnection.getCnx().channel().close();
            }
        });
    }

    private void test() {
        CompletableFuture<List<String>> children = proxyService.getPulsarService().getLocalMetadataStore().getChildren("/namespace/public/vhost1");
        children.thenApply(strings -> {
            for (String string : strings) {
                log.info("CompletableFuture-----------getChildren-------" + string);
            }
            return null;
        });

        CompletableFuture<Optional<GetResult>> dataMsg = proxyService.getPulsarService().getLocalMetadataStore().get(event.getData().getPath());
        dataMsg.thenApply(option -> {
            GetResult getResult = option.get();
            byte[] value = getResult.getValue();
            String s = new String(value);
            log.info("CompletableFuture--------dataMsg--------" + s);
            return null;
        });

        OwnedBundle ownedBundle = proxyService.getPulsarService().getNamespaceService().getOwnershipCache().getOwnedBundle(proxyService.getPulsarService().getNamespaceService().getBundle(TopicName.get("persistent://public/vhost1/__amqp_queue__test")));
        CompletableFuture<Optional<NamespaceEphemeralData>> ownerAsync = proxyService.getPulsarService().getNamespaceService().getOwnershipCache().getOwnerAsync(proxyService.getPulsarService().getNamespaceService().getBundle(TopicName.get("persistent://public/vhost1/__amqp_queue__test")));
        ownerAsync.thenApply(option -> {
            String nativeUrl1 = option.get().getNativeUrl();
            log.info("CompletableFuture--------nativeUrl---------" + nativeUrl1);
            return null;
        });
    }

    /**
     * @param path /namespace/public/vhost1/0xaaaaaaaa_0xffffffff
     * @return
     */
    private Bundle pathToBundle(String path) {
        String[] range = path.split("/");
        String[] bundle = range[range.length - 1].split("_");
        return new Bundle(bundle[0], bundle[1]);
    }

}
