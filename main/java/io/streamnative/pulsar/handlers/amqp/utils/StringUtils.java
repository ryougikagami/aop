package io.streamnative.pulsar.handlers.amqp.utils;

import io.streamnative.pulsar.handlers.amqp.proxy.BrokerConf;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringUtils {

    public static BrokerConf parseUrl(String url){
        // log.info(url);
        String[] split = url.split("//");
        String[] conf = split[1].split(":");
        return new BrokerConf(conf[0], Integer.parseInt(conf[1]));
    }

}
