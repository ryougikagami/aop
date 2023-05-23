package io.streamnative.pulsar.handlers.amqp.proxy;

import io.streamnative.pulsar.handlers.amqp.Bundle;
import io.swagger.models.auth.In;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class BrokerConf {
    private String aopBrokerHost;
    private Integer aopBrokerPort;

    public BrokerConf(String aopBrokerHost, Integer aopBrokerPort ) {
        this.aopBrokerHost = aopBrokerHost;
        this.aopBrokerPort = aopBrokerPort;
    }

    public BrokerConf(Pair<String, Integer> pair) {
        if (Objects.isNull(pair)){
            log.info("pair isNull");
        }
        this.aopBrokerHost = pair.getKey();
        this.aopBrokerPort = pair.getValue();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerConf brokerConf = (BrokerConf) o;
        return Objects.equals(aopBrokerHost, brokerConf.aopBrokerHost) &&
                Objects.equals(aopBrokerPort, brokerConf.aopBrokerPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aopBrokerPort, aopBrokerPort);
    }


    @Override
    public String toString(){
        return "aopBrokerHost:"+this.aopBrokerHost+"----aopBrokerPort:"+aopBrokerPort;
    }
}
