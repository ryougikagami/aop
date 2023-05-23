package io.streamnative.pulsar.handlers.amqp.utils;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;

import java.util.ArrayList;
import java.util.List;

public class ExDispatchRule {
    public static List<String> exDispatchRule(AMQShortString exchange){
        List<String> list = new ArrayList<>();
        list.add(exchange+"_177");
        list.add(exchange+"_175");
        list.add(exchange+"_14");
        return list;
    }

}
