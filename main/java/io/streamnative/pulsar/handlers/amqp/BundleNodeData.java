package io.streamnative.pulsar.handlers.amqp;

import lombok.*;

import java.net.URI;
import java.util.HashMap;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BundleNodeData {

    private String nativeUrl;

    private String httpUrl;

    private boolean disabled;

    private HashMap<String, String> advertisedListeners;

}


