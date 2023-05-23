/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.utils;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Pulsar topic metadata utils.
 */
@Slf4j
public class PulsarTopicMetadataUtils {
    /**
     * Update Pulsar topic MetaData.
     *
     * @param topic      pulsar topic
     * @param properties key-values data
     * @param name       exchange or queue name
     */
    public static void updateMetaData(PersistentTopic topic, Map<String, String> properties, String name) {
        if (null == topic) {
            log.error("name:{}, topic is null.", name);
            return;
        }

        long nowTime = System.currentTimeMillis();
        topic.getManagedLedger().asyncSetProperties(properties, new AsyncCallbacks.UpdatePropertiesCallback() {
            @Override
            public void updatePropertiesComplete(Map<String, String> map, Object o) {



                if (log.isDebugEnabled()) {
                    log.debug("[{}] update properties succeed, properties:{}", name, properties);
                }

                long setProTime = System.currentTimeMillis() - nowTime;

                // properties.forEach((key, value)->{
                //     log.info("updatePropertiesComplete key=[{}], value=[{}]", key, value);
                // });

                // FIXME properties要分是exchange还是queue
                if (properties.containsKey("QUEUES")){
                    int queueLength = properties.get("QUEUES").length();
                    log.info("updatePropertiesOK topic={}, queuesLength={}, exchange={}", name, queueLength, properties.get("EXCHANGE"));
                    if (setProTime >= 1000) {
                        log.info("took too long {}ms to set properties, topic={}, queuesLength={}, exchange={}",
                            setProTime, name, queueLength, properties.get("EXCHANGE"));
                    }
                }
                // else {
                //     log.info("normally took {}ms to set properties, topic={}, queuesLength={}",
                //         setProTime, name, properties.get("QUEUES").length());
                // }
            }

            @Override
            public void updatePropertiesFailed(ManagedLedgerException e, Object o) {
                log.error("[{}] update properties failed message: {}, properties:{}",
                        name, e.getMessage(), properties);
            }
        }, null);
    }
}
