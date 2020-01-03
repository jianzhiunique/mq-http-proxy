package io.github.jianzhiunique.mqproxy.distributed;

import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Preemption
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class Preemption {

    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private KafkaConsumerStateManager kafkaConsumerStateManager;

    public void start() {
        List<String> proxys = zkUtil.getProxys();
        List<String> instances = zkUtil.getInstances();

        //TODO check if current consumers count > average, if reach average, stop resume

        // resume
        for (String instanceId : instances) {
            // check instance is locked
            if (!zkUtil.isInstanceLocked(instanceId)) {
                // try to lock instance
                if (zkUtil.lockInstance(instanceId)) {
                    log.warn("---> instanceId lock success : " + instanceId);
                    try {
                        // TODO need exception?
                        kafkaConsumerStateManager.resume(instanceId);
                    } catch (Exception e) {
                        // if resume fail, unlock
                        zkUtil.unlockInstance(instanceId);
                    }
                } else {
                    log.warn("---> instanceId lock failed : " + instanceId);
                }
            } else {
                log.warn("---> instanceId locked : " + instanceId);
            }
        }
    }
}
