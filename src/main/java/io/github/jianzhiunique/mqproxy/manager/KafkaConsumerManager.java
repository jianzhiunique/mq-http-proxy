package io.github.jianzhiunique.mqproxy.manager;

import io.github.jianzhiunique.mqproxy.config.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * create kafka consumers then return
 * then it can be used by consumer state
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaConsumerManager {

    @Autowired
    private KafkaConfig kafkaConfig;

    // create a new consumer
    public KafkaConsumer createConsumer(Map<String, String> config) {
        // merge configs to default configs
        Map<String, String> mergedConfig = kafkaConfig.getConsumer();
        config.forEach((key, value) -> {
            mergedConfig.put(key, value);
        });

        return new KafkaConsumer(mergedConfig);
    }
}
