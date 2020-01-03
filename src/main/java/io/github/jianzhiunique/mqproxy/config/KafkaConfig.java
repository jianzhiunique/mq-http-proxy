package io.github.jianzhiunique.mqproxy.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Data
@ConfigurationProperties(prefix = "kafka")
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaConfig {
    private Map<String, String> producer;
    private Map<String, String> consumer;
}
