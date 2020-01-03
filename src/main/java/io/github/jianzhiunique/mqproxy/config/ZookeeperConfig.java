package io.github.jianzhiunique.mqproxy.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Data
@ConfigurationProperties(prefix = "zookeeper")
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class ZookeeperConfig {
    private Map<String, String> config;
}
