package io.github.jianzhiunique.mqproxy.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "localdb")
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class LocalDbConfig {
    private Map<String, String> config;
}
