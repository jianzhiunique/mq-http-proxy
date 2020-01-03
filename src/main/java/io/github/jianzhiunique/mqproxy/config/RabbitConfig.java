package io.github.jianzhiunique.mqproxy.config;

import lombok.Data;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Data
@ConfigurationProperties(prefix = "rabbit")
@ConditionalOnProperty(value = "proxy.config.rabbitmq", havingValue = "true")
public class RabbitConfig {
    private Map<String, String> address;
}
