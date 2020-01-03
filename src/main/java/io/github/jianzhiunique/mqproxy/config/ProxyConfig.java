package io.github.jianzhiunique.mqproxy.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Data
@ConfigurationProperties(prefix = "proxy")
public class ProxyConfig {
    private Map<String, String> config;
}

