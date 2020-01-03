package io.github.jianzhiunique.mqproxy.distributed;


import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class AutoOffline {
}
