package io.github.jianzhiunique.mqproxy.task;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * commit to kafka
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class CommitOffset implements Runnable {

    @Autowired
    private KafkaConsumerStateManager kafkaConsumerStateManager;

    @Autowired
    private KafkaSender kafkaSender;

    @Autowired
    private ProxyConfig proxyConfig;

    @Override
    public void run() {
        kafkaConsumerStateManager.commitOffset(proxyConfig.getConfig().get("commitStatus"), kafkaSender);
    }

    @PostConstruct
    public void init() {
        MqproxyApplication.defaultEventExecutorGroup.scheduleAtFixedRate(this, 0, 1, TimeUnit.SECONDS);
    }
}
