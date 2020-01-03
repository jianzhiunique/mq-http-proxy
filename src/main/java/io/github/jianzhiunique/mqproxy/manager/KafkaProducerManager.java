package io.github.jianzhiunique.mqproxy.manager;

import io.github.jianzhiunique.mqproxy.config.KafkaConfig;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * manage kafka producers
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaProducerManager {

    @Getter
    private KafkaProducer kafkaProducer;

    @Autowired
    private KafkaConfig kafkaConfig;

    @PostConstruct
    public void init() {
        kafkaProducer = new KafkaProducer(kafkaConfig.getProducer());
    }
}
