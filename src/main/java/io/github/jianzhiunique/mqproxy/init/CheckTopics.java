package io.github.jianzhiunique.mqproxy.init;

import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.manager.KafkaTopicManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class CheckTopics {

    @Autowired
    private KafkaTopicManager kafkaTopicManager;

    @Autowired
    private ProxyConfig proxyConfig;

    @PostConstruct
    public void init() {
        String[] topics = proxyConfig.getConfig().get("delayLevel").split(" ");
        ArrayList<String> topicList = new ArrayList<>();
        Arrays.asList(topics).forEach(topic -> {
            topicList.add(proxyConfig.getConfig().get("delayPrefix") + topic);
        });
        kafkaTopicManager.createTopic(topicList, 3, (short) 2, null);

        topicList.clear();


    }

}
