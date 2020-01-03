package io.github.jianzhiunique.mqproxy.manager;

import io.github.jianzhiunique.mqproxy.config.KafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CURD topic dynamically
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaTopicManager {

    @Autowired
    private KafkaConfig kafkaConfig;

    private AdminClient adminClient;

    @PostConstruct
    public void init() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getProducer().get("bootstrap.servers"));
        adminClient = AdminClient.create(config);
    }


    public void createTopic(List<String> topics, int partitions, short replication, Map<String, String> configs) {
        List<NewTopic> topicList = new ArrayList<>();

        topics.forEach(topic -> {
            NewTopic newTopic = new NewTopic(topic, partitions, replication);
            if(configs != null){
                newTopic.configs(configs);
            }

            topicList.add(newTopic);
        });

        adminClient.createTopics(topicList);
    }
}
