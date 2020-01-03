package io.github.jianzhiunique.mqproxy.manager;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;

@SpringBootTest
class KafkaTopicManagerTest {

    @Autowired
    private KafkaTopicManager kafkaTopicManager;

    @Test
    void createTopic() {
        ArrayList<String> topics = new ArrayList<>();
        topics.add("mqp-test");
        kafkaTopicManager.createTopic(topics, 3, (short) 2, null);
    }
}