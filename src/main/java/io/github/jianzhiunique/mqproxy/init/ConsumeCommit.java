package io.github.jianzhiunique.mqproxy.init;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.KafkaConfig;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.helper.ConsumerStatus;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerManager;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.manager.KafkaTopicManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * consume the consumer status topic when proxy start
 * then preemption start
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class ConsumeCommit {
    private KafkaConsumer kafkaConsumer;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private ProxyConfig proxyConfig;

    @Autowired
    private KafkaConsumerManager kafkaConsumerManager;

    @Autowired
    private KafkaConsumerStateManager kafkaConsumerStateManager;

    @Autowired
    private KafkaTopicManager kafkaTopicManager;

    private Gson gson = new Gson();

    // mark for the consumption to the tail of the topic
    public static boolean status = false;

    private Map<TopicPartition, Long> endOffsets = null;

    @PostConstruct
    public void init() {
        // first check the topic
        ArrayList<String> topicList = new ArrayList<>();
        topicList.add(proxyConfig.getConfig().get("commitStatus"));
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "compact");
        configs.put("min.cleanable.dirty.ratio", "0.1");
        configs.put("segment.ms", "5000");
        kafkaTopicManager.createTopic(topicList, 20, (short) 2, configs);


        // consume it
        HashMap<String, String> config = new HashMap<>();
        config.put("group.id", MqproxyApplication.hostname + MqproxyApplication.port);
        config.put("auto.offset.reset", "earliest");
        kafkaConsumer = kafkaConsumerManager.createConsumer(config);
        kafkaConsumer.subscribe(Arrays.asList(proxyConfig.getConfig().get("commitStatus")));

        // seek to beginning
        kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

        MqproxyApplication.defaultEventExecutorGroup.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                ConsumerRecords<String, String> rs = kafkaConsumer.poll(Duration.ofSeconds(Long.parseLong(proxyConfig.getConfig().get("fetchCommitTimeout"))));

                if (endOffsets == null) {
                    // store the end offsets position
                    endOffsets = kafkaConsumer.endOffsets(kafkaConsumer.assignment());

                    ArrayList<TopicPartition> del = new ArrayList<>();
                    endOffsets.keySet().forEach(topicPartition -> {
                        // this partition has no message
                        if (endOffsets.get(topicPartition) == 0) {
                            del.add(topicPartition);
                        }
                    });

                    del.forEach(topicPartition -> {
                        endOffsets.remove(topicPartition);
                    });
                }


                // we just put the latest status to kafkaConsumerStateManager
                if (rs != null) {
                    for (ConsumerRecord<String, String> record : rs) {
                        // some wrong data may lead to exception
                        try {
                            Type aType = new TypeToken<LinkedList<ConsumerStatus>>() {
                            }.getType();
                            LinkedList<ConsumerStatus> consumerStatus = gson.fromJson(record.value(), aType);

                            kafkaConsumerStateManager.getWaitForRecover().put(record.key(), consumerStatus);
                        } catch (Exception e) {
                            log.warn("---> error consume from commit status topic : " + record.key() + " : " + record.value());
                        }

                        TopicPartition key = new TopicPartition(record.topic(), record.partition());

                        if (endOffsets != null && endOffsets.containsKey(key) && endOffsets.get(key) == record.offset() + 1) {
                            endOffsets.remove(key);
                        }

                        if (endOffsets.size() == 0) {
                            status = true;
                        }
                    }
                }

                // we recover the status if fail over and resume consumer
            }
        }, 0, 1, TimeUnit.SECONDS);
    }
}
