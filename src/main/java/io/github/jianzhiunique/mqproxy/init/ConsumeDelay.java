package io.github.jianzhiunique.mqproxy.init;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerManager;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * consume the consumer status topic when proxy start
 * then preemption start
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class ConsumeDelay {
    private KafkaConsumer kafkaConsumer;

    @Autowired
    private ProxyConfig proxyConfig;

    @Autowired
    private KafkaConsumerManager kafkaConsumerManager;

    private Gson gson = new Gson();

    private ReentrantLock lock = new ReentrantLock();

    private HashedWheelTimer timer = new HashedWheelTimer();

    @Autowired
    private KafkaSender kafkaSender;

    @PostConstruct
    public void init() {
        HashMap<String, String> config = new HashMap<>();
        config.put("group.id", "mqp-delay-consumer");
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true");
        kafkaConsumer = kafkaConsumerManager.createConsumer(config);

        String[] topics = proxyConfig.getConfig().get("delayLevel").split(" ");
        ArrayList<String> topicList = new ArrayList<>();
        Arrays.asList(topics).forEach(topic -> {
            topicList.add(proxyConfig.getConfig().get("delayPrefix") + topic);
        });
        log.info("---> delay : " + topicList);
        kafkaConsumer.subscribe(topicList);
        timer.start();

        MqproxyApplication.defaultEventExecutorGroup.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                log.info("----> consume delay topics");
                lock.lock();
                ConsumerRecords<String, String> rs = kafkaConsumer.poll(Duration.ofSeconds(Long.parseLong(proxyConfig.getConfig().get("fetchCommitTimeout"))));
                lock.unlock();

                if (rs != null) {
                    for (ConsumerRecord<String, String> record : rs) {

                        log.info("---> record is " + record.value());

                        String payload = record.value();
                        String key = record.key();
                        Map<String, Object> headers = new HashMap<>();
                        record.headers().forEach(header -> {
                            headers.put(header.key(), new String(header.value()));
                        });
                        String queue = (String) headers.get("originTopic");
                        long time = Long.parseLong((String) headers.get("expireAt"));

                        if (time <= System.currentTimeMillis()) {
                            Message message = new Message();
                            message.setKey(key);
                            message.setQueue(queue);
                            message.setPayload(payload);
                            message.setHeaders(headers);
                            log.info("---> resend to new topic " + message);
                            kafkaSender.send(message);
                        } else {
                            timer.newTimeout(new TimerTask() {
                                @Override
                                public void run(Timeout timeout) throws Exception {
                                    Message message = new Message();
                                    message.setKey(key);
                                    message.setQueue(queue);
                                    message.setPayload(payload);
                                    message.setHeaders(headers);
                                    kafkaSender.send(message);
                                }
                            }, time - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                        }
                    }
                }
            }
        }, 0, 1, TimeUnit.SECONDS);
    }
}
