package io.github.jianzhiunique.mqproxy.sender;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.manager.KafkaProducerManager;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.util.LocalDbUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.UUID;

@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaSender implements MqSender {

    @Autowired
    private KafkaProducerManager kafkaProducerManager;

    @Autowired
    private LocalDbUtil localDbUtil;

    @Autowired
    private ProxyConfig proxyConfig;

    @Override
    public boolean send(Message message) {
        MqproxyApplication.defaultEventExecutorGroup.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        if (message.getMid() == null || message.getMid().equals("")) {
                            message.setMid(UUID.randomUUID().toString());
                        }
                        message.setRetry(message.getRetry() + 1);
                        // save mid => message, put if absent
                        // once send success, delete it
                        localDbUtil.getMap().put(message.getMid(), message);
                        ProducerRecord<String, String> producerRecord = null;

                        if (message.getHeaders() != null && message.getHeaders().size() > 0) {
                            ArrayList<Header> headers = new ArrayList<>();
                            message.getHeaders().forEach((headerKey, headerValue) -> {

                                headers.add(new Header() {
                                    @Override
                                    public String key() {
                                        return headerKey;
                                    }

                                    @Override
                                    public byte[] value() {
                                        return headerValue.toString().getBytes();
                                    }
                                });
                            });

                            if (message.getKey() == null || message.getKey().equals("")) {
                                final String key = null;
                                producerRecord = new ProducerRecord<>(message.getQueue(), null, key, message.getPayload(), headers);
                            } else {
                                producerRecord = new ProducerRecord<>(message.getQueue(), null, message.getKey(), message.getPayload(), headers);
                            }
                        } else {
                            if (message.getKey() == null || message.getKey().equals("")) {
                                final String key = null;
                                producerRecord = new ProducerRecord<>(message.getQueue(), key, message.getPayload());
                            } else {
                                producerRecord = new ProducerRecord<>(message.getQueue(), message.getKey(), message.getPayload());
                            }
                        }


                        kafkaProducerManager.getKafkaProducer().send(producerRecord, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e != null) {
                                    // TODO if the topic not exists
                                    // we will get [Producer clientId=producer-1] Error while fetching metadata with correlation id 53 : {test_exchange=UNKNOWN_TOPIC_OR_PARTITION}
                                    // then after timeout, recv org.apache.kafka.common.errors.TimeoutException: Topic test_exchange not present in metadata after 60000 ms.
                                    // but we don't know this, so we must drop message when message reach max retry
                                    e.printStackTrace();
                                    // retry message
                                    // incr retry times

                                    message.setRetry(message.getRetry() + 1);

                                    /*
                                    if(e.getMessage().contains("not present in metadata")){
                                        // delete message
                                        localDbUtil.getMap().remove(message.getMid());
                                        log.error(e.getMessage() + ", and drop message " + message.toString());
                                        return;
                                    }
                                    */

                                    if (message.getRetry() <= Long.parseLong(proxyConfig.getConfig().get("maxSendRetry"))) {
                                        send(message);
                                    } else {
                                        // delete message
                                        localDbUtil.getMap().remove(message.getMid());
                                        log.error("message reach max retry, and drop message " + message.toString());
                                    }

                                } else {
                                    // delete message
                                    localDbUtil.getMap().remove(message.getMid());
                                }
                            }
                        });
                    }
                }
        );

        return true;
    }
}
