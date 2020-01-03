package io.github.jianzhiunique.mqproxy.controller.v1;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.fetcher.KafkaFetcher;
import io.github.jianzhiunique.mqproxy.helper.CommitData;
import io.github.jianzhiunique.mqproxy.helper.CreateTopicData;
import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.helper.ReturnData;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.manager.KafkaTopicManager;
import io.github.jianzhiunique.mqproxy.message.BatchMessages;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@Slf4j
@RequestMapping("/v1/kafka")
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaController {

    private Gson gson = new Gson();

    @Autowired
    private KafkaSender kafkaSender;

    @Autowired
    private KafkaFetcher kafkaFetcher;

    @Autowired
    private ProxyConfig proxyConfig;

    @Autowired
    private KafkaConsumerStateManager kafkaConsumerStateManager;

    @Autowired
    private KafkaTopicManager kafkaTopicManager;

    @RequestMapping("/create_topic")
    public Mono<String> create(@RequestBody CreateTopicData createTopicData){
        List<String> topics = new ArrayList<>();
        topics.add(createTopicData.getName());
        kafkaTopicManager.createTopic(topics, createTopicData.getPartitions(), createTopicData.getReplication(), createTopicData.getConfig());
        return Mono.just(gson.toJson(new ReturnData(0, null, "create success")));
    }

    @RequestMapping("/send")
    public Mono<String> send(@RequestBody Message message) {
        if (message.getExtra() != null && message.getExtra().containsKey("mq_type") && message.getExtra().get("mq_type").equals("rabbitmq")) {
            return Mono.just(gson.toJson(new ReturnData(400, null, "this uri only support kafka")));
        } else {
            // check delay
            if (message.getExtra() != null && message.getExtra().containsKey("x-delay")) {
                String delayLevel = (String) message.getExtra().get("x-delay");
                if (!proxyConfig.getConfig().get("delayLevel").contains(delayLevel)) {
                    return Mono.just(gson.toJson(new ReturnData(400, null, "server can not support this delay level")));
                } else {
                    //'1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h'
                    switch (delayLevel) {
                        case "1s":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 1000);
                            break;
                        case "5s":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 5000);
                            break;
                        case "10s":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 10000);
                            break;
                        case "30s":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 30000);
                            break;
                        case "1m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 60000);
                            break;
                        case "2m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 120000);
                            break;
                        case "3m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 180000);
                            break;
                        case "4m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 240000);
                            break;
                        case "5m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 300000);
                            break;
                        case "6m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 360000);
                            break;
                        case "7m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 420000);
                            break;
                        case "8m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 480000);
                            break;
                        case "9m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 540000);
                            break;
                        case "10m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 600000);
                            break;
                        case "20m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 1200000);
                            break;
                        case "30m":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 1800000);
                            break;
                        case "1h":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 3600000);
                            break;
                        case "2h":
                            message.getHeaders().put("expireAt", System.currentTimeMillis() + 7200000);
                            break;

                    }
                    message.getHeaders().put("originTopic", message.getQueue());

                    message.setQueue(proxyConfig.getConfig().get("delayPrefix") + delayLevel);
                }
            }
            log.warn("send to topic " + message.getQueue());
            kafkaSender.send(message);
        }

        return Mono.just(gson.toJson(new ReturnData(0, null, "send success")));
    }

    @RequestMapping("/send_batch")
    public Mono<String> sendBatch(@RequestBody BatchMessages messages) {
        ArrayList<Message> failed = new ArrayList<>();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        messages.getMessages().forEach(message -> {
            if (message.getExtra() != null && message.getExtra().containsKey("mq_type") && message.getExtra().get("mq_type").equals("rabbitmq")) {
                hasFailed.set(true);
            } else {
                kafkaSender.send(message);
            }
        });

        if (hasFailed.get()) {
            return Mono.just(gson.toJson(new ReturnData(500, failed, "send failed")));
        }

        return Mono.just(gson.toJson(new ReturnData(0, null, "send success")));
    }

    @RequestMapping("/fetch")
    public Mono<String> fetch(@RequestBody FetchData fetchData) {
        if (fetchData.getReset().equals("rabbitmq")) {
            return Mono.just(gson.toJson(new ReturnData(400, null, "this uri only support kafka")));
        } else {
            String instanceId = "g-" + fetchData.getGroup() + "-q-" + fetchData.getQueues();
            if (!kafkaConsumerStateManager.isOnThisNode(instanceId)) {
                try {
                    String addr = kafkaConsumerStateManager.whereIsInstance(instanceId);
                    return Mono.just(gson.toJson(new ReturnData(404, addr, "not on this node")));
                } catch (ZkNoNodeException e) {
                    kafkaConsumerStateManager.register(fetchData.getQueues(), fetchData.getGroup(), fetchData.getReset());
                }
            }
            if (!kafkaConsumerStateManager.isInstanceReady(instanceId)) {
                return Mono.just(gson.toJson(new ReturnData(503, null, "consumer not ready")));
            }
            fetchData.setInstanceId(instanceId);
            List<Message> messages = kafkaFetcher.get(fetchData);

            return Mono.just(gson.toJson(new ReturnData(0, messages, "fetch success")));
        }

    }

    @RequestMapping("/commit")
    public Mono<String> commit(@RequestBody CommitData commitData) {
        String instanceId = "g-" + commitData.getGroup() + "-q-" + commitData.getQueues();
        if (!kafkaConsumerStateManager.isOnThisNode(instanceId)) {
            try {
                String addr = kafkaConsumerStateManager.whereIsInstance(instanceId);
                return Mono.just(gson.toJson(new ReturnData(404, addr, "not on this node")));
            } catch (ZkNoNodeException e) {
                return Mono.just(gson.toJson(new ReturnData(400, "", "not fetch yet, no instance")));
            }
        }
        if (!kafkaConsumerStateManager.isInstanceReady(instanceId)) {
            return Mono.just(gson.toJson(new ReturnData(503, null, "consumer not ready")));
        }
        commitData.setInstanceId(instanceId);
        boolean success = kafkaConsumerStateManager.commit(commitData);
        return Mono.just(gson.toJson(new ReturnData(0, success, "commit success")));
    }
}
