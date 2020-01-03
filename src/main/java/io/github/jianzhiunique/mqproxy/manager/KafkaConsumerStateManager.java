package io.github.jianzhiunique.mqproxy.manager;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.KafkaConfig;
import io.github.jianzhiunique.mqproxy.config.LocalDbConfig;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.helper.*;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import io.github.jianzhiunique.mqproxy.state.KafkaConsumerState;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * manage all the consumer state
 * map instance id to consumer state
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaConsumerStateManager {

    private ConcurrentHashMap<String, KafkaConsumerState> instances = new ConcurrentHashMap<>();

    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private KafkaConsumerManager kafkaConsumerManager;

    @Autowired
    private LocalDbConfig localDbConfig;

    @Autowired
    private ProxyConfig proxyConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private KafkaSender kafkaSender;

    //hold all the instances status for fail over quickly
    @Getter
    private Map<String, List<ConsumerStatus>> waitForRecover = new HashMap<>();

    /**
     * check if instance on this node
     *
     * @param instanceId
     * @return
     */
    public boolean isOnThisNode(String instanceId) {
        return instances.containsKey(instanceId);
    }

    /**
     * check instance READY
     *
     * @param instanceId
     * @return
     */
    public boolean isInstanceReady(String instanceId) {
        return instances.get(instanceId).getConsumerState() == ConsumerState.READY;
    }

    /**
     * get the instance's node addr
     *
     * @param instanceId
     * @return
     */
    public String whereIsInstance(String instanceId) {
        String proxyName = zkUtil.getInstanceInfo(instanceId).get("name");
        Map<String, String> proxy = zkUtil.getProxyInfo(proxyName);
        return proxy == null ? null : proxy.get("addr");
    }

    public void register(String topics, String group, String reset) {
        String instanceId = "g-" + group + "-q-" + topics;

        Map<String, String> instanceInfo = new HashMap<>();
        instanceInfo.put("group", group);
        instanceInfo.put("reset", reset);
        instanceInfo.put("queues", topics);
        newState(instanceId, instanceInfo);
    }

    /**
     * resume consumer by instanceId
     *
     * @param instanceId
     */
    public void resume(String instanceId) {
        // at this point, previous consumer status is ready
        log.warn("---> start resume " + instanceId);

        // fetch data from zk
        Map<String, String> instanceInfo = zkUtil.getInstanceInfo(instanceId);

        // new state
        newState(instanceId, instanceInfo);

    }

    public void newState(String instanceId, Map<String, String> instanceInfo) {
        Map<String, String> config = new HashMap<>();

        config.put("group.id", instanceInfo.get("group"));
        if (instanceInfo.containsKey("reset") && instanceInfo.get("reset") != null &&
                instanceInfo.get("reset").equals("latest")) {
            config.put("auto.offset.reset", "latest");
        } else {
            config.put("auto.offset.reset", "earliest");
        }

        // new ConsumerState
        KafkaConsumerState state = new KafkaConsumerState();
        state.setInstanceId(instanceId);
        state.setDb(DBMaker
                .fileDB(localDbConfig.getConfig().get("path") + instanceId)
                .checksumHeaderBypass()
                .fileMmapEnable()
                .make());

        state.setMap(state.getDb().hashMap(instanceId).createOrOpen());

        //remove all previous data
        state.getMap().clear();

        // new Kafka consumer
        KafkaConsumer consumer = kafkaConsumerManager.createConsumer(config);
        state.setKafkaConsumer(consumer);

        // register to zookeeper for new fetch requests can find correct node
        // the fetch request may receive consumer not ready response when fail over
        instanceInfo.put("name", MqproxyApplication.serviceInfo);

        try {
            zkUtil.createInstance(instanceId, instanceInfo);
        } catch (ZkNodeExistsException e) {
            zkUtil.updateInstance(instanceId, instanceInfo);
        }

        //subscribe
        state.subscribe(Arrays.asList(instanceInfo.get("queues").split(",")));

        //add state into map
        instances.putIfAbsent(instanceId, state);

        // recover previous consumer status to avoid message re-consume
        // we do this check before prefetch
        if (waitForRecover.containsKey(instanceId)) {
            recoverState(instanceId);
        }

        // start prefetch
        state.startPrefetch(Integer.parseInt(proxyConfig.getConfig().get("preFetchInterval")),
                Long.parseLong(proxyConfig.getConfig().get("maxPrefetch")),
                Long.parseLong(proxyConfig.getConfig().get("fetchTimeout")));
    }

    public void delete(String instanceId) {
    }

    /**
     * consumer proxy instances list on this node
     */
    public void instances() {
    }

    public ArrayList<Message> fetch(FetchData fetchData) {
        KafkaConsumerState kafkaConsumerState = instances.get(fetchData.getInstanceId());
        FetchResult fetchResult = kafkaConsumerState.fetch(fetchData);
        // only update status to kafka if messages > 0 or need update status
        if (fetchResult.getMessages().size() > 0 || fetchResult.isNeedUpdateStatus()) {
            kafkaConsumerState.status(proxyConfig.getConfig().get("commitStatus"), kafkaSender);
        }

        return fetchResult.getMessages();
    }

    public boolean commit(CommitData commitData) {
        KafkaConsumerState kafkaConsumerState = instances.get(commitData.getInstanceId());
        boolean commitResult = kafkaConsumerState.commit(commitData);
        kafkaConsumerState.status(proxyConfig.getConfig().get("commitStatus"), kafkaSender);

        return commitResult;
    }

    public void recoverState(String instanceId) {

        List<ConsumerStatus> statuses = waitForRecover.get(instanceId);
        log.warn("---> recover state : " + instanceId + " status: " + statuses);

        if (statuses.size() == 0) {
            return;
        }


        statuses.forEach(consumerStatus -> {
            TopicAndPartition key = new TopicAndPartition(consumerStatus.getTopic(), consumerStatus.getPartition());

            if (consumerStatus.getPosition() != null) {
                // for check first poll
                consumerStatus.getPosition().setFetchedOffset(new AtomicLong(-1));
                consumerStatus.getPosition().setReturnedOffset(new AtomicLong(-1));
                instances.get(instanceId).getPositions().put(key, consumerStatus.getPosition());
            }

            if (consumerStatus.getUnsafeMessages() != null) {
                instances.get(instanceId).getUnsafe().put(key, consumerStatus.getUnsafeMessages());
            }

        });

        // at this point, the consumer status is recovered, it's safe to start prefetch
        // in prefetch, for every message, we will check status
        log.warn("---> state recovered: " + instanceId + ", positions: " + instances.get(instanceId).getPositions());
        log.warn("---> state recovered: " + instanceId + ", unsafe: " + instances.get(instanceId).getUnsafe());
    }

    @Synchronized
    public void commitOffset(String topic, KafkaSender kafkaSender) {
        log.info("---> commitOffset" + System.currentTimeMillis());
        instances.forEach((instanceId, state) -> {
            state.commitOffset(topic, kafkaSender);
        });
    }

    public void clear() {
        instances.forEach((instanceId, state) -> {
            state.clear();
            log.warn("===> unlock instanceId " + instanceId);
            zkUtil.unlockInstance(instanceId);
        });

        instances.clear();

        log.warn("===> instances clear end " + instances.size());
    }
}
