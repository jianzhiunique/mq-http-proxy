package io.github.jianzhiunique.mqproxy.state;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.helper.*;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.mapdb.DB;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


/**
 * kafka consumer state
 */
@Data
@Slf4j
public class KafkaConsumerState {

    //instance id
    private String instanceId;

    //是否可以被拉取数据，在failover之后需要恢复原来的进度，所以需要等待
    private ConsumerState consumerState = ConsumerState.NOT_READY;

    //kafka消费者
    private KafkaConsumer kafkaConsumer;

    // position
    private ConcurrentMap<TopicAndPartition, Position> positions = new ConcurrentHashMap<>();

    private DB db;

    private ConcurrentMap map;

    // unsafe messages
    private ConcurrentHashMap<TopicAndPartition, UnsafeMessages> unsafe = new ConcurrentHashMap<>();

    private Gson gson = new Gson();

    //for lock consumer
    private ReentrantLock lock = new ReentrantLock();

    public void startPrefetch(int interval, long maxPrefetch, long timeout) {
        consumerState = ConsumerState.READY;
        MqproxyApplication.defaultEventExecutorGroup.schedule(new Runnable() {
            @Override
            public void run() {
                log.info("---> prefetch " + instanceId);
                if (consumerState == ConsumerState.CLOSE) {
                    log.warn("---> consumer closed " + instanceId);
                    return;
                }

                // check previous message count
                if (map.size() > maxPrefetch) {
                    pausePrefetch();
                }

                if (map.size() < maxPrefetch) {
                    resumePrefetch();
                }

                prefetch(timeout);

                //reschedule
                MqproxyApplication.defaultEventExecutorGroup.schedule(this, interval, TimeUnit.SECONDS);

            }
        }, 0, TimeUnit.SECONDS);
    }

    @Synchronized
    public void subscribe(List<String> topics) {
        lock.lock();
        kafkaConsumer.subscribe(topics);
        lock.unlock();
    }

    @Synchronized
    public void prefetch(long timeout) {
        // we catch exception to avoid prefetch stop
        try {
            // before this point, previous consumer status is recovered or no previous status
            lock.lock();
            ConsumerRecords<String, String> rs = kafkaConsumer.poll(Duration.ofSeconds(timeout));
            lock.unlock();
            if (rs != null) {
                for (ConsumerRecord<String, String> record : rs) {
                    TopicAndPartition topicAndPartition = new TopicAndPartition(record.topic(), record.partition());

                    // if no previous status, init
                    positions.putIfAbsent(topicAndPartition, new Position());
                    unsafe.putIfAbsent(topicAndPartition, new UnsafeMessages());

                    Position position = positions.get(topicAndPartition);
                    UnsafeMessages unsafeMessages = unsafe.get(topicAndPartition);

                    // if first poll, init fetched and commit position must correct
                    if (position.getFetchedOffset().get() == -1) {
                        position.getFetchedOffset().set(record.offset());

                        // check continuity
                        // check record's offset and status's commit position
                        if (position.getCommitOffset().get() == -1) {
                            position.getCommitOffset().set(record.offset() - 1);
                        } else if (record.offset() > position.getCommitOffset().get() + 1) {
                            record.headers().add("mqproxy-notice", ("previous offset is " + position.getReturnedOffset().get()
                                    + " and next offset is " + record.offset()).getBytes());
                            // no need to skip
                            // commit + 1 ~ record.offset - 1  impossible
                            CommitData commitData = new CommitData();
                            commitData.setInstanceId(instanceId);
                            commitData.getData().add(new CommitSection(position.getCommitOffset().get() + 1, record.offset() - 1, record.topic(), record.partition()));
                            commit(commitData);
                        }

                    } else {
                        // check continuity
                        if (record.offset() > position.getFetchedOffset().get() + 1) {
                            record.headers().add("mqproxy-notice", ("previous offset is " + position.getFetchedOffset().get()
                                    + " and next offset is " + record.offset()).getBytes());

                            // skip
                            position.getSkipOffset().put((position.getFetchedOffset().get() + 1) + "", record.offset());

                            // commit this range, may be > returned !!!
                            CommitData commitData = new CommitData();
                            commitData.setInstanceId(instanceId);
                            commitData.getData().add(new CommitSection(position.getFetchedOffset().get() + 1, record.offset() - 1, record.topic(), record.partition()));

                            position.getFetchedOffset().set(record.offset());

                            commit(commitData);
                        }
                    }

                    String dataKey = record.topic() + "-" + record.partition() + "-" + record.offset();

                    // check the previous status, if message commit already, we'll not put it into map
                    // and if message is in flight, we recover the retry times and nextAvailableTime

                    // message already committed
                    if (record.offset() < position.getCommitOffset().get()) {
                        log.warn("--> message already committed : " + topicAndPartition + record.offset());
                        log.warn("--> message already committed : " + position);
                        continue;
                    }

                    // message is committing
                    List<CommitSection> sections = unsafeMessages.getCommitting();
                    boolean committing = false;
                    for (CommitSection s : sections) {
                        if (record.offset() >= s.getLeft() && record.offset() <= s.getRight()) {
                            committing = true;
                            break;
                        }
                    }
                    if (committing == true) {
                        log.warn("--> message is committing : " + topicAndPartition + record.offset());
                        continue;
                    }

                    Message message = new Message();
                    // message is in flight
                    if (unsafeMessages != null && unsafeMessages.getInflight().containsKey(record.offset() + "")) {
                        InflightMessage inflightMessage = unsafeMessages.getInflight().get(record.offset() + "");
                        log.warn("--> recover message in flight : " + topicAndPartition + inflightMessage);
                        message.setAvailableAt(inflightMessage.getNextAvailableTime());
                        message.setRetry(inflightMessage.getRetryTimes());
                        message.setMaxConsume(inflightMessage.getMaxConsumeTimes());
                    }

                    message.setPayload(record.value());
                    message.setQueue(record.topic());
                    message.setKey(record.key());

                    Map<String, Object> props = new HashMap<>();
                    props.put("offset", record.offset());
                    props.put("partition", record.partition());
                    props.put("timestamp", record.timestamp());
                    props.put("timestampType", record.timestampType().name);
                    message.setProps(props);

                    Map<String, Object> headers = new HashMap<>();
                    record.headers().forEach(header -> {
                        headers.put(header.key(), new String(header.value()));
                    });
                    message.setHeaders(headers);


                    map.putIfAbsent(dataKey, message);
                    log.info("---> map.size" + map.size());

                    // update the fetched position
                    positions.get(topicAndPartition).getFetchedOffset().set(record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Synchronized
    public void pausePrefetch() {
        lock.lock();
        if (kafkaConsumer.paused().size() == 0) {
            kafkaConsumer.pause(kafkaConsumer.assignment());
        }
        lock.unlock();
    }

    @Synchronized
    public void resumePrefetch() {
        lock.lock();
        if (kafkaConsumer.paused().size() > 0) {
            kafkaConsumer.resume(kafkaConsumer.paused());
        }
        lock.unlock();
    }


    public FetchResult fetch(FetchData fetchData) {
        ArrayList<Message> messages = new ArrayList<>();
        FetchResult fetchResult = new FetchResult();

        positions.forEach(((topicAndPartition, position) -> {
            long current = position.getCommitOffset().get() + 1;
            while (current <= position.getFetchedOffset().get()) {

                //check skip
                if (position.getSkipOffset().containsKey(current + "")) {
                    current = position.getSkipOffset().get(current + "");
                }

                // get message
                String key = topicAndPartition.getTopic() + "-" + topicAndPartition.getPartition() + "-" + current;
                if (map.containsKey(key)) {
                    Message message = (Message) map.get(key);

                    // message reach max consume times, drop it and commit it, then
                    if (message.getMaxConsume() >= 0 && message.getRetry() >= message.getMaxConsume()) {
                        log.warn("---> reach max consume times, drop message" + key);
                        map.remove(key);

                        CommitData commitData = new CommitData();
                        commitData.setGroup(fetchData.getGroup());
                        commitData.setQueues(fetchData.getQueues());
                        ArrayList<CommitSection> data = new ArrayList<>();
                        data.add(new CommitSection(current, current, topicAndPartition.getTopic(), topicAndPartition.getPartition()));
                        commitData.setData(data);

                        commit(commitData);
                        fetchResult.setNeedUpdateStatus(true);
                    } else if (message.getAvailableAt() == -1 || System.currentTimeMillis() >= message.getAvailableAt()) {

                        // add to result
                        messages.add(message);

                        // store to local map
                        message.setRetry(message.getRetry() + 1);
                        long nextAvailableTime = System.currentTimeMillis() + fetchData.getCommitTimeout() * 1000;
                        message.setAvailableAt(nextAvailableTime);
                        message.setMaxConsume(fetchData.getMaxConsumeTimes());
                        map.put(key, message);

                        // store inflight message
                        unsafe.putIfAbsent(topicAndPartition, new UnsafeMessages());
                        InflightMessage inflightMessage = new InflightMessage(nextAvailableTime, message.getRetry(), fetchData.getMaxConsumeTimes());
                        unsafe.get(topicAndPartition).getInflight().put(current + "", inflightMessage);

                        // save returned offset
                        if (current > position.getReturnedOffset().get()) {
                            position.getReturnedOffset().set(current);
                        }

                        break;
                    }
                }
                current++;
            }
        }));

        fetchResult.setMessages(messages);
        return fetchResult;
    }

    /**
     * handle commit request
     *
     * @param commitData
     * @return
     */
    public boolean commit(CommitData commitData) {
        // check the commit offset position vs. commitData's offset range
        commitData.getData().forEach(commitSection -> {
            String topic = commitSection.getTopic();
            int partition = commitSection.getPartition();
            long left = commitSection.getLeft();
            long right = commitSection.getRight();

            for (long i = left; i <= right; i++) {
                String dataKey = topic + "-" + partition + "-" + i;
                if (map.containsKey(dataKey)) {
                    log.info("---> receive commit, delete local message " + dataKey);
                    map.remove(dataKey);
                }
            }

            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Position position = positions.get(topicAndPartition);

            //|| left > position.getReturnedOffset().get()
            if (right < position.getCommitOffset().get()) {
                // invalid offset
                log.warn("---> invalid offset" + position + "，" + commitSection);
            } else if (left >= position.getCommitOffset().get() && right <= position.getReturnedOffset().get()) {
                // valid offset
                log.info("---> valid offset");
                unsafe.get(topicAndPartition).getCommitting().add(commitSection);
            } else if (left < position.getCommitOffset().get() && right > position.getReturnedOffset().get()) {
                // fix offset
                log.info("---> fix offset");
                commitSection.setLeft(position.getCommitOffset().get());
                commitSection.setRight(position.getReturnedOffset().get());
                unsafe.get(topicAndPartition).getCommitting().add(commitSection);

            } else if (left < position.getCommitOffset().get() && right >= position.getCommitOffset().get() && right <= position.getReturnedOffset().get()) {
                // fix left offset
                log.info("---> fix left offset");
                commitSection.setLeft(position.getCommitOffset().get());
                unsafe.get(topicAndPartition).getCommitting().add(commitSection);

            } else if (right > position.getReturnedOffset().get() && left >= position.getCommitOffset().get() && left <= position.getReturnedOffset().get()) {
                // fix right offset
                log.info("---> fix right offset");
                commitSection.setRight(position.getReturnedOffset().get());
                unsafe.get(topicAndPartition).getCommitting().add(commitSection);

            } else {
                log.warn("---> warning! left > position.getReturnedOffset().get()");
                unsafe.get(topicAndPartition).getCommitting().add(commitSection);
            }

            unsafe.get(topicAndPartition).merge();
        });
        return true;
    }

    /**
     * save status to kafka
     *
     * @param topic
     * @param kafkaSender
     */
    public void status(String topic, KafkaSender kafkaSender) {
        //send to kafka
        Message message = new Message();
        message.setKey(instanceId);
        message.setQueue(topic);


        LinkedList<ConsumerStatus> consumerStatuses = new LinkedList<>();

        positions.forEach((topicAndPartition, position) -> {
            ConsumerStatus consumerStatus = new ConsumerStatus();
            consumerStatus.setTopic(topicAndPartition.getTopic());
            consumerStatus.setPartition(topicAndPartition.getPartition());
            consumerStatus.setPosition(position);
            consumerStatuses.add(consumerStatus);
        });

        unsafe.forEach((topicAndPartition, unsafeMessages) -> {
            ConsumerStatus consumerStatus = new ConsumerStatus();
            consumerStatus.setTopic(topicAndPartition.getTopic());
            consumerStatus.setPartition(topicAndPartition.getPartition());
            consumerStatus.setUnsafeMessages(unsafeMessages);
            consumerStatuses.add(consumerStatus);
        });

        message.setPayload(gson.toJson(consumerStatuses));
        log.info("---> send to kafka status : " + message.getPayload());
        kafkaSender.send(message);
    }

    /**
     * commit to kafka
     *
     * @param topic
     * @param kafkaSender
     */
    public void commitOffset(String topic, KafkaSender kafkaSender) {
        unsafe.forEach((topicAndPartition, unsafeMessages) -> {
            if (unsafeMessages.getCommitting().size() > 0) {
                CommitSection commitSection = unsafeMessages.getCommitting().get(0);
                Position position = positions.get(topicAndPartition);

                // delete first if it's right <= commit position
                log.info("---> check commitSection 0:" + commitSection + " , " + position.getCommitOffset().get());
                if (commitSection.getRight() <= position.getCommitOffset().get()) {
                    unsafeMessages.getCommitting().remove(0);
                    status(topic, kafkaSender);
                    return;
                }

                final long nextCommit = position.getCommitOffset().get() + 1;
                if (commitSection.getLeft() <= nextCommit && commitSection.getRight() >= nextCommit) {
                    //commit
                    final long offset = commitSection.getRight();
                    Map commits = new HashMap<>();
                    commits.putIfAbsent(new TopicPartition(topicAndPartition.getTopic(), topicAndPartition.getPartition()), new OffsetAndMetadata(offset + 1));
                    log.info("---> commit to " + topicAndPartition + "@" + offset);


                    try {
                        lock.lock();
                        kafkaConsumer.commitSync(commits, Duration.ofSeconds(5));
                        lock.unlock();
                    } catch (Exception e) {
                        e.printStackTrace();
                        lock.unlock();
                        return;
                    }

                    log.info("---> committed to " + topicAndPartition + "@" + offset);
                    if (offset > position.getCommitOffset().get()) {
                        position.getCommitOffset().set(offset);

                        // delete messages inflight if it's offset <= offset
                        LinkedList<String> toDelete = new LinkedList<>();
                        unsafeMessages.getInflight().keySet().forEach(offsetKey -> {
                            if (Long.parseLong(offsetKey) <= offset) {
                                toDelete.add(offsetKey);
                            }
                        });
                        toDelete.forEach(offsetKey -> {
                            log.info("---> delete inflight to " + topicAndPartition + "@" + offsetKey);
                            unsafeMessages.getInflight().remove(offsetKey);
                        });

                        // delete messages skip if key <= offset
                        LinkedList<String> skipDelete = new LinkedList<>();
                        position.getSkipOffset().keySet().forEach(offsetKey -> {
                            if (Long.parseLong(offsetKey) <= offset) {
                                skipDelete.add(offsetKey);
                            }
                        });
                        skipDelete.forEach(offsetKey -> {
                            log.warn("---> delete skip " + topicAndPartition + "@" + offsetKey);
                            position.getSkipOffset().remove(offsetKey);
                        });
                    }

                    status(topic, kafkaSender);
                }
            }
        });
    }

    public void clear() {
        consumerState = ConsumerState.CLOSE;

        //close consumer
        lock.lock();
        kafkaConsumer.close();
        lock.unlock();

        //clean map
        map.clear();

        //close db
        db.close();
    }
}
