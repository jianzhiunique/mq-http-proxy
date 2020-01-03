package io.github.jianzhiunique.mqproxy.fetcher;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class KafkaFetcher implements MqFetcher {

    private Gson gson = new Gson();

    @Autowired
    private KafkaConsumerStateManager kafkaConsumerStateManager;

    @Override
    public List<Message> get(FetchData fetchData) {
        return kafkaConsumerStateManager.fetch(fetchData);
    }
}
