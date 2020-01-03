package io.github.jianzhiunique.mqproxy.fetcher;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.manager.RabbitManager;
import io.github.jianzhiunique.mqproxy.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * we just give the basic features for rabbitmq
 * in fetch, we reuse kafka's data struct
 * <p>
 * With AMQP 0-9-1 it is possible to fetch messages one by one using the basic.get protocol method.
 * Messages are fetched in the FIFO order.
 * It is possible to use automatic or manual acknowledgements, just like with consumers (subscriptions).
 * <p>
 * Fetching messages one by one is highly discouraged as it is very inefficient compared to regular long-lived consumers.
 * As with any polling-based algorithm, it will be extremely wasteful in systems where message publishing is sporadic
 * and queues can stay empty for prolonged periods of time.
 * <p>
 * When in doubt, prefer using a regular long-lived a consumer.
 */
@Component
@ConditionalOnProperty(value = "proxy.config.rabbitmq", havingValue = "true")
public class RabbitFetcher implements MqFetcher {

    private Gson gson = new Gson();

    @Autowired
    private RabbitManager rabbitManager;

    @Override
    public List<Message> get(FetchData fetchData) throws Exception {
        // host vhost username password will in this field
        Map<String, String> info = gson.fromJson(fetchData.getGroup(), Map.class);
        String vhost = info.get("vhost");
        String username = info.get("username");
        String password = info.get("password");

        Channel channel = rabbitManager.newChannel(vhost, username, password);

        GetResponse response = channel.basicGet(fetchData.getQueues(), true);
        channel.close();

        Message message = new Message();
        message.setPayload(new String(response.getBody()));
        message.setQueue(fetchData.getQueues());
        Map<String, Object> headers = new HashMap<>();

        response.getProps().getHeaders().forEach((key, value) -> {
            headers.put(key, value.toString());
        });
        message.setHeaders(headers);
        message.setMid(response.getEnvelope().getDeliveryTag() + "");
        message.setAvailableAt(System.currentTimeMillis());
        if (response.getEnvelope().isRedeliver()) {
            message.setRetry(1);
        }

        ArrayList<Message> messages = new ArrayList<>();
        messages.add(message);

        return messages;
    }
}
