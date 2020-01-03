package io.github.jianzhiunique.mqproxy.sender;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import io.github.jianzhiunique.mqproxy.manager.RabbitManager;
import io.github.jianzhiunique.mqproxy.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * we just give the basic features for rabbitmq
 * like direct, fanout and delay queue
 * and we don't handle lot's of arguments or properties
 * <p>
 * with type x-delayed-message, we set extra arguments:
 * x-delayed-type:	direct
 * durable:	true
 * <p>
 * other type, direct, fanout, we don't handle arguments
 * <p>
 * -_- sorry
 * <p>
 * for exchange:
 * name, type, durable
 * <p>
 * for queue:
 * name, durable,
 */
@Component
@ConditionalOnProperty(value = "proxy.config.rabbitmq", havingValue = "true")
public class RabbitSender implements MqSender {

    @Autowired
    private RabbitManager rabbitManager;

    @Override
    public boolean send(Message message) throws Exception {
        String vhost = (String) message.getExtra().get("vhost");
        String username = (String) message.getExtra().get("username");
        String password = (String) message.getExtra().get("password");
        String exchangeName = message.getQueue();
        String routingKey = message.getKey();

        List<Map<String, String>> exchanges = (List<Map<String, String>>) message.getExtra().get("exchanges");
        List<Map<String, String>> queues = (List<Map<String, String>>) message.getExtra().get("queues");
        List<Map<String, String>> bindings = (List<Map<String, String>>) message.getExtra().get("bindings");


        Channel channel = rabbitManager.newChannel(vhost, username, password);

        for (Map<String, String> exchange : exchanges) {
            if (exchange.get("type").equals("x-delayed-message")) {
                Map<String, Object> arguments = new HashMap<>();
                arguments.put("x-delayed-type", "direct");

                channel.exchangeDeclare(exchange.get("name"),
                        "x-delayed-message",
                        Boolean.parseBoolean(exchange.get("durable")),
                        false,
                        false,
                        arguments);
            } else {
                channel.exchangeDeclare(exchange.get("name"),
                        exchange.get("type"),
                        Boolean.parseBoolean(exchange.get("durable")));
            }
        }


        for (Map<String, String> queue : queues) {
            channel.queueDeclare(queue.get("name"),
                    Boolean.parseBoolean(queue.get("durable")),
                    false,
                    false,
                    null);
        }
        for (Map<String, String> binding : bindings) {
            channel.queueBind(binding.get("queue"),
                    binding.get("exchange"),
                    binding.get("routingkey"));
        }

        // wrap message
        byte[] messageBodyBytes = message.getPayload().getBytes("UTF-8");
        // we put all the props as headers
        Map<String, Object> headers = message.getHeaders() == null ? new HashMap<>() : message.getHeaders();
        // check if message is delay message
        if (message.getExtra().containsKey("x-delay")) {
            headers.put("x-delay", message.getExtra().get("x-delay"));
        }
        AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();
        props.deliveryMode(2);
        props.timestamp(new Date());
        props.headers(headers);

        // publish message
        channel.basicPublish(exchangeName, routingKey, props.build(), messageBodyBytes);

        // close channel
        channel.close();
        return true;
    }
}
