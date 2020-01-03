package io.github.jianzhiunique.mqproxy.manager;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.jianzhiunique.mqproxy.config.RabbitConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.rabbitmq", havingValue = "true")
public class RabbitManager {

    @Autowired
    private RabbitConfig rabbitConfig;

    private Map<String, Connection> connections = new ConcurrentHashMap<>();

    private void newConnection(String vhost, String username, String password) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setVirtualHost(vhost);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setAutomaticRecoveryEnabled(true);


        ArrayList<Address> addresses = new ArrayList<>();
        //System.out.println(rabbitConfig.getAddress());
        //rabbitConfig.getAddress().forEach(stringStringMap -> {
            addresses.add(new Address(rabbitConfig.getAddress().get("host"), Integer.parseInt(rabbitConfig.getAddress().get("port"))));
        //});

        Connection connection = connectionFactory.newConnection(addresses);
        connections.put(vhost, connection);
    }

    public Channel newChannel(String vhost, String username, String password) throws IOException, TimeoutException {
        if (!connections.containsKey(vhost)) {
            newConnection(vhost, username, password);
        }

        Channel channel = connections.get(vhost).createChannel();
        return channel;
    }
}
