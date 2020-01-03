package io.github.jianzhiunique.mqproxy.sender;

import io.github.jianzhiunique.mqproxy.message.Message;

public interface MqSender {
    boolean send(Message message) throws Exception;
}
