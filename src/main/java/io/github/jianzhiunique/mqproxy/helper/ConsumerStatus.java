package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

@Data
public class ConsumerStatus {
    private UnsafeMessages unsafeMessages;

    private String topic;

    private int partition;

    private Position position;
}
