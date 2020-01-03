package io.github.jianzhiunique.mqproxy.helper;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicAndPartition {
    private String topic;
    private int partition;
}
