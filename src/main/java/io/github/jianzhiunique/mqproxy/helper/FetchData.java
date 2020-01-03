package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

@Data
public class FetchData {
    private String instanceId;
    private String queues;
    private String group;
    private String reset;
    private long commitTimeout;
    private int maxConsumeTimes = -1;
}
