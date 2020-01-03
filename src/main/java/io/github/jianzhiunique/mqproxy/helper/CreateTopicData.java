package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

import java.util.Map;

@Data
public class CreateTopicData {
    private String name;
    private int partitions;
    private short replication;
    private Map<String, String> config;
}
