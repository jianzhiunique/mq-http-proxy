package io.github.jianzhiunique.mqproxy.helper;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class CommitData {
    private String instanceId;
    private String queues;
    private String group;
    private List<CommitSection> data = new LinkedList<>();
}
