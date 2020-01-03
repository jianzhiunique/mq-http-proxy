package io.github.jianzhiunique.mqproxy.message;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Message implements Serializable {
    private String mid;
    private long retry = -1;
    private String queue;
    private String payload;
    private String key;
    // headers for message
    private Map<String, Object> headers;
    // properties for message
    private Map<String, Object> props;
    // extra for proxy
    private Map<String, Object> extra;
    // for fetch
    private long availableAt = -1;
    // for fetch
    private int maxConsume = -1;
}
