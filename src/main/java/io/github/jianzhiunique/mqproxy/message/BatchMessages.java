package io.github.jianzhiunique.mqproxy.message;

import lombok.Data;

import java.util.List;

@Data
public class BatchMessages {
    private List<Message> messages;
}
