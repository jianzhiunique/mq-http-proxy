package io.github.jianzhiunique.mqproxy.helper;

import io.github.jianzhiunique.mqproxy.message.Message;
import lombok.Data;

import java.util.ArrayList;

@Data
public class FetchResult {
    private boolean needUpdateStatus;
    private ArrayList<Message> messages;
}
