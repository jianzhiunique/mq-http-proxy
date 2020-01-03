package io.github.jianzhiunique.mqproxy.init;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.helper.*;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;

class ConsumeCommitTest {
    @Test
    void test() {
        Gson gson = new Gson();

        ConsumerStatus consumerStatus = new ConsumerStatus();
        consumerStatus.setTopic("test");
        consumerStatus.setPartition(1);
        consumerStatus.setPosition(new Position());
        consumerStatus.getPosition().getSkipOffset().put("test", 100L);
        consumerStatus.setUnsafeMessages(new UnsafeMessages());
        consumerStatus.getUnsafeMessages().getCommitting().add(new CommitSection(1, 2, "test", 1));
        consumerStatus.getUnsafeMessages().getInflight().put("test", new InflightMessage(100, 1, 3));

        LinkedList<ConsumerStatus> list = new LinkedList<>();
        list.add(consumerStatus);


        String json = gson.toJson(list);
        System.out.println(json);


        LinkedList<ConsumerStatus> list2 = gson.fromJson(json, LinkedList.class);
        System.out.println(list2);
    }

}