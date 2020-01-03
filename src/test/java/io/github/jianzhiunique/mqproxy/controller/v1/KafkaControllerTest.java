package io.github.jianzhiunique.mqproxy.controller.v1;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.helper.CommitData;
import io.github.jianzhiunique.mqproxy.helper.CommitSection;
import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.helper.ReturnData;
import io.github.jianzhiunique.mqproxy.message.Message;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaControllerTest {

    private WebTestClient webTestClient = WebTestClient.bindToServer().baseUrl("http://localhost:8080").build();

    private Gson gson = new Gson();

    @Test
    void send() {
        String url = "/v1/kafka/send";
        for (int i = 0; i < 9; i++) {
            Message message = new Message();
            message.setQueue("mq-pressure-test");
            //message.setQueue("mqp-commit-status");
            //message.setKey("key1");
            message.setPayload("key1 test.data" + i);

            System.out.println("-------->request:" + gson.toJson(message));

            WebTestClient.BodyContentSpec bodyContentSpec = webTestClient.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(gson.toJson(message))
                    .exchange()
                    .expectStatus()
                    .isOk()
                    .expectBody();

            System.out.println("-------->response:" + new String(bodyContentSpec.returnResult().getResponseBody()));
        }
    }

    @Test
    void sendBatch() {
    }

    @Test
    void fetch() {
        String url = "/v1/fetch";
        FetchData message = new FetchData();
        message.setGroup("test");
        message.setQueues("mq-pressure-test");
        message.setCommitTimeout(10);

        System.out.println("-------->request:" + gson.toJson(message));

        WebTestClient.BodyContentSpec bodyContentSpec = webTestClient.post()
                .uri(url)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(gson.toJson(message))
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody();

        String response = new String(bodyContentSpec.returnResult().getResponseBody());
        ReturnData returnData = gson.fromJson(response, ReturnData.class);
        ArrayList<Map> messages = (ArrayList<Map>) returnData.getData();

        System.out.println("-------->response:" + response);
        //System.out.println(messages);

        String url1 = "/v1/commit";
        CommitData commitData = new CommitData();
        commitData.setGroup("test");
        commitData.setQueues("mq-pressure-test");
        ArrayList<CommitSection> data = new ArrayList<>();
        System.out.println("============> recv message count: " + messages.size());
        messages.forEach(msg -> {
            //System.out.println(msg.get("props"));
            Double offset = (Double) ((Map) msg.get("props")).get("offset");
            Double partition = (Double) ((Map) msg.get("props")).get("partition");
            data.add(new CommitSection(offset.longValue(), offset.longValue(), msg.get("queue").toString(), partition.intValue()));
        });

        commitData.setData(data);

        System.out.println("-------->request:" + gson.toJson(commitData));

        bodyContentSpec = webTestClient.post()
                .uri(url1)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(gson.toJson(commitData))
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody();

        System.out.println("-------->response:" + new String(bodyContentSpec.returnResult().getResponseBody()));
    }

    @Test
    void commit() {
        String url = "/v1/commit";
        CommitData message = new CommitData();
        message.setInstanceId("e9628c07-456d-4d1a-a37b-5be4a9548949");

        ArrayList<CommitSection> data = new ArrayList<>();
        long commit = 39404523;
        long returned = 39404523;
        String topic = "mq-pressure-test";
        int partition = 2;

        data.add(new CommitSection(commit, returned, topic, partition));

        data.add(new CommitSection(commit - 1, returned + 1, topic, partition));

        data.add(new CommitSection(commit - 1, commit, topic, partition));
        data.add(new CommitSection(commit - 1, returned, topic, partition));

        data.add(new CommitSection(commit, returned + 1, topic, partition));
        data.add(new CommitSection(returned, returned + 1, topic, partition));

        data.add(new CommitSection(commit - 2, commit - 1, topic, partition));
        data.add(new CommitSection(returned + 1, returned + 2, topic, partition));


        message.setData(data);

        System.out.println("-------->request:" + gson.toJson(message));

        WebTestClient.BodyContentSpec bodyContentSpec = webTestClient.post()
                .uri(url)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(gson.toJson(message))
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody();

        System.out.println("-------->response:" + new String(bodyContentSpec.returnResult().getResponseBody()));
    }
}