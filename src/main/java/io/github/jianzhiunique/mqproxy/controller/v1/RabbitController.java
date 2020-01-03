package io.github.jianzhiunique.mqproxy.controller.v1;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.fetcher.RabbitFetcher;
import io.github.jianzhiunique.mqproxy.helper.FetchData;
import io.github.jianzhiunique.mqproxy.helper.ReturnData;
import io.github.jianzhiunique.mqproxy.message.BatchMessages;
import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.RabbitSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
@Slf4j
@RequestMapping("/v1/rabbit")
@ConditionalOnProperty(value = "proxy.config.rabbitmq", havingValue = "true")
public class RabbitController {
    private Gson gson = new Gson();

    @Autowired
    private RabbitSender rabbitSender;

    @Autowired
    private RabbitFetcher rabbitFetcher;

    @RequestMapping("/send")
    public Mono<String> send(@RequestBody Message message) {
        if (message.getExtra() != null && message.getExtra().containsKey("mq_type") && message.getExtra().get("mq_type").equals("rabbitmq")) {
            try {
                rabbitSender.send(message);
            } catch (Exception e) {
                return Mono.just(gson.toJson(new ReturnData(500, e.getMessage(), "send failed")));
            }
        } else {
            return Mono.just(gson.toJson(new ReturnData(400, null, "this uri only support rabbitmq")));
        }

        return Mono.just(gson.toJson(new ReturnData(0, null, "send success")));
    }

    @RequestMapping("/send_batch")
    public Mono<String> sendBatch(@RequestBody BatchMessages messages) {
        ArrayList<Message> failed = new ArrayList<>();
        AtomicBoolean hasFailed = new AtomicBoolean(false);
        messages.getMessages().forEach(message -> {
            if (message.getExtra() != null && message.getExtra().containsKey("mq_type") && message.getExtra().get("mq_type").equals("rabbitmq")) {
                try {
                    rabbitSender.send(message);
                } catch (Exception e) {
                    hasFailed.set(true);
                }
            } else {
                hasFailed.set(true);
            }
        });

        if (hasFailed.get()) {
            return Mono.just(gson.toJson(new ReturnData(500, failed, "send failed")));
        }

        return Mono.just(gson.toJson(new ReturnData(0, null, "send success")));
    }

    @RequestMapping("/fetch")
    public Mono<String> fetch(@RequestBody FetchData fetchData) {
        if (fetchData.getReset().equals("rabbitmq")) {
            try {
                List<Message> messages = rabbitFetcher.get(fetchData);
                return Mono.just(gson.toJson(new ReturnData(0, messages, "fetch success")));
            } catch (Exception e) {
                return Mono.just(gson.toJson(new ReturnData(0, e.getMessage(), "fetch failed")));
            }

        } else {
            return Mono.just(gson.toJson(new ReturnData(400, null, "this uri only support rabbitmq")));
        }

    }
}
