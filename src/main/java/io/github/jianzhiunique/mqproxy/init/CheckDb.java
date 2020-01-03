package io.github.jianzhiunique.mqproxy.init;

import io.github.jianzhiunique.mqproxy.message.Message;
import io.github.jianzhiunique.mqproxy.sender.KafkaSender;
import io.github.jianzhiunique.mqproxy.util.LocalDbUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * when proxy start, we check the local db to find out failed messages
 * and then retry those messages
 * use stream api to avoid memory crash
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class CheckDb {

    @Autowired
    private LocalDbUtil localDbUtil;

    @Autowired
    private KafkaSender kafkaSender;

    @PostConstruct
    public void init() {
        log.info("---> checking local db size : " + localDbUtil.getMap().size());
        if (localDbUtil.getMap().size() > 0) {
            localDbUtil.getMap().keySet().stream().forEach(key -> {
                log.info("---> check db: " + localDbUtil.getMap().get(key));
                kafkaSender.send((Message) localDbUtil.getMap().get(key));
            });
        }
    }
}
