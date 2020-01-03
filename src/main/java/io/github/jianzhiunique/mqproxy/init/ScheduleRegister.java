package io.github.jianzhiunique.mqproxy.init;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.config.ProxyConfig;
import io.github.jianzhiunique.mqproxy.distributed.Register;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * start the register to zookeeper task after proxy started
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class ScheduleRegister {

    @Autowired
    private Register register;

    @Autowired
    private ProxyConfig proxyConfig;

    @PostConstruct
    public void start() {
        MqproxyApplication.defaultEventExecutorGroup.scheduleAtFixedRate(register, 0, Long.parseLong(proxyConfig.getConfig().get("registerInterval")), TimeUnit.SECONDS);
    }
}
