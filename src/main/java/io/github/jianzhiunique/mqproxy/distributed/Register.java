package io.github.jianzhiunique.mqproxy.distributed;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * register to zookeeper
 */

@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class Register implements Runnable {
    @Autowired
    private ZkUtil zkUtil;

    @Override
    public void run() {
        if (MqproxyApplication.serviceInfo == null || MqproxyApplication.serviceInfo.equals("")) {
            return;
        }

        HashMap<String, Object> proxy = new HashMap<>();
        proxy.put("name", MqproxyApplication.serviceInfo);
        proxy.put("addr", MqproxyApplication.serviceUrl);

        try {
            zkUtil.createProxy(MqproxyApplication.serviceInfo, proxy);
        } catch (ZkNodeExistsException e) {
            // TODO we don't need update every time if there is no more proxy properties
            zkUtil.updateProxy(MqproxyApplication.serviceInfo, proxy);
        }

        log.info("---> register to zookeeper " + proxy);
    }
}
