package io.github.jianzhiunique.mqproxy.distributed;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.init.ConsumeCommit;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class WatchProxy {

    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private Preemption preemption;

    private final IZkChildListener listener = new IZkChildListener() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                if (ConsumeCommit.status == true) {
                    log.warn("---> start preemption");
                    preemption.start();
                } else {
                    log.warn("---> waiting for consume commit status end");
                    MqproxyApplication.defaultEventExecutorGroup.schedule(this, 1, TimeUnit.SECONDS);
                }
            }
        };

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            /**
             * if proxy schedule to stop
             * there's no need to resume
             */
            if (!MqproxyApplication.isStop.get()) {
                runnable.run();
            }
        }
    };

    public void startWatch() {
        zkUtil.watchProxy(listener);
    }
}
