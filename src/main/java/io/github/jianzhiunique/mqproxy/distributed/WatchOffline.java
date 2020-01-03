package io.github.jianzhiunique.mqproxy.distributed;

import io.github.jianzhiunique.mqproxy.MqproxyApplication;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import org.I0Itec.zkclient.IZkDataListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class WatchOffline {
    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private Preemption preemption;

    private final IZkDataListener noticeListener = new IZkDataListener() {
        @Override
        public void handleDataChange(String s, Object o) throws Exception {
            /**
             * if proxy schedule to stop or notice is from proxy itself
             * there's no need to resume
             */
            if (!MqproxyApplication.isStop.get() && o != null && !((Map) o).get("proxy").toString().equals(MqproxyApplication.serviceInfo)) {
                if (!MqproxyApplication.isStop.get()) {
                    preemption.start();
                    zkUtil.watchNotices(this);
                }
            }
        }

        @Override
        public void handleDataDeleted(String s) throws Exception {

        }
    };

    public void startWatch() {
        zkUtil.watchNotices(noticeListener);
    }
}
