package io.github.jianzhiunique.mqproxy.distributed;

import io.github.jianzhiunique.mqproxy.manager.KafkaConsumerStateManager;
import io.github.jianzhiunique.mqproxy.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * watch zookeeper state
 * when disconnect, session expire from zookeeper
 * check instance on this node
 * if fail over to other node, clear instance
 */
@Component
@Slf4j
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class WatchState {
    @Autowired
    KafkaConsumerStateManager kafkaConsumerStateManager;

    @Autowired
    private ZkUtil zkUtil;

    @Autowired
    private Preemption preemption;

    private final IZkStateListener listener = new IZkStateListener() {
        @Override
        public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
            // if consumers on current node fail over to other nodes
            // for example, when temporary lose connection to zookeeper
            // and then reconnect to zookeeper
            // other nodes may handle node event and resume all the consumers on this node to new nodes
            // we must close current consumer state and the consumer etc.
            if (keeperState == Watcher.Event.KeeperState.Disconnected || keeperState == Watcher.Event.KeeperState.Expired) {
                log.warn("===>" + keeperState);
                kafkaConsumerStateManager.clear();
            }

            // when reconnect to zookeeper, preemption again
            if (keeperState == Watcher.Event.KeeperState.SyncConnected) {
                log.warn("===>" + keeperState);
                preemption.start();
            }
        }

        @Override
        public void handleNewSession() throws Exception {
            log.warn("===> handleNewSession");
        }

        @Override
        public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
            log.warn("===> handleSessionEstablishmentError");
        }
    };

    public void startWatch() {
        log.info("===> startWatch state");
        zkUtil.watchState(listener);
    }
}
