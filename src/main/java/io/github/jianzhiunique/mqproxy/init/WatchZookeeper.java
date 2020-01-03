package io.github.jianzhiunique.mqproxy.init;

import io.github.jianzhiunique.mqproxy.distributed.WatchOffline;
import io.github.jianzhiunique.mqproxy.distributed.WatchProxy;
import io.github.jianzhiunique.mqproxy.distributed.WatchState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * start watch zookeeper
 * 1. watch proxy change
 * 2. watch zookeeper state
 * 3. watch offline notice
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class WatchZookeeper {

    @Autowired
    private WatchProxy watchProxy;

    @Autowired
    private WatchState watchState;

    @Autowired
    private WatchOffline watchOffline;

    @PostConstruct
    public void init() {
        watchProxy.startWatch();
        watchOffline.startWatch();
        watchState.startWatch();
    }
}
