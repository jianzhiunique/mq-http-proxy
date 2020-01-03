package io.github.jianzhiunique.mqproxy.util;

import io.github.jianzhiunique.mqproxy.config.LocalDbConfig;
import lombok.Getter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.ConcurrentMap;

@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class LocalDbUtil {

    @Autowired
    private LocalDbConfig localDbConfig;

    private DB db;

    @Getter
    private ConcurrentMap map;

    @PostConstruct
    public void init() {
        db = DBMaker
                .fileDB(localDbConfig.getConfig().get("path") + localDbConfig.getConfig().get("producer"))
                .checksumHeaderBypass()
                .fileMmapEnable()
                .make();

        map = db.hashMap(localDbConfig.getConfig().get("name")).createOrOpen();
    }
}
