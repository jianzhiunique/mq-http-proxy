package io.github.jianzhiunique.mqproxy.util;

import com.google.gson.Gson;
import io.github.jianzhiunique.mqproxy.config.ZookeeperConfig;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * zk operate
 */
@Component
@ConditionalOnProperty(value = "proxy.config.kafka", havingValue = "true")
public class ZkUtil {
    @Autowired
    private ZookeeperConfig config;

    private final Gson json = new Gson();
    private final ZkSerializer serializer = new ZkSerializer() {
        @Override
        public byte[] serialize(Object o) throws ZkMarshallingError {
            return json.toJson(o).getBytes();
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {

            return json.fromJson(new String(bytes), HashMap.class);
        }
    };
    private String servers;
    private String rootPath;
    private int sessionTimeout;
    private int connectionTimeout;

    private final String proxys = "/proxys";
    private final String instances = "/instances";
    private final String notices = "/notices";
    private final String locks = "/locks";

    private ZkClient zkClient;

    @PostConstruct
    public void init() {
        servers = config.getConfig().get("url");
        rootPath = config.getConfig().get("rootPath");
        sessionTimeout = Integer.parseInt(config.getConfig().get("sessionTimeout"));
        connectionTimeout = Integer.parseInt(config.getConfig().get("connectionTimeout"));

        zkClient = new ZkClient(servers, sessionTimeout, connectionTimeout, serializer);

        ensureRoot();
    }

    private void ensureRoot() {
        try {
            if (!zkClient.exists(rootPath)) {
                zkClient.createPersistent(rootPath, true);
            }
            if (!zkClient.exists(rootPath + proxys)) {
                zkClient.createPersistent(rootPath + proxys, true);
            }

            if (!zkClient.exists(rootPath + instances)) {
                zkClient.createPersistent(rootPath + instances, true);
            }
            if (!zkClient.exists(rootPath + notices)) {
                zkClient.createPersistent(rootPath + notices, true);
            }
            if (!zkClient.exists(rootPath + locks)) {
                zkClient.createPersistent(rootPath + locks, true);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createProxy(String name, Object data) {
        zkClient.createEphemeral(rootPath + proxys + "/" + name, data);
    }

    public void createInstance(String name, Object data) {
        zkClient.createPersistent(rootPath + instances + "/" + name, data);
    }

    public void updateProxy(String name, Object data) {
        zkClient.writeData(rootPath + proxys + "/" + name, data);
    }

    public void updateInstance(String name, Object data) {
        zkClient.writeData(rootPath + instances + "/" + name, data);
    }

    public void updateNotices(Object data) {
        zkClient.writeData(rootPath + notices, data);
    }

    // get list
    public List<String> getInstances() {
        return zkClient.getChildren(rootPath + instances);
    }

    public List<String> getProxys() {
        return zkClient.getChildren(rootPath + proxys);
    }

    // get info
    public Map<String, String> getInstanceInfo(String name) {
        return (Map<String, String>) zkClient.readData(rootPath + instances + "/" + name);
    }

    public Map<String, String> getProxyInfo(String name) {
        return (Map<String, String>) zkClient.readData(rootPath + proxys + "/" + name);
    }


    // for consumer proxy lock and then resume
    public boolean lockInstance(String instanceId) {
        try {
            zkClient.create(rootPath + instances + "/" + instanceId + "/lock", "", CreateMode.EPHEMERAL);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void unlockInstance(String instanceId) {
        zkClient.delete(rootPath + instances + "/" + instanceId + "/lock");
    }

    public boolean isInstanceLocked(String instanceId) {
        return zkClient.exists(rootPath + instances + "/" + instanceId + "/lock");
    }

    // for watch
    public void watchProxy(IZkChildListener listener) {
        zkClient.subscribeChildChanges(rootPath + proxys, listener);
    }

    // for watch auto offline notices
    public void watchNotices(IZkDataListener listener) {
        zkClient.subscribeDataChanges(rootPath + notices, listener);
    }

    // for watch zk state change
    public void watchState(IZkStateListener listener) {
        zkClient.subscribeStateChanges(listener);
    }
}
