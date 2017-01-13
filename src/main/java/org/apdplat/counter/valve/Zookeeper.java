package org.apdplat.counter.valve;

import org.apdplat.counter.util.ConfUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ysc on 1/9/2017.
 */
public class Zookeeper {
    private static final Logger LOGGER = LoggerFactory.getLogger(Zookeeper.class);

    private static final String API_CALL_COUNT_PREFIX = ConfUtils.get("api.call.count.prefix");
    private static final String COUNTER_PREFIX = ConfUtils.get("atomic.counter.zookeeper.prefix");

    private static final String ZK = ConfUtils.get("atomic.counter.zookeeper.connect");

    private static final int TIMEOUT = ConfUtils.getInt("zookeeper.connection.timeout.ms");

    private static CuratorFramework curatorFramework = null;

    static {
        try {
            LOGGER.info("开始初始化ZOOKEEPER: {}", ZK);
            curatorFramework = CuratorFrameworkFactory.newClient(ZK, TIMEOUT, TIMEOUT, new ExponentialBackoffRetry(1000, 300));
            curatorFramework.start();
            LOGGER.info("初始化ZOOKEEPER完成: {}", ZK);
        }catch (Exception e){
            LOGGER.error("初始化ZOOKEEPER失败: "+ZK, e);
        }
    }

    public static String getApiCallCountPrefix() {
        return API_CALL_COUNT_PREFIX;
    }

    public static String getCounterPrefix() {
        return COUNTER_PREFIX;
    }

    public static CuratorFramework getCuratorFramework() {
        return curatorFramework;
    }
}
