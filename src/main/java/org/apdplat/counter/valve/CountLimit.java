package org.apdplat.counter.valve;

import org.apdplat.counter.util.ConfUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by ysc on 1/9/2017.
 */
public class CountLimit {
    private static final Logger LOGGER = LoggerFactory.getLogger(CountLimit.class);

    private static final String MAX_API_CALL_COUNT_LIMIT = Zookeeper.getApiCallCountPrefix()+"/limit";

    private static final CuratorFramework CURATOR_FRAMEWORK = Zookeeper.getCuratorFramework();

    private static Map<String, Long> limits = new ConcurrentHashMap<>();

    public static List<Map.Entry<String, Long>> getLimits() {
        return Collections.unmodifiableMap(limits)
                .entrySet()
                .stream()
                .sorted((a,b)->b.getValue().compareTo(a.getValue()))
                .collect(Collectors.toList());
    }

    public static Long getLimit(String apiType) {
        Long limit = limits.get(apiType);
        if(limit == null){
            limit = Long.MAX_VALUE;
        }
        return limit;
    }

    public static boolean setLimit(String apiType, long newLimit){
        try{
            LOGGER.info("修改最大限制值, apiType: {}, 现有最大限制值为: {}, 修改为: {}", apiType, getLimit(apiType), newLimit);
            String path = MAX_API_CALL_COUNT_LIMIT + "_" + apiType;
            ZKPaths.mkdirs(CURATOR_FRAMEWORK.getZookeeperClient().getZooKeeper(), path);
            CURATOR_FRAMEWORK.setData().forPath(path, String.valueOf(newLimit).getBytes());
            LOGGER.info("成功为apiType: {} 设置最大限制值: {}", apiType, newLimit);
            return true;
        }catch (Exception e){
            LOGGER.error("为apiType: "+apiType+" 指定的最大限制值: "+newLimit+" 非法", e);
        }
        return false;
    }

    static {
        try {
            String[] attrs = ConfUtils.get("api.call.count.limit").split(",");
            for(String attr : attrs){
                String[] field = attr.split(":");
                if(field != null && field.length == 2){
                    String apiType = field[0];
                    String limit = field[1];
                    if(StringUtils.isNotBlank(apiType) && StringUtils.isNumeric(limit)) {
                        String path = MAX_API_CALL_COUNT_LIMIT + "_" + apiType;
                        ZKPaths.mkdirs(CURATOR_FRAMEWORK.getZookeeperClient().getZooKeeper(), path);
                        String oldLimit = new String(CURATOR_FRAMEWORK.getData().forPath(path));
                        if (StringUtils.isBlank(oldLimit)) {
                            CURATOR_FRAMEWORK.setData().forPath(path, limit.getBytes());
                            LOGGER.info("成功为apiType: {} 设置初始最大限制值: {}", apiType, new String(CURATOR_FRAMEWORK.getData().forPath(path)));
                            limits.put(apiType, Long.parseLong(limit));
                        } else {
                            LOGGER.info("apiType: {} 旧的最大限制值: {}", apiType, oldLimit);
                            limits.put(apiType, Long.parseLong(oldLimit));
                        }
                    }
                    watch(apiType);
                }
            }
        } catch (Exception e) {
            LOGGER.error("监听百度MSSP广告最大限制值失败", e);
        }
    }

    private static void watch(String apiType){
        try {
            String path = MAX_API_CALL_COUNT_LIMIT + "_" + apiType;
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        String newLimitValue = null;
                        try {
                            newLimitValue = new String(CURATOR_FRAMEWORK.getData().forPath(path));
                            limits.put(apiType, Long.parseLong(newLimitValue));
                            LOGGER.info("最大限制值发生变化, apiType: {}, 成功获取到新的最大值: {}", apiType, newLimitValue);
                        } catch (Exception e) {
                            LOGGER.error("apiType: " + apiType + " 的新的最大限制值: " + newLimitValue + " 非法", e);
                        }
                        try {
                            CURATOR_FRAMEWORK.checkExists().usingWatcher(this).forPath(path);
                        } catch (Exception e) {
                            LOGGER.error("重新注册watcher错误, apiType: " + apiType + ", newLimitValue: " + newLimitValue, e);
                        }
                    }
                }
            };
            CURATOR_FRAMEWORK.checkExists().usingWatcher(watcher).forPath(path);
        }catch (Exception e){
            LOGGER.error("监听最大值失败, apiType: "+apiType, e);
        }
    }

    public static void main(String[] args) throws Exception{
        String apiType = "1";
        CountLimit.setLimit(apiType, new Random().nextInt(100000)+1000000);
        Thread.sleep(10000);
        CountLimit.setLimit(apiType, new Random().nextInt(100000)+1000000);
        Thread.sleep(10000);
        CountLimit.setLimit(apiType, new Random().nextInt(100000)+1000000);
        Thread.sleep(10000);
        CountLimit.setLimit(apiType, new Random().nextInt(100000)+1000000);
        Thread.sleep(10000);
    }
}
