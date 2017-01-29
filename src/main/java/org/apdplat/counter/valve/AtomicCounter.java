package org.apdplat.counter.valve;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apdplat.counter.util.ConfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by ysc on 1/9/2017.
 */
public class AtomicCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtomicCounter.class);

    private static final String NO_RESPONSE_COUNT = Zookeeper.getCounterPrefix()+"/api_call_atomic_counter_zookeeper_no_response";
    private static final String WRONG_CONTENT_COUNT = Zookeeper.getCounterPrefix()+"/api_call_atomic_counter_zookeeper_wrong_content";
    private static final String RESPONSE_SUCCESS_COUNT = Zookeeper.getCounterPrefix()+"/api_call_atomic_counter_zookeeper_response_success";
    private static final String EXCEPTION_COUNT = Zookeeper.getCounterPrefix()+"/api_call_atomic_counter_zookeeper_exception";
    private static final String BEYOND_COUNT = Zookeeper.getCounterPrefix()+"/api_call_atomic_counter_zookeeper_beyond";

    private static final BlockingQueue<Counter> BLOCKING_QUEUE = new ArrayBlockingQueue<>(10000000);

    private static final RetryNTimes RETRY_N_TIMES = new RetryNTimes(10, 10);
    private static final CuratorFramework CURATOR_FRAMEWORK = Zookeeper.getCuratorFramework();
    private static final Map<String, DistributedAtomicLong> COUNTERS = new ConcurrentHashMap<>();

    private static final boolean ASYNC = ConfUtils.getBoolean("async", false);

    static {
        if(ASYNC) {
            Executors.newSingleThreadExecutor()
                    .submit(() -> {
                        while (true) {
                            try {
                                Counter counter = BLOCKING_QUEUE.take();
                                if (counter != null) {
                                    addInSync(counter.getPath(), counter.getDelta());
                                }
                            } catch (Throwable e) {
                                LOGGER.error("执行计数器出错", e);
                            }
                        }
                    });
        }
    }

    public static void noResponse(long delta, String adType){
        add(NO_RESPONSE_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType, delta);
    }

    public static void wrongContent(long delta, String adType){
        add(WRONG_CONTENT_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType, delta);
    }

    public static void responseSuccess(long delta, String adType){
        responseSuccess(delta, adType, null, null);
    }

    public static void responseSuccess(long delta, String adType, Integer productId, Integer tvId){
        add(RESPONSE_SUCCESS_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType, delta);
        if(productId != null){
            add(RESPONSE_SUCCESS_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType+"_p_"+productId, delta);
        }
        if(tvId != null){
            add(RESPONSE_SUCCESS_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType+"_t_"+tvId, delta);
        }
    }

    public static void exception(long delta, String adType){
        add(EXCEPTION_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType, delta);
    }

    public static void beyond(long delta, String adType){
        add(BEYOND_COUNT+"_"+new SimpleDateFormat("yyyyMMdd").format(new Date())+"_"+adType, delta);
    }

    public static long getNoResponseCount(String adType){
        return getNoResponseCount(new SimpleDateFormat("yyyyMMdd").format(new Date()), adType);
    }

    public static long getNoResponseCount(String day, String adType){
        return getValue(NO_RESPONSE_COUNT+"_"+day+"_"+adType);
    }

    public static long getWrongContentCount(String adType){
        return getWrongContentCount(new SimpleDateFormat("yyyyMMdd").format(new Date()), adType);
    }

    public static long getWrongContentCount(String day, String adType){
        return getValue(WRONG_CONTENT_COUNT+"_"+day+"_"+adType);
    }

    public static long getResponseSuccessCount(String adType){
        return getResponseSuccessCount(new SimpleDateFormat("yyyyMMdd").format(new Date()), adType);
    }

    public static long getResponseSuccessCount(String day, String adType){
        return getValue(RESPONSE_SUCCESS_COUNT+"_"+day+"_"+adType);
    }

    public static long getResponseSuccessCountForProduct(String day, String adType, Integer productId){
        return getValue(RESPONSE_SUCCESS_COUNT+"_"+day+"_"+adType+"_p_"+productId);
    }

    public static long getResponseSuccessCountForTv(String day, String adType, Integer tvId){
        return getValue(RESPONSE_SUCCESS_COUNT+"_"+day+"_"+adType+"_t_"+tvId);
    }

    public static long getExceptionCount(String adType){
        return getExceptionCount(new SimpleDateFormat("yyyyMMdd").format(new Date()), adType);
    }

    public static long getExceptionCount(String day, String adType){
        return getValue(EXCEPTION_COUNT+"_"+day+"_"+adType);
    }

    public static long getBeyondCount(String adType){
        return getBeyondCount(new SimpleDateFormat("yyyyMMdd").format(new Date()), adType);
    }

    public static long getBeyondCount(String day, String adType){
        return getValue(BEYOND_COUNT+"_"+day+"_"+adType);
    }

    private static void add(String path, long delta){
        if(ASYNC){
            addInAsync(path, delta);
        }else{
            addInSync(path, delta);
        }
    }

    private static void addInAsync(String path, long delta){
        try{
            BLOCKING_QUEUE.put(new Counter(path, delta));
        }catch (Exception e){
            LOGGER.error("将计数器加入阻塞队列出错", e);
        }
    }

    private static void addInSync(String path, long delta){
        try {
            ZKPaths.mkdirs(CURATOR_FRAMEWORK.getZookeeperClient().getZooKeeper(), path);
            COUNTERS.putIfAbsent(path, new DistributedAtomicLong(CURATOR_FRAMEWORK, path, RETRY_N_TIMES));
            DistributedAtomicLong counter = COUNTERS.get(path);
            AtomicValue<Long> returnValue = counter.add(delta);
            while (!returnValue.succeeded()) {
                returnValue = counter.add(delta);
            }
        }catch (Exception e){
            LOGGER.error("addInSync "+delta+" failed for "+path, e);
        }
    }

    private static void subtract(String path, long delta){
        try {
            ZKPaths.mkdirs(CURATOR_FRAMEWORK.getZookeeperClient().getZooKeeper(), path);
            COUNTERS.putIfAbsent(path, new DistributedAtomicLong(CURATOR_FRAMEWORK, path, RETRY_N_TIMES));
            DistributedAtomicLong counter = COUNTERS.get(path);
            AtomicValue<Long> returnValue = counter.subtract(delta);
            while (!returnValue.succeeded()) {
                returnValue = counter.subtract(delta);
            }
        }catch (Exception e){
            LOGGER.error("subtract "+delta+" failed for "+path, e);
        }
    }

    public static long getValue(String path) {
        try {
            DistributedAtomicLong dal = new DistributedAtomicLong(CURATOR_FRAMEWORK, path, RETRY_N_TIMES);
            return dal.get().postValue();
        }catch (Exception e){
            LOGGER.error("get counter exception: "+path, e);
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        String apiType = "1";

        System.err.println("---------------------------------------------------------------");
        System.err.println("NoResponseCount: "+ AtomicCounter.getNoResponseCount(apiType));
        System.err.println("WrongContentCount: "+ AtomicCounter.getWrongContentCount(apiType));
        System.err.println("ExceptionCount: "+ AtomicCounter.getExceptionCount(apiType));
        System.err.println("ResponseSuccessCount: "+ AtomicCounter.getResponseSuccessCount(apiType));
        System.err.println("BeyondCount: "+ AtomicCounter.getBeyondCount(apiType));

        int thread = 10;
        CountDownLatch countDownLatch = new CountDownLatch(thread);
        for(int i=0; i<thread; i++){
            new Thread(()->{
                for(int j=0; j<100; j++){
                    AtomicCounter.responseSuccess(1, apiType);
                }
                countDownLatch.countDown();
            })
            .start();
        }
        countDownLatch.await();
        System.err.println("---------------------------------------------------------------");
        System.err.println("NoResponseCount: "+ AtomicCounter.getNoResponseCount(apiType));
        System.err.println("WrongContentCount: "+ AtomicCounter.getWrongContentCount(apiType));
        System.err.println("ExceptionCount: "+ AtomicCounter.getExceptionCount(apiType));
        System.err.println("ResponseSuccessCount: "+ AtomicCounter.getResponseSuccessCount(apiType));
        System.err.println("BeyondCount: "+ AtomicCounter.getBeyondCount(apiType));
        System.err.println("---------------------------------------------------------------");
    }
}
