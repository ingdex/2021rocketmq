package io.openmessaging;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
// import java.util.regex.Pattern;

// import javax.sound.midi.VoiceStatus;
// import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;
// import org.apache.log4j.Logger;
// import org.apache.logging.log4j.core.layout.SyslogLayout;

public class iStorage {

    ConcurrentHashMap <String, iStoragePool> topicPools = new ConcurrentHashMap<>();
    iStoragePool pool1 = new iStoragePool("pool1");
    private LinkedBlockingQueue<AppendRequest> appendQueue = new LinkedBlockingQueue<>();
    // static AtomicInteger count = new AtomicInteger(0);
    static int count = 0;
    static ScheduledFuture<?> t;
    class AppendRequest {
        String topic;
        int queueId;
        ByteBuffer data;
        long offset;
        CompletableFuture<Integer> future;
        AppendRequest(String topic, int queueId, long offset, ByteBuffer data) {
            this.topic = topic;
            this.queueId = queueId;
            this.data = data;
            this.offset = offset;
        }
    }

    class GetRequest {

    }

    iStorage() {
        // disk benchmark
        // Integer[] fileSizes = {1, 2, 4};
        // Integer[] blockSizes = {1024, 4096};
        // runTests(fileSizes, blockSizes);
        String dir = iConfig.dataDir;
        File dirFile = new File(dir);
        File[] files = dirFile.listFiles();
        for (File file:files) {
            if (!file.isDirectory()) {
                String path = file.toString();
                if(!path.endsWith(".data")) {
                    System.out.println(path);
                    continue;
                }
                String[] pieces1 = path.split("/", 0);
                String dataFilename = pieces1[pieces1.length-1];
                int lastIndex1 = dataFilename.lastIndexOf("_");
                int lastIndex2 = dataFilename.lastIndexOf(".");
                if (lastIndex1 == -1 || lastIndex2 == -1) {
                    System.out.println("err" + path);
                    continue;
                }
                String poolName = dataFilename.substring(0, lastIndex1);
                long offset = Long.valueOf(dataFilename.substring(lastIndex1+1, lastIndex2));
                iStoragePool pool = getStoragePoolByPoolName(poolName);
                // if (pool == null) {
                //     topicPools.put(topic, new iStoragePool(topic));
                //     pool = topicPools.get(topic);
                // }
                pool.appendByFile(path, offset);
            }
        }
        init();
    }

    iStoragePool getStoragePoolByPoolName(String poolName) {
        return pool1;
    }
    
    public void init(){
        System.out.println("init backend thread");
    //在init方法中初始化一个定时任务线程，去定时执行我们的查询任务.具体的任务实现是我们根据唯一code查询出来的结果集，以code为key转成map，然后我们队列中的每个Request对象都有自己的唯一code，我们根据code一一对应，给相应的future返回对应的查询结果。
        ScheduledExecutorService poolExecutor = new ScheduledThreadPoolExecutor(1);
        t = poolExecutor.scheduleAtFixedRate(()->{
            // System.out.println("run backend thread");
            int size = appendQueue.size();
            //如果没有请求直接返回
            if(size==0) {
                // count++;
                // System.out.println(count);
                // if (count > 5) {
                //     System.out.println("shutdown");
                    
                //     t.cancel(true);
                //     poolExecutor.shutdown();
                //     return;
                // }
                return;
            }
            List<AppendRequest> list = new ArrayList<>();
            for (int i = 0; i < size;i++){
                AppendRequest request = appendQueue.poll();
                list.add(request);
            }
            // System.out.println("批量处理:"+size);
            // List<String> codes = list.stream().map(s->s.code).collect(Collectors.toList());
            //合并之后的结果集
            long startTime = System.nanoTime();
            List<Integer> batchResult = batchAppend(list);
            long endTime = System.nanoTime();
            long timeElapsed = endTime - startTime;
            // System.out.println("Execution time in nanoseconds: " + timeElapsed);

            // Map<String,Integer> responseMap = new HashMap<>();
            // for (Integer result : batchResult) {
            //     responseMap.put(result.topic, result);
            // }

            //返回对应的请求结果
            for (AppendRequest request : list) {
                // Map<String, Object> response = responseMap.get(request.code);
                
                request.future.complete(1);
            }
        },0,200000,TimeUnit.NANOSECONDS);
    }

    //这个是个模拟批量查询的方法
    public List<Integer> batchQuery(List<AppendRequest> requestList){

        return null;
    }

    public List<Integer> batchAppend(List<AppendRequest> requestList){
        ArrayList<String> keyList = new ArrayList<>();
        ArrayList<ByteBuffer> dataList = new ArrayList<>();
        for (AppendRequest request : requestList) {
            // iStoragePool pool = getStoragePoolByTopic(request.topic);
            String key = request.topic + "_" + String.valueOf(request.queueId) + "_" + String.valueOf(request.offset);
            // pool.append(key, request.data);
            keyList.add(key);
            dataList.add(request.data);
        }
        iStoragePool pool = getStoragePoolByTopic("request.topic");
        pool.append(keyList, dataList);
        return null;
    }

    iStoragePool getStoragePoolByTopic(String topic) {
        return pool1;
    }

    public void append(String topic, int queueId, long offset, ByteBuffer data){
        // iStoragePool pool = getStoragePoolByTopic(topic);
        // String key = topic + String.valueOf(queueId) + String.valueOf(offset);
        // pool.append(key, data);  
        AppendRequest appendRequest = new AppendRequest(topic, queueId, offset, data);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        appendRequest.future = future;
        appendQueue.add(appendRequest);
        try {
            future.get();
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        iStoragePool pool = getStoragePoolByTopic(topic);
        if (pool == null) {
            return ret;
        }
        ret = pool.getRange(topic, queueId, offset, fetchNum);

        return ret;

        // Map<Integer, ByteBuffer> ret = new HashMap<>();
        // iStoragePool pool = getStoragePoolByTopic(topic);
        // if (pool == null) {
        //     return ret;
        // }
        // for(int i = 0; i < fetchNum; i++){
        //     String key = topic + "_" + String.valueOf(queueId) + "_" + String.valueOf(offset);
        //     ByteBuffer buf = pool.get(key);
            
        //     if (buf == null) {
        //         break;
        //     }
        //     buf.flip();
        //     ret.put(i, buf);
        // }
        // return ret;
    }

}
