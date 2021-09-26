package io.openmessaging;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.concurrent.*;
import org.apache.log4j.Logger;

public class iStorage {

    ConcurrentHashMap <String, iStoragePool> topicPools = new ConcurrentHashMap<>();
    iStoragePool pool1 = new iStoragePool("pool1");
    private LinkedBlockingQueue<AppendRequest> appendQueue = new LinkedBlockingQueue<>();
    
    class AppendRequest {
        String topic;
        int queueId;
        ByteBuffer data;
        CompletableFuture<Integer> future;
    }

    class GetRequest {

    }

    iStorage() {
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
    }

    iStoragePool getStoragePoolByPoolName(String poolName) {
        return pool1;
    }

    public void init(){
    //在init方法中初始化一个定时任务线程，去定时执行我们的查询任务.具体的任务实现是我们根据唯一code查询出来的结果集，以code为key转成map，然后我们队列中的每个Request对象都有自己的唯一code，我们根据code一一对应，给相应的future返回对应的查询结果。
        ScheduledExecutorService poolExecutor = new ScheduledThreadPoolExecutor(1);
        poolExecutor.scheduleAtFixedRate(()->{
            int size = appendQueue.size();
            //如果没有请求直接返回
            if(size==0)
                return ;
            List<AppendRequest> list = new ArrayList<>();
            for (int i = 0; i < size;i++){
                AppendRequest request = appendQueue.poll();
                list.add(request);
            }
            System.out.println("批量处理:"+size);
            // List<String> codes = list.stream().map(s->s.code).collect(Collectors.toList());
            //合并之后的结果集
            List<Integer> batchResult = batchAppend(list);

            // Map<String,Integer> responseMap = new HashMap<>();
            // for (Integer result : batchResult) {
            //     responseMap.put(result.topic, result);
            // }

            //返回对应的请求结果
            for (AppendRequest request : list) {
                // Map<String, Object> response = responseMap.get(request.code);
                request.future.complete(1);
            }
        },0,10,TimeUnit.MILLISECONDS);
    }

    //这个是个模拟批量查询的方法
    public List<Integer> batchQuery(List<AppendRequest> requestList){

        return null;
    }

    public List<Integer> batchAppend(List<AppendRequest> requestList){
        return null;
    }
    
    // public Integer append(String topic, int queueId, long offset, ByteBuffer data){
    //     iStoragePool pool = topicPools.get(topic);
    //     String key = topic + String.valueOf(queueId) + String.valueOf(offset);
    //     pool.append(key, data);

    //     return 1;
    // }

    iStoragePool getStoragePoolByTopic(String topic) {
        return pool1;
    }

    public void append(String topic, int queueId, long offset, ByteBuffer data){
        iStoragePool pool = getStoragePoolByTopic(topic);
        String key = topic + String.valueOf(queueId) + String.valueOf(offset);
        pool.append(key, data);

        return;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        iStoragePool pool = getStoragePoolByTopic(topic);
        if (pool == null) {
            return ret;
        }
        for(int i = 0; i < fetchNum; i++){
            String key = topic + String.valueOf(queueId) + String.valueOf(offset + i);
            ByteBuffer buf = pool.get(key);
            
            if (buf == null) {
                break;
            }
            buf.flip();
            ret.put(i, buf);
        }

        return ret;
    }
}

// public class Storage {

//     StoragePool pool = new StoragePool("single");

//     // Storage() {
//     //     String dir = Config.dataDir;
//     //     File dirFile = new File(dir);
//     //     File[] files = dirFile.listFiles();
//     //     for (File file:files) {
//     //         if (!file.isDirectory()) {
//     //             String path = file.toString();
//     //             if(!path.endsWith(".data")) {
//     //                 System.out.println(path);
//     //                 continue;
//     //             }
//     //             String[] pieces1 = path.split("/", 0);
//     //             String dataFilename = pieces1[pieces1.length-1];
//     //             int lastIndex1 = dataFilename.lastIndexOf("_");
//     //             int lastIndex2 = dataFilename.lastIndexOf(".");
//     //             if (lastIndex1 == -1 || lastIndex2 == -1) {
//     //                 System.out.println("err" + path);
//     //                 continue;
//     //             }
//     //             String topic = dataFilename.substring(0, lastIndex1);
//     //             long offset = Long.valueOf(dataFilename.substring(lastIndex1+1, lastIndex2));
//     //             StoragePool pool = topicPools.get(topic);
//     //             if (pool == null) {
//     //                 topicPools.put(topic, new StoragePool(topic));
//     //                 pool = topicPools.get(topic);
//     //             }
//     //             pool.appendByFile(path, offset);
//     //         }
//     //     }
//     // }

//     public void append(String topic, int queueId, long offset, ByteBuffer data){
//         String key = topic + String.valueOf(queueId) + String.valueOf(offset);
//         pool.append(key, data);

//         return;
//     }

//     public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
//         // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
//         Map<Integer, ByteBuffer> ret = new HashMap<>();
//         // StoragePool pool = topicPools.get(topic);
//         // if (pool == null) {
//         //     return ret;
//         // }
//         for(int i = 0; i < fetchNum; i++){
//             String key = topic + String.valueOf(queueId) + String.valueOf(offset + i);
//             ByteBuffer buf = pool.get(key);
//             if (buf == null) {
//                 break;
//             }
//             ret.put(i, buf);
//         }

//         return ret;
//     }
// }