package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import java.util.concurrent.atomic.AtomicLong;
/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, Map<Integer, Long>> appendOffset = new ConcurrentHashMap<>();
    // private AtomicLong appendId = new AtomicLong(0L);

    // ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();
    iStorage storage = new iStorage();
    // Logger logger = Logger.getLogger(DefaultMessageQueueImpl.class);

    // DefaultMessageQueueImpl() {
    //     Integer[] fileSizes = {1, 2, 4};
    //     Integer[] blockSizes = {1024, 4096};
    //     runTests(fileSizes, blockSizes);
    // }

    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        // 获取该 topic-queueId 下的最大位点 offset
        // long x = appendId.getAndIncrement();
        Map<Integer, Long> topicOffset = getOrPutDefault(appendOffset, topic, new HashMap<>());
        long offset = topicOffset.getOrDefault(queueId, 0L);
        // 更新最大位点
        topicOffset.put(queueId, offset+1);
        // if (queueId == 1527 && (topic.equals("topic31"))){
        //     logger.debug("append: topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", datasize: " + String.valueOf(data.remaining()));
        //     logger.debug(data);
        //     for(int i=0; i<data.limit()-1; i++) {
        //         System.out.print(data.getChar(i));
        //     }
        //     System.out.print('\n');
        // } 
        // logger.debug("append: topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", datasize: " + String.valueOf(data.remaining()));
        // logger.debug("append: topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", datasize: " + String.valueOf(data.remaining()));
        storage.append(topic, queueId, offset, data);
        
        // Map<Integer, Map<Long, ByteBuffer>> map1 = getOrPutDefault(appendData, topic, new HashMap<>());
        // Map<Long, ByteBuffer> map2 = getOrPutDefault(map1, queueId, new HashMap<>());
        // // 保存 data 中的数据
        // ByteBuffer buf = ByteBuffer.allocate(data.remaining());
        // buf.put(data);
        // map2.put(offset, buf);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = storage.getRange(topic, queueId, offset, fetchNum);
        // if (queueId == 1527 && topic.equals("topic31")) {
        //     logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", fetchNum: " + String.valueOf(fetchNum) + " }");
        //     for (Map.Entry<Integer, ByteBuffer> entry : ret.entrySet()) {
        //         ByteBuffer buf = entry.getValue();
        //         logger.debug("Key = " + entry.getKey() + ", DataSize = " + String.valueOf(buf.remaining()) + "\n" + buf);
        //         for(int i=0; i<buf.limit()-1; i++) {
        //             System.out.print(buf.getChar(i));
        //         }
        //         System.out.print('\n');
        //     }
        // }
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", fetchNum: " + String.valueOf(fetchNum) + " }\n\tret: " + ret.toString());
        return ret;
    }

    public static void printMap(Map<Integer, ByteBuffer> map) {
                if (map.isEmpty()) {
            System.out.println("{ }");
            return;
        }
        System.out.println("{");
        for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
            ByteBuffer buf = entry.getValue();
            System.out.println("Key = " + entry.getKey() + ", DataSize = " + buf.capacity());
        }
        System.out.println("}");
    }


    private static String resultList = "";

    private static void runTests(Integer[] fileSizes, Integer[] blockSizes) {
        System.out.println("If you stop the program whilst running you may need to delete a 'DiskBenchFile' file.");
        System.out.println("Mode - Size - BlockSize - Duration - Speed");
        for (Integer fileSize : fileSizes) {
            for (Integer blockSize : blockSizes) {
                DiskBenchmarker diskBenchmarker = new DiskBenchmarker(fileSize, blockSize);

                long t0 = System.currentTimeMillis();
                diskBenchmarker.writeTest();
                long t1 = System.currentTimeMillis();
                System.out.println(resultPrinter(t0, t1, fileSize, blockSize, "Seq Write"));

                t0 = System.currentTimeMillis();
                diskBenchmarker.readTest();
                t1 = System.currentTimeMillis();
                System.out.println(resultPrinter(t0, t1, fileSize, blockSize, "Seq Read"));

                diskBenchmarker.deleteFile();
            }
            
        }
        System.out.println("Completed!");
        System.out.println(resultList);
    }

    private static String resultPrinter(long t0, long t1, int fileSize, int blockSize, String mode) {
        double duration = (t1 - t0) / 1000d; // in seconds.
        double speedInMBs = (1024 * fileSize) / duration;
        double speedRounded = (double) Math.round(speedInMBs * 1000d) / 1000d;
        String output = (mode + " - " + fileSize + " GB - " + blockSize + " Bytes - " + duration + " s  - " + speedRounded + " MB/s");
        resultList += output + "\n";
        return output;
    }
}
