package io.openmessaging;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class iStorage {

    HashMap <String, iStoragePool> topicPools = new HashMap<>();

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
                String topic = dataFilename.substring(0, lastIndex1);
                long offset = Long.valueOf(dataFilename.substring(lastIndex1+1, lastIndex2));
                iStoragePool pool = topicPools.get(topic);
                if (pool == null) {
                    topicPools.put(topic, new iStoragePool(topic));
                    pool = topicPools.get(topic);
                }
                pool.appendByFile(path, offset);
            }
        }
    }

    public void append(String topic, int queueId, long offset, ByteBuffer data){
        Long ret = null;
        iStoragePool pool = topicPools.get(topic);
        if (pool == null) {
            topicPools.put(topic, new iStoragePool(topic));
            pool = topicPools.get(topic);
        }
        String key = topic + String.valueOf(queueId) + String.valueOf(offset);
        pool.append(key, data);

        return;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        iStoragePool pool = topicPools.get(topic);
        if (pool == null) {
            return ret;
        }
        for(int i = 0; i < fetchNum; i++){
            String key = topic + String.valueOf(queueId) + String.valueOf(offset + i);
            ByteBuffer buf = pool.get(key);
            if (buf == null) {
                break;
            }
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