package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, Topic> topicMap = new ConcurrentHashMap<>();
    private static Logger logger = Logger.getLogger(DefaultMessageQueueImpl.class);
    private int test = 2;
    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }


    public static String bb_to_str(ByteBuffer buffer){
        byte[] bytes;
        if(buffer.hasArray()) {
            bytes = buffer.array();
        } else {
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        }
        return new String(bytes);
    }

    public static void printMap(Map<Integer, ByteBuffer> map) {
        if (map.isEmpty()) {
            System.out.println("{ }");
            return;
        }
        System.out.println("{");
        for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
            ByteBuffer buf = entry.getValue();
            String str = bb_to_str(buf);
            System.out.println("Key = " + entry.getKey() + ", Value = " + str);
        }
        System.out.println("}");
    }
    
    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        // logger.debug("append: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", data size" + String.valueOf(data.capacity()) + " }");
        Topic topic_t = topicMap.get(topic);
        if (topic_t == null) {
            topicMap.put(topic, new Topic(topic));
            topic_t = topicMap.get(topic);
        }
        long ret = topic_t.append(queueId, data);
        logger.debug("append: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", data size: " + String.valueOf(data.capacity()) + " } \n\tret: offset = " + String.valueOf(ret));
        if (test-- > 0) {
            Map<Integer, ByteBuffer> retMap = getRange(topic, queueId, ret, 100);
            ByteBuffer buf = retMap.get(0);
            int same = buf.compareTo(data);
            System.out.println("correct test: " + same);
        }
        return ret;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        // Map<Integer, ByteBuffer> ret = new HashMap<>();
        
        Topic topic_t = topicMap.get(topic);
        
        Map<Integer, ByteBuffer> ret = topic_t.getRange(queueId, offset, fetchNum);
        logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", fetchNum: " + String.valueOf(fetchNum) + " }\n\tret: " + ret.toString());
        return ret;
    }
}
