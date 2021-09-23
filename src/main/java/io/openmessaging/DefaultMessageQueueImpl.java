package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.Logger;
/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, Map<Integer, Long>> appendOffset = new ConcurrentHashMap<>();
    // ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();
    iStorage storage = new iStorage();
    Logger logger = Logger.getLogger(DefaultMessageQueueImpl.class);
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
        Map<Integer, Long> topicOffset = getOrPutDefault(appendOffset, topic, new HashMap<>());
        long offset = topicOffset.getOrDefault(queueId, 0L);
        // 更新最大位点
        topicOffset.put(queueId, offset+1);
        logger.debug("append: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", fetchNum: ");
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
        logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset: " + String.valueOf(offset) + ", fetchNum: " + String.valueOf(fetchNum) + " }\n\tret: " + ret.toString());
        return ret;
    }
}
