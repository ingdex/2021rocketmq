package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.io.File;


public class Topic {
    String topicId;
    // queueId用Integer表示
    Map<Integer, MyQueue> queueMap = new HashMap<>();
    
    // Map<Integer, Long> appendOffset;
    Topic(String topicId) {
        this.topicId = topicId;
    }
    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

    public long append(int queueId, ByteBuffer data){
        // 获取该 queueId 下的最大位点 offset
        MyQueue que = queueMap.get(queueId);
        // Queue que = queueMap.getOrPutDefault(queueMap, Integer.valueOf(queueId), new Query());
        if (que == null) {
            queueMap.put(queueId, new MyQueue(null, "/essd/" + topicId + String.valueOf(queueId)));
            que = queueMap.get(queueId);
        }
        
        return que.append(data);
        // 更新最大位点
        // queueOffset.put(queueId, offset+1);
        // return 0;
    }

    public Map<Integer, ByteBuffer> getRange(int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        MyQueue que = queueMap.get(queueId);
        if (que == null) {
            return ret;
        }
        
        return que.getRange(offset, fetchNum);
        // return ret;
    }
}
