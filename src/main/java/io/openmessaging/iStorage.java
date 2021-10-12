package io.openmessaging;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import org.apache.log4j.Logger;
// import org.apache.logging.log4j.core.layout.SyslogLayout;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
public class iStorage {

    ConcurrentHashMap <String, iStoragePool> topicPools = new ConcurrentHashMap<>();
    final int poolNum = 1;
    final int appendQueueNum = 2;
    ArrayList<iStoragePool> poolList = new ArrayList<>();
    // iStoragePool pool1 = new iStoragePool("pool1");
    // iStoragePool pool2 = new iStoragePool("pool2");
    // iStoragePool pool1 = new iStoragePool("pool1");
    // iStoragePool pool1 = new iStoragePool("pool1");
    private LinkedBlockingQueue<AppendRequest> appendQueueRead = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<AppendRequest> appendQueueWrite = new LinkedBlockingQueue<>();
    
    // ArrayList<LinkedBlockingQueue<AppendRequest>> appendQueueList = new ArrayList<>();
    // static AtomicInteger count = new AtomicInteger(0);
    static int count = 0;
    static ScheduledFuture<?> t;
    // Logger logger = Logger.getLogger(iStorage.class);

    List<AppendRequest> appendList = new ArrayList<>();
    private final Lock lock = new ReentrantLock();
    //表示生产者线程
    private final Condition notFull = lock.newCondition();
    //表示消费者线程
    private final Condition notEmpty = lock.newCondition();
    private final Condition appendThread = lock.newCondition();
    class AppendRequest {
        String topic;
        int queueId;
        ByteBuffer data;
        long offset;
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

        for (int i=0; i<poolNum; i++) {
            String poolName = "pool" + i;
            poolList.add(new iStoragePool(poolName));
        }

        // for (int i=0; i<appendQueueNum; i++) {
        //     appendQueueList.add(new LinkedBlockingQueue<>());
        // }

        String dir = iConfig.dataDir;
        File dirFile = new File(dir);
        File[] files = dirFile.listFiles();
        for (File file:files) {
            if (!file.isDirectory()) {
                String path = file.toString();
                if(!path.endsWith(".data")) {
                    // System.out.println(path);
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
                if (pool == null) {
                    System.out.println("err pool name");
                    break;
                }
                pool.appendByFile(path, offset);
            }
        }
        // init();
    }

    iStoragePool getStoragePoolByPoolName(String poolName) {
        for (int i=0; i<poolList.size(); i++) {
            iStoragePool pool = poolList.get(i);
            if (pool.poolName.equals(poolName)) {
                return pool;
            }
        }
        return null;
    }
    
    public void init(){
        System.out.println("init backend thread");
        int[] sizeList = new int[appendQueueNum];
        
    //在init方法中初始化一个定时任务线程，去定时执行我们的查询任务.具体的任务实现是我们根据唯一code查询出来的结果集，以code为key转成map，然后我们队列中的每个Request对象都有自己的唯一code，我们根据code一一对应，给相应的future返回对应的查询结果。
        ScheduledExecutorService poolExecutor = new ScheduledThreadPoolExecutor(1);
        t = poolExecutor.scheduleAtFixedRate(()->{
            // System.out.println("run backend thread");
            lock.lock();
            int size = appendList.size();
            // ArrayList<Integer> sizeList = new ArrayList<>();
            // boolean allEmpty = true;
            // for (int i=0; i<appendQueueNum; i++) {
            //     sizeList[i] = appendQueueList.get(i).size();
            //     if (sizeList[i] != 0) {
            //         allEmpty = false;
            //     }
            // }

            //如果没有请求直接返回
            if(size == 0) {
                // System.out.println("size = 0");
                // count++;
                // if (count == 10) {
                //     System.exit(0);
                // }
                lock.unlock();
                return;
            }
            // System.out.println(size);
            // count = 0;
            // LinkedBlockingQueue<AppendRequest> tmp = appendQueueWrite;
            // appendQueueWrite = appendQueueRead;
            // appendQueueRead = tmp;
            List<AppendRequest> list = new ArrayList<>();
            for (int i=0; i<size; i++) {
                AppendRequest request = null;
                appendList.remove(request);
                list.add(request);
            }
            // for (int i=0; i<appendQueueNum; i++) {
            //     for (int j=0; j<sizeList[i]; j++){
            //         AppendRequest request = appendQueueList.get(i).poll();
            //         list.add(request);
            //     }
            // }
            // System.out.println("批量处理:"+size);
            // List<String> codes = list.stream().map(s->s.code).collect(Collectors.toList());
            // 合并之后的结果集
            // long startTime = System.nanoTime();
            List<Integer> batchResult = batchAppend(list);
            // long endTime = System.nanoTime();
            // long timeElapsed = endTime - startTime;
            // System.out.println("Execution time in nanoseconds: " + timeElapsed);

            // Map<String,Integer> responseMap = new HashMap<>();
            // for (Integer result : batchResult) {
            //     responseMap.put(result.topic, result);
            // }

            //返回对应的请求结果
            appendThread.signalAll();
            lock.unlock();
        },0,50000,TimeUnit.NANOSECONDS);
    }

    //这个是个模拟批量查询的方法
    public List<Integer> batchQuery(List<AppendRequest> requestList){

        return null;
    }


    static class poolIOWorker extends Thread {
        iStoragePool pool;
        ArrayList<String> keyList;
        ArrayList<ByteBuffer> dataList;

        class FileMessage {
            String topic;
            int queueId;
            long offset;
            int dataSize;
            ByteBuffer data;
            
        }

        poolIOWorker(iStoragePool pool, ArrayList<String> keyList, ArrayList<ByteBuffer> dataList) {
            this.pool = pool;
            this.keyList = keyList;
            this.dataList = dataList;
        }

        @Override
        public void run() {
            // System.out.println("start append thread" + Thread.currentThread().getName());
            pool.append(keyList, dataList);
        }
    }

    public List<Integer> batchAppend(List<AppendRequest> requestList){
        ArrayList<String> keyList;
        ArrayList<ByteBuffer> dataList;
        ArrayList<ArrayList<String>> keyListEachPool = new ArrayList<>();
        ArrayList<ArrayList<ByteBuffer>> dataListEachPool = new ArrayList<>();
        ArrayList<Thread> appendThreads = new ArrayList<>();

        // init 
        for (int i=0; i<poolNum; i++) {
            keyListEachPool.add(new ArrayList<>());
            dataListEachPool.add(new ArrayList<>());
        }

        for (AppendRequest request : requestList) {
            // iStoragePool pool = getStoragePoolByTopic(request.topic);
            String key = request.topic + "_" + String.valueOf(request.queueId) + "_" + String.valueOf(request.offset);
            int topicHash = Math.abs(request.topic.hashCode());
            int poolIndex = topicHash % poolNum;
            keyList = keyListEachPool.get(poolIndex);
            dataList = dataListEachPool.get(poolIndex);
            keyList.add(key);
            dataList.add(request.data);
        }
        iStoragePool pool = poolList.get(0);
        pool.append(keyListEachPool.get(0), dataListEachPool.get(0));
        // for (int i=0; i<poolNum; i++) {
        //     iStoragePool pool = poolList.get(i);
        //     // pool.append(keyListEachPool.get(i), dataListEachPool.get(i));
        //     Thread t = new poolIOWorker(pool, keyListEachPool.get(i), dataListEachPool.get(i));
        //     appendThreads.add(t);
        //     t.start();
        // }
        // try {
        //     for (int i=0; i<poolNum; i++) {
        //         Thread t = appendThreads.get(i);
        //         t.join();
        //     }
        // } catch (InterruptedException e) {
        //     e.printStackTrace();
        // }

        return null;
    }

    iStoragePool getStoragePoolByTopic(String topic) {
        // int topicIndex = Integer.valueOf(topic.substring(5));
        // int poolIndex = topicIndex % poolNum;
        int topicHash = Math.abs(topic.hashCode());
        int poolIndex = topicHash % poolNum;
        return poolList.get(poolIndex);
    }

    public void append(String topic, int queueId, long offset, ByteBuffer data){
        // iStoragePool pool = getStoragePoolByTopic(topic);
        // String key = topic + String.valueOf(queueId) + String.valueOf(offset);
        // pool.append(key, data);  
        AppendRequest appendRequest = new AppendRequest(topic, queueId, offset, data);
        lock.lock();
        try {
            appendList.add(appendRequest);
            // System.out.println(appendList.size());
            if (appendList.size() == 40) {
                batchAppend(appendList);
                appendList.clear();
                appendThread.signalAll();
            } else {
                appendThread.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
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
    }

}
