// package io.openmessaging;
// import java.nio.ByteBuffer;
// import java.util.*;
// import java.util.concurrent.ConcurrentHashMap;

// // import com.alibaba.fastjson.serializer.ByteBufferCodec;

// import java.nio.channels.FileChannel;
// import java.io.RandomAccessFile;
// import java.io.File;
// import java.io.IOException;
// import org.apache.log4j.Logger;
// // 线程不安全
// public class iStoragePool {
//     ConcurrentHashMap <String, iMessage> appendMsg = new ConcurrentHashMap<>();
//     ConcurrentHashMap <String, FileChannel> channelMap = new ConcurrentHashMap<>();
//     // private FileChannel channel;
//     long pyhsicalOffset = 0;    // 当前写入位置距离StoragePool首地址的绝对偏移
//     long currentBarrierOffset = 0;
//     public final static long FILESIZE = 1 << 30;
//     String dir = iConfig.dataDir;
//     String poolName;
//     ArrayList<ByteBuffer> writeBufList;
//     final int writeBufNum = 50;
//     final int writeBufSize = 17 * 1024 * 50;
//     Logger logger = Logger.getLogger(iStoragePool.class);
//     ByteBuffer writeBuf = ByteBuffer.allocateDirect(writeBufSize);

//     public iStoragePool(String poolName) {
//         this.poolName = poolName;
//         // for (int i=0; i<writeBufNum; i++) {
//         //     writeBufList.add(ByteBuffer.allocateDirect(writeBufSize));
//         // }
        
//     }

//     public void appendByFile(String path, long barrierOffset) {
//         File file = new File(path);
//         try {
//             RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(path), "rw");
//             FileChannel channel = memoryMappedFile.getChannel();
//             channelMap.put(path, channel);
//             ByteBuffer keySizeByteBuffer = ByteBuffer.allocate(4);
//             ByteBuffer keyBuffer;
//             ByteBuffer dataSizeByteBuffer = ByteBuffer.allocate(4);
//             int readPos = 0;
//             int ret;
//             while((ret = channel.read(keySizeByteBuffer, readPos)) != -1) {
//                 keySizeByteBuffer.flip();
//                 int keySize = keySizeByteBuffer.getInt();
//                 readPos += 4;
//                 keyBuffer = ByteBuffer.allocate(keySize);
//                 channel.read(keyBuffer, readPos);
//                 keyBuffer.flip();
//                 String key = new String(keyBuffer.array());
//                 readPos += keySize;
                
//                 channel.read(dataSizeByteBuffer, readPos);
//                 dataSizeByteBuffer.flip();
//                 int dataSize = dataSizeByteBuffer.getInt();
//                 iMessage msg = new iMessage(barrierOffset, Long.valueOf(readPos+4), dataSize, path);
//                 readPos += dataSize + 4;
//                 appendMsg.put(key, msg);
//                 keySizeByteBuffer.clear();
//                 dataSizeByteBuffer.clear();
//             }
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }
    
//     // 之后可能引入cache
//     public FileChannel getFileChannel(String filename) {
//         FileChannel ret = null;
//         ret = channelMap.get(filename);
//         if (ret == null) {
//             try {
//                 RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(filename), "rw");
//                 ret = memoryMappedFile.getChannel();
//                 channelMap.put(filename, ret);
//             } catch (Exception e) {
//                 e.printStackTrace();
//             }
//         }
        

//         return ret;
//     }

//     // 线程不安全
//     public long append(String key, ByteBuffer data){
//         // Disk layout:
//         // || sizeof(key) | key | sizeof(data) | data ||
//         int dataOffset = 4 + key.getBytes().length + 4;
//         data.flip();
//         int writeBufSize = dataOffset + data.remaining();
//         if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
//             currentBarrierOffset = pyhsicalOffset;
//             // pyhsicalOffset = currentBarrierOffset;
//             // channel = null;
//         }
//         String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//         FileChannel channel = getFileChannel(filename);

//         long dataPhysicalOffset = pyhsicalOffset + dataOffset;
//         iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.capacity(), filename);
//         appendMsg.put(key, msg);
//         // 
//         try {
//             // Disk layout:
//             // || sizeof(key) | key | sizeof(data) | data ||
//             long realtiveWritePos = pyhsicalOffset - currentBarrierOffset;
//             ByteBuffer buf = ByteBuffer.allocate(writeBufSize);
//             buf.putInt(key.length());
//             buf.put(key.getBytes());
//             int dataSize = data.remaining();
//             buf.putInt(data.remaining());

//             buf.put(data);
//             // channel.position(realtiveWritePos);
//             buf.flip();
//             channel.write(buf, realtiveWritePos);
//             pyhsicalOffset += writeBufSize;
//             channel.force(true);
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
        
//         return dataPhysicalOffset;
//     }

//     class Task {
//         int totalWriteBufSize;
//         int requestNum;
//         FileChannel channel;
//         long relativeWritePosition;
//         String filename;
//         Task(int totalWriteBufSize, int requestNum, FileChannel channel, long relativeWritePosition, String filename) {
//             this.totalWriteBufSize = totalWriteBufSize;
//             this.requestNum = requestNum;
//             this.channel = channel;
//             this.relativeWritePosition = relativeWritePosition;
//             this.filename = filename;
//         }

//         void reset() {
//             totalWriteBufSize = 0;
//             requestNum = 0;
//             channel = null;
//             relativeWritePosition = 0;
//         }
//     }
 
//     public void append(ArrayList<String> keyList, ArrayList<ByteBuffer> dataList){
//         int num = keyList.size();
//         int writeBufSize = 0;
//         // int totalWriteBufSize = 0;
//         // ByteBuffer writeBuf;
//         ArrayList<Task> taskList = new ArrayList<>();
//         // int requestNum = 0;
//         long dataPhysicalOffset;   // 数据段起始地址在storagepool中的偏移
//         String filename_t = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//         FileChannel channel_t = getFileChannel(filename_t);
//         Task curTask = new Task(0, 0, channel_t, pyhsicalOffset, dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data");
//         // may overflow
//         for (int i=0; i<num; i++) {
//             String key = keyList.get(i);
//             ByteBuffer data = dataList.get(i);
//             // Disk layout:
//             // || sizeof(key) | key | sizeof(data) | data ||
//             writeBufSize = 4 + key.getBytes().length + 4 + data.remaining();
//             // 当前文件空间不足
//             if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
//                 if (curTask.totalWriteBufSize == 0) {
//                     currentBarrierOffset = pyhsicalOffset;
//                     curTask.filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//                     curTask.requestNum = 1;
//                     curTask.totalWriteBufSize = writeBufSize;
//                     curTask.channel = getFileChannel(curTask.filename);
//                     curTask.relativeWritePosition = 0;
//                     pyhsicalOffset += writeBufSize;
//                     dataPhysicalOffset = pyhsicalOffset - data.remaining();
//                     iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//                     appendMsg.put(key, msg);
//                     continue;
//                 }
//                 taskList.add(curTask);
//                 curTask.reset();

//                 currentBarrierOffset = pyhsicalOffset;
//                 curTask.requestNum = 1;
//                 curTask.totalWriteBufSize = writeBufSize;
//                 curTask.relativeWritePosition = 0;
//                 curTask.filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//                 curTask.channel = getFileChannel(curTask.filename);
//                 pyhsicalOffset += writeBufSize;
//                 dataPhysicalOffset = pyhsicalOffset - data.remaining();
//                 iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//                 appendMsg.put(key, msg);
//                 continue;
//             }
//             pyhsicalOffset += writeBufSize;
//             curTask.requestNum++;
//             curTask.totalWriteBufSize += writeBufSize;
//             dataPhysicalOffset = pyhsicalOffset - data.remaining();
//             iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//             appendMsg.put(key, msg);
//         }
//         if (curTask.requestNum != 0) {
//             taskList.add(curTask);
//         }
//         int currentRequestPos = 0;
//         logger.debug("taskList.size " + taskList.size() + " all requestNum " + keyList.size());
//         for (int i=0; i<taskList.size(); i++) {
//             curTask = taskList.get(i);
//             logger.debug("taskList.requestNum " + curTask.requestNum);
//             writeBuf.clear();
//             // writeBuf.rewind();
//             for (int j=0; j<curTask.requestNum; j++) {
//                 String key = keyList.get(currentRequestPos);
//                 ByteBuffer data = dataList.get(currentRequestPos);
//                 currentRequestPos++;
//                 writeBuf.putInt(key.length());
//                 writeBuf.put(key.getBytes());
//                 writeBuf.putInt(data.remaining());
//                 writeBuf.put(data);
//             }
//             // writeBuf.rewind();
//             writeBuf.limit(curTask.totalWriteBufSize);
//             writeBuf.position(0);
//             try {
//                 curTask.channel.write(writeBuf, curTask.relativeWritePosition);
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }
//         }
//         // force to ssd
//         for (int i=0; i<taskList.size(); i++) {
//             try {
//                 curTask.channel.force(true);
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }
//         }
        
//         return;
//     }
 
//     // public void append(ArrayList<String> keyList, ArrayList<ByteBuffer> dataList){
//     //     int num = keyList.size();
//     //     int writeBufSize = 0;
//     //     // int totalWriteBufSize = 0;
//     //     ByteBuffer writeBuf;
//     //     ArrayList<Integer> totalWriteBufLenList = new ArrayList<>();
//     //     ArrayList<Integer> requestNumList = new ArrayList<>();
//     //     ArrayList<FileChannel> channelList = new ArrayList<>();
//     //     ArrayList<Long> relativeWritePositionList = new ArrayList<>();
//     //     ArrayList<Task> taskList = new ArrayList<>();
//     //     // int requestNum = 0;
//     //     long dataPhysicalOffset;   // 数据段起始地址在storagepool中的偏移
//     //     String filename_t = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//     //     FileChannel channel_t = getFileChannel(filename_t);
//     //     Task curTask = new Task(0, 0, channel_t, pyhsicalOffset, dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data");
//     //     // may overflow
//     //     for (int i=0; i<num; i++) {
//     //         String key = keyList.get(i);
//     //         ByteBuffer data = dataList.get(i);
//     //         // Disk layout:
//     //         // || sizeof(key) | key | sizeof(data) | data ||
//     //         writeBufSize = 4 + key.getBytes().length + 4 + data.remaining();
//     //         // 当前文件空间不足
//     //         if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
//     //             if (curTask.totalWriteBufSize == 0) {
//     //                 currentBarrierOffset = pyhsicalOffset;
//     //                 curTask.filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//     //                 curTask.requestNum = 1;
//     //                 curTask.totalWriteBufSize = writeBufSize;
//     //                 curTask.channel = getFileChannel(curTask.filename);
//     //                 curTask.relativeWritePosition = pyhsicalOffset;
//     //                 pyhsicalOffset += writeBufSize;
//     //                 dataPhysicalOffset = pyhsicalOffset - data.remaining();
//     //                 iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//     //                 appendMsg.put(key, msg);
//     //                 continue;
//     //             }
//     //             taskList.add(curTask);
//     //             curTask.reset();

//     //             currentBarrierOffset = pyhsicalOffset;
//     //             curTask.requestNum = 1;
//     //             curTask.totalWriteBufSize = writeBufSize;
//     //             curTask.relativeWritePosition = pyhsicalOffset;
//     //             curTask.filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
//     //             curTask.channel = getFileChannel(curTask.filename);
//     //             pyhsicalOffset += writeBufSize;
//     //             dataPhysicalOffset = pyhsicalOffset - data.remaining();
//     //             iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//     //             appendMsg.put(key, msg);
//     //             continue;
//     //         }
//     //         pyhsicalOffset += writeBufSize;
//     //         curTask.requestNum++;
//     //         curTask.totalWriteBufSize += writeBufSize;
//     //         dataPhysicalOffset = pyhsicalOffset - data.remaining();
//     //         iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), curTask.filename);
//     //         appendMsg.put(key, msg);
//     //     }
//     //     if (curTask.requestNum != 0) {
//     //         taskList.add(curTask);
//     //     }
//     //     int currentRequestPos = 0;
//     //     for (int i=0; i<taskList.size(); i++) {
//     //         curTask = taskList.get(i);
//     //         writeBuf = ByteBuffer.allocate(curTask.totalWriteBufSize);
//     //         for (int j=0; j<curTask.requestNum; j++) {
//     //             String key = keyList.get(currentRequestPos);
//     //             ByteBuffer data = dataList.get(currentRequestPos);
//     //             currentRequestPos++;
//     //             writeBuf.putInt(key.length());
//     //             writeBuf.put(key.getBytes());
//     //             writeBuf.putInt(data.remaining());
//     //             writeBuf.put(data);
//     //         }
//     //         writeBuf.flip();
//     //         try {
//     //             curTask.channel.write(writeBuf, curTask.relativeWritePosition);
//     //         } catch (IOException e) {
//     //             e.printStackTrace();
//     //         }
//     //     }
//     //     // force to ssd
//     //     for (int i=0; i<taskList.size(); i++) {
//     //         try {
//     //             curTask.channel.force(true);
//     //         } catch (IOException e) {
//     //             e.printStackTrace();
//     //         }
//     //     }
        
//     //     return;
//     // }

//     public synchronized ByteBuffer get(String key) {
//         // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
//         ByteBuffer ret = null;
//         iMessage msg = appendMsg.get(key);
//         if (msg == null) {
//             return ret;
//         }
//         FileChannel channel = getFileChannel(msg.fielname);
//         // System.out.println(msg.fielname);
//         try {
//             // channel.position(msg.offset);
//             ret = ByteBuffer.allocate(msg.size);
//             int offset = (int)(msg.offset - msg.currentBarrierOffset);
//             channel.read(ret, offset);
//         } catch (IOException e) {
//             e.printStackTrace();
//         }
//         return ret;
//     }

//     public synchronized Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
//         // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
//         Map<Integer, ByteBuffer> ret = new HashMap<>();
//         for(int i = 0; i < fetchNum; i++){
//             String key = topic + "_" + String.valueOf(queueId) + "_" + String.valueOf(offset + i);
//             iMessage msg = appendMsg.get(key);
//             if (msg == null) {
//                 break;
//             }
//             ByteBuffer buf = ByteBuffer.allocate(msg.size);
//             int dataOffset = (int)(msg.offset - msg.currentBarrierOffset);
//             FileChannel channel = getFileChannel(msg.fielname);
//             try {
//                 channel.read(buf, dataOffset);
//                 logger.debug("dataOffset " + dataOffset + " msg.size " + msg.size);
//             } catch (IOException e) {
//                 e.printStackTrace();
//             }
//             if (buf == null) {
//                 break;
//             }
//             buf.flip();
//             ret.put(i, buf);
//         }

//         return ret;
//     }
// }
