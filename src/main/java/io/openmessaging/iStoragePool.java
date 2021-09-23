package io.openmessaging;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.kerberos.KeyTab;
import javax.swing.event.ChangeEvent;

import com.alibaba.fastjson.serializer.ByteBufferCodec;

import java.nio.channels.FileChannel;
import org.apache.log4j.Logger;
import org.apache.logging.log4j.core.impl.MementoMessage;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.FileOutputStream;

// 线程不安全
public class iStoragePool {
    HashMap <String, iMessage> appendMsg = new HashMap<>();
    HashMap <String, FileChannel> channelMap = new HashMap<>();
    // private FileChannel channel;
    long pyhsicalOffset = 0;    // 当前写入位置距离StoragePool首地址的绝对偏移
    long currentBarrierOffset = 0;
    public final static long FILESIZE = 1 << 30;
    String dir = iConfig.dataDir;
    private String poolName;

    public iStoragePool(String poolName) {
        this.poolName = poolName;
    }

    public void appendByFile(String path, long barrierOffset) {
        File file = new File(path);
        try {
            RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(path), "rw");
            FileChannel channel = memoryMappedFile.getChannel();
            channelMap.put(path, channel);
            ByteBuffer keySizeByteBuffer = ByteBuffer.allocate(4);
            ByteBuffer keyBuffer;
            ByteBuffer dataSizeByteBuffer = ByteBuffer.allocate(4);
            int readPos = 0;
            int ret;
            while((ret = channel.read(keySizeByteBuffer, readPos)) != -1) {
                keySizeByteBuffer.rewind();
                int keySize = keySizeByteBuffer.getInt();
                readPos += 4;
                keyBuffer = ByteBuffer.allocate(keySize);
                channel.read(keyBuffer, readPos);
                keyBuffer.flip();
                String key = new String(keyBuffer.array());
                readPos += keySize;
                channel.read(dataSizeByteBuffer, readPos);
                dataSizeByteBuffer.rewind();
                int dataSize = dataSizeByteBuffer.getInt();                
                iMessage msg = new iMessage(barrierOffset, Long.valueOf(readPos+4), (short)(dataSize), path);
                readPos += dataSize + 4;
                appendMsg.put(key, msg);
                keySizeByteBuffer.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    // 之后可能引入cache
    public FileChannel getFileChannel(String filename) {
        FileChannel ret = null;
        ret = channelMap.get(filename);
        if (ret == null) {
            try {
                RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(filename), "rw");
                ret = memoryMappedFile.getChannel();
                channelMap.put(filename, ret);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        

        return ret;
    }

    public long append(String key, ByteBuffer data){
        // Disk layout:
        // || sizeof(key) | key | sizeof(data) | data ||
        int dataOffset = 4 + key.getBytes().length + 4;
        int writeBufSize = dataOffset + data.remaining();
        if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
            currentBarrierOffset += FILESIZE;
            pyhsicalOffset = currentBarrierOffset;
            // channel = null;
        }
        String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
        FileChannel channel = getFileChannel(filename);

        long dataPhysicalOffset = pyhsicalOffset + dataOffset;
        iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, (short)(data.capacity()), filename);
        appendMsg.put(key, msg);
        // 
        try {
            // Disk layout:
            // || sizeof(key) | key | sizeof(data) | data ||
            long realtiveWritePos = pyhsicalOffset - currentBarrierOffset;
            ByteBuffer buf = ByteBuffer.allocate(writeBufSize);
            buf.putInt(key.length());
            buf.put(key.getBytes());
            buf.putInt(data.remaining());
            buf.put(data);
            // channel.position(realtiveWritePos);
            buf.flip();
            channel.write(buf, realtiveWritePos);
            pyhsicalOffset += writeBufSize;
            channel.force(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return dataPhysicalOffset;
    }

    public ByteBuffer get(String key) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        ByteBuffer ret = null;
        iMessage msg = appendMsg.get(key);
        if (msg == null) {
            return ret;
        }
        FileChannel channel = getFileChannel(msg.fielname);
        try {
            // channel.position(msg.offset);
            ret = ByteBuffer.allocate(msg.size);
            int offset = (int)(msg.offset - msg.currentBarrierOffset);
            channel.read(ret, offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        // logger.debug("getRange: { topic: " + String.valueOf(topic) + ", queueId: " + String.valueOf(queueId) + ", offset" + String.valueOf(offset) + ", fetchNum" + String.valueOf(fetchNum) + " }");
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        
        return ret;
    }
}
