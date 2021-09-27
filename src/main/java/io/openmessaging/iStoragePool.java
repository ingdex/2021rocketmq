package io.openmessaging;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

// 线程不安全
public class iStoragePool {
    ConcurrentHashMap <String, iMessage> appendMsg = new ConcurrentHashMap<>();
    ConcurrentHashMap <String, FileChannel> channelMap = new ConcurrentHashMap<>();
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
                dataSizeByteBuffer.flip();
                int dataSize = dataSizeByteBuffer.getInt();
                iMessage msg = new iMessage(barrierOffset, Long.valueOf(readPos+4), dataSize, path);
                readPos += dataSize + 4;
                appendMsg.put(key, msg);
                keySizeByteBuffer.clear();
                dataSizeByteBuffer.clear();
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

    // 线程不安全
    public long append(String key, ByteBuffer data){
        // Disk layout:
        // || sizeof(key) | key | sizeof(data) | data ||
        int dataOffset = 4 + key.getBytes().length + 4;
        data.flip();
        int writeBufSize = dataOffset + data.remaining();
        if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
            currentBarrierOffset = pyhsicalOffset;
            // pyhsicalOffset = currentBarrierOffset;
            // channel = null;
        }
        String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
        FileChannel channel = getFileChannel(filename);

        long dataPhysicalOffset = pyhsicalOffset + dataOffset;
        iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.capacity(), filename);
        appendMsg.put(key, msg);
        // 
        try {
            // Disk layout:
            // || sizeof(key) | key | sizeof(data) | data ||
            long realtiveWritePos = pyhsicalOffset - currentBarrierOffset;
            ByteBuffer buf = ByteBuffer.allocate(writeBufSize);
            buf.putInt(key.length());
            buf.put(key.getBytes());
            int dataSize = data.remaining();
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

    public void append(ArrayList<String> keyList, ArrayList<ByteBuffer> dataList){
        int num = keyList.size();
        int writeBufSize = 0;
        ByteBuffer writeBuf;
        ArrayList<Integer> writeBufLenList = new ArrayList<>();
        ArrayList<Integer> requestNumList = new ArrayList<>();
        ArrayList<FileChannel> channelList = new ArrayList<>();
        ArrayList<Long> relativeWritePositionList = new ArrayList<>();
        int fileNum = 0;
        int requestNum = 0;
        long dataPhysicalOffset = pyhsicalOffset;
        // may overflow
        for (int i=0; i<num; i++) {
            String key = keyList.get(i);
            ByteBuffer data = dataList.get(i);
            // Disk layout:
            // || sizeof(key) | key | sizeof(data) | data ||
            int preWriteBufSize = writeBufSize;
            writeBufSize += 4 + key.getBytes().length + 4 + data.remaining();
            // 当前文件空间不足
            if (pyhsicalOffset + writeBufSize > currentBarrierOffset + FILESIZE) {
                if (preWriteBufSize == 0) {
                    requestNum = 1;
                    currentBarrierOffset = pyhsicalOffset;
                    pyhsicalOffset += writeBufSize;
                    dataPhysicalOffset = pyhsicalOffset - data.remaining();
                    String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
                    iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), filename);
                    appendMsg.put(key, msg);
                    continue;
                }
                writeBufLenList.add(preWriteBufSize);
                requestNumList.add(requestNum);
                fileNum++;
                requestNum = 1;
                writeBufSize -= preWriteBufSize;
                String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
                FileChannel channel = getFileChannel(filename);
                channelList.add(channel);
                currentBarrierOffset = pyhsicalOffset;
                pyhsicalOffset += writeBufSize;
                dataPhysicalOffset = pyhsicalOffset - data.remaining();
                long relativeWritePos = 0;
                relativeWritePositionList.add(relativeWritePos);
                // long dataPhysicalOffset = 
                iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), filename);
                appendMsg.put(key, msg);
                continue;
            }
            requestNum++;
            pyhsicalOffset += writeBufSize;
            dataPhysicalOffset = pyhsicalOffset - data.remaining();
            String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
            iMessage msg = new iMessage(currentBarrierOffset, dataPhysicalOffset, data.remaining(), filename);
            appendMsg.put(key, msg);
        }
        if (requestNum != 0) {
            writeBufLenList.add(writeBufSize);
            requestNumList.add(requestNum);
            String filename = dir + poolName + "_" + String.valueOf(currentBarrierOffset) + ".data";
            FileChannel channel = getFileChannel(filename);
            channelList.add(channel);
            long relativeWritePos = pyhsicalOffset - writeBufSize - currentBarrierOffset;
            relativeWritePositionList.add(relativeWritePos);
            fileNum++;
        }
        int currentRequestPos = 0;
        for (int i=0; i<fileNum; i++) {
            int writeBufLenBatch = writeBufLenList.get(i);
            int requestNumBatch = requestNumList.get(i);
            writeBuf = ByteBuffer.allocate(writeBufLenBatch);
            for (int j=0; j<requestNumBatch; j++) {
                String key = keyList.get(currentRequestPos);
                ByteBuffer data = dataList.get(currentRequestPos);
                currentRequestPos++;
                writeBuf.putInt(key.length());
                writeBuf.put(key.getBytes());
                writeBuf.putInt(data.remaining());
                writeBuf.put(data);
            }
            writeBuf.flip();
            FileChannel channel = channelList.get(i);
            long relativeWritePos = relativeWritePositionList.get(i);
            try {
                channel.write(writeBuf, relativeWritePos);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // force to ssd
        for (int i=0; i<fileNum; i++) {
            FileChannel channel = channelList.get(i);
            try {
                channel.force(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        return;
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
