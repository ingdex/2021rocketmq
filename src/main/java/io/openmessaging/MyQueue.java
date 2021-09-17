package io.openmessaging;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class MyQueue {
    private long queueLength = 0;
    String path;
    private FileChannel channel;
    private FileChannel commonFileChannel;
    RandomAccessFile RAFileChannel;
    private FileOutputStream out;
    // private AtomicLong wrotePosition;
    private Long writePostion = Long.valueOf(0);
    private Map<Long, Long> dataOffset = new HashMap<>();
    private Map<Long, Integer> dataSize = new HashMap<>();

    // private FileWriter writer;

    public MyQueue(FileChannel channel, String path) {
        this.channel = channel;
        // this.wrotePosition = wrotePosition;
        this.path = path;
        try {
            File file = new File(path);
            // out = new FileOutputStream(file);
            // commonFileChannel = out.getChannel();
            RAFileChannel = new RandomAccessFile(file, "rw");
            commonFileChannel = RAFileChannel.getChannel();
        } catch (IOException e) {
            System.out.println("exception occoured"+ e);
        }
        // writer = new FileWriter(path);
    } 

    public long append(ByteBuffer data) {
        try {
            int remain = data.remaining();
            ByteBuffer buf = ByteBuffer.allocate(4);
            // ByteBuffer buf_t = ByteBuffer.allocate(data.remaining());
            buf.putInt(remain);
            commonFileChannel.position(writePostion);
            commonFileChannel.write(buf);
            commonFileChannel.position(writePostion+4);
            commonFileChannel.write(data);
            // commonFileChannel.read(buf_t);
            dataOffset.put(queueLength, writePostion);
            writePostion += remain + 4;
            dataSize.put(queueLength, remain);
            commonFileChannel.force(true);
        } catch (IOException e) {
            System.out.println("exception occoured"+ e);
        }
        
        return queueLength++;
    }

    public Map<Integer, ByteBuffer> getRange(long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        for(int i = 0; i < fetchNum; i++){
            Long offset_t = dataOffset.get(offset + i);
            Integer size_t = dataSize.get(offset + i);
            if(offset_t == null || size_t == null){
                break;
            }
            // skip the data size
            try {
                commonFileChannel.position(offset_t + 4);
                ByteBuffer buf = ByteBuffer.allocate(size_t);
                commonFileChannel.read(buf);
                ret.put(i, buf);
            } catch (IOException e){
                System.out.println("exception occoured"+ e);
            }
        }
        return ret;
    }
}
