package io.openmessaging;
import java.util.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.io.RandomAccessFile;
// import com.intel.pmem.llpl.TransactionalHeap;
// import com.intel.pmem.llpl.Transaction;
// import com.intel.pmem.llpl.util.LongART;
// import java.util.Iterator;

import static java.nio.file.StandardOpenOption.*;

public class DiskBenchmarker {
    private final int gigaBytes;
    private final long NBLOCKS;
    private final int BLOCKSIZE; // 8KB
    private Path file;
    private String fileName;
    ByteBuffer buff;

    static class IOWorker extends Thread {
        FileChannel channel;
        long NBLOCKS;
        int BLOCKSIZE;
        ByteBuffer buff;

        IOWorker(FileChannel channel, long NBLOCKS, int BLOCKSIZE) {
            this.channel = channel; 
            this.NBLOCKS = NBLOCKS;
            this.BLOCKSIZE = BLOCKSIZE;
            buff = ByteBuffer.allocate(BLOCKSIZE);;
        }

        @Override
        public void run() {
            // System.out.println("start append thread" + Thread.currentThread().getName());
            for (int i=0; i<NBLOCKS; i++) {
                buff.rewind();
                try {
                    channel.write(buff);
                    channel.force(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public DiskBenchmarker(int gigaBytes, int blockSize) {
        this.gigaBytes = gigaBytes;
        this.BLOCKSIZE = blockSize;
        this.NBLOCKS = 1024000000 / this.BLOCKSIZE * gigaBytes;

        this.fileName = iConfig.dataDir + "DiskBenchFile" + gigaBytes;
        this.file = Paths.get(fileName);
        buff = ByteBuffer.allocate(BLOCKSIZE);
    }

    public void pmWriteTest() {

    }

    public void writeTest() {
        try {
            // SeekableByteChannel out = Files.newByteChannel(file, EnumSet.of(CREATE, APPEND));
            RandomAccessFile memoryMappedFile = new RandomAccessFile(fileName, "rw");
            FileChannel channel = memoryMappedFile.getChannel();
            for (int i = 0; i < NBLOCKS; i++) {
                buff.rewind();
                long bytes = buff.remaining();
                long t0 = System.nanoTime();
                channel.write(buff);
                channel.force(true);
                long t1 = System.nanoTime();
                System.out.println(resultPrinter(t0, t1, bytes, "ChannelWrite"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static String resultPrinter(long t0, long t1, long bytes, String info) {
        double duration = (t1 - t0) / 1000000000d; // in seconds.
        double MB = (bytes / 1024d / 1024d);
        double speedInMBs = MB / duration;
        String output = (info + " - " + MB + " MB in " + duration + "s - " + speedInMBs + " MB/s");
        return output;
    }
    public void readTest() {
        try {
            SeekableByteChannel in = Files.newByteChannel(file, EnumSet.of(CREATE, READ));
            ByteBuffer buff = ByteBuffer.allocate(BLOCKSIZE);
            for (int i = 0; i < NBLOCKS; i++) {
                int bytesRead = in.read(buff);
                buff.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteFile() {
        try {
            Files.delete(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

