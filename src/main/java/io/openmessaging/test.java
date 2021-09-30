// package io.openmessaging;
// import java.lang.String;
// import java.lang.System.Logger;
// import java.nio.ByteBuffer;
// import java.nio.channels.Channel;
// import java.nio.channels.FileChannel;
// import java.rmi.Remote;
// import java.io.*;
// import java.util.*;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.HashMap;
// import java.util.concurrent.ThreadLocalRandom;

// // import org.apache.commons.beanutils.converters.IntegerConverter;
// import java.io.FileOutputStream;
// import java.io.IOException;
// import java.io.ObjectInputStream;
// import java.io.ObjectOutputStream;
// import java.io.RandomAccessFile;
// import java.io.File;
// import java.io.IOException;

// public class test {
//     List<File> dataFileList = new ArrayList<>();
//     List<String> dataFilePathList = new ArrayList<>();
//     // List<FileWriter> dataFileWriterList = new ArrayList<>();
//     List<ObjectOutputStream> dataFileOutputStreamList = new ArrayList<>();
//     File dataFile;
//     String topics[] = {"a", "b", "c", "d", "e", "f", "g"};

//     test() {};

//     public ByteBuffer randomByteBuffer(int len) {
//         char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
//         String str = new String();
//         Random random = new Random();
//         for (int i = 0; i < len; i++) {
//             char c = chars[random.nextInt(chars.length)];
//             str += c;
//         }
//         System.out.println(str);
//         ByteBuffer res = ByteBuffer.wrap(str.getBytes());

//         return res;
//     }
    
//     private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
//         V retObj = map.get(key);
//         if(retObj != null){
//             return retObj;
//         }
//         map.put(key, defaultValue);
//         return defaultValue;
//     }

//     public void putData() {

//     }

//     public void getData() {

//     }
//     // StandardCharsets.UTF_8.decode(byteBuffer).toString();

//     public static String bb_to_str(ByteBuffer buffer){
//         byte[] bytes;
//         if(buffer.hasArray()) {
//             bytes = buffer.array();
//         } else {
//             bytes = new byte[buffer.remaining()];
//             buffer.get(bytes);
//         }
//         return new String(bytes);
//     }

//     public static void printMap(Map<Integer, ByteBuffer> map, Boolean printDataStr) {
//         if (map.isEmpty()) {
//             System.out.println("{ }");
//             return;
//         }
//         System.out.println("{");
//         for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
//             ByteBuffer buf = entry.getValue();
//             if (printDataStr) {
//                 String str = bb_to_str(buf);
//                 System.out.println("Key = " + entry.getKey() + ", Value = " + str);
//             } else {
//                 System.out.println("Key = " + entry.getKey() + ", DataSize = " + buf.capacity());
//             }
            
//         }
//         System.out.println("}");
//     }

    

//     static void singleThreadTest(DefaultMessageQueueImpl mq) {
//         long ret = mq.append("a", 0, ByteBuffer.wrap("111".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("b", 1, ByteBuffer.wrap("222".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("a", 0, ByteBuffer.wrap("333".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("b", 2, ByteBuffer.wrap("444".getBytes()));
//         System.out.println(ret);

//         // byte[] b = new byte[17408];
//         // new Random().nextBytes(b);
//         // ByteBuffer buf = ByteBuffer.wrap(b);
//         // // buf.position(536870912);
//         // for (int i=0; i<150000; i++) {
//         //     buf.rewind();
//         //     ret = mq.append("a", 1000, buf);
//         // }

//         Map<Integer, ByteBuffer> retMap;
//         retMap = mq.getRange("a", 1, 0, 4);
//         System.out.println("getRange(a, 1000, 0, 4): ");
//         printMap(retMap, true);
//         retMap = mq.getRange("b", 1, 0, 2);
//         System.out.println("getRange(b, 1001, 0, 2): ");
//         printMap(retMap, true);
//         retMap = mq.getRange("b", 2, 1, 2);
//         System.out.println("getRange(b, 1001, 1, 2): ");
//         printMap(retMap, true);
//         // retMap = mq.;
//         // System.out.println(" " + retMap);
//     }

//     // static void multiThreadTest1(DefaultMessageQueueImpl mq) {
//     //     int topicNum = 10;
//     //     ArrayList<Thread> appendThreads = new ArrayList<>();
//     //     ArrayList<Thread> getThreads = new ArrayList<>();
//     //     for (int i=0; i<topicNum; i++) {
//     //         String topic = "topic" + i;
//     //         Thread apt = new appendThread(topic, mq);
//     //         appendThreads.add(apt);
//     //         apt.start();
//     //     }
//     //     try {
//     //         for (int i=0; i<topicNum; i++) {
//     //             Thread apt = appendThreads.get(i);
//     //             apt.join();
//     //         }
//     //         System.out.println("append complete");
//     //     } catch (InterruptedException e) {
//     //         e.printStackTrace();
//     //     }
//     //     for (int i=0; i<topicNum; i++) {
//     //         String topic = "topic" + i;
//     //         Thread apt = new getRangeAndCheckThread(topic, mq);
//     //         getThreads.add(apt);
//     //         apt.start();
//     //     }
//     //     try {
//     //         for (int i=0; i<topicNum; i++) {
//     //             Thread apt = getThreads.get(i);
//     //             apt.join();
//     //         }
//     //     } catch (InterruptedException e) {
//     //         e.printStackTrace();
//     //     }
//     // }

//     static void multiThreadTest(DefaultMessageQueueImpl mq) {
//         int topicNum = 10;
//         ArrayList<Thread> appendThreads = new ArrayList<>();
//         ArrayList<Thread> getThreads = new ArrayList<>();
//         for (int i=0; i<topicNum; i++) {
//             String topic = "topic" + i;
//             Thread apt = new appendThread(topic, mq);
//             appendThreads.add(apt);
//             apt.start();
//         }
//         try {
//             for (int i=0; i<topicNum; i++) {
//                 Thread apt = appendThreads.get(i);
//                 apt.join();
//             }
//             System.out.println("append complete");
//         } catch (InterruptedException e) {
//             e.printStackTrace();
//         }
//         for (int i=0; i<topicNum; i++) {
//             String topic = "topic" + i;
//             Thread apt = new getRangeAndCheckThread(topic, mq);
//             getThreads.add(apt);
//             apt.start();
//         }
//         try {
//             for (int i=0; i<topicNum; i++) {
//                 Thread apt = getThreads.get(i);
//                 apt.join();
//             }
//         } catch (InterruptedException e) {
//             e.printStackTrace();
//         }
//     }

//     static class appendThread extends Thread {
//         String topic;
//         DefaultMessageQueueImpl mq;

//         class FileMessage {
//             String topic;
//             int queueId;
//             long offset;
//             int dataSize;
//             ByteBuffer data;
            
//         }

//         appendThread(String topic, DefaultMessageQueueImpl mq) {
//             this.topic = topic;
//             this.mq = mq;
//         }

//         public void append(String topic, DefaultMessageQueueImpl mq) {
//             int queueNum = 200;
//             int msgNum = 2000;
//             int dataSize = 1000;
//             String path = "/essd/" + topic;
//             FileChannel channel;
//             try {
//                 RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(path), "rw");
//                 channel = memoryMappedFile.getChannel();
//             } catch (Exception e) {
//                 e.printStackTrace();
//                 return;
//             }
            
//             // 生成随机数据，<topic, queueId, data>
//             ByteBuffer data;
//             byte[] b = new byte[dataSize];
//             new Random().nextBytes(b);
//             data = ByteBuffer.wrap(b);
//             // data = ByteBuffer.allocate(dataSize);
//             // data.flip();
//             ArrayList<Integer> queueIdList = new ArrayList<>();
//             for (int i=0; i<queueNum; i++) {
//                 queueIdList.add(i);
//             }
//             for (int i=0; i<queueNum; i++) {
//                 int queueId = queueIdList.get(i);
//                 for (int j=0; j<msgNum; j++) {
//                     // dataSize++;
//                     // dataSize = 5;
//                     // data = ByteBuffer.allocate(dataSize);
//                     data.rewind();
//                     long offset = mq.append(topic, queueId, data);
//                     if (offset != j) {
//                         System.out.println("err");
//                         return;
//                     }
//                     data.rewind();
//                     try {
//                         ByteBuffer buf = ByteBuffer.allocate(16);
//                         buf.putInt(queueId);
//                         buf.putInt(data.remaining());
//                         buf.putLong(offset);
//                         buf.flip();
//                         channel.write(buf);
//                         channel.write(data);
//                     } catch (IOException e) {
//                         e.printStackTrace();
//                     }
//                 }
//             }
//         }

//         @Override
//         public void run() {
//             System.out.println("start append thread" + Thread.currentThread().getName());
//             append(topic, mq);
//         }
//     }


//     static class getRangeAndCheckThread extends Thread {
//         String path;
//         String topic;
//         FileChannel channel;
//         long currentOffset = 0;
//         int fetchNum = 4;
//         DefaultMessageQueueImpl mq;
//         class FileMessage {
//             String topic;
//             int queueId;
//             long offset;
//             int dataSize;
//             ByteBuffer data;
            
//         }

//         getRangeAndCheckThread(String topic, DefaultMessageQueueImpl mq) {
//             this.path = "/essd/" + topic;
//             this.topic = topic;
//             this.mq = mq;
//         }

//         public FileMessage getMsg(String topic, FileChannel channel) {
            
//             FileMessage msg = new FileMessage();
//             try {
//                 ByteBuffer buf = ByteBuffer.allocate(16);
//                 int ret = channel.read(buf);
//                 if (ret == -1 || ret == 0) {
//                     return null;
//                 }
//                 msg.topic = topic;
//                 buf.flip();
//                 msg.queueId = buf.getInt();
//                 msg.dataSize = buf.getInt();
//                 msg.offset = buf.getLong();
//                 msg.data = ByteBuffer.allocate(msg.dataSize);
//                 channel.read(msg.data);
//                 return msg;
//             } catch (Exception e) {
//                 e.printStackTrace();
//             }
            
//             return null;
//         }

//         void getRangeAndCheck(String topic, DefaultMessageQueueImpl mq) {
//             this.topic = topic;
//             String path = "/essd/" + topic;
//             FileChannel channel;
//             long currentOffset = 0;
//             int fetchNum = 4;
//             try {
//                 RandomAccessFile memoryMappedFile = new RandomAccessFile(new File(path), "r");
//                 channel = memoryMappedFile.getChannel();
//             } catch (Exception e) {
//                 e.printStackTrace();
//                 return;
//             }
//             try {
//                 while(true) {
//                     ArrayList<FileMessage> msgList = new ArrayList<>();
//                     for (int i=0; i<fetchNum; i++) {
//                         FileMessage msg = getMsg(topic, channel);
//                         if (msg == null) {
//                             break;
//                         }
//                         currentOffset++;
//                     }
//                     if (msgList.size() == 0) {
//                         System.out.println("check complete" + topic);
//                         return;
//                     }
//                     FileMessage msg = msgList.get(0);
//                     Map<Integer, ByteBuffer> retMap = mq.getRange(msg.topic, msg.queueId, msg.offset, msgList.size());
//                     if (retMap.size() != msgList.size()) {
//                         System.out.println("size err");
//                     }
//                     for (int i=0; i<retMap.size(); i++) {
//                         FileMessage msg1 = msgList.get(i);
//                         ByteBuffer data = retMap.get(i);
//                         if (!msg1.data.equals(data)) {
//                             System.out.println("data err");
//                         }
//                     }
//                 }
//             } catch (Exception e) {
//                 e.printStackTrace();
//             }
//         }

//         @Override
//         public void run() {
//             System.out.println("start getRange and check thread" + Thread.currentThread().getName());
//             getRangeAndCheck(topic, mq);
//         }
//     }

//     public static void main(String[] args) {
        
//         int fileCount = 2;
//         test t = new test();
//         DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
//         // singleThreadTest(mq);
//         multiThreadTest(mq);
//         return;
//     }
// }