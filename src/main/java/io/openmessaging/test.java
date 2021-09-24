// package io.openmessaging;
// import java.lang.String;
// import java.lang.System.Logger;
// import java.nio.ByteBuffer;
// import java.io.*;
// import java.util.*;
// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.HashMap;

// // import org.apache.commons.beanutils.converters.IntegerConverter;
// import java.io.FileOutputStream;
// import java.io.IOException;
// import java.io.ObjectInputStream;
// import java.io.ObjectOutputStream;
// import java.nio.charset.StandardCharsets;

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

//     // public void genData(int fileCount) {
//     //     int topicNum = 2;
//     //     int queueNum = 2;
//     //     int dataNum = 2;
//     //     String topic;
//     //     Integer queueId;
//     //     Long offset;
//     //     ByteBuffer data;
//     //     Random rnd = new Random(System.currentTimeMillis());
//     //     HashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new HashMap<>();
//     //     // 创建fileCount个json文件，之后每个线程使用一个文件发送请求
//     //     try {
//     //         for (int i=0; i<fileCount; i++) {
//     //             String path = "data" + String.valueOf(i) + ".json";
//     //             File file = new File(path);
//     //             file.createNewFile();
//     //             // FileWriter fileWriter = new FileWriter(file);
//     //             FileOutputStream fileOS = new FileOutputStream(path);
//     //             ObjectOutputStream out = new ObjectOutputStream(fileOS);
//     //             dataFilePathList.add(path);
//     //             dataFileList.add(file);
//     //             dataFileOutputStreamList.add(out);
//     //             String str = "test";
//     //             mqRequest mqReq = new mqRequest("a", Integer.valueOf(10), Long.valueOf(0), ByteBuffer.wrap(str.getBytes()));
//     //             out.writeObject(mqReq);
//     //             out.flush();
//     //         }
//     //     } catch(java.io.IOException e) {
//     //         System.out.println("can not create file: " + e);
//     //     }
        
//     //     // 生成随机数据，<topic, queueId, data>
//     //     try {
//     //         for (int i_topic = 0; i_topic < topicNum; i_topic++) {
//     //             topic = topics[i_topic];
//     //             for (int i_queue = 0; i_queue < queueNum; queueNum++) {
//     //                 queueId = rnd.nextInt(10000) + 1;
//     //                 for (int i_data = 0; i_data < dataNum; i_data++) {
//     //                     data = randomByteBuffer(10);
//     //                     // appendData
//     //                     Map<Integer, Map<Long, ByteBuffer>> map1 = getOrPutDefault(appendData, topic, new HashMap<>());
//     //                     Map<Long, ByteBuffer> map2 = getOrPutDefault(map1, queueId, new HashMap<>());
//     //                     offset = Long.valueOf(i_data);
//     //                     map2.put(offset, data);
//     //                     mqRequest mqReq = new mqRequest(topic, queueId, offset, data);
//     //                     // 随机写入一个文件
//     //                     ObjectOutputStream out = dataFileOutputStreamList.get(rnd.nextInt(fileCount));
//     //                     out.writeObject(mqReq);
//     //                 }
//     //             }
//     //         }

//     //     } catch (FileNotFoundException e) {
//     //         System.out.println("File not found");
//     //     } catch (IOException e) {
//     //         System.out.println("Error initializing stream");
//     //     }
//     //     // 所有数据以json写入一个dataFile
//     //     try {
//     //         String path = "data.json";
//     //         dataFile = new File(path);
//     //         dataFile.createNewFile();
//     //         JSONObject obj = new JSONObject(appendData);
//     //         // String json = new ObjectMapper().writeValueAsString(appendData);
//     //         FileWriter fileWriter= new FileWriter(dataFile);
//     //         fileWriter.write(obj.toString());
//     //         fileWriter.flush();  
//     //         fileWriter.close(); 
//     //     } catch(java.io.IOException e) {
//     //         System.out.println("can not create file: " + e);
//     //     }
        
//     //     // for (int i_topic=0; i_topic<topicNum; i_topic++) {
//     //     //     topic = topics[i_topic];
//     //     //     for (int i_queue = 0; i_queue < queueNum; queueNum++) {
//     //     //         queueId = rnd.nextInt(10000) + 1;
//     //     //         for (int i_data = 0; i_data < dataNum; i_data++) {
//     //     //             data = randomByteBuffer(10);
//     //     //             // appendData
//     //     //             Map<Integer, Map<Long, ByteBuffer>> map1 = getOrPutDefault(appendData, topic, new HashMap<>());
//     //     //             Map<Long, ByteBuffer> map2 = getOrPutDefault(map1, queueId, new HashMap<>());
//     //     //             offset = Long.valueOf(i_data);
//     //     //             map2.put(offset, data);
//     //     //         }
//     //     //     }
//     //     // }
//     // }

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

//     public static void main(String[] args) {
        
//         int fileCount = 2;
//         test t = new test();
//         // System.out.println("generate mq request");
//         // t.genData(fileCount);
//         DefaultMessageQueueImpl mq = new DefaultMessageQueueImpl();
//         long ret = mq.append("a", 1001, ByteBuffer.wrap("2021".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("b", 1001, ByteBuffer.wrap("20212".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("a", 1000, ByteBuffer.wrap("20213".getBytes()));
//         System.out.println(ret);
//         ret = mq.append("b", 1001, ByteBuffer.wrap("20214".getBytes()));
//         System.out.println(ret);

//         byte[] b = new byte[17408];
//         new Random().nextBytes(b);
//         ByteBuffer buf = ByteBuffer.wrap(b);
//         // buf.position(536870912);
//         for (int i=0; i<2; i++) {
//             buf.rewind();
//             ret = mq.append("a", 1000, buf);
//         }

//         Map<Integer, ByteBuffer> retMap;
//         retMap = mq.getRange("a", 1000, 0, 4);
//         System.out.println("getRange(a, 1000, 0, 4): ");
//         printMap(retMap, false);
//         retMap = mq.getRange("b", 1001, 0, 2);
//         System.out.println("getRange(b, 1001, 0, 2): ");
//         printMap(retMap, false);
//         retMap = mq.getRange("b", 1001, 1, 2);
//         System.out.println("getRange(b, 1001, 1, 2): ");
//         printMap(retMap, false);
//         // retMap = mq.;
//         // System.out.println(" " + retMap);
        
//     }
// }