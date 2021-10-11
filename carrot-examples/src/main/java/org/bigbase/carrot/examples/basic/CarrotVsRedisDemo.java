/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 */

package org.bigbase.carrot.examples.basic;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.util.Utils;

@SuppressWarnings("unused")

public class CarrotVsRedisDemo {
  
  static long N = 10000000;
      
  static AtomicLong index = new AtomicLong(0);
  
  static int NUM_THREADS = 2;
  static int BATCH_SIZE = 100;
  static int SET_SIZE = 1000;
  static int SET_KEY_TOTAL = 100000;
  static AtomicLong loaded = new AtomicLong(0);
  static long totalLoaded = 0;
  
  static List<String> clusterNodes = Arrays.asList("localhost:6379" /*, "localhost:6380",
    "localhost:6381", "localhost:6382", "localhost:6383", "localhost:6384", "localhost:6385", "localhost:6386"*/); 
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("Run Carrot vs. Redis Demo");
    RawClusterClient client = new RawClusterClient(clusterNodes);
    long start = System.currentTimeMillis();
    flushAll(client);
    long end = System.currentTimeMillis();
    System.out.println("flush all " + (end - start) + "ms");
    
    // main 3 tests
    start = System.currentTimeMillis();
    
    // Load sets with cardinality 1000 each
    runClusterSaddCycle();
    totalLoaded += loaded.get();
    loaded.set(0);
    
    flushAll(client);
    
    // Load hashes, each 1000 length
    runClusterHSetCycle();
    totalLoaded += loaded.get();
    loaded.set(0);
    
    flushAll(client);
    
    // Load ordered sets each 1000 elements length
    runClusterZAddCycle();
    totalLoaded += loaded.get();
    
    shutdownAll(client, true);

    end = System.currentTimeMillis();
    
    NumberFormat formatter = NumberFormat.getInstance();
    

    System.out.println("\n************************ RESULTS *************************************");
    System.out.println("\nTOTAL ELEMENTS LOADED =" + formatter.format(totalLoaded) + " TOTAL TIME =" 
    + ((double)(end - start))/1000 + "s, EPS ="+  formatter.format((totalLoaded) * 1000/ (end - start)));
    
    client.close();
  }
  
  private static void runClusterSetCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterSet();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " sets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
  }
  
  private static void runClusterGetCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterGet();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " gets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
  }
  
  private static void runClusterMGetCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterMGet();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " gets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
  }
  
  private static void runClusterMSetCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterMSet();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " sets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
  }
  
  private static void runClusterPingPongCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterPingPong();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N * NUM_THREADS + " ping - pongs in "+ (end - start) + "ms. RPS="+ 
    (((long) NUM_THREADS * N) * 1000) / (end - start));
    
  }

  private static void runClusterSaddCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterSadd();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ (loaded.get()/SET_SIZE) + " sadd x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (loaded.get() * 1000) / (end - start));
    
  }
  
  private static void runClusterSismemberCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterSetIsMember();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ SET_KEY_TOTAL + " sismember x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (((long) SET_KEY_TOTAL * SET_SIZE) * 1000) / (end - start));
    
  }
  
  private static void runClusterHSetCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterHSet();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ (loaded.get() / SET_SIZE) + " hset x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (loaded.get() * 1000) / (end - start));
    
  }
  
  private static void runClusterHexistsCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterHexists();;} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ SET_KEY_TOTAL + " hexists x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (((long) SET_KEY_TOTAL * SET_SIZE) * 1000) / (end - start));
    
  }
  
  private static void runClusterZAddCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterZAdd();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ (loaded.get()/SET_SIZE) + " zadd x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (loaded.get() * 1000) / (end - start));
    
  }
  
  private static void runClusterZScoreCycle() {
    index.set(0);

    Runnable r = () -> { try {runClusterZScore();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
      workers[i].setName(Integer.toString(i));
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ SET_KEY_TOTAL + " hexists x ("+ SET_SIZE +") in "+ (end - start) + "ms. RPS="+ 
    (((long) SET_KEY_TOTAL * SET_SIZE) * 1000) / (end - start));
    
  }
  
  private static void flushAll(RawClusterClient client) throws IOException {
    client.flushAll();
  }

  private static void shutdownAll(RawClusterClient client, boolean save) throws IOException {
    long start = System.currentTimeMillis();
    client.saveAll();
    long end = System.currentTimeMillis();
    System.out.println("Save time="+ (end - start));
  }
  
  private static void runClusterSet() throws IOException, OperationFailedException {
    
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    System.out.println(Thread.currentThread().getName() + " SET started. , connect to :"+ list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;

    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx);//userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      String v = client.set(skey, svalue);
      //assertTrue(v.indexOf(svalue) > 0);
      if (count % 100000 == 0) {
        System.out.println(Thread.currentThread().getId() +": set "+ count);
      }
    }
    
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Loaded " + count +" user sessions," 
      + " in "+ (endTime - startTime) );
   
    client.close();
  }
  
 private static void runClusterMSet() throws IOException, OperationFailedException {
    
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    System.out.println(Thread.currentThread().getName() + " SET started. , connect to :"+ list.get(0));

    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    List<String> argList = new ArrayList<String>();
    int batch = 1;
    while (idx < N) {
      
      // Load up to BATCH_SIZE k-v pairs
      for (int i = 0; i < BATCH_SIZE; i++, idx += NUM_THREADS) {
        if (idx >= N) break;      
        UserSession us = UserSession.newSession(idx);//userSessions.get(idx);
        count++;
        String skey = us.getUserId();
        String svalue = us.toString();
        argList.add(skey);
        argList.add(svalue);
      }
      
      if (argList.size() == 0) break;
      
      String[] args = new String[argList.size()];
      argList.toArray(args);
      client.mset(args);
      if (count / 100000 >= batch) {
        System.out.println(Thread.currentThread().getId() +": set "+ count);
        batch++;
      }
      argList.clear();
    }
    
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Loaded " + count +" user sessions,"
      + " in "+ (endTime - startTime) );
   
    client.close();
  }
  
 private static void runClusterSadd() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " SADD started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);

   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     String result = client.sadd(key, setMembers);
     int added = toInt(result);
     count += added;
     if (added == 0) {
       break;
     }
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": sadd "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
       
   System.out.println(Thread.currentThread().getId() +": Loaded " + count +" set members,"
     + " in "+ (endTime - startTime) );
   loaded.addAndGet(count);
   client.close();
 } 
 
 private static void runClusterSetIsMember() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " SISMBER started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);
   
   Arrays.sort(setMembers);
   
   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     for(int i = 0; i < SET_SIZE; i++) {
       String member = setMembers[i];
       String result = client.sismember(key, member);
       if (":1\r\n".equals(result) == false) {
         System.err.println("sismember failed result="+ result);
         System.exit(-1);
       }
     }
     count += SET_SIZE;
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": sismember "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
       
   System.out.println(Thread.currentThread().getId() +": Loaded " + count +" set members,"
     + " in "+ (endTime - startTime) );
  
   client.close();
 } 
 
 private static void runClusterHSet() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " HSET started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);
   setMembers = interleave(setMembers); 
   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     String result = client.hset(key, setMembers);
     int added = toInt(result);
     count += added;
     if (added == 0) {
       break;
     }
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": hset "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
   loaded.addAndGet(count);
   System.out.println(Thread.currentThread().getId() +": Loaded " + count +" hash members,"
     + " in "+ (endTime - startTime) );
  
   client.close();
 } 
 
 private static String[] interleave(String[] arr) {
   String[] ret = new String[2 * arr.length];
   for (int i = 0, j = 0; i < arr.length; i++, j += 2) {
     ret[j] = arr[i];
     ret[j + 1] = arr[i];
   }
   return ret;
 }
 
 private static void runClusterHexists() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " HEXISTS started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);
   
   Arrays.sort(setMembers);
   
   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     for(int i = 0; i < SET_SIZE; i++) {
       String member = setMembers[i];
       String result = client.hexists(key, member);
       if (":1\r\n".equals(result) == false) {
         System.err.println("hexists failed result="+ result);
         System.exit(-1);
       }
     }
     count += SET_SIZE;
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": hexists "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
       
   System.out.println(Thread.currentThread().getId() +": Checked " + count +" hash fields,"
     + " in "+ (endTime - startTime) );
  
   client.close();
 } 
 
 private static void populate(String[] setMembers, long seed) {
   Random r = new Random(seed);
   for (int i = 0; i < setMembers.length; i++) {
     setMembers[i] = Integer.toString(r.nextInt());
   }
 }

 private static void runClusterZAdd() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " ZADD started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);
   double[] scores = new double[SET_SIZE];
   populate(scores, id);
   
   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     String result = client.zadd(key, scores, setMembers);
     int added = toInt(result);
     count += added;
     if (added == 0) {
       break;
     }
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": zadd "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
   loaded.addAndGet(count);
   System.out.println(Thread.currentThread().getId() +": Loaded " + count +" zset members,"
     + " in "+ (endTime - startTime) );
  
   client.close();
 } 
 
 private static int toInt(String s) {
   if (s.charAt(0) != ':') return 0;
   int len = s.length();
   return Integer.parseInt(s.substring(1, len - 2));
 }
 
 private static void populate(double[] scores, int id) {
   Random r = new Random(id);
   for (int i = 0; i < scores.length; i++) {
     scores[i] = r.nextDouble() * 1000;
   }
 }


 private static void runClusterZScore() throws IOException, OperationFailedException {
   
   int id = Integer.parseInt(Thread.currentThread().getName());
   List<String> list = new ArrayList<String>();
   list.add(clusterNodes.get(id % clusterNodes.size()));
   RawClusterClient client = new RawClusterClient(list);
   System.out.println(Thread.currentThread().getName() + " ZSCORE started. , connect to :"+ list.get(0));

   long startTime = System.currentTimeMillis();
   int count = 0;
   int idx = id;
   int batch = 1;
   String[] setMembers = new String[SET_SIZE];
   populate(setMembers, id);
   double[] scores = new double[SET_SIZE];
   populate(scores, id);
   
   for (; idx < SET_KEY_TOTAL; idx += NUM_THREADS) {
     UserSession us = UserSession.newSession(idx);
     String key = us.getUserId();
     for(int i = 0; i < SET_SIZE; i++) {
       String member = setMembers[i];
       String result = client.zscore(key, member);
       if ("$-1\r\n".equals(result) == true) {
         System.err.println("zscore failed result="+ result);
         System.exit(-1);
       }
       String exp = Double.toString(scores[i]);
       if (result.indexOf(exp) < 0) {
         System.err.println("zscore failed result="+ result);
         System.exit(-1);
       }
     }
     count += SET_SIZE;
     if (count / 1000000 >= batch) {
       System.out.println(Thread.currentThread().getId() +": zscore "+ count);
       batch++;
     }
   }
   
   long endTime = System.currentTimeMillis();
       
   System.out.println(Thread.currentThread().getId() +": Checked " + count +" zset fields,"
     + " in "+ (endTime - startTime) );
  
   client.close();
 } 


 private static void runClusterGet() throws IOException, OperationFailedException {
    
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    System.out.println(Thread.currentThread().getName() + " GET started. , connect to :"+ list.get(0));    
    long startTime = System.currentTimeMillis();
    int count = 0;
    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx);//userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      String v = client.get(skey);
      //assertTrue(v.indexOf(svalue) > 0);
      if (count % 100000 == 0) {
        System.out.println(Thread.currentThread().getId() +": get "+ count);
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Read " + count +" user sessions"
      + " in "+ (endTime - startTime) );
    client.close();
  }
 
  private static void runClusterMGet() throws IOException, OperationFailedException {
    
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    System.out.println(Thread.currentThread().getName() + " GET started. , connect to :"+ list.get(0));
    long startTime = System.currentTimeMillis();
    int count = 0;
    int idx = id;
    List<String> argList = new ArrayList<String>();
    //List<String> valList = new ArrayList<String>();
    int batch = 1;
    while (idx < N) {
      
      // Load up to BATCH_SIZE k-v pairs
      for (int i = 0; i < BATCH_SIZE; i++, idx += NUM_THREADS) {
        if (idx >= N) break;      
        UserSession us = UserSession.newSession(idx);//.get(idx);
        count++;
        String skey = us.getUserId();
        //String svalue = us.toString();
        argList.add(skey);
        //valList.add(svalue);
      }
      
      if (argList.size() == 0) break;
      
      String[] args = new String[argList.size()];
      argList.toArray(args);
      String s = client.mget(args);
      if (s.length() < 1000) {
        System.out.println("\n" + s + "\n");
      }
      //verify(valList, s);
      if (count / 100000 >= batch) {
        System.out.println(Thread.currentThread().getId() +": get "+ count+" s.length="+ s.length());
        batch++;
      }
      argList.clear();
      //valList.clear();
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Read " + count +" user sessions"
      + " in "+ (endTime - startTime) );
    client.close();
  }
  
  
  private static void verify(List<String> valList, String s) {
    for (String val: valList) {
      assertTrue(s.indexOf(val) > 0);
    }
  }
  
  public static void runClusterPingPong() throws IOException {
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    System.out.println(Thread.currentThread().getName() + " PING/PONG started. , connect to :"+ list.get(0));
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < N; i++) {
      String reply = client.ping();
      assertTrue(reply.indexOf("PONG") > 0);
      if ((i + 1) % 100000 == 0) {
        System.out.println(Thread.currentThread().getName() + ": pings "+ (i + 1));
      }
    }
    long endTime = System.currentTimeMillis();
    
    System.out.println(Thread.currentThread().getId() +": Ping-Pong " + N +" messages"
      + " in "+ (endTime - startTime) );
    client.close();
  }

  static class RawClusterClient {
    
    byte[] CRLF = new byte[] {(byte) '\r', (byte) '\n'};
    byte ARRAY = (byte) '*';
    byte STR = (byte) '$';
    
    List<SocketChannel> connList ;
    
    ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
    
    public RawClusterClient(List<String> nodes) {
      try {
        connList = new ArrayList<SocketChannel>();
        for (String node: nodes) {
          connList.add(openConnection(node));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    private SocketChannel openConnection(String node) throws IOException {
      String[] parts = node.split(":");
      String host = parts[0];
      int port = Integer.parseInt(parts[1]);
      
      SocketChannel sc = SocketChannel.open(new InetSocketAddress(host, port));
      sc.configureBlocking(false);
      sc.setOption(StandardSocketOptions.TCP_NODELAY, true);
      sc.setOption(StandardSocketOptions.SO_SNDBUF, 64 * 1024);
      sc.setOption(StandardSocketOptions.SO_RCVBUF, 64 * 1024);
      return sc;
    } 
    
    public String mset(String[] args) throws IOException {
      String[] newArgs = new String[args.length + 1];
      System.arraycopy(args, 0, newArgs, 1, args.length);
      newArgs[0] = "MSET";
      writeRequest(buf, newArgs);
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String set(String key, String value) throws IOException {
      writeRequest(buf, new String[] {"SET", key, value});
      int slot = Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }

    public String get(String key) throws IOException {
      writeRequest(buf, new String[] {"GET", key});
      int slot = Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }

    public String mget(String[] keys) throws IOException {
      
      String[] newArgs = new String[keys.length + 1];
      System.arraycopy(keys, 0, newArgs, 1, keys.length);
      newArgs[0] = "MGET";
      writeRequest(buf, newArgs);
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      int pos = buf.position();
      while(!Utils.arrayResponseIsComplete(buf)) {
        // Hack
       buf.position(pos);
       buf.limit(buf.capacity());
       channel.read(buf);
       pos = buf.position();
       continue;
      }
      buf.position(pos);
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public void close() throws IOException {
      for (SocketChannel sc: connList) {
        sc.close();
      }
    }
    
    static String[] ping_cmd = new String[] {"PING"};
    
    public String ping() throws IOException {
      int slot = 0;
      SocketChannel channel = connList.get(slot);
      writeRequest(buf, ping_cmd);
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    private String flushAll(SocketChannel channel) throws IOException {
      writeRequest(buf, new String[] {"FLUSHALL"});
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    private String save(SocketChannel channel) throws IOException {
      writeRequest(buf, new String[] {"SAVE"});
      buf.flip();
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public void flushAll() throws IOException {
      for (SocketChannel sc: connList) {
        flushAll(sc);
      }
    }
    
    public String sscan(String key, long cursor) throws IOException {
      writeRequest(buf, new String[] {"SSCAN", key, Long.toString(cursor)});
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String sadd(String key, String[] args) throws IOException {
      String[] newArgs = new String[args.length + 2];
      System.arraycopy(args, 0, newArgs, 2, args.length);
      newArgs[0] = "SADD";
      newArgs[1] = key;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String sismember(String key, String v) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "SISMEMBER";
      newArgs[1] = key;
      newArgs[2] = v;
      
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String hset(String key, String[] args) throws IOException {
      String[] newArgs = new String[args.length + 2];
      System.arraycopy(args, 0, newArgs, 2, args.length);
      newArgs[0] = "HSET";
      newArgs[1] = key;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String hexists(String key, String field) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "HEXISTS";
      newArgs[1] = key;
      newArgs[2] = field;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }

    public String expire(String key, int seconds) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "HEXISTS";
      newArgs[1] = key;
      newArgs[2] = Integer.toString(seconds);
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String zadd(String key, double[] scores, String[] fields) throws IOException {
      String[] newArgs = new String[2 * fields.length + 2];
      newArgs[0] = "ZADD";
      newArgs[1] = key;
      
      for(int i = 2; i < newArgs.length; i += 2) {
        newArgs[i] = Double.toString(scores[(i - 2)/2]);
        newArgs[i + 1] = fields[(i - 2) / 2];
      }
      
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }
    
    public String zscore(String key, String field) throws IOException {
      String[] newArgs = new String[3];
      newArgs[0] = "ZSCORE";
      newArgs[1] = key;
      newArgs[2] = field;
      writeRequest(buf, newArgs);
      buf.flip();
      int slot = 0;//Math.abs(key.hashCode()) % connList.size();
      SocketChannel channel = connList.get(slot);
      while(buf.hasRemaining()) {
        channel.write(buf);
      }
      buf.clear();
      
      while(buf.position() == 0) {
        // Hack
        channel.read(buf);
      }
      buf.flip();
      byte[] bytes = new byte[buf.limit()];
      buf.get(bytes);
      return new String (bytes);
    }

    
    public void saveAll() throws IOException {
      for (SocketChannel sc: connList) {
        save(sc);
      }
    }
    
    private void writeRequest(ByteBuffer buf, String[] args) {
      buf.clear();
      // Array
      buf.put(ARRAY);
      // number of elements
      buf.put(Integer.toString(args.length).getBytes());
      // CRLF
      buf.put(CRLF);
      for(int i = 0; i < args.length; i++) {
        buf.put(STR);
        buf.put(Integer.toString(args[i].length()).getBytes());
        buf.put(CRLF);
        buf.put(args[i].getBytes());
        buf.put(CRLF);
      }
    }    
  }
  
}
