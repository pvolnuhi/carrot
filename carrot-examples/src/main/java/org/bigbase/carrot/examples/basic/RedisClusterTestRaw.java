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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.ops.OperationFailedException;
import org.bigbase.carrot.redis.util.Utils;


public class RedisClusterTestRaw {
  
  
  static long N = 1000000;
  
  static long totalDataSize = 0;
    
  static AtomicLong index = new AtomicLong(0);
  
  static int NUM_THREADS = 1;
  
  static int BATCH_SIZE = 100;
  
  static List<String> clusterNodes = Arrays.asList("localhost:6379" /*, "localhost:6380",
    "localhost:6381", "localhost:6382", "localhost:6383", "localhost:6384", "localhost:6385", "localhost:6386"*/); 
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("Run Redis Cluster");
    RawClusterClient client = new RawClusterClient(clusterNodes);
    flushAll(client);
    
//    runClusterPingPongCycle();   
    runClusterSetCycle();
    runClusterGetCycle();
//    runClusterMSetCycle();
//    runClusterMGetCycle();
        
    shutdownAll(client, true);
    client.close();
  }
  
  
  @SuppressWarnings("unused")
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
  
  @SuppressWarnings("unused")
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
  
  @SuppressWarnings("unused")
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
  
  @SuppressWarnings("unused")
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
  
  @SuppressWarnings("unused")
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
  
  private static void flushAll(RawClusterClient client) throws IOException {
    client.flushAll();
  }

  private static void shutdownAll(RawClusterClient client, boolean save) throws IOException {
    client.saveAll();
  }
  
  @SuppressWarnings("unused")
  private static void runClusterSet() throws IOException, OperationFailedException {
    
    RawClusterClient client = new RawClusterClient(clusterNodes);

    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count = 0;

    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx);//userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      totalDataSize += skey.length() + svalue.length();    
      String v = client.set(skey, svalue);
      //assertTrue(v.indexOf(svalue) > 0);
      if (count % 100000 == 0) {
        System.out.println(Thread.currentThread().getId() +": set "+ count);
      }
    }
    
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Loaded " + count +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
   
    client.close();
  }
  
 private static void runClusterMSet() throws IOException, OperationFailedException {
    
    int id = Integer.parseInt(Thread.currentThread().getName());
    List<String> list = new ArrayList<String>();
    list.add(clusterNodes.get(id % clusterNodes.size()));
    RawClusterClient client = new RawClusterClient(list);
    totalDataSize = 0;
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
        totalDataSize += skey.length() + svalue.length(); 
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
        
    System.out.println(Thread.currentThread().getId() +": Loaded " + count +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
   
    client.close();
  }
  
  
  @SuppressWarnings("unused")
  private static void runClusterGet() throws IOException, OperationFailedException {
    
    RawClusterClient client = new RawClusterClient(clusterNodes);
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count = 0;
    for (;;) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      UserSession us = UserSession.newSession(idx);//userSessions.get(idx);
      count++;
      String skey = us.getUserId();
      String svalue = us.toString();
      totalDataSize += skey.length() + svalue.length();    
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
    
    totalDataSize = 0;
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
  
  
  @SuppressWarnings("unused")
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
