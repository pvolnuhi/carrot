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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.bigbase.carrot.examples.util.UserSession;
import org.bigbase.carrot.ops.OperationFailedException;

import redis.clients.jedis.Jedis;

/**
 * This example shows how to use Redis Strings to store user sessions objects 
 * 
 * User Session structure:
 * "SessionID" - A unique, universal identifier for the session data structure (16 bytes).
 * "Host" - host name or IP Address The location from which the client (browser) is making the request.
 * "UserID" - Set to the user's distinguished name (DN) or the application's principal name.
 * "Type" - USER or APPLICATION
 * "State" - session state: VALID, INVALID Defines whether the session is valid or invalid.
 * "MaxIdleTime" - Maximum Idle Time Maximum number of minutes without activity before the session will 
 *   expire and the user must reauthenticate.
 * "MaxSessionTime" - Maximum Session Time. Maximum number of minutes (activity or no activity) before 
 *   the session expires and the user must reauthenticate.
 * "MaxCachingTime" - Maximum number of minutes before the client contacts OpenSSO Enterprise to refresh 
 * cached session information.
 * 
 * Test description: <br>
 * 
 * UserSession object has 8 fields, one field (UserId) is used as a String key
 * 
 * Average key + session object size is 222 bytes. We load 100K user session objects
 * 
 * Results:
 * 0. Average user session data size = 222 bytes (includes key size)
 * 1. No compression. Used RAM per session object is 275 bytes (COMPRESSION= 0.8)
 * 2. LZ4 compression. Used RAM per session object is 94 bytes (COMPRESSION = 2.37)
 * 3. LZ4HC compression. Used RAM per session object is 88 bytes (COMPRESSION = 2.5)
 * 
 * Redis usage per session object, using String encoding is ~290 bytes
 * 
 * RAM usage (Redis-to-Carrot)
 * 
 * 1) No compression    290/275 ~ 1.16x
 * 2) LZ4   compression 290/94 ~ 3.4x
 * 3) LZ4HC compression 290/88 ~ 3.64x 
 * 
 * Effect of a compression:
 * 
 * LZ4  - 2.37/0.8 = 2.96    (to no compression)
 * LZ4HC - 2.5/0.8 = 3.13  (to no compression)
 * 
 *
 */
public class RedisStringsUserSessions {
  

  static long N = 10000000;
  static long totalDataSize = 0;
  static List<UserSession> userSessions = new ArrayList<UserSession>();
  static AtomicLong index = new AtomicLong(0);
  static int NUM_THREADS = 4;
  static {
    for (int i = 0; i < N; i++) {
      userSessions.add(UserSession.newSession(i));
    }
    Collections.shuffle(userSessions);
  }
  
  public static void main(String[] args) throws IOException, OperationFailedException {

    System.out.println("Run Redis Cluster");
    Jedis client = getClient();
    client.flushAll();
    Runnable r = () -> { try {runLoad();} catch (Exception e) {e.printStackTrace();}};
    Thread[] workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
    }
    long start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    long end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " sets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
    index.set(0);
    
    r = () -> { try {runGets();} catch (Exception e) {e.printStackTrace();}};
    workers = new Thread[NUM_THREADS];
    for (int i = 0; i < NUM_THREADS; i++) {
      workers[i] = new Thread(r);
    }
    start = System.currentTimeMillis();
    Arrays.stream(workers).forEach( x -> x.start());
    Arrays.stream(workers).forEach( x -> {try { x.join();} catch(Exception e) {}});
    end = System.currentTimeMillis();
    
    System.out.println("Finished "+ N + " gets in "+ (end - start) + "ms. RPS="+ 
    (((long) N) * 1000) / (end - start));
    
    start = System.currentTimeMillis();
    client.save();
    end = System.currentTimeMillis();
    System.out.println("DB save took "+ (end -start)+"ms");
    client.close();
  }
    
  private static Jedis getClient() {
    return new Jedis("localhost");
  }
  
  private static int getIndexes(int[] to) {
    int count = 0;
    for (int i = 0; i < to.length; i++) {
      int idx = (int) index.getAndIncrement();
      if (idx >= N) break;
      count++;
      to[i] = idx;
    }
    return count;
  }
  
  private static String[] getSetArgs(int[] idxs, int len) {
    String[] ret = new String[2 * len];
    for(int i = 0; i < len; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String skey = us.getUserId();
      String svalue = us.toString();
      ret[2 * i] = skey;
      ret[2 * i + 1] = svalue;
    }
    return ret;
  }
  
  private static String[] getGetArgs(int[] idxs, int len) {
    String[] ret = new String[len];
    for(int i = 0; i < len; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String skey = us.getUserId();
      ret[i] = skey;
    }
    return ret;
  }
  
  private static void runLoad() throws IOException, OperationFailedException {
    
    Jedis client = getClient();    
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count = 0;
    int[] idxs = new int[20];
    int batches = 1;
    for (;;) {
//      int idx = (int) index.getAndIncrement();
//      if (idx >= N) break;
//      UserSession us = userSessions.get(idx);
//      count++;
//      String skey = us.getUserId();
//      String svalue = us.toString();
//      totalDataSize += skey.length() + svalue.length();    
//      client.set(skey, svalue);
      int len = getIndexes(idxs);
      if (len == 0) break;
      String[] args = getSetArgs(idxs, len);
      client.mset(args);
      count += len;
      if (count / 100000 >= batches) {
        System.out.println(Thread.currentThread().getId() +": set "+ count);
        batches++;
      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Loaded " + count +" user sessions, total size="+totalDataSize
      + " in "+ (endTime - startTime) );
   
    client.close();
  }
  
  private static void runGets() throws IOException, OperationFailedException {
    
    Jedis client = getClient();    
    totalDataSize = 0;
    
    long startTime = System.currentTimeMillis();
    int count = 0;
    int batches = 1;
    int[] idxs = new int[20];
    for (;;) {
//      int idx = (int) index.getAndIncrement();
//      if (idx >= N) break;
//      UserSession us = userSessions.get(idx);
//      count++;
//      String skey = us.getUserId();
//      String svalue = us.toString();
//      totalDataSize += skey.length() + svalue.length();    
//      String v = client.get(skey);
//      assertTrue(v != null && v.length() == svalue.length());
      
      int len = getIndexes(idxs);
      if (len == 0) break;
      String[] args = getGetArgs(idxs, len);
      List<String> result = client.mget(args);
      count += len;
      if (count / 100000 >= batches) {
        System.out.println(Thread.currentThread().getId() +": get "+ count);
        batches++;
      }
      assertEquals(args.length, result.size());
      verify(result, idxs);
      
//      if (count % 10000 == 0) {
//        System.out.println(Thread.currentThread().getId() +": get "+ count);
//      }
    }
    long endTime = System.currentTimeMillis();
        
    System.out.println(Thread.currentThread().getId() +": Read " + count +" user sessions"
      + " in "+ (endTime - startTime) );
    client.close();
  }

  private static void verify(List<String> result, int[] idxs) {
    int size = result.size();
    for (int i= 0; i< size; i++) {
      UserSession us = userSessions.get(idxs[i]);
      String expected = us.toString();
      String value = result.get(i);
      assertNotNull(value);
      assertEquals(expected, value);
    }
  }
 
  
}
