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

package org.bigbase.carrot.examples.adserver;

import java.io.IOException;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Utils;

import redis.clients.jedis.Jedis;

/**
 * ----- Data structures to keep user targeting:
 * 
 * 7. UserActionWords: ZSET keeps user behavior userId -> {word,score} We record user actions for every 
 *    ad he/she acts on in the following way: if user acts on ad, we get the list of words targeted by 
 *    this ad and increment score for every word in the user's ordered set. 
 * 8. UserViewWords: ZSET - the same as above but only for views (this data set is much bigger than in 7.)   
 * 9. UserViewAds: HASH keeps history of all ads shown to a user during last XXX minutes, hours, days. 
 * 10 UserActionAds: HASH keeps history of ads user clicked on during last XX minutes, hours, days. 
 * 
 * Results:
 * 
 * Redis 6.0.10             = 992,291,824
 * Carrot no compression    = 254,331,520
 * Carrot LZ4 compression   = 234,738,048
 * Carrot LZ4HC compression = 227,527,936
 * 
 * Notes:
 * 
 * The test uses synthetic data, which is mostly random and not compressible
 * 
 */
public class TestRedisAdServerUserTarget {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_USERS = 1000;
  
  public static void main(String[] args) throws IOException {
    runTest();
  }

  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");
    doUserViewWords(client);
    doUserActionWords(client);
    doUserViewAdds(client);
    doUserActionAdds(client);
    System.out.println("Press any button ...");
    System.in.read();
    client.flushAll();
    client.close();
  }  
  
  private static void doUserViewWords(Jedis client) {
    // SET
    System.out.println("Loading UserViewWords data");
    String key = "user:viewwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS);
      String k = key + i;      
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        client.zadd(k.getBytes(), r.nextDouble(), word.getBytes());
        count++;
        if (count % 100000 == 0) {
          System.out.println("UserViewWords :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("UserViewWords : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doUserActionWords(Jedis client) {
    // SET
    System.out.println("Loading UserActionWords data");
    String key = "user:actionwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS/100);
      String k = key + i;      
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        client.zadd(k.getBytes(), r.nextDouble(), word.getBytes());
        count++;
        if (count % 100000 == 0) {
          System.out.println("UserViewWords :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("UserActionWords : loaded "+ count + " in "+ (end-start)+"ms");
  }
  
  private static void doUserViewAdds(Jedis client) {
    System.out.println("Loading User View Ads data");
    String key = "user:viewads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS);
      String k = key + i;
      for (int j=0; j < n; j++) {
        count++;
        int id = MAX_ADS - j;
        int views = r.nextInt(100); 
        client.hset(k.getBytes(), Bytes.toBytes(id), Bytes.toBytes(views));
        if (count % 100000 == 0) {
          System.out.println("UserViewAds :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("UserViewAds : loaded "+ count + " in "+ (end-start)+"ms");
  }
  
  private static void doUserActionAdds(Jedis client) {
    System.out.println("Loading User Action Ads data");
    String key = "user:actionads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS/100);
      String k = key + i;
      for (int j=0; j < n; j++) {
        count++;
        int id = MAX_ADS - j;
        int views = r.nextInt(100); 
        client.hset(k.getBytes(), Bytes.toBytes(id), Bytes.toBytes(views));
        if (count % 100000 == 0) {
          System.out.println("UserViewAds :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("UserActionAds : loaded "+ count + " in "+ (end-start)+"ms");
  }
}
