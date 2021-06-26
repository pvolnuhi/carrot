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

import redis.clients.jedis.Jedis;


public class TestRedisAdServerAdIndexing {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_LOCATIONS = 10000;
  
  public static void main(String[] args) throws IOException {
    runTest();
  }  
  
  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");
    doLocAds(client);
    doAddTypes(client);
    doAdeCPM(client);
    doAdBase(client);
    doWordAds(client);
    doAdWords(client);
    System.out.println("Press any button ...");
    System.in.read();
    client.flushAll();
  }  
  
  private static void doLocAds(Jedis client) {
    // SET
    System.out.println("Loading Location - AdId data");
    String key = "idx:locads:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_LOCATIONS; i++) {
      int n = r.nextInt(1000);
      String k = key + i;
      for (int j=0; j < n; j++) {
        int id = r.nextInt(MAX_ADS) + 1;
        client.sadd(k.getBytes(), Bytes.toBytes(id));
        count++;
        if (count % 100000 == 0) {
          System.out.println("LocAds :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("LocAds : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static String getWord(Random r) {
    int start = 'A';
    int stop = 'z';
    StringBuffer sb = new StringBuffer(8);
    for (int i=0; i < 8; i++) {
      int v = r.nextInt(stop - start) + start;
      sb.append((char)v);
    }
    return sb.toString();
  }
  
  private static void doWordAds(Jedis client) {
    System.out.println("Loading Word - AdId data");
    String key = "idx:wordads:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_WORDS; i++) {
      int n = r.nextInt(1000);
      String k = key + getWord(r);
      for (int j=0; j < n; j++) {
        int id = r.nextInt(MAX_ADS) + 1;
        client.zadd(k.getBytes(), 0, Bytes.toBytes(id));
        count++;
        if (count % 100000 == 0) {
          System.out.println("WordAds :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("WordAds : loaded "+ count + " in "+ (end-start)+"ms");
  }
  
  static enum Type {
    CPM, CPA, CPC;
  }
  
  private static void doAddTypes(Jedis client) {
    System.out.println("Loading Ad - Type data");
    String key = "type:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      int n = r.nextInt(3);
      Type type = Type.values()[n];
      String val = type.name();
      client.hset(key.getBytes(), Bytes.toBytes(i), val.getBytes());
    }
    long end = System.currentTimeMillis();
    System.out.println("AdType : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");
  }
  
  private static void doAdeCPM(Jedis client) {
    System.out.println("Loading Ad - eCPM data");
    String key = "idx:ad:value:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {     
      double eCPM = 0.5 * r.nextDouble();
      client.zadd(key.getBytes(), eCPM, Bytes.toBytes(i));
    }
    long end = System.currentTimeMillis();
    System.out.println("AdeCPM : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");
  }
  
  private static void doAdBase(Jedis client) {
    System.out.println("Loading Ad - Base value data");

    String key = "idx:ad:base_value:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      double base = 5 * r.nextDouble();
      client.zadd(key.getBytes(), base, Bytes.toBytes(i));
    }
    long end = System.currentTimeMillis();
    System.out.println("AdBase : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");
  }
  
  private static void doAdWords(Jedis client) {
    // SET
    System.out.println("Loading Ad - Words data");
    String key = "idx:terms:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      int n = r.nextInt(100);
      String k = key + i;

      for (int j=0; j < n; j++) {
        String word = getWord(r);
        client.sadd(k.getBytes(), word.getBytes());
        count++;
        if (count % 100000 == 0) {
          System.out.println("AdWords :"+ count);
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("AdWords : loaded "+ count + " in "+ (end-start)+"ms");
  }
}
