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

package org.bigbase.carrot.examples.appcomps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.util.Bytes;


/**
 * Inverted index implemented accordingly to Redis Book:
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-7-search-based-applications/7-1-searching-in-redis/7-1-1-basic-search-theory/
 * 
 * We emulate 1000 words and some set of docs. Maximum occurrence of a single word is 5000 docs 
 * (random number between 1 and 5000). Each doc is coded by 4-byte integer. Each word is a random 8 byte string.
 * 
 * Format of an inverted index:
 * 
 * word -> {id1, id2, ..idk}, idn - 4 - byte integer
 * 
 * Redis takes 64.5 bytes per one doc id (which is 4 byte long)
 * Carrot takes 5.8 bytes
 * 
 * Redis - to - Carrot memory usage = 64.5/5.8 = 11.1
 * Because the data is poorly compressible we tested only Carrot w/o compression.
 * 
 * 
 */
import redis.clients.jedis.Jedis;


public class TestRedisInvertedIndex {
  static int numWords = 1000;
  static int maxDocs = 5000;
  
  public static void main(String[] args) throws IOException {
    runTest();
  }
  
  private static void runTest() throws IOException {
    Jedis client = new Jedis("localhost");
    
    Random r = new Random();
    long totalSize = 0;
    List<byte[]> keys = new ArrayList<byte[]>();
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < numWords; i++) {
      // all words are size of 4;
      byte[] key = new byte[4];
      r.nextBytes(key);
      keys.add(key);
      int max = r.nextInt(maxDocs) + 1;
      for (int j = 0; j < max; j++) {
        int v = r.nextInt();
        byte[] value = Bytes.toBytes(v);
        client.sadd(key, value);
        totalSize++;
      }
      if (i % 100 == 0) {
        System.out.println("Loaded " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Loaded " + totalSize + " in " + (end - start) + "ms. Press any button ...");
    System.in.read();
    for (byte[] k: keys) {
      client.del(k);
    }
    client.close();
  }
}
