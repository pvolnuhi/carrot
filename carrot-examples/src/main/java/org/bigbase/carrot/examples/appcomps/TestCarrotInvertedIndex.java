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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Inverted index implemented accordingly to the Redis Book:
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

public class TestCarrotInvertedIndex {
  static int numWords = 1000;
  static int maxDocs = 5000;
  
  public static void main(String[] args) {
    runTestNoCompression();
    //runTestCompressionLZ4();
    //runTestCompressionLZ4HC();
  }
  
  private static void runTestNoCompression() {
    System.out.println("\nTest , compression = None");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    runTest();
  }
  

  private static void runTestCompressionLZ4() {
    System.out.println("\nTest , compression = LZ4");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    runTest();
  }
  
  @SuppressWarnings("unused")
  private static void runTestCompressionLZ4HC() {
    System.out.println("\nTest , compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
  }
  
  private static void runTest() {
    BigSortedMap map = new BigSortedMap(1000000000);
    Random r = new Random();
    int kSize = 8;
    long vPtr = UnsafeAccess.malloc(4);
    int vSize = 4;
    byte[] buf = new byte[kSize];
    long totalSize = 0;
    List<Key> keys = new ArrayList<Key>();
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < numWords; i++) {
      // all words are size of 8;
      r.nextBytes(buf);
      long kPtr = UnsafeAccess.malloc(kSize);
      UnsafeAccess.copy(buf,  0,  kPtr, kSize);
      keys.add(new Key(kPtr, kSize));
      int max = r.nextInt(maxDocs) + 1;
      for (int j =0; j < max; j++) {
        int v = Math.abs(r.nextInt());
        UnsafeAccess.putInt(vPtr, v);
        Sets.SADD(map, kPtr, kSize, vPtr, vSize);
        totalSize++;
      }
      if (i % 100 == 0) {
        System.out.println("Loaded " + i);
      }
    }
    
    long end = System.currentTimeMillis();
    
    System.out.println("Loaded " + totalSize + " in " + (end - start)+"ms");
    
    long total = 0;
    int totalKeys = 0;
    start = System.currentTimeMillis();
    for (Key k: keys) {
      long card = Sets.SCARD(map, k.address, k.length);
      if (card > 0) {
        totalKeys++;
        total+= card;
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Check CARD " + totalSize + " in " + (end - start)+"ms");

    
    if (totalKeys != numWords) {
      System.err.println("total keys=" + totalKeys + " expected="+ numWords);
      //System.exit(-1);
    }
    
    if (total != totalSize) {
      System.err.println("total set=" + total + " expected="+ totalSize);
      //System.exit(-1);
    }
    
    long allocced = BigSortedMap.getGlobalAllocatedMemory();
    System.out.println("Memory usage per (4-bytes) doc ID: " + ((double)allocced)/totalSize);
    System.out.println("Memory usage: " + allocced);

    map.dispose();
    UnsafeAccess.free(vPtr);
    Utils.freeKeys(keys);
  }
}
