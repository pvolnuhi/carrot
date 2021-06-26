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

import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.hashes.Hashes;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

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
public class TestCarrotAdServerUserTarget {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_USERS = 1000;
  
  public static void main(String[] args) {
    runTestNoCompression();
    runTestCompressionLZ4();
    runTestCompressionLZ4HC();
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
  
  private static void runTestCompressionLZ4HC() {
    System.out.println("\nTest , compression = LZ4HC");
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    runTest();
  }
  
  private static void runTest() {
    BigSortedMap map = new BigSortedMap(1000000000L);
    doUserViewWords(map);
    doUserActionWords(map);
    doUserViewAds(map);
    doUserActionAds(map);
    long memory = BigSortedMap.getTotalAllocatedMemory();
    System.out.println("Total memory=" + memory);
    map.dispose();
  }  
  
  private static void doUserViewWords(BigSortedMap map) {
    // SET
    System.out.println("Loading UserViewWords data");
    String key = "user:viewwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        long mPtr = UnsafeAccess.allocAndCopy(word, 0, word.length());
        int mSize = word.length();
        ZSets.ZADD(map, keyPtr, keySize, new double[] {r.nextDouble()}, new long[] {mPtr}, 
          new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("UserViewWords :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("UserViewWords : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doUserActionWords(BigSortedMap map) {
    // SET
    System.out.println("Loading UserActionWords data");
    String key = "user:actionwords:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_WORDS/100);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        long mPtr = UnsafeAccess.allocAndCopy(word, 0, word.length());
        int mSize = word.length();
        ZSets.ZADD(map, keyPtr, keySize, new double[] {r.nextDouble()}, new long[] {mPtr}, 
          new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("UserActionWords :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("UserActionWords : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doUserViewAds(BigSortedMap map) {
    System.out.println("Loading User View Ads data");
    String key = "user:viewads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < n; j++) {
        count++;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr, MAX_ADS - j);
        int views = r.nextInt(100); 
        long vPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int vSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(vPtr,  views);
        
        Hashes.HSET(map, keyPtr, keySize, mPtr, mSize, vPtr, vSize);
        UnsafeAccess.free(vPtr);
        UnsafeAccess.free(mPtr);
        if (count % 100000 == 0) {
          System.out.println("UserViewAds :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    
    long end = System.currentTimeMillis();
    System.out.println("UserViewAds : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doUserActionAds(BigSortedMap map) {
    System.out.println("Loading User Action Ads data");
    String key = "user:actionads:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_USERS; i++) {
      int n = r.nextInt(MAX_ADS/100);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < n; j++) {
        count++;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr, MAX_ADS - j);
        int views = r.nextInt(100); 
        long vPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int vSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(vPtr,  views);
        
        Hashes.HSET(map, keyPtr, keySize, mPtr, mSize, vPtr, vSize);
        UnsafeAccess.free(vPtr);
        UnsafeAccess.free(mPtr);
        if (count % 100000 == 0) {
          System.out.println("UserActionAds :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    
    long end = System.currentTimeMillis();
    System.out.println("UserActionAds : loaded "+ count + " in "+ (end-start)+"ms");

  }

}
