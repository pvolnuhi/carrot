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
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * Redis Book. Chapter 7.3 "Ads targeting":
 * https://redislabs.com/ebook/part-2-core-concepts/chapter-7-search-based-applications/7-3-ad-targeting/
 * 
 * ----- Data structure used to keep ads indexes (please refer to the book above to understand the context):
 * 
 * 1. LocAd:   SET keeps location -> {id} - for every location keeps list of ad ids
 * 2. WordAds:  ZSET keeps list of ad ids for every word/keyword. word -> {id}
 * 3. AdType:  HASH keeps id-> type association (type = cpm, cpa, cpc) 
 * 4. AdeCPM:  ZSET keeps eCPM (estimated CPM) for every ad
 * 5. AdBase:  ZSET keeps base values for every ads
 * 6. AdWords: SET keeps list of words which can be targeted by a given ad.
 * 
 * 
 * ----- Data structures to keep user targeting:
 * 
 * 7. UserActionWords: ZSET keeps user behavior userId -> {word,score} We record user actions for every ad he/she acts on 
 *    the following way: if user acts on ad, we get the list of words targeted by this ad and increment score
 *    for every word in the user's ordered set. 
 * 8. UserViewWords: ZSET - the same as above but only for views (this data set is much bigger than in 7.)   
 * 9. UserViewAds: HASH keeps history of all ads shown to a user during last XXX minutes, hours, days. 
 * 10 UserActionAds: HASH keeps history of ads user clicked on during last XX minutes, hours, days. 
 * 
 * ----- Data structures to keep ads performance:
 * 
 * 11. AdSitePerf: HASH key = adId, {member1= siteId%'-V' value1 = views}, {member2= siteId%'-A' value2 = actions}
 * 
 * For every Ad and every site, this data keeps total number of views and actions
 * 
 * 12. AdSitesRank : ZSET key = adID, member = siteID, score = CTR (click-through-rate)
 * 
 * This data set allows to estimate performance of a given ad on a different sites.
 * 
 * ----- Data structures to keep site performance
 * 
 * 13. SiteAds: ZSET - ordered set. key = siteId, member = adId, score = CTR (click through rate). This data
 * allows us to estimate how does the ad perform on a particular site relative to other ads.
 * 
 * 14. SiteWords: ZSET - ordered set. key = siteId, member = word, score - word's value. This data store keeps 
 *    keywords with corresponding scores. Every time someone acts on ads on the site, all keywords from the ad 
 *    are added to the site's ordered set with a some score value. The more a keyword appears in the ads - the higher
 *    it is going to be in the site's list.   This data allows us to estimate the most important keywords for the site
 *    as well as targeting attributes.
 * 
 * What is the 'word' in this application? Word is not only keyword of an add, such as "car", "luxury", "lease", 
 * but also can be a targeting attribute, such as "@sex=female", "@age=20-30", "@income=100K+" etc. 
 * 
 * Notes (assumptions for our tests):
 * 
 * 1. Number of ads is 10K
 * 2. Number of words we estimate as 10K 
 * 3. Number of locations we estimate as 10K
 * 4. Number of users of the ad targeting platforms we estimate as 10M 
 * 5. Number of sites is 10K.
 *   
 * 
 * Results (memory usage):
 * 
 * -- Ads indexing
 * 
 * Redis 6.0.10               - 888,155,696
 * Carrot (no compression)    - 154,989,056
 * Carrot (LZ4 compression)   -  73,236,608
 * Carrot (LZ4HC compression) -  72,454,784
 * 
 * -- User targeting
 * 
 * -- Ads performance
 * 
 * -- Site performance
 * 
 * @author vrodionov
 *
 */
public class TestCarrotAdServerAdIndexing {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_LOCATIONS = 10000;
  
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
    BigSortedMap map = new BigSortedMap(10000000000L);
    doLocAds(map);
    doAddTypes(map);
    doAdeCPM(map);
    doAdBase(map);
    doWordAds(map);
    doAdWords(map);
    long memory = BigSortedMap.getGlobalAllocatedMemory();
    System.out.println("Total memory=" + memory);
    map.dispose();
  }  
  
  private static void doLocAds(BigSortedMap map) {
    // SET
    System.out.println("Loading Location - AdId data");
    String key = "idx:locads:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_LOCATIONS; i++) {
      int n = r.nextInt(1000);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < n; j++) {
        int id = r.nextInt(MAX_ADS) + 1;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr,  id);
        Sets.SADD(map, keyPtr, keySize, mPtr, mSize);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("LocAds :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("LocAds : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doWordAds(BigSortedMap map) {
    System.out.println("Loading Word - AdId data");

    String key = "idx:wordads:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_WORDS; i++) {
      int n = r.nextInt(1000);
      String k = key + Utils.getRandomStr(r, 8);
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < n; j++) {
        int id = r.nextInt(MAX_ADS) + 1;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr,  id);
        ZSets.ZADD(map, keyPtr, keySize, new double[] {0}, new long[] {mPtr}, new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("WordAds :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("WordAds : loaded "+ count + " in "+ (end-start)+"ms");
  }
  
  static enum Type {
    CPM, CPA, CPC;
  }
  
  private static void doAddTypes(BigSortedMap map) {
    System.out.println("Loading Ad - Type data");

    String key = "type:";
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      int n = r.nextInt(3);
      Type type = Type.values()[n];
      long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
      int mSize = Utils.SIZEOF_INT;
      UnsafeAccess.putInt(mPtr, i);
      String val = type.name();
      long vPtr = UnsafeAccess.allocAndCopy(val, 0, val.length());
      int vSize = val.length();
      Hashes.HSET(map, keyPtr, keySize, mPtr, mSize, vPtr, vSize);
      UnsafeAccess.free(vPtr);
      UnsafeAccess.free(mPtr);
    }
    UnsafeAccess.free(keyPtr);
    
    long end = System.currentTimeMillis();
    System.out.println("AdType : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");

  }
  
  private static void doAdeCPM(BigSortedMap map) {
    System.out.println("Loading Ad - eCPM data");

    String key = "idx:ad:value:";
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
      int mSize = Utils.SIZEOF_INT;
      UnsafeAccess.putInt(mPtr, i);
      double eCPM = 0.5 * r.nextDouble();
      ZSets.ZADD(map, keyPtr, keySize, new double[] {eCPM}, new long[] {mPtr}, new int[] {mSize}, true);
      UnsafeAccess.free(mPtr);
    }
    UnsafeAccess.free(keyPtr);
    long end = System.currentTimeMillis();
    System.out.println("AdeCPM : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");
  }
  
  private static void doAdBase(BigSortedMap map) {
    System.out.println("Loading Ad - Base value data");

    String key = "idx:ad:base_value:";
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int keySize = key.length();
    Random r = new Random();
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
      int mSize = Utils.SIZEOF_INT;
      UnsafeAccess.putInt(mPtr, i);
      double base = 5 * r.nextDouble();
      ZSets.ZADD(map, keyPtr, keySize, new double[] {base}, new long[] {mPtr}, new int[] {mSize}, true);
      UnsafeAccess.free(mPtr);
    }
    UnsafeAccess.free(keyPtr);
    long end = System.currentTimeMillis();
    System.out.println("AdBase : loaded "+ MAX_ADS + " in "+ (end-start)+"ms");
  }
  
  private static void doAdWords(BigSortedMap map) {
    // SET
    System.out.println("Loading Ad - Words data");
    String key = "idx:terms:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      int n = r.nextInt(100);
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < n; j++) {
        String word = Utils.getRandomStr(r, 8);
        long mPtr = UnsafeAccess.allocAndCopy(word, 0, word.length());
        int mSize = word.length();
        Sets.SADD(map, keyPtr, keySize, mPtr, mSize);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("AdWords :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("AdWords : loaded "+ count + " in "+ (end-start)+"ms");
  }
}
