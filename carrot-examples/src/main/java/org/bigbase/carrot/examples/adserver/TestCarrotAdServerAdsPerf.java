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
 * 
 * Results:
 * 
 * Redis 6.0.10             = 1,524,734,024
 * Carrot no compression    =   634,412,288
 * Carrot LZ4 compression   =   455,271,552
 * Carrot LZ4HC compression =   426,146,816
 * 
 * Notes:
 * 
 * 1. The test uses synthetic data, which is mostly random and not compressible
 * 2. For AdSitePerf dataset we use compact hashes in Redis to minimize memory usage. 
 * 
 */
public class TestCarrotAdServerAdsPerf {
  
  final static int MAX_ADS = 1000;
  final static int MAX_SITES = 10000;
  
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
    doAdsSitePerf(map);
    doAdsSiteRank(map);
    long memory = BigSortedMap.getTotalAllocatedMemory();
    System.out.println("Total memory=" + memory);
    map.dispose();
  }  
  
  private static void doAdsSiteRank(BigSortedMap map) {
    System.out.println("Loading AdsSiteRank data");
    String key = "ads:sites:rank";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_SITES; j++) {
        int siteId = j;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr,  siteId);
        ZSets.ZADD(map, keyPtr, keySize, new double[] {r.nextDouble()}, new long[] {mPtr}, 
          new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("AdsSiteRank :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("AdsSiteRank : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doAdsSitePerf(BigSortedMap map) {
    System.out.println("Loading Ads-Site Performance data");
    String key = "ads:site:perf:";
    Random r = new Random();
    long start = System.currentTimeMillis();
    long count = 0;
    for (int i = 0; i < MAX_ADS; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j=0; j < MAX_SITES; j++) {
        count++;
        long[] mPtrs = new long[] { UnsafeAccess.malloc(Utils.SIZEOF_INT + 1), 
                                    UnsafeAccess.malloc(Utils.SIZEOF_INT + 1)};
        
        int[] mSizes = new int[] { Utils.SIZEOF_INT + 1, Utils.SIZEOF_INT + 1};
        UnsafeAccess.putInt(mPtrs[0], MAX_ADS - j);
        UnsafeAccess.putByte(mPtrs[0] + Utils.SIZEOF_INT,(byte) 0);
        UnsafeAccess.putInt(mPtrs[1], MAX_ADS - j);
        UnsafeAccess.putByte(mPtrs[1] + Utils.SIZEOF_INT,(byte) 1);
        
        
        long[] vPtrs = new long[] { UnsafeAccess.malloc(Utils.SIZEOF_INT), 
                                    UnsafeAccess.malloc(Utils.SIZEOF_INT)
        };
        int[] vSizes = new int[] { Utils.SIZEOF_INT, Utils.SIZEOF_INT};
        UnsafeAccess.putInt(vPtrs[0],  r.nextInt(100));
        UnsafeAccess.putInt(vPtrs[1],  r.nextInt(100));

        Hashes.HSET(map, keyPtr, keySize, mPtrs, mSizes, vPtrs, vSizes);
        UnsafeAccess.free(vPtrs[0]);
        UnsafeAccess.free(vPtrs[1]);
        UnsafeAccess.free(mPtrs[0]);
        UnsafeAccess.free(mPtrs[1]);
        
        if (count % 100000 == 0) {
          System.out.println("AdsSitePerf :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    
    long end = System.currentTimeMillis();
    System.out.println("AdsSitePerf : loaded "+ count + " in "+ (end-start)+"ms");

  }
  

}
