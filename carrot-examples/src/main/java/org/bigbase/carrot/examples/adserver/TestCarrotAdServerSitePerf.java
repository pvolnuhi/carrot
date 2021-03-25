package org.bigbase.carrot.examples.adserver;

import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.zsets.ZSets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

/**
 * ----- Data structures to keep site performance
 * 
 * 13. SiteAdsRank: ZSET - ordered set. key = siteId, member = adId, score = CTR (click through rate). This data
 * allows us to estimate how does the ad perform on a particular site relative to other ads.
 * 
 * 14. SiteWordsRank: ZSET - ordered set. key = siteId, member = word, score - word's value. This data store keeps 
 *    keywords with corresponding scores. Every time someone acts on ads on the site, all keywords from the ad 
 *    are added to the site's ordered set with a some score value. The more a keyword appears in the ads - the higher
 *    it is going to be in the site's list.   This data allows us to estimate the most important keywords for the site
 *    as well as targeting attributes.
 * 
 * 
 * Results:
 * 
 * Redis 6.0.10             = 2,415,441,296 
 * Carrot no compression    =   713,665,920
 * Carrot LZ4 compression   =   672,803,840
 * Carrot LZ4HC compression =   670,337,280
 * 
 * Notes:
 * 
 * 1. The test uses synthetic data, which is mostly random and not compressible
 * 
 */
public class TestCarrotAdServerSitePerf {
  
  final static int MAX_ADS = 10000;
  final static int MAX_WORDS = 10000;
  final static int MAX_SITES = 1000;
  
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
    doSiteAdsRank(map);
    doSiteWordsRank(map);
    long memory = BigSortedMap.getTotalAllocatedMemory();
    System.out.println("Total memory=" + memory);
    map.dispose();
  }  
  
  private static void doSiteWordsRank(BigSortedMap map) {
    System.out.println("Loading SiteWordsRank data");
    String key = "sites:words:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_WORDS; j++) {
        String word = Utils.getRandomStr(r, 8);
        long mPtr = UnsafeAccess.allocAndCopy(word, 0, word.length());
        int mSize = word.length();
        ZSets.ZADD(map, keyPtr, keySize, new double[] {r.nextDouble()}, new long[] {mPtr}, 
          new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("SiteWordsRank :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("SiteWordsRank : loaded "+ count + " in "+ (end-start)+"ms");

  }
  
  private static void doSiteAdsRank(BigSortedMap map) {
    System.out.println("Loading  SiteAdsRank data");
    String key = "sites:ads:rank:";
    Random r = new Random();
    long count = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < MAX_SITES; i++) {
      String k = key + i;
      long keyPtr = UnsafeAccess.allocAndCopy(k, 0, k.length());
      int keySize = k.length();
      for (int j = 0; j < MAX_ADS; j++) {
        int adsId = j;
        long mPtr = UnsafeAccess.malloc(Utils.SIZEOF_INT);
        int mSize = Utils.SIZEOF_INT;
        UnsafeAccess.putInt(mPtr,  adsId);
        ZSets.ZADD(map, keyPtr, keySize, new double[] {r.nextDouble()}, new long[] {mPtr}, 
          new int[] {mSize}, true);
        UnsafeAccess.free(mPtr);
        count++;
        if (count % 100000 == 0) {
          System.out.println("SiteAdsRank :"+ count);
        }
      }
      UnsafeAccess.free(keyPtr);
    }
    long end = System.currentTimeMillis();
    System.out.println("SiteAdsRank : loaded "+ count + " in "+ (end-start)+"ms");

  }  
}
