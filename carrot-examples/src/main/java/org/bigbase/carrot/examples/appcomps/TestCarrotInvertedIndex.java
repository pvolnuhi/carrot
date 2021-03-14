package org.bigbase.carrot.examples.appcomps;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.sets.Sets;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;

public class TestCarrotInvertedIndex {
  static int numWords = 1000;
  static int maxDocs = 5000;
  
  public static void main(String[] args) {
    runTestNoCompression();
    runTestCompressionLZ4();
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
    long vPtr = UnsafeAccess.malloc(8);
    int vSize = 8;
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
        long v = r.nextLong();
        UnsafeAccess.putLong(vPtr, v);
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
      System.exit(-1);
    }
    
    if (total != totalSize) {
      System.err.println("total set=" + total + " expected="+ totalSize);
      System.exit(-1);
    }
    
    long allocced = BigSortedMap.getTotalAllocatedMemory();
    System.out.println("Memory usage per (4-bytes) doc ID: " + ((double)allocced)/totalSize);
    map.dispose();
    UnsafeAccess.free(vPtr);
    Utils.freeKeys(keys);
  }
}
