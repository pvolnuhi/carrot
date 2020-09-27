package org.bigbase.carrot.redis.zsets;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ZSetsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  long n = 100000;
  List<Key> values;
  List<Double> scores;
  
  static {
   // UnsafeAccess.debug = true;
  }
  
  private List<Key> getKeys(long n) {
    List<Key> keys = new ArrayList<Key>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEYS SEED=" + seed);
    byte[] buf = new byte[keySize];
    for (int i=0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(keySize);
      UnsafeAccess.copy(buf, 0, ptr, keySize);
      keys.add(new Key(ptr, keySize));
    }
    return keys;
  }
  
  private List<Double> getScores(long n) {
    List<Double> scores = new ArrayList<Double>();
    Random r = new Random(1);
    for (int i = 0; i < n; i++) {
      scores.add(r.nextDouble());
    }
    return scores;
  }
  
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return key = new Key(ptr, keySize);
  }
  
  @Before
  public void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    values = getKeys(n);
    scores = getScores(n);
  }
  
  @Test
  public void testAddGetScore () {
    System.out.println("Test ZSet Add Get Score");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      scores[0] = this.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int)num);
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (keySize + Utils.SIZEOF_DOUBLE) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - (keySize + Utils.SIZEOF_DOUBLE))+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    int notFound = 0;
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      Double res = ZSets.ZSCORE(map, key.address, key.length, values.get(i).address, values.get(i).length);
      assertEquals(this.scores.get(i), res);
      notFound += res == null? 1: 0; 
    }
    end = System.currentTimeMillis();
    System.out.println("not found="+ notFound + " Time ZSCORE="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    //TODO 
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)ZSets.ZCARD(map, key.address, key.length));
 
  }
  
  @Test
  public void testAddRemove() {
    System.out.println("Test ZSet Add Get Score");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      scores[0] = this.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int)num);
      if (i % 1000000 == 0) {
        System.out.println(i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getTotalAllocatedMemory() 
    + " for "+ n + " " + (keySize + Utils.SIZEOF_DOUBLE) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getTotalAllocatedMemory()/n - (keySize + Utils.SIZEOF_DOUBLE))+
    " bytes per value. Time to load: "+(end -start)+"ms");
    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    int notFound = 0;
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtrs[0] = values.get(i).address;
      elemSizes[0] = values.get(i).length;
      long n = ZSets.ZREM(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, (int) n);
      notFound += n == 0? 1: 0; 
    }
    end = System.currentTimeMillis();
    System.out.println("not found="+ notFound + " Time ZREM="+(end -start)+"ms");
    BigSortedMap.memoryStats();
    //TODO 
    //ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)ZSets.ZCARD(map, key.address, key.length));
 
  }
  
  @After
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.free(key.address);
    for (Key k: values) {
      UnsafeAccess.free(k.address);
    }
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.memoryStats();
  }
}
