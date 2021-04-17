package org.bigbase.carrot.redis.zsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Pair;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class ZSetsAPITest {
  BigSortedMap map;
  long n = 1000;
  static Random rnd = new Random();
  
  static {
    long seed = rnd.nextLong();
    rnd.setSeed(seed);
    System.out.println("Global seed="+ seed);
  }
  
  private List<Pair<String>> loadData(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 8);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] {m}, new double[] {sc}, true);
      assertEquals(1, (int)res);
      if ((i+1) % 100000 == 0) {
        System.out.println("Loaded "+ i);
      }
    }
    Collections.sort(list);
    return list;
  }
  
  private List<Pair<String>> getData(int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 8);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
    }
    Collections.sort(list);
    return list;
  }
  
  private Map<String,List<Pair<String>>> loadDataMap(int numKeys, int n) {
    
    Map<String, List<Pair<String>>> map = new HashMap<String, List<Pair<String>>>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Data map seed="+ seed);
    for (int i = 0; i < numKeys; i++) {
      String key = Utils.getRandomStr(r, 10);
      map.put (key, loadData(key, n));
    }
    return map;
  }
  
  private List<Pair<String>> loadDataRandomSize(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    for (int i=0; i < n; i++) {
      int size = rnd.nextInt(10) + 5;
      String m = Utils.getRandomStr(rnd, size);
      
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] {m}, new double[] {sc}, true);
      assertEquals(1, (int)res);
      if ((i+1) % 100000 == 0) {
        System.out.println("Loaded "+ i);
      }
    }
    Collections.sort(list);
    return list;
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4");
      allTests();
      BigSortedMap.printMemoryAllocationStats();      
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  @Ignore
  @Test
  public void runAllCompressionLZ4HC() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() throws IOException {
//    setUp();
//    testAddScoreMultiple();
//    tearDown();
//    setUp();
//    testAddScoreIncrementMultiple();
//    tearDown();
//    setUp();
//    testAddDelete();
//    tearDown();
//    setUp();
//    testAddRemove();
//    tearDown();
    setUp();
    testIncrement();
    tearDown();
    setUp();
    testADDCorrectness();
    tearDown();
  }
  
  @Ignore
  @Test
  public void testAddScoreMultiple() {
    System.out.println("ZSets ZADD/ZSCORE multiple keys test");
    int numKeys = 1000;
    int numMembers = 100;
    Map<String, List<Pair<String>>> data = loadDataMap(numKeys, numMembers);
    
    for(String key: data.keySet()) {
      List<Pair<String>> list = data.get(key);
      for(Pair<String> p: list) {
        String member = p.getFirst();
        double score = ZSets.ZSCORE(map, key, member);
        assertEquals(score, Double.parseDouble(p.getSecond()));
      }
    }
  }
  
  @Ignore
  @Test
  public void testAddScoreIncrementMultiple() {
    System.out.println("ZSets ZADD/ZSCORE increment multiple keys test");
    int numKeys = 1000;
    int numMembers = 1000;
    Map<String, List<Pair<String>>> data = loadDataMap(numKeys, numMembers);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    int count = 0;
    for(String key: data.keySet()) {
      count++;
      List<Pair<String>> list = data.get(key);
      for(Pair<String> p: list) {
        String member = p.getFirst();
        double expected = Double.parseDouble(p.getSecond());
        double score = ZSets.ZSCORE(map, key, member);
        assertEquals(score, expected);
        double incr = r.nextDouble() * r.nextInt();
        double newValue = ZSets.ZINCRBY(map, key, incr, member);
        assertEquals(expected + incr , newValue);
        score = ZSets.ZSCORE(map, key, member);
        assertEquals(expected + incr, score);
      }
      if (count % 100 == 0) {
        System.out.println(count);
      }
    }
  }
  
  @Ignore
  @Test
  public void testAddDelete() {
    System.out.println("ZSets ZADD/DELETE multiple keys test");
    int numKeys = 1000;
    int numMembers = 1000;
    Map<String, List<Pair<String>>> data = loadDataMap(numKeys, numMembers);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    for(String key: data.keySet()) {
      boolean res = ZSets.DELETE(map, key);
      assertTrue(res);
    }
    long count = BigSortedMap.countRecords(map);
    assertEquals(0, (int) count);
    BigSortedMap.printMemoryAllocationStats();
  }
  
  @Ignore
  @Test
  public void testAddRemove() {
    System.out.println("ZSets ZADD/ZREM multiple keys test");
    int numKeys = 1000;
    int numMembers = 1000;
    Map<String, List<Pair<String>>> data = loadDataMap(numKeys, numMembers);
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    int count = 0;
    for(String key: data.keySet()) {
      count++;
      List<Pair<String>> list = data.get(key);
      for(Pair<String> p: list) {
        long res = ZSets.ZREM(map, key, p.getFirst());
        assertEquals(1, (int) res);
      }
      if (count % 100 == 0) {
        System.out.println(count);
      }
    }
    long c = BigSortedMap.countRecords(map);
    assertEquals(0, (int) c);
    BigSortedMap.printMemoryAllocationStats();
  }
  
  @Ignore
  @Test
  public void testIncrement() {
    System.out.println("ZSets ZINCRBY multiple keys test");
    int numMembers = 10000;
    String key = "key";
    List<Pair<String>> data = getData(numMembers); 
    int count = 0;
    for (Pair<String> p: data) {
      count++;
      double score = Double.parseDouble(p.getSecond());
      double value = ZSets.ZINCRBY(map, key, score, p.getFirst());
      assertEquals(score, value);
      long card = ZSets.ZCARD(map, key);
      assertEquals(count, (int) card);
      value = ZSets.ZINCRBY(map, key, score, p.getFirst());
      assertEquals(score + score, value);
      card = ZSets.ZCARD(map, key);
      assertEquals(count, (int) card);
    }
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    long c = BigSortedMap.countRecords(map);
    assertEquals(0, (int) c);
    BigSortedMap.printMemoryAllocationStats();
  }
  
  @Ignore
  @Test
  public void testADDCorrectness() {
    System.out.println("Test ZADD/ZADDNX/ZADDXX API correctness");
    // ZADD adds new or replaces existing
    // ZADDNX adds only if not exists
    // ZADDXX replaces existing one
    Random r = new Random();
    int numMembers = 10000;
    String key = "key";
    List<Pair<String>> data = loadData(key, numMembers); 
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int)card);
    
    // load again with new scores
    
    for(Pair<String> p: data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADD(map, key,  new String[] {p.getFirst()}, new double[] {score}, false);
      assertEquals(1, (int)res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(newScore);
      assertEquals(score, newScore);
    }
    
    // ZADDNX
    
    for(Pair<String> p: data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADDNX(map, key,  new String[] {p.getFirst()}, new double[] {score}, false);
      assertEquals(0, (int)res);
    }
    
    // Delete set
    boolean result = ZSets.DELETE(map, key);
    assertTrue(result);
    
    // load again with ZADDNX
    for(Pair<String> p: data) {
      double score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADDNX(map, key,  new String[] {p.getFirst()}, new double[] {score}, false);
      assertEquals(1, (int)res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(newScore);
      assertEquals(score, newScore);
    }
    
    // ZADDXX
    for(Pair<String> p: data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADDXX(map, key,  new String[] {p.getFirst()}, new double[] {score}, false);
      assertEquals(1, (int)res);
    }
    
    // Delete set
    result = ZSets.DELETE(map, key);
    assertTrue(result);
    
    // Try loading again with ZADDXX
    for(Pair<String> p: data) {
      double score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADDXX(map, key,  new String[] {p.getFirst()}, new double[] {score}, false);
      assertEquals(0, (int)res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNull(newScore);
    }
  }
  
  public void setUp() {
    map = new BigSortedMap(200000000);
  }
  
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
