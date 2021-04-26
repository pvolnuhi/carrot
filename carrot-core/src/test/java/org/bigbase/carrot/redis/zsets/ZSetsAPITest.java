package org.bigbase.carrot.redis.zsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    long seed = -3824502021763433509L;//rnd.nextLong();
    rnd.setSeed(seed);
    System.out.println("Global seed=" + seed);
  }

  /*
   * Loads data and sort it my field
   */
  private List<Pair<String>> loadData(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();

    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 12);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] { m }, new double[] { sc }, false);
      assertEquals(1, (int) res);
      if ((i + 1) % 100000 == 0) {
        System.out.println("Loaded " + i);
      }
    }
    Collections.sort(list);
    return list;
  }

  
  private long dataSeed;
  /*
   * Loads data and sort it my field
   */
  private List<Pair<String>> loadDataSortByScore(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    Random rnd = new Random();
    long seed = rnd.nextLong();
    rnd.setSeed(seed);
    dataSeed = seed;
    //System.out.println("Data seed="+ seed);
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 12);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      long res = ZSets.ZADD(map, key, new String[] { m }, new double[] { sc }, false);
      assertEquals(1, (int) res);
      list.add(new Pair<String>(m, score));
      if ((i + 1) % 100000 == 0) {
        System.out.println("Loaded " + i);
      }
    }
    Collections.sort(list, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    }); 
    return list;
  }
  
  private List<Pair<String>> getData(int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 12);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
    }
    Collections.sort(list);
    return list;
  }

  private Map<String, List<Pair<String>>> loadDataMap(int numKeys, int n) {

    Map<String, List<Pair<String>>> map = new HashMap<String, List<Pair<String>>>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Data map seed=" + seed);
    for (int i = 0; i < numKeys; i++) {
      String key = Utils.getRandomStr(r, 12);
      map.put(key, loadData(key, n));
    }
    return map;
  }

  private List<Pair<String>> loadDataSameScore(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 12);

      double sc = 1.08E8D; // some score
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] { m }, new double[] { sc }, false);
      assertEquals(1, (int) res);
      if ((i + 1) % 100000 == 0) {
        System.out.println("Loaded " + i);
      }
    }
    Collections.sort(list);
    return list;
  }

  // @Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 1000; i++) {
      System.out.println("*************** RUN = " + (i + 1) + " Compression=NULL");
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
      System.out.println("*************** RUN = " + (i + 1) + " Compression=LZ4");
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
      System.out.println("*************** RUN = " + (i + 1) + " Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }

  private void allTests() throws IOException {
    setUp();
    testAddScoreMultiple();
    tearDown();
    setUp();
    testAddScoreIncrementMultiple();
    tearDown();
    setUp();
    testAddDelete();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testIncrement();
    tearDown();
    setUp();
    testADDCorrectness();
    tearDown();
    setUp();
    testZCOUNT();
    tearDown();
    setUp();
    testZRANGEBYLEX();
    tearDown();
    setUp();
    testZRANGEBYLEX_WOL();
    tearDown();
    setUp();
    testZRANK();
    tearDown();
    setUp();
    testZREVRANK();
    tearDown();
    setUp();
    testZREVRANGEBYLEX();
    tearDown();
    setUp();
    testZREVRANGEBYLEX_WOL();
    tearDown();
    setUp();
    testZRANGEBYSCORE();
    tearDown();
    setUp();
    testZRANGEBYSCORE_WOL();
    tearDown();
    setUp();
    testZREVRANGEBYSCORE();
    tearDown();
    setUp();
    testZREVRANGEBYSCORE_WOL();
    tearDown();
    setUp();
    testZRANDMEMBER();
    tearDown();
    setUp();
    testZSCANNoRegex();
    tearDown();
    setUp();
    testZSCANWithRegex();
    tearDown();
    
    setUp();
    testZREMRANGEBYSCORE();
    tearDown();
    setUp();
    testZREMRANGEBYRANK();
    tearDown();
    setUp();
    testZREMRANGEBYLEX();
    tearDown();
  }
  
  
  private List<String> fieldList(List<Pair<String>> list){
    List<String> ll = new ArrayList<String>();
    for (Pair<String> p: list) {
      ll.add(p.getFirst());
    }
    return ll;
  }
  
  @Ignore
  @Test
  public void testZRANDMEMBER () {
    System.out.println("ZSets ZRANDMEMBER multiple keys test");
    int numMembers = 1000;
    int numIterations = 100;
    String key = "key";
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    for(int i = 0; i < numIterations; i++) {
      List<Pair<String>> result = ZSets.ZRANDMEMBER(map, key, 10, true, bufSize);
      assertEquals(10, result.size());
      assertTrue(Utils.unique(result));
      assertTrue(data.containsAll(result));
    }
    
    // Check negatives
    for(int i = 0; i < numIterations; i++) {
      List<Pair<String>> result = ZSets.ZRANDMEMBER(map, key, -10, true, bufSize);
      assertEquals(10, result.size());
      assertTrue(data.containsAll(result));
    }
    
    List<String> allfields = fieldList(data);
    for(int i = 0; i < numIterations; i++) {
      List<Pair<String>> result = ZSets.ZRANDMEMBER(map, key, 10, true, bufSize);
      assertEquals(10, result.size());
      assertTrue(Utils.unique(result));
      assertTrue(allfields.containsAll(fieldList(result)));
    }
    
    // Check negatives
    for(int i = 0; i < numIterations; i++) {
      List<Pair<String>> result = ZSets.ZRANDMEMBER(map, key, -10, true, bufSize);
      assertEquals(10, result.size());
      assertTrue(allfields.containsAll(fieldList(result)));
    }  
  }
  
  @Ignore
  @Test
  public void testZSCANNoRegex() {
    System.out.println("Test ZSets ZSCAN API call w/o regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    Random r = new Random();
    List<Pair<String>> list = loadDataSortByScore(key, X);
    // Check cardinality
    assertEquals(X, (int)ZSets.ZCARD(map, key));
    
    // Check full scan
    String lastSeenMember = null;
    int count = 11;
    int total = scan(map, key, 0, lastSeenMember, count, 200, null);
    assertEquals(X, total);
    // Check correctness of partial scans
    
    for(int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String lastSeenField =  list.get(index).getFirst();
      double score = Double.parseDouble(list.get(index).getSecond());
      int expected = list.size() - index - 1;
      total = scan(map, key, score, lastSeenField, count, 200, null) ;
      assertEquals(expected, total);
      if (i % 100 == 0) {
        System.out.println(i);
      }
    }

    // Check edge cases
    
    String before = "A";
    String after  = "zzzzzzzzzzzzzzzz";
    double min = - Double.MAX_VALUE;
    double max = Double.MAX_VALUE;
    
    total = scan(map, key, min, before, count, 200, null);
    assertEquals(X, total);
    total = scan(map, key, max, after, count, 200, null);
    assertEquals(0, total);
    
    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members
    
    total = scan(map, key, min, before, count, 100, null);
    assertEquals(X, total);
    total = scan(map, key, max, after, count, 100, null);
    assertEquals(0, total);
    
  }
  
  private int scan(BigSortedMap map, String key, double lastScore, String lastSeenMember, 
      int count, int bufferSize, String regex)
  {
    int total = 0;
    List<Pair<String>> result = null;
    // Check overall functionality - full scan
    while((result = ZSets.ZSCAN(map, key, lastScore, lastSeenMember, count, bufferSize, regex)) != null) {
      total += result.size() - 1;
      lastScore = Double.parseDouble(result.get(result.size() - 1).getSecond());
      lastSeenMember = result.get(result.size() - 1).getFirst();
    }
    return total;
  }

  private int countMatches(List<Pair<String>> list, int startIndex, String regex)
  {
    int total = 0;
    List<Pair<String>> subList = list.subList(startIndex, list.size());
    for (Pair<String> p: subList) {
      String s = p.getFirst();
      if (s.matches(regex)) {
        total++;
      }
    }
    return total;
  }
  
  @Ignore
  @Test
  public void testZSCANWithRegex() {
    System.out.println("Test Sets ZSCAN API call with regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    String regex = "^A.*";
    Random r = new Random();
    List<Pair<String>> list = loadDataSortByScore(key, X);
    // Check cardinality
    assertEquals(X, (int)ZSets.ZCARD(map, key));    // Check cardinality
    
    // Check full scan
    int expected = countMatches(list, 0, regex);
    String lastSeenMember = null;
    int count = 11;
    int total = scan(map, key, 0, lastSeenMember, count, 200, regex);
    assertEquals(expected, total);

    // Check correctness of partial scans
    
    for(int i = 0; i < 100; i++) {
      int index = r.nextInt(list.size());
      String lastSeen =  list.get(index).getFirst();
      double score = Double.parseDouble(list.get(index).getSecond());
      String pattern = "^" + lastSeen.charAt(0) + ".*";
      expected = index == list.size() -1? 0: countMatches(list, index + 1, pattern);
      total = scan(map, key, score, lastSeen, count, 200, pattern) ;
      assertEquals(expected, total);
      if (i % 100 == 0) {
        System.out.println(i);
      }
    }
    
    // Check edge cases
    
    String before = "A"; // less than any values
    String after  = "zzzzzzzzzzzzzzzz"; // larger than any values
    double min = -Double.MAX_VALUE;
    double max = Double.MAX_VALUE;
    expected = countMatches(list, 0, regex);
    
    total = scan(map, key, min, before, count, 200, regex);
    assertEquals(expected, total);
    total = scan(map, key, max, after, count, 200, regex);
    assertEquals(0, total);
    
    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members
    expected = countMatches(list, 0, regex);
    total = scan(map, key, min, before, count, 100, regex);
    assertEquals(expected, total);
    total = scan(map, key, max, after, count, 100, regex);
    assertEquals(0, total);
    
  }

  
  @Ignore
  @Test
  public void testAddScoreMultiple() {
    System.out.println("ZSets ZADD/ZSCORE multiple keys test");
    int numKeys = 1000;
    int numMembers = 100;
    Map<String, List<Pair<String>>> data = loadDataMap(numKeys, numMembers);

    for (String key : data.keySet()) {
      List<Pair<String>> list = data.get(key);
      for (Pair<String> p : list) {
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
    for (String key : data.keySet()) {
      count++;
      List<Pair<String>> list = data.get(key);
      for (Pair<String> p : list) {
        String member = p.getFirst();
        double expected = Double.parseDouble(p.getSecond());
        double score = ZSets.ZSCORE(map, key, member);
        assertEquals(score, expected);
        double incr = r.nextDouble() * r.nextInt();
        double newValue = ZSets.ZINCRBY(map, key, incr, member);
        assertEquals(expected + incr, newValue);
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
    for (String key : data.keySet()) {
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
    for (String key : data.keySet()) {
      count++;
      List<Pair<String>> list = data.get(key);
      for (Pair<String> p : list) {
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
    for (Pair<String> p : data) {
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
    assertEquals(numMembers, (int) card);

    // load again with new scores

    for (Pair<String> p : data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res = ZSets.ZADD(map, key, new String[] { p.getFirst() }, new double[] { score }, false);
      assertEquals(0, (int) res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(newScore);
      assertEquals(score, newScore);
    }

    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // ZADDNX

    for (Pair<String> p : data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res =
          ZSets.ZADDNX(map, key, new String[] { p.getFirst() }, new double[] { score }, false);
      assertEquals(0, (int) res);
    }
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Delete set
    boolean result = ZSets.DELETE(map, key);
    assertTrue(result);

    // load again with ZADDNX
    for (Pair<String> p : data) {
      double score = r.nextDouble() * r.nextInt();
      long res =
          ZSets.ZADDNX(map, key, new String[] { p.getFirst() }, new double[] { score }, false);
      assertEquals(1, (int) res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(newScore);
      assertEquals(score, newScore);
    }

    // ZADDXX
    for (Pair<String> p : data) {
      Double score = ZSets.ZSCORE(map, key, p.getFirst());
      assertNotNull(score);
      score = r.nextDouble() * r.nextInt();
      long res =
          ZSets.ZADDXX(map, key, new String[] { p.getFirst() }, new double[] { score }, true);
      assertEquals(1, (int) res);
    }

    // Delete set
    result = ZSets.DELETE(map, key);
    assertTrue(result);

    // Try loading again with ZADDXX
    for (Pair<String> p : data) {
      double score = r.nextDouble() * r.nextInt();
      long res =
          ZSets.ZADDXX(map, key, new String[] { p.getFirst() }, new double[] { score }, true);
      assertEquals(0, (int) res);
      Double newScore = ZSets.ZSCORE(map, key, p.getFirst());
      assertNull(newScore);
    }
  }

  @Ignore
  @Test
  public void testZCOUNT() {
    System.out.println("Test ZCOUNT API");
    Random r = new Random();
    int numMembers = 1000;
    String key = "key";
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);


    // 1. test both inclusive: start and end
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      double min = Double.parseDouble(data.get(start).getSecond());
      double max = Double.parseDouble(data.get(stop).getSecond());

      int expected = stop - start + 1; // both are inclusive
      long count = ZSets.ZCOUNT(map, key, min, true, max, true);
      assertEquals(expected, (int) count);
    }
    // 2. test both non-inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      double min = Double.parseDouble(data.get(start).getSecond());
      double max = Double.parseDouble(data.get(stop).getSecond());

      int expected = stop - start - 1; // both are exclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZCOUNT(map, key, min, false, max, false);
      assertEquals(expected, (int) count);
    }

    // 3. test start inclusive end non-inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      double min = Double.parseDouble(data.get(start).getSecond());
      double max = Double.parseDouble(data.get(stop).getSecond());

      int expected = stop - start; // both are inclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZCOUNT(map, key, min, true, max, false);
      assertEquals(expected, (int) count);
    }

    // 4. test start non-inclusive, end - inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      double min = Double.parseDouble(data.get(start).getSecond());
      double max = Double.parseDouble(data.get(stop).getSecond());

      int expected = stop - start; // both are inclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZCOUNT(map, key, min, false, max, true);
      assertEquals(expected, (int) count);
    }
    // Test Edges

    // Both start and stop are out of range () less than minimum - all 4 inclusive
    // combos
    double MIN = Double.parseDouble(data.get(0).getSecond());
    double MAX = Double.parseDouble(data.get(data.size() - 1).getSecond());
    long expected = 0;
    long count = ZSets.ZCOUNT(map, key, MIN - 2, false, MIN - 1, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 2, true, MIN - 1, true);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 2, true, MIN - 1, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 2, false, MIN - 1, true);
    assertEquals(expected, count);
    // Both start and stop are greater than maximum score
    count = ZSets.ZCOUNT(map, key, MAX + 1, false, MAX + 2, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MAX + 1, true, MAX + 2, true);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MAX + 1, true, MAX + 2, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MAX + 1, false, MAX + 2, true);
    assertEquals(expected, count);
    // Start is less than minimum, stop is greater than max
    expected = data.size();
    count = ZSets.ZCOUNT(map, key, MIN - 1, false, MAX + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 1, true, MAX + 1, true);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 1, true, MAX + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, MIN - 1, false, MAX + 1, true);
    assertEquals(expected, count);

    // Start is less than minimum score and stop is in the range
    double min = MIN - 1;
    int index = r.nextInt(data.size());
    double max = Double.parseDouble(data.get(index).getSecond());
    expected = index;
    count = ZSets.ZCOUNT(map, key, min, false, max, false);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = Double.parseDouble(data.get(index).getSecond());
    expected = index + 1;
    count = ZSets.ZCOUNT(map, key, min, true, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = Double.parseDouble(data.get(index).getSecond());
    expected = index + 1;
    count = ZSets.ZCOUNT(map, key, min, false, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = Double.parseDouble(data.get(index).getSecond());
    expected = index;
    count = ZSets.ZCOUNT(map, key, min, true, max, false);
    assertEquals(expected, count);

    // Start is in the range and stop is greater than maximum score
    max = MAX + 1;
    index = r.nextInt(data.size());
    min = Double.parseDouble(data.get(index).getSecond());
    expected = data.size() - index - 1;
    count = ZSets.ZCOUNT(map, key, min, false, max, false);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = Double.parseDouble(data.get(index).getSecond());
    expected = data.size() - index;
    count = ZSets.ZCOUNT(map, key, min, true, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = Double.parseDouble(data.get(index).getSecond());
    expected = data.size() - index - 1;
    count = ZSets.ZCOUNT(map, key, min, false, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = Double.parseDouble(data.get(index).getSecond());
    expected = data.size() - index;
    count = ZSets.ZCOUNT(map, key, min, true, max, false);
    assertEquals(expected, count);
    // Last one: check -inf, +inf
    min = -Double.MAX_VALUE;
    max = Double.MAX_VALUE;
    expected = data.size();

    count = ZSets.ZCOUNT(map, key, min, false, max, false);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, min, true, max, true);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, min, false, max, true);
    assertEquals(expected, count);
    count = ZSets.ZCOUNT(map, key, min, true, max, false);
    assertEquals(expected, count);

  }

  @Ignore
  @Test
  public void testZLEXCOUNT() {
    System.out.println("Test ZLEXCOUNT API");
    Random r = new Random();
    int numMembers = 1000;
    String key = "key";
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);

    Collections.sort(data);

    // 1. test both inclusive: start and end
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      String min = data.get(start).getFirst();
      String max = data.get(stop).getFirst();

      int expected = stop - start + 1; // both are inclusive
      long count = ZSets.ZLEXCOUNT(map, key, min, true, max, true);
      assertEquals(expected, (int) count);
    }
    // 2. test both non-inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      String min = data.get(start).getFirst();
      String max = data.get(stop).getFirst();

      int expected = stop - start - 1; // both are exclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZLEXCOUNT(map, key, min, false, max, false);
      assertEquals(expected, (int) count);
    }

    // 3. test start inclusive end non-inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      String min = data.get(start).getFirst();
      String max = data.get(stop).getFirst();

      int expected = stop - start; // both are inclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZLEXCOUNT(map, key, min, true, max, false);
      assertEquals(expected, (int) count);
    }

    // 4. test start non-inclusive, end - inclusive
    for (int i = 0; i < 100; i++) {
      int id1 = r.nextInt(data.size());
      int id2 = r.nextInt(data.size());
      int start, stop;
      if (id1 < id2) {
        start = id1;
        stop = id2;
      } else {
        start = id2;
        stop = id1;
      }
      String min = data.get(start).getFirst();
      String max = data.get(stop).getFirst();

      int expected = stop - start; // both are inclusive
      if (expected < 0) expected = 0;
      long count = ZSets.ZLEXCOUNT(map, key, min, false, max, true);
      assertEquals(expected, (int) count);
    }
    // Test Edges

    // Both start and stop are out of range () less than minimum -
    // all 4 inclusive combinations
    String MIN = "0"; // All members in the test are random strings [A,Z], '0' < 'A'
    String MAX = data.get(data.size() - 1).getFirst() + "0";
    long expected = 0;
    long count = ZSets.ZLEXCOUNT(map, key, MIN, false, MIN + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, MIN + 1, true);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, MIN + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, false, MIN + 1, true);
    assertEquals(expected, count);
    // Both start and stop are greater than maximum score
    count = ZSets.ZLEXCOUNT(map, key, MAX, false, MAX + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MAX, true, MAX + 1, true);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MAX, true, MAX + 1, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MAX, false, MAX + 1, true);
    assertEquals(expected, count);
    // Start is less than minimum, stop is greater than max
    expected = data.size();
    count = ZSets.ZLEXCOUNT(map, key, MIN, false, MAX, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, MAX, true);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, MAX, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, MIN, false, MAX, true);
    assertEquals(expected, count);

    // Start is less than minimum score and stop is in the range
    int index = r.nextInt(data.size());
    String max = data.get(index).getFirst();
    expected = index;
    count = ZSets.ZLEXCOUNT(map, key, MIN, false, max, false);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = data.get(index).getFirst();
    expected = index + 1;
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = data.get(index).getFirst();
    expected = index + 1;
    count = ZSets.ZLEXCOUNT(map, key, MIN, false, max, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    max = data.get(index).getFirst();
    expected = index;
    count = ZSets.ZLEXCOUNT(map, key, MIN, true, max, false);
    assertEquals(expected, count);

    // Start is in the range and stop is greater than maximum score
    index = r.nextInt(data.size());
    String min = data.get(index).getFirst();
    expected = data.size() - index - 1;
    count = ZSets.ZLEXCOUNT(map, key, min, false, MAX, false);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = data.get(index).getFirst();
    expected = data.size() - index;
    count = ZSets.ZLEXCOUNT(map, key, min, true, MAX, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = data.get(index).getFirst();
    expected = data.size() - index - 1;
    count = ZSets.ZLEXCOUNT(map, key, min, false, MAX, true);
    assertEquals(expected, count);

    index = r.nextInt(data.size());
    min = data.get(index).getFirst();
    expected = data.size() - index;
    count = ZSets.ZLEXCOUNT(map, key, min, true, MAX, false);
    assertEquals(expected, count);
    // Last one: check -inf, +inf
    expected = data.size();

    count = ZSets.ZLEXCOUNT(map, key, null, false, null, false);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, null, true, null, true);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, null, false, null, true);
    assertEquals(expected, count);
    count = ZSets.ZLEXCOUNT(map, key, null, true, null, false);
    assertEquals(expected, count);

  }

  @Ignore
  @Test
  public void testZPOPMAX() {
    System.out.println("Test ZPOPMAX API");
    Random r = new Random();
    int numMembers = 1000;
    int numIterations = 100;
    String key = "key";
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    for (int i = 0; i < numIterations; i++) {
      List<Pair<String>> data = loadDataSortByScore(key, numMembers);
      long card = ZSets.ZCARD(map, key);
      assertEquals(numMembers, (int) card);
      
      // For the last iteration we check count > data size
      int num = i < (numIterations - 1)? r.nextInt(data.size()): data.size() + 100;
      int expected = i < (numIterations - 1)? num: data.size();
      List<Pair<String>> list = ZSets.ZPOPMAX(map, key, num, bufSize);
      assertEquals(expected, list.size());
      for (int j = 0; j < expected; j++) {
        Pair<String> p1 = list.get(j);
        Pair<String> p2 = data.get(data.size() - 1 - j);
        assertEquals(p2, p1);
      }
      card = ZSets.ZCARD(map, key);
      assertEquals(data.size() - expected, (int) card);
      boolean res = ZSets.DELETE(map, key); 
      if (expected < data.size()) {
        assertTrue(res);
      } else {
        assertFalse(res);
      }
    }
    // Last test: test small buffer
    List<Pair<String>> data = loadData(key, numMembers);
    List<Pair<String>> list = ZSets.ZPOPMAX(map, key, 100, 100);
    // we expect empty list
    assertEquals(0, list.size());
    boolean res = ZSets.DELETE(map, key); 
    assertTrue(res);
  }
  
  @Ignore
  @Test
  public void testZPOPMIN() {
    System.out.println("Test ZPOPMIN API");
    Random r = new Random();
    int numMembers = 1000;
    int numIterations = 100;
    String key = "key";
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    for (int i = 0; i < numIterations; i++) {
      List<Pair<String>> data = loadDataSortByScore(key, numMembers);
      long card = ZSets.ZCARD(map, key);
      assertEquals(numMembers, (int) card);      
      // For the last iteration we check count > data size
      int num = i < (numIterations - 1)? r.nextInt(data.size()): data.size() + 100;
      int expected = i < (numIterations - 1)? num: data.size();
      List<Pair<String>> list = ZSets.ZPOPMIN(map, key, num, bufSize);
      assertEquals(expected, list.size());
      for (int j = 0; j < expected; j++) {
        Pair<String> p1 = list.get(j);
        Pair<String> p2 = data.get(j);
        assertEquals(p2, p1);
      }
      card = ZSets.ZCARD(map, key);
      assertEquals(data.size() - expected, (int) card);
      boolean res = ZSets.DELETE(map, key); 
      if (expected < data.size()) {
        assertTrue(res);
      } else {
        assertFalse(res);
      }
    }
    // Last test: test small buffer
    List<Pair<String>> data = loadData(key, numMembers);
    List<Pair<String>> list = ZSets.ZPOPMIN(map, key, 100, 100);
    // we expect empty list
    assertEquals(0, list.size());
    boolean res = ZSets.DELETE(map, key); 
    assertTrue(res);
  }
  
  @Ignore
  @Test
  public void testZRANGE() {
    System.out.println("Test ZRANGE API");
    Random r = new Random();
    int numMembers = 1000;
    int numIterations = 100;
    String key = "key";
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges (positive between 0 and data cardinality)
    // w/o scores
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      int start, end;
      if (i1 < i2) {
        start = i1;
        end = i2;
      } else {
        start = i2;
        end = i1;
      }
      
      int expectedNum = end - start + 1;
      List<Pair<String>> list = ZSets.ZRANGE(map, key, start, end, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = start; k <= end; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - start);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    // Test with normal ranges (positive between 0 and data cardinality)
    // with scores
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      int start, end;
      if (i1 < i2) {
        start = i1;
        end = i2;
      } else {
        start = i2;
        end = i1;
      }
      int expectedNum = end - start + 1;
      List<Pair<String>> list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = start; k <= end; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - start);
        assertEquals(expected, result);
      }
    }
    
    // Test some edge cases
    // 1. start = 0, end = last
    int start = 0;
    int end = data.size() - 1;
    
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    // Verify that we are correct
    for (int k = start; k <= end; k++) {
      Pair<String> expected = data.get(k);
      Pair<String> result = list.get(k - start);
      assertEquals(expected, result);
    }
    
    // start = end
    start = end = 1;
    expectedNum = 1;
    list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertEquals(data.get(start), list.get(0));
    
    // start > end
    start = 2;
    end = 1;
    expectedNum = 0;
    list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    // negative offsets
    start = -10;
    end = -1;
    expectedNum = end - start + 1;
    
    list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    start += data.size();
    end   += data.size();
    for (int i = start; i <= end; i++) {
      Pair<String> expected = data.get(i);
      Pair<String> result = list.get(i - start);
      assertEquals(expected, result);
    }
    
    // end is larger than cardinality
    start = -25;
    end = data.size() + 1;
    expectedNum = -start;
    list = ZSets.ZRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    start += data.size();
    end    = data.size() - 1;
    for (int i = start; i <= end; i++) {
      Pair<String> expected = data.get(i);
      Pair<String> result = list.get(i-start);
      assertEquals(expected, result);
    }
  }
  
  @Ignore
  @Test
  public void testZREVRANGE() {
    System.out.println("Test ZREVRANGE API");
    Random r = new Random();
    int numMembers = 1000;
    int numIterations = 100;
    String key = "key";
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);

    // Test with normal ranges (positive between 0 and data cardinality)
    // w/o scores
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      int start, end;
      if (i1 < i2) {
        start = i1;
        end = i2;
      } else {
        start = i2;
        end = i1;
      }  
      int expectedNum = end - start + 1;
      List<Pair<String>> list = ZSets.ZREVRANGE(map, key, start, end, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = start; k <= end; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(end - k);
        
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    // Test with normal ranges (positive between 0 and data cardinality)
    // with scores
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      int start, end;
      if (i1 < i2) {
        start = i1;
        end = i2;
      } else {
        start = i2;
        end = i1;
      }
      int expectedNum = end - start + 1;
      List<Pair<String>> list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = start; k <= end; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(end - k);
        assertEquals(expected, result);
      }
    }
    
    // Test some edge cases
    // 1. start = 0, end = last
    int start = 0;
    int end = data.size() - 1;
    
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    // Verify that we are correct
    for (int k = start; k <= end; k++) {
      Pair<String> expected = data.get(k);
      Pair<String> result = list.get(end - k);
      assertEquals(expected, result);
    }
    
    // start = end
    start = end = 1;
    expectedNum = 1;
    list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertEquals(data.get(start), list.get(0));
    
    // start > end
    start = 2;
    end = 1;
    expectedNum = 0;
    list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    // negative offsets
    start = -10;
    end = -1;
    expectedNum = end - start + 1;
    
    list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    start += data.size();
    end   += data.size();
    for (int i = start; i <= end; i++) {
      Pair<String> expected = data.get(i);
      Pair<String> result = list.get(end - i);
      assertEquals(expected, result);
    }
    
    // end is larger than cardinality
    start = -25;
    end = data.size() + 1;
    expectedNum = -start;
    list = ZSets.ZREVRANGE(map, key, start, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    start += data.size();
    end    = data.size() - 1;
    for (int i = start; i <= end; i++) {
      Pair<String> expected = data.get(i);
      Pair<String> result = list.get(end - i);
      assertEquals(expected, result);
    }
  }
  
  @Ignore
  void testZREVRANGEBYLEX_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      String start = null;
      String end   = null;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        start = data.get(i1).getFirst();
        end = data.get(i2).getFirst();
        startIdx = i1;
        endIdx = i2;
      } else {
        start = data.get(i2).getFirst();
        end = data.get(i1).getFirst();
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      };
            
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    // Test some edge cases
    // 1. start = 0, end = last
    String start = null;
    String end = null;
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());
    Collections.reverse(list);
    assertTrue(equals(data, list));
        
    // start = end
    start = end = data.get(1).getFirst();
    expectedNum = startInclusive && endInclusive? 1: 0;
    list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());
    if (expectedNum == 1) {
      assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
    }
    
    // start > end
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());

  }
  
  @Ignore
  @Test
  public void testZREVRANGEBYLEX() {
    System.out.println("Test ZREVRANGEBYLEX API (no offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREVRANGEBYLEX_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREVRANGEBYLEX_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREVRANGEBYLEX_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREVRANGEBYLEX_core(data, key, false, true);
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREVRANGEBYLEX_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREVRANGEBYLEX_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREVRANGEBYLEX_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREVRANGEBYLEX_core(data, key, false, true);    
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }

  
  @Ignore
  void testZRANGEBYLEX_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      String start = null;
      String end   = null;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        start = data.get(i1).getFirst();
        end = data.get(i2).getFirst();
        startIdx = i1;
        endIdx = i2;
      } else {
        start = data.get(i2).getFirst();
        end = data.get(i1).getFirst();
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      }
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test some edge cases
    // 1. start = 0, end = last
    String start = null;
    String end = null;
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
        
    // start = end
    start = end = data.get(1).getFirst();
    expectedNum = startInclusive && endInclusive? 1: 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());
    if (expectedNum == 1) {
      assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
    }
    
    // start > end
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, endInclusive, bufSize);
    assertEquals(expectedNum, list.size());
    
  }
  
  @Ignore
  @Test
  public void testZRANGEBYLEX() {
    System.out.println("Test ZRANGEBYLEX API (no offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYLEX_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYLEX_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYLEX_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYLEX_core(data, key, false, true);
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYLEX_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYLEX_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYLEX_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYLEX_core(data, key, false, true);    
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  private boolean equals(List<Pair<String>> first, List<Pair<String>> second) {
    // we do not check nulls b/c there are no
    if (first.size() != second.size()) return false;
    // Verify that we are correct
    for (int k = 0; k < first.size(); k++) {
      String expected = first.get(k).getFirst();
      String result = second.get(k).getFirst();
      if (expected.equals(result) != true) {
        return false;
      }
    }
    return true;
  }
  @Ignore
  void testZREVRANGEBYLEX_wol_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive, int offset, int limit) {
    
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      String start = null;
      String end   = null;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        start = data.get(i1).getFirst();
        end = data.get(i2).getFirst();
        startIdx = i1;
        endIdx = i2;
      } else {
        start = data.get(i2).getFirst();
        end = data.get(i1).getFirst();
        startIdx = i2;
        endIdx = i1;
      }
      
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);

      if (loopStart < 0 || loopEnd < 0) {
        expectedNum = 0;
      } else {
        expectedNum = loopEnd - loopStart;
      }
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test Edge cases
    // 1. start = end = null
    for (int i = 0; i < 1; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = Integer.MIN_VALUE / 2, endIdx = Integer.MAX_VALUE / 2;
      int expectedNum = 0;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 2. start = null, end != null
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = Integer.MIN_VALUE / 2;
      int endIdx = r.nextInt(data.size());
      end = data.get(endIdx).getFirst();
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 2. start != null, end == null
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      int endIdx = Integer.MAX_VALUE / 2;
      start = data.get(startIdx).getFirst();
      
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    // 3. start = end
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      int endIdx = startIdx;
      start = data.get(startIdx).getFirst();
      end = data.get(endIdx).getFirst();
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 3. start > end
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      if (startIdx == 0) startIdx = 1;
      int endIdx = startIdx - 1;
      start = data.get(startIdx).getFirst();
      end = data.get(endIdx).getFirst();
      int expectedNum = 0;
      List<Pair<String>> list = ZSets.ZREVRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
    }
  }
  
  @Ignore
  @Test
  public void testZREVRANGEBYLEX_WOL() {
    System.out.println("Test ZREVRANGEBYLEX API (with offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    int[] offsets = new int[] { -10, -100, 0, 20, 30, 50, 500, 999, 1000, 1001};
    int[] limits = new int[] {-100, 100, 200, 300, -100, 200, 200, 500, -1, 10};
    
    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZREVRANGEBYLEX_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZREVRANGEBYLEX_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZREVRANGEBYLEX_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZREVRANGEBYLEX_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    offsets = new int[] { -10, -100, 0, 20, 30, 50, 250, 499, 500, 501};
    limits = new int[] {-100, 50, 100, 150, -50, 100, 100, 250, -1, 10};

    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZREVRANGEBYLEX_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZREVRANGEBYLEX_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZREVRANGEBYLEX_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZREVRANGEBYLEX_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZRANGEBYLEX_wol_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive, int offset, int limit) {
    
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      String start = null;
      String end   = null;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        start = data.get(i1).getFirst();
        end = data.get(i2).getFirst();
        startIdx = i1;
        endIdx = i2;
      } else {
        start = data.get(i2).getFirst();
        end = data.get(i1).getFirst();
        startIdx = i2;
        endIdx = i1;
      }
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      if (loopStart < 0 || loopEnd < 0) {
        expectedNum = 0;
      } else {
        expectedNum = loopEnd - loopStart;
      }
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test Edge cases
    // 1. start = end = null
    for (int i = 0; i < 1; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = Integer.MIN_VALUE / 2, endIdx = Integer.MAX_VALUE / 2;
      int expectedNum = 0;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 2. start = null, end != null
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = Integer.MIN_VALUE / 2;
      int endIdx = r.nextInt(data.size());
      end = data.get(endIdx).getFirst();
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 2. start != null, end == null
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      int endIdx = Integer.MAX_VALUE / 2;
      start = data.get(startIdx).getFirst();
      
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    // 3. start = end
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      int endIdx = startIdx;
      start = data.get(startIdx).getFirst();
      end = data.get(endIdx).getFirst();
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // 3. start > end
    for (int i = 0; i < numIterations; i++) {
      
      String start = null;
      String end   = null;
      int startIdx = r.nextInt(data.size());
      if (startIdx == 0) startIdx = 1;
      int endIdx = startIdx - 1;
      start = data.get(startIdx).getFirst();
      end = data.get(endIdx).getFirst();
      int expectedNum = 0;
      List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive, offset, limit, bufSize);
      assertEquals(expectedNum, list.size());
    }
  }
  
  /**
   * Inclusive
   * @param dataSize
   * @param startIndex
   * @param startInclusive
   * @param stopIndex
   * @param stopInclusive
   * @param offset
   * @param limit
   * @return start offset or -1
   */
  private int getRangeStart(int dataSize, int startIndex, boolean startInclusive, int stopIndex,
      boolean stopInclusive, int offset, int limit)
  {
    if (offset >= dataSize) {
      return -1;
    } else if (offset < 0) {
      offset += dataSize;
      if (offset < 0) {
        offset = 0;
      }
    }
    if (limit < 0) {
      limit = Integer.MAX_VALUE / 2; // VERY LARGE
    }
    if (!startInclusive && startIndex >= 0) {
      startIndex++;
    }
    if (stopInclusive && stopIndex >= 0) {
      stopIndex++;
    }
    if (startIndex > stopIndex) {
      return -1;
    }
    if (offset > stopIndex) {
      return -1;
    }
    
    if ((offset + limit) < startIndex) {
      return -1;
    }
    int start = offset > startIndex? offset: startIndex;

    return start;
  }
  /**
   * Exclusive
   * @param dataSize
   * @param startIndex
   * @param startInclusive
   * @param stopIndex
   * @param stopInclusive
   * @param offset
   * @param limit
   * @return stop offset or -1
   */
  private int getRangeStop(int dataSize, int startIndex, boolean startInclusive, int stopIndex,
      boolean stopInclusive, int offset, int limit)
  {
    if (offset >= dataSize) {
      return -1;
    } else if (offset < 0) {
      offset += dataSize;
      if (offset < 0) {
        offset = 0;
      }
    }
    
    if (limit < 0) {
      limit = Integer.MAX_VALUE / 2; // VERY LARGE
    }
    
    if (!startInclusive && startIndex >= 0) {
      startIndex++;
    }
    if (stopInclusive && stopIndex >= 0) {
      stopIndex++;
    }
    
    if (stopIndex >= dataSize) {
      stopIndex = dataSize;
    }

    if (startIndex > stopIndex) {
      return -1;
    }
    if (offset > stopIndex) {
      return -1;
    }
    
    if (offset + limit < startIndex) {
      return -1;
    }
    int stop = (offset + limit) >= stopIndex? stopIndex: offset + limit;
    
    return stop;
  }
  
  @Ignore
  @Test
  public void testZRANGEBYLEX_WOL() {
    System.out.println("Test ZRANGEBYLEX API (with offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    int[] offsets = new int[] { -10, -100, 0, 20, 30, 50, 500, 999, 1000, 1001};
    int[] limits = new int[] {-100, 100, 200, 300, -100, 200, 200, 500, -1, 10};
    
    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYLEX_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYLEX_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYLEX_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYLEX_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    offsets = new int[] { -10, -100, 0, 20, 30, 50, 250, 499, 500, 501};
    limits = new int[] {-100, 50, 100, 150, -50, 100, 100, 250, -1, 10};

    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYLEX_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYLEX_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYLEX_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYLEX_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  @Test
  public void testZRANK() {
    System.out.println("Test ZRANK API ");
    String key = "key";
    int numMembers = 1000;
    Random r = new Random();
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(data.size());
      String member = data.get(index).getFirst();
      long expected = index;
      long rank = ZSets.ZRANK(map, key, member);
      assertEquals(expected, rank);
    }
    
    // Check non-existent
    String member = "member";
    long rank = ZSets.ZRANK(map, key, member);
    assertEquals(-1L, rank);
  }
  
  @Ignore
  @Test
  public void testZREVRANK() {
    System.out.println("Test ZREVRANK API ");
    String key = "key";
    int numMembers = 1000;
    Random r = new Random();
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(data.size());
      String member = data.get(index).getFirst();
      long expected = data.size() - index - 1;
      long rank = ZSets.ZREVRANK(map, key, member);
      assertEquals(expected, rank);
    }
    
    // Check non-existent
    String member = "member";
    long rank = ZSets.ZREVRANK(map, key, member);
    assertEquals(-1L, rank);
  }
  
  @Ignore
  void testZRANGEBYSCORE_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      double min = 0;
      double max = 0;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        min = Double.parseDouble(data.get(i1).getSecond());
        max = Double.parseDouble(data.get(i2).getSecond());
        startIdx = i1;
        endIdx = i2;
      } else {
        min = Double.parseDouble(data.get(i2).getSecond());
        max = Double.parseDouble(data.get(i1).getSecond());
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      };
      //*DEBUG*/ System.out.println("min="+ min + " max="+ max+ " ");
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
      
      list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      loopStart = startInclusive? startIdx: startIdx + 1;
      loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test some edge cases
    // 1. start = -inf, end = +inf
    double min = - Double.MAX_VALUE;
    double max = Double.MAX_VALUE;
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
        
    // start = end
    min = max = Double.parseDouble(data.get(1).getSecond());
    expectedNum = startInclusive && endInclusive? 1: 0;
    list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    if (expectedNum == 1) {
      assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
      assertEquals(Double.parseDouble(data.get(1).getSecond()), Double.parseDouble(list.get(0).getSecond()));
    }
    
    // start > end
    min = Double.parseDouble(data.get(2).getSecond());
    max = Double.parseDouble(data.get(1).getSecond());
    expectedNum = 0;
    list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    
  }
  
  @Ignore
  @Test
  public void testZRANGEBYSCORE() {
    System.out.println("Test ZRANGEBYSCORE API (no offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYSCORE_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYSCORE_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYSCORE_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYSCORE_core(data, key, false, true);
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSortByScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYSCORE_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYSCORE_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYSCORE_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYSCORE_core(data, key, false, true);    
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZRANGEBYSCORE_wol_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive, int offset, int limit) {
    
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      double min, max;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        min = Double.parseDouble(data.get(i1).getSecond());
        max = Double.parseDouble(data.get(i2).getSecond());
        startIdx = i1;
        endIdx = i2;
      } else {
        min = Double.parseDouble(data.get(i2).getSecond());
        max = Double.parseDouble(data.get(i1).getSecond());
        startIdx = i2;
        endIdx = i1;
      }
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      if (loopStart < 0 || loopEnd < 0) {
        expectedNum = 0;
      } else {
        expectedNum = loopEnd - loopStart;
      }
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
      
      list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test Edge cases
    // 1. start = end = null
    for (int i = 0; i < 1; i++) {
      
      double min = - Double.MAX_VALUE;
      double max   = Double.MAX_VALUE;
      int startIdx = Integer.MIN_VALUE / 2, endIdx = Integer.MAX_VALUE / 2;
      int expectedNum = 0;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true,  bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 2. start = null, end != null
    for (int i = 0; i < numIterations; i++) {
      
      double min = - Double.MAX_VALUE;
      double max;
      int startIdx = Integer.MIN_VALUE / 2;
      int endIdx = r.nextInt(data.size());
      max = Double.parseDouble(data.get(endIdx).getSecond());
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 2. start != null, end == null
    for (int i = 0; i < numIterations; i++) {
      
      double min;
      double max   = Double.MAX_VALUE;
      int startIdx = r.nextInt(data.size());
      int endIdx = Integer.MAX_VALUE / 2;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    // 3. start = end
    for (int i = 0; i < numIterations; i++) {
      
      double min, max;
      int startIdx = r.nextInt(data.size());
      int endIdx = startIdx;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      max = min;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 3. start > end
    for (int i = 0; i < numIterations; i++) {
      double min, max;
      int startIdx = r.nextInt(data.size());
      if (startIdx == 0) startIdx = 1;
      int endIdx = startIdx - 1;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      max = Double.parseDouble(data.get(endIdx).getSecond());
      int expectedNum = 0;
      List<Pair<String>> list = ZSets.ZRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
    }
  }
  
  @Ignore
  @Test
  public void testZRANGEBYSCORE_WOL() {
    System.out.println("Test ZRANGEBYSCORE API (with offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
        
    int[] offsets = new int[] { -10, -100, 0, 20, 30, 50, 500, 999, 1000, 1001};
    int[] limits = new int[] {-100, 100, 200, 300, -100, 200, 200, 500, -1, 10};
    
    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSortByScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    offsets = new int[] { -10, -100, 0, 20, 30, 50, 250, 499, 500, 501};
    limits = new int[] {-100, 50, 100, 150, -50, 100, 100, 250, -1, 10};

    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZREVRANGEBYSCORE_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      double min = 0;
      double max = 0;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        min = Double.parseDouble(data.get(i1).getSecond());
        max = Double.parseDouble(data.get(i2).getSecond());
        startIdx = i1;
        endIdx = i2;
      } else {
        min = Double.parseDouble(data.get(i2).getSecond());
        max = Double.parseDouble(data.get(i1).getSecond());
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      };
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
      
      list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      loopStart = startInclusive? startIdx: startIdx + 1;
      loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test some edge cases
    // 1. start = -inf, end = +inf
    double min = - Double.MAX_VALUE;
    double max = Double.MAX_VALUE;
    int expectedNum = data.size();
    List<Pair<String>> list = 
        ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    Collections.reverse(list);
    assertTrue(equals(data, list));
        
    // start = end
    min = max = Double.parseDouble(data.get(1).getSecond());
    expectedNum = startInclusive && endInclusive? 1: 0;
    list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    if (expectedNum == 1) {
      assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
      assertEquals(Double.parseDouble(data.get(1).getSecond()), Double.parseDouble(list.get(0).getSecond()));
    }
    
    // start > end
    min = Double.parseDouble(data.get(2).getSecond());
    max = Double.parseDouble(data.get(1).getSecond());
    expectedNum = 0;
    list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive, true, bufSize);
    assertEquals(expectedNum, list.size());
    
  }
  @Ignore
  @Test
  public void testZREVRANGEBYSCORE() {
    System.out.println("Test ZREVRANGEBYSCORE API (no offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREVRANGEBYSCORE_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREVRANGEBYSCORE_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREVRANGEBYSCORE_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREVRANGEBYSCORE_core(data, key, false, true);
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSortByScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREVRANGEBYSCORE_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREVRANGEBYSCORE_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREVRANGEBYSCORE_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREVRANGEBYSCORE_core(data, key, false, true);    
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZREVRANGEBYSCORE_wol_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive, int offset, int limit) {
    
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.

    for (int i = 0; i < numIterations; i++) {
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      double min, max;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        min = Double.parseDouble(data.get(i1).getSecond());
        max = Double.parseDouble(data.get(i2).getSecond());
        startIdx = i1;
        endIdx = i2;
      } else {
        min = Double.parseDouble(data.get(i2).getSecond());
        max = Double.parseDouble(data.get(i1).getSecond());
        startIdx = i2;
        endIdx = i1;
      }
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      if (loopStart < 0 || loopEnd < 0) {
        expectedNum = 0;
      } else {
        expectedNum = loopEnd - loopStart;
      }
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
      
      list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, false, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
      }
    }
    
    // Test Edge cases
    // 1. start = end = null
    for (int i = 0; i < 1; i++) {
      
      double min = - Double.MAX_VALUE;
      double max   = Double.MAX_VALUE;
      int startIdx = Integer.MIN_VALUE / 2, endIdx = Integer.MAX_VALUE / 2;
      int expectedNum = 0;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true,  bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 2. start = null, end != null
    for (int i = 0; i < numIterations; i++) {
      
      double min = - Double.MAX_VALUE;
      double max;
      int startIdx = Integer.MIN_VALUE / 2;
      int endIdx = r.nextInt(data.size());
      max = Double.parseDouble(data.get(endIdx).getSecond());
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 2. start != null, end == null
    for (int i = 0; i < numIterations; i++) {
      
      double min;
      double max   = Double.MAX_VALUE;
      int startIdx = r.nextInt(data.size());
      int endIdx = Integer.MAX_VALUE / 2;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    // 3. start = end
    for (int i = 0; i < numIterations; i++) {
      
      double min, max;
      int startIdx = r.nextInt(data.size());
      int endIdx = startIdx;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      max = min;
      int loopStart = getRangeStart(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int loopEnd = getRangeStop(data.size(), startIdx, startInclusive, endIdx, endInclusive, offset, limit);
      int expectedNum = loopEnd - loopStart;
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
      // Verify that we are correct
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(loopEnd + loopStart - k - 1);
        Pair<String> result = list.get(k - loopStart);
        assertEquals(expected.getFirst(), result.getFirst());
        assertEquals(Double.parseDouble(expected.getSecond()), Double.parseDouble(result.getSecond()));
      }
    }
    
    // 3. start > end
    for (int i = 0; i < numIterations; i++) {
      double min, max;
      int startIdx = r.nextInt(data.size());
      if (startIdx == 0) startIdx = 1;
      int endIdx = startIdx - 1;
      min = Double.parseDouble(data.get(startIdx).getSecond());
      max = Double.parseDouble(data.get(endIdx).getSecond());
      int expectedNum = 0;
      List<Pair<String>> list = ZSets.ZREVRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive, offset, limit, true, bufSize);
      assertEquals(expectedNum, list.size());
    }
  }
  
  @Ignore
  @Test
  public void testZREVRANGEBYSCORE_WOL() {
    System.out.println("Test ZREVRANGEBYSCORE API (with offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    List<Pair<String>> data = loadDataSortByScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
        
    int[] offsets = new int[] { -10, -100, 0, 20, 30, 50, 500, 999, 1000, 1001};
    int[] limits = new int[] {-100, 100, 200, 300, -100, 200, 200, 500, -1, 10};
    
    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    data = loadDataSortByScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    offsets = new int[] { -10, -100, 0, 20, 30, 50, 250, 499, 500, 501};
    limits = new int[] {-100, 50, 100, 150, -50, 100, 100, 250, -1, 10};

    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYSCORE_wol_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYSCORE_wol_core(data, key, false, true, offsets[i], limits[i]);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }

  @Ignore
  void testZREMRANGEBYSCORE_core(String key, int numMembers,
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    /*DEBUG*/
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    List<Pair<String>> data;
    int numIterations = 1000;

    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      // Load data
      data = loadDataSortByScore(key, numMembers);
      
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      double min = 0;
      double max = 0;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        min = Double.parseDouble(data.get(i1).getSecond());
        max = Double.parseDouble(data.get(i2).getSecond());
        startIdx = i1;
        endIdx = i2;
      } else {
        min = Double.parseDouble(data.get(i2).getSecond());
        max = Double.parseDouble(data.get(i1).getSecond());
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      };
        
      long total = ZSets.ZREMRANGEBYSCORE(map, key, min, startInclusive, max, 
        endInclusive);
      if (total != expectedNum) {
        System.out.println("min="+ min + " max="+ max + " startIdx="+ startIdx + " endIdx="+ endIdx + 
          " minInclusive=" + startInclusive + " maxInclusive=" + endInclusive +
          " test seed=" + seed + " data seed=" + dataSeed);
        System.out.println("START: score=" + min + " field="+ data.get(startIdx).getFirst());
        System.out.println("STOP : score=" + max + " field="+ data.get(endIdx).getFirst());

      } else {
        assertEquals(expectedNum, (int) total);
      }
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        String member = expected.getFirst();
        long rank = ZSets.ZRANK(map, key, member);
        if (rank >=0 ) {
          /*DEBUG*/ System.out.println("FOUND #"+ (k - loopStart) + " expected score="+ expected.getSecond() + 
            " field=" + expected.getFirst());
          /*DEBUG*/ System.out.println("Actual score=" + ZSets.ZSCORE(map, key, member));
        }
        assertEquals(-1, (int) rank);
      }
      // Delete set
      ZSets.DELETE(map, key);
      if (i % 100 == 0) {
      /*DEBUG*/  System.out.println(i);
      }
    }
    // Test some edge cases
    // 1. start = -inf, end = +inf
    double min = - Double.MAX_VALUE;
    double max = Double.MAX_VALUE;
    data = loadDataSortByScore(key, numMembers);
    int expectedNum = data.size();
    long total = ZSets.ZREMRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive);
    assertEquals(expectedNum, (int) total);
    boolean res = ZSets.DELETE(map, key);
    assertFalse(res);
        
    // start = end
    data = loadDataSortByScore(key, numMembers);
    min = max = Double.parseDouble(data.get(1).getSecond());
    expectedNum = startInclusive && endInclusive? 1: 0;
    total = ZSets.ZREMRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive);
    assertEquals(expectedNum, (int) total);
    if (expectedNum == 1) {
      String member = data.get(1).getFirst();
      long rank = ZSets.ZRANK(map, key, member);
      assertEquals(-1, (int) rank);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    // start > end
    min = Double.parseDouble(data.get(3).getSecond());
    max = Double.parseDouble(data.get(2).getSecond());
    expectedNum = 0;
    total = ZSets.ZREMRANGEBYSCORE(map, key, min, startInclusive, max, endInclusive);
    assertEquals(expectedNum, (int) total);
    
  }
  
  @Ignore
  @Test
  public void testZREMRANGEBYSCORE() {
    System.out.println("Test ZREMRANGEBYSCORE API");
    String key = "key";

    int numMembers = 1000;
    
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREMRANGEBYSCORE_core(key, numMembers, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREMRANGEBYSCORE_core(key, numMembers, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREMRANGEBYSCORE_core(key, numMembers, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREMRANGEBYSCORE_core(key, numMembers, false, true);
    
    // 2. CARDINALITY < compact size (512)
    //boolean res = ZSets.DELETE(map, key);
    //assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
   
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREMRANGEBYSCORE_core(key, numMembers, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREMRANGEBYSCORE_core(key, numMembers, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREMRANGEBYSCORE_core(key, numMembers, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREMRANGEBYSCORE_core(key, numMembers, false, true);    
    ZSets.DELETE(map, key);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZREMRANGEBYRANK_core(String key, int numMembers) {
    Random r = new Random();
    /*DEBUG*/
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    List<Pair<String>> data;
    int numIterations = 1000;

    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      // Load data
      data = loadDataSortByScore(key, numMembers);
      
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());

      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        startIdx = i1;
        endIdx = i2;
      } else {
        startIdx = i2;
        endIdx = i1;
      }
      expectedNum = endIdx  - startIdx + 1;
      long total = ZSets.ZREMRANGEBYRANK(map, key, startIdx, endIdx);
      if (total != expectedNum) {
        System.out.println(" startIdx="+ startIdx + " endIdx="+ endIdx + 
          " test seed=" + seed + " data seed=" + dataSeed);
      }
      assertEquals(expectedNum, (int) total);
      // Verify that we are correct
      int loopStart =  startIdx;
      int loopEnd =  endIdx;
      for (int k = loopStart; k <= loopEnd; k++) {
        Pair<String> expected = data.get(k);
        String member = expected.getFirst();
        long rank = ZSets.ZRANK(map, key, member);
        assertEquals(-1, (int) rank);
      }
      // Delete set
      ZSets.DELETE(map, key);
      if (i % 100 == 0) {
      /*DEBUG*/  System.out.println(i);
      }
    }
    
    // Test some edge cases
    // 1. start = -inf, end = +inf
    int min = 0;
    int max = -1;
    data = loadDataSortByScore(key, numMembers);
    int expectedNum = data.size();
    long total = ZSets.ZREMRANGEBYRANK(map, key, min, max);
    assertEquals(expectedNum, (int) total);
    boolean res = ZSets.DELETE(map, key);
    assertFalse(res);
        
    // start = end
    data = loadDataSortByScore(key, numMembers);
    min = max = 10;
    expectedNum = 1;
    total = ZSets.ZREMRANGEBYRANK(map, key, min, max);
    assertEquals(expectedNum, (int) total);
    if (expectedNum == 1) {
      String member = data.get(min).getFirst();
      long rank = ZSets.ZRANK(map, key, member);
      assertEquals(-1, (int) rank);
    }
    res = ZSets.DELETE(map, key);
    assertTrue(res);
    // start > end
    min = 10;
    max = 9;
    expectedNum = 0;
    total = ZSets.ZREMRANGEBYRANK(map, key, min, max);
    assertEquals(expectedNum, (int) total);
    
  }
  
  @Ignore
  @Test
  public void testZREMRANGEBYRANK() {
    System.out.println("Test ZREMRANGEBYRANK API");
    String key = "key";
    int numMembers = 1000;    
    testZREMRANGEBYRANK_core(key, numMembers);
    // 2. CARDINALITY < compact size (512)
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREMRANGEBYRANK_core(key, numMembers);
    ZSets.DELETE(map, key);
    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  @Ignore
  void testZREMRANGEBYLEX_core( String key, int numMembers,
      boolean startInclusive, boolean endInclusive) {
    
    System.out.println("numMembers=" + numMembers + " startInclusive=" + startInclusive + 
      " endInclusive=" + endInclusive);
    
    Random r = new Random();
    long seed = 276634853598895472L;//r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    int numIterations = 1000;
    List<Pair<String>> data = null;
    // Test with normal ranges startInclusive = false, endInclusive = false
    for (int i = 0; i < numIterations; i++) {
      // data is sorted by name (field)
      data = loadData(key, numMembers);
      int i1 = r.nextInt(data.size());
      int i2 = r.nextInt(data.size());
      String start = null;
      String end   = null;
      int startIdx, endIdx;
      int expectedNum = 0;
      if (i1 < i2) {
        start = data.get(i1).getFirst();
        end = data.get(i2).getFirst();
        startIdx = i1;
        endIdx = i2;
      } else {
        start = data.get(i2).getFirst();
        end = data.get(i1).getFirst();
        startIdx = i2;
        endIdx = i1;
      }
      if (startInclusive && endInclusive) {
        expectedNum = endIdx - startIdx + 1;
      } else if (!startInclusive && !endInclusive) {
        expectedNum = endIdx - startIdx - 1;
      } else {
        expectedNum = endIdx - startIdx;
      }
      if (expectedNum < 0) {
        expectedNum = 0;
      }
      long total = ZSets.ZREMRANGEBYLEX(map, key, start, startInclusive, end, 
        endInclusive);
      assertEquals(expectedNum, (int) total);
      long cardinality =  ZSets.ZCARD(map, key);
      assertEquals(numMembers - expectedNum, (int) cardinality);
      // Verify that we are correct
      int loopStart = startInclusive? startIdx: startIdx + 1;
      int loopEnd = endInclusive? endIdx + 1: endIdx;
      for (int k = loopStart; k < loopEnd; k++) {
        Pair<String> expected = data.get(k);
        long rank = ZSets.ZRANK(map, key, expected.getFirst());
        assertEquals(-1, (int) rank); 
      }
      boolean res = ZSets.DELETE(map, key);
      if (cardinality > 0) { 
        assertTrue(res);
      } else {
        assertFalse(res);
      }
      if (i % 100 == 0) {
        System.out.println(i);
      }
    }
    
    data = loadData(key, numMembers);
    // Test some edge cases
    // 1. start = 0, end = last
    String start = null;
    String end = null;
    int expectedNum = data.size();
    long total = ZSets.ZREMRANGEBYLEX(map, key, start, startInclusive, end, endInclusive);
    assertEquals(expectedNum, (int) total);
    long cardinality = ZSets.ZCARD(map, key);
    assertEquals(0, (int) cardinality);
    boolean res = ZSets.DELETE(map, key);
    assertFalse(res);
    // start = end
    data = loadData(key, numMembers);
    start = end = data.get(1).getFirst();
    expectedNum = startInclusive && endInclusive? 1: 0;
    total = ZSets.ZREMRANGEBYLEX(map, key, start, startInclusive, end, endInclusive);
    assertEquals(expectedNum, (int) total);
    if (expectedNum == 1) {
      String member = data.get(1).getFirst();
      long rank = ZSets.ZRANK(map, key, member);
      assertEquals(-1, (int) rank); 
    }
    
    // start > end
    start = data.get(3).getFirst();
    end = data.get(2).getFirst();
    expectedNum = 0;
    total = ZSets.ZREMRANGEBYLEX(map, key, start, startInclusive, end, endInclusive);
    assertEquals(expectedNum, (int) total);
    res = ZSets.DELETE(map, key);
    assertTrue(res);
  }
  
  @Ignore
  @Test
  public void testZREMRANGEBYLEX() {
    System.out.println("Test ZREMRANGEBYLEX API");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;

    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREMRANGEBYLEX_core(key, numMembers, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREMRANGEBYLEX_core(key, numMembers, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREMRANGEBYLEX_core(key, numMembers, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREMRANGEBYLEX_core(key, numMembers, false, true);
    
    // 2. CARDINALITY < compact size (512)

    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;

    // Test with normal ranges startInclusive = false, endInclusive = false
    testZREMRANGEBYLEX_core(key, numMembers, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZREMRANGEBYLEX_core(key, numMembers, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZREMRANGEBYLEX_core(key, numMembers, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZREMRANGEBYLEX_core(key, numMembers, false, true);    

    assertEquals(0L, BigSortedMap.countRecords(map));
  }
  
  
  public void setUp() {
    map = new BigSortedMap(100000000);
  }

  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
