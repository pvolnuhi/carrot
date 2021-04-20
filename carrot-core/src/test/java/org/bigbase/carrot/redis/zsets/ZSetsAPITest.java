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
    long seed = rnd.nextLong();
    rnd.setSeed(seed);
    System.out.println("Global seed=" + seed);
  }

  private List<Pair<String>> loadData(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();

    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 8);
      double sc = rnd.nextDouble() * rnd.nextInt();
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] { m }, new double[] { sc }, true);
      assertEquals(1, (int) res);
      if ((i + 1) % 100000 == 0) {
        System.out.println("Loaded " + i);
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

  private Map<String, List<Pair<String>>> loadDataMap(int numKeys, int n) {

    Map<String, List<Pair<String>>> map = new HashMap<String, List<Pair<String>>>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Data map seed=" + seed);
    for (int i = 0; i < numKeys; i++) {
      String key = Utils.getRandomStr(r, 10);
      map.put(key, loadData(key, n));
    }
    return map;
  }

  @SuppressWarnings("unused")
  private List<Pair<String>> loadDataSameScore(String key, int n) {
    List<Pair<String>> list = new ArrayList<Pair<String>>();
    for (int i = 0; i < n; i++) {
      String m = Utils.getRandomStr(rnd, 8);

      double sc = 1.08E8D; // some score
      String score = Double.toString(sc);
      list.add(new Pair<String>(m, score));
      long res = ZSets.ZADD(map, key, new String[] { m }, new double[] { sc }, true);
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
    for (int i = 0; i < 100; i++) {
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
    // setUp();
    // testAddScoreMultiple();
    // tearDown();
    // setUp();
    // testAddScoreIncrementMultiple();
    // tearDown();
    // setUp();
    // testAddDelete();
    // tearDown();
    // setUp();
    // testAddRemove();
    // tearDown();
    // setUp();
    // testIncrement();
    // tearDown();
    // setUp();
    // testADDCorrectness();
    // tearDown();
    // setUp();
    // testZCOUNT();
    // tearDown();
//    setUp();
//    testZLEXCOUNT();
//    tearDown();
//    setUp();
//    testZPOPMAX();
//    tearDown();
//    setUp();
//    testZPOPMIN();
//    tearDown();
//    setUp();
//    testZRANGE();
//    tearDown();
//    setUp();
//    testZREVRANGE();
//    tearDown();
    
//    setUp();
//    testZRANGEBYLEX_no_offset_limit();
//    tearDown();
    setUp();
    testZRANK();
    tearDown();
    setUp();
    testZREVRANK();
    tearDown();
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
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);

    Collections.sort(data, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    });

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
    if (count == 0) {
      /* DEBUG */ System.out.println("Repeat CALL");
      count = ZSets.ZLEXCOUNT(map, key, null, false, null, false);
    }
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
      List<Pair<String>> data = loadData(key, numMembers);
      long card = ZSets.ZCARD(map, key);
      assertEquals(numMembers, (int) card);

      Collections.sort(data, new Comparator<Pair<String>>() {
        @Override
        public int compare(Pair<String> o1, Pair<String> o2) {
          double d1 = Double.parseDouble(o1.getSecond());
          double d2 = Double.parseDouble(o2.getSecond());
          if (d1 < d2) return -1;
          if (d1 > d2) return 1;
          return 0;
        }
      });
      
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
      List<Pair<String>> data = loadData(key, numMembers);
      long card = ZSets.ZCARD(map, key);
      assertEquals(numMembers, (int) card);

      Collections.sort(data, new Comparator<Pair<String>>() {
        @Override
        public int compare(Pair<String> o1, Pair<String> o2) {
          double d1 = Double.parseDouble(o1.getSecond());
          double d2 = Double.parseDouble(o2.getSecond());
          if (d1 < d2) return -1;
          if (d1 > d2) return 1;
          return 0;
        }
      });
      
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
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);

    Collections.sort(data, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    });
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
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);

    Collections.sort(data, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    });
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
  void testZRANGEBYLEX_no_offset_limit_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 100;
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
      };
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
  }
  @Ignore
  @Test
  public void testZRANGEBYLEX_no_offset_limit() {
    System.out.println("Test ZRANGEBYLEX API (no offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYLEX_no_offset_limit_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYLEX_no_offset_limit_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYLEX_no_offset_limit_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYLEX_no_offset_limit_core(data, key, false, true);
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    //BigSortedMap.dumpRecords(map);
    // Test with normal ranges startInclusive = false, endInclusive = false
    testZRANGEBYLEX_no_offset_limit_core(data, key, false, false);
    // Test with normal ranges startInclusive = true, endInclusive = true
    testZRANGEBYLEX_no_offset_limit_core(data, key, true, true);
    // Test with normal ranges startInclusive = true, endInclusive = false
    testZRANGEBYLEX_no_offset_limit_core(data, key, true, false);
    // Test with normal ranges startInclusive = false, endInclusive = true
    testZRANGEBYLEX_no_offset_limit_core(data, key, false, true);
    
    // Test some edge cases
    // 1. start = 0, end = last
    String start = null;
    String end = null;
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    // start = end
    start = end = data.get(1).getFirst();
    expectedNum = 1;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    // start > end
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());

    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
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
  void testZRANGEBYLEX_with_offset_limit_core(List<Pair<String>> data, String key, 
      boolean startInclusive, boolean endInclusive, int offset, int limit) {
    Random r = new Random();
    int numMembers = data.size();
    int numIterations = 10;
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
      limit = Integer.MAX_VALUE; // VERY LARGE
    }
    // adjust start and stop index
    if (!startInclusive) {
      startIndex++;
    }
    if (!stopInclusive) {
      stopIndex--;
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
    
    if (offset <= startIndex && (offset + limit) >= stopIndex) {
      return startIndex;
    } else if (offset > startIndex && (offset + limit) >= stopIndex) {
      return offset;
    } else if (offset <= startIndex && (offset + limit) < stopIndex) {
      return startIndex;
    } else if (offset > startIndex && (offset + limit) < stopIndex) {
      return offset;
    }
    // should not be here
    return -1;  
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
      limit = Integer.MAX_VALUE; // VERY LARGE
    }
    // adjust start and stop index
    if (!startInclusive) {
      startIndex++;
    }
    if (!stopInclusive) {
      stopIndex--;
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
    
    if (offset <= startIndex && (offset + limit) >= stopIndex) {
      return stopIndex + 1;
    } else if (offset > startIndex && (offset + limit) >= stopIndex) {
      return stopIndex + 1;
    } else if (offset <= startIndex && (offset + limit) < stopIndex) {
      return offset + limit + 1;
    } else if (offset > startIndex && (offset + limit) < stopIndex) {
      return offset + limit + 1;
    }
    // should not be here
    return -1;
  }
  
  @Ignore
  @Test
  public void testZRANGEBYLEX_with_offset_limit() {
    System.out.println("Test ZRANGEBYLEX API (with offset and limit)");
    String key = "key";

    // 1. CARDINALITY > compact size (512)

    int numMembers = 1000;
    int bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    List<Pair<String>> data = loadDataSameScore(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    int[] offsets = new int[] { -10, -100, 0, 20, 30, 50, 500, 999, 1000, 1001};
    int[] limits = new int[] {-100, 100, 200, 300, -100, 200, 200, 500, -1, 10};
    
    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYLEX_with_offset_limit_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYLEX_with_offset_limit_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYLEX_with_offset_limit_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYLEX_with_offset_limit_core(data, key, false, true, offsets[i], limits[i]);
    }
    
    // 2. CARDINALITY < compact size (512)
    boolean res = ZSets.DELETE(map, key);
    assertTrue(res);
    assertEquals(0L, BigSortedMap.countRecords(map));
    numMembers = 500;
    bufSize = numMembers * 100; // to make sure that the whole set will fit in.
    data = loadDataSameScore(key, numMembers);
    card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    
    offsets = new int[] { -10, -100, 0, 20, 30, 50, 250, 499, 500, 501};
    limits = new int[] {-100, 50, 100, 150, -50, 100, 100, 250, -1, 10};

    for (int i = 0; i < offsets.length; i++) {
      // Test with normal ranges startInclusive = false, endInclusive = false
      testZRANGEBYLEX_with_offset_limit_core(data, key, false, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = true
      testZRANGEBYLEX_with_offset_limit_core(data, key, true, true, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = true, endInclusive = false
      testZRANGEBYLEX_with_offset_limit_core(data, key, true, false, offsets[i], limits[i]);
      // Test with normal ranges startInclusive = false, endInclusive = true
      testZRANGEBYLEX_with_offset_limit_core(data, key, false, true, offsets[i], limits[i]);
    }
    // Test some edge cases
    // 1. start = 0, end = last
    String start = null;
    String end = null;
    int expectedNum = data.size();
    List<Pair<String>> list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    expectedNum = data.size();
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    assertTrue(equals(data, list));
    
    // start = end
    start = end = data.get(1).getFirst();
    expectedNum = 1;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    assertEquals(data.get(1).getFirst(), list.get(0).getFirst());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    // start > end
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, false, bufSize);
    assertEquals(expectedNum, list.size());
    
    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, true, end, false, bufSize);
    assertEquals(expectedNum, list.size());

    start = data.get(2).getFirst();
    end = data.get(1).getFirst();
    expectedNum = 0;
    list = ZSets.ZRANGEBYLEX(map, key, start, false, end, true, bufSize);
    assertEquals(expectedNum, list.size());
    
  }
  
  @Ignore
  @Test
  public void testZRANK() {
    System.out.println("Test ZRANK API ");
    String key = "key";
    int numMembers = 1000;
    Random r = new Random();
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    Collections.sort(data, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    });
    
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
    List<Pair<String>> data = loadData(key, numMembers);
    long card = ZSets.ZCARD(map, key);
    assertEquals(numMembers, (int) card);
    Collections.sort(data, new Comparator<Pair<String>>() {
      @Override
      public int compare(Pair<String> o1, Pair<String> o2) {
        double d1 = Double.parseDouble(o1.getSecond());
        double d2 = Double.parseDouble(o2.getSecond());
        if (d1 < d2) return -1;
        if (d1 > d2) return 1;
        return 0;
      }
    });
    
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
  
  public void setUp() {
    map = new BigSortedMap(100000000);
  }

  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
