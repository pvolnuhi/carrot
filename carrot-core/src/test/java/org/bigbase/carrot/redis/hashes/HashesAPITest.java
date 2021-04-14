package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.util.Pair;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HashesAPITest {
  BigSortedMap map;
  long n = 1000;

  private List<String> loadData(String key, int n) {
    List<String> list = new ArrayList<String>();
    Random r = new Random();
    for (int i=0; i < n; i++) {
      String m = Utils.getRandomStr(r, 10);
      list.add(m);
      int res = Hashes.HSET(map, key, m, m);
      assertEquals(1, res);
      if (i % 100000 == 0) {
        System.out.println("Loaded "+ i);
      }
    }
    return list;
  }
  
  private List<String> loadDataRandomSize(String key, int n) {
    List<String> list = new ArrayList<String>();
    Random r = new Random();
    for (int i=0; i < n; i++) {
      int size = r.nextInt(10) + 5;
      String m = Utils.getRandomStr(r, size);
      list.add(m);
      int res = Hashes.HSET(map, key, m, m);
      assertEquals(1, res);
      if (i % 100000 == 0) {
        System.out.println("Loaded "+ i);
      }
    }
    return list;
  }
  
  @Ignore
  @Test
  public void testHashSetNonExistent() {
    System.out.println("Test Hashes HSETNX API call");
    int X = 1000;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    Random r = new Random();
    
    // Check existing fields
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      int res = Hashes.HSETNX(map, key, v, v);
      assertEquals(0, res);
    }
    // Check non-existing fields
    for (int i = 0; i < 1000; i++) {
      String v = Utils.getRandomStr(r, 10);
      int res = Hashes.HSETNX(map, key, v, v);
      assertEquals(1, res);
    }
  }
  
  @Ignore
  @Test
  public void testHashValueLength() {
    System.out.println("Test Hashes HSTRLEN API call");
    int X = 1000;
    String key = "key";
    List<String> list = loadDataRandomSize(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    Random r = new Random();
    
    // Check existing fields
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      int size = Hashes.HSTRLEN(map, key, v);
      assertEquals(v.length(), size);
    }
    // Check non-existing fields
    for (int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String v = list.get(index);
      v += "111111111111111";
      int size = Hashes.HSTRLEN(map, key, v);
      assertEquals(-1, size);
    }
  }
  
  @Ignore
  @Test
  public void testHashMultiGet() {
    System.out.println("Test Sets HMGET API call");
    int X = 1000;
    String key = "key";
    List<String> list = loadData(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    int m = 10;
    // Run calls with correct buffer size
    int bufSize = m * (10 + 4) + 5;
    for (int i = 0; i < 100; i++) {
      String[] fields = getRandom(list, m);
      List<String> result = Hashes.HMGET(map, key, fields, bufSize);
      assertEquals(m, result.size());
      for (int k = 0; k < m; k++) {
        assertEquals(fields[k], result.get(k));
      }
    }
    
    // Run call with a small buffer
    String[] fields = getRandom(list, m + 1);
    List<String> result = Hashes.HMGET(map, key, fields, bufSize);
    assertEquals(0, result.size());
    
    // Check nulls
    fields = new String[] {list.get(0), "absd", list.get(2), "123", "679"};
    result = Hashes.HMGET(map, key, fields, bufSize);
    
    assertEquals(fields.length, result.size());
    
    assertEquals(list.get(0), result.get(0));
    assertEquals(null, result.get(1));
    assertEquals(list.get(2), result.get(2));
    assertEquals(null, result.get(3));
    assertEquals(null, result.get(4));

  }
  
  private String[] getRandom(List<String> list, int count) {
    long[] arr = Utils.randomDistinctArray(list.size(), count);
    String[] ss = new String[count];
    for (int i = 0; i < arr.length; i++) {
      ss[i] = list.get((int)arr[i]);
    }
    return ss;
  }
  
  @Ignore
  @Test
  public void testHashGetAllAPI() {
    System.out.println("Test Hashes HGETALL API call");
    // Load X elements
    int X = 1000;
    String key = "key";
    List<String> list = loadData(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    // Call with a large buffer
    List<Pair<String>> fieldValues = Hashes.HGETALL(map, key, 22005);
    assertEquals(list.size(), fieldValues.size());
    for (int i=0; i < list.size(); i++) {
      String s = list.get(i);
      String ss = fieldValues.get(i).getFirst();
      assertEquals(s, ss);
    }
    // Call with a smaller buffer
    fieldValues = Hashes.HGETALL(map, key, 22000);
    assertEquals(0, fieldValues.size());
  }
  
  
  @Ignore
  @Test
  public void testHashKeysAPI() {
    System.out.println("Test Hashes HKEYS API call");
    // Load X elements
    int X = 1000;
    String key = "key";
    List<String> list = loadData(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    // Call with a large buffer
    List<String> keys = Hashes.HKEYS(map, key, 11005);
    assertEquals(list.size(), keys.size());
    assertTrue(list.containsAll(keys));
    // Call with a smaller buffer
    keys = Hashes.HKEYS(map, key, 11000);
    assertEquals(0, keys.size());
  }
  
  @Ignore
  @Test
  public void testHashValuesAPI() {
    System.out.println("Test Hashes HVALUES API call");
    // Load X elements
    int X = 1000;
    String key = "key";
    List<String> list = loadData(key, X);    
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    // Call with a large buffer
    List<String> values = Hashes.HVALS(map, key, 11005);
    assertEquals(list.size(), values.size());
    assertTrue(list.containsAll(values));
    // Call with a smaller buffer
    values = Hashes.HVALS(map, key, 11000);
    assertEquals(0, values.size());
    assertTrue(list.containsAll(values));
  }
  
  @Ignore
  @Test
  public void testSscanNoRegex() {
    System.out.println("Test Hashes HSCAN API call w/o regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    Random r = new Random();
    List<String> list = loadData(key, X);
    
    Collections.sort(list);
    
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    
    // Check full scan
    String lastSeenMember = null;
    int count = 11; // required buffer size 22 * 11 + 4 = 246
    int total = scan(map, key, lastSeenMember, count, 250, null);
    assertEquals(X, total);

    // Check correctness of partial scans
    
    for(int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String lastSeen =  list.get(index);
      int expected = list.size() - index - 1;
      total = scan(map, key, lastSeen, count, 250, null) ;
      assertEquals(expected, total);
    }
    
    // Check edge cases
    
    String before = "A";
    String after  = "zzzzzzzzzzzzzzzz";
    
    total = scan(map, key, before, count, 250, null);
    assertEquals(X, total);
    total = scan(map, key, after, count, 250, null);
    assertEquals(0, total);
    
    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members
    
    total = scan(map, key, before, count, 100, null);
    assertEquals(X, total);
    total = scan(map, key, after, count, 100, null);
    assertEquals(0, total);
    
  }
  
  private int countMatches(List<String> list, int startIndex, String regex)
  {
    int total = 0;
    List<String> subList = list.subList(startIndex, list.size());
    for (String s: subList) {
      if (s.matches(regex)) {
        total++;
      }
    }
    return total;
  }
  
  @Ignore
  @Test
  public void testSscanWithRegex() {
    System.out.println("Test Hashes HSCAN API call with regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    String regex = "^A.*";
    Random r = new Random();
    List<String> list = loadData(key, X);
    Collections.sort(list);
    // Check cardinality
    assertEquals(X, (int)Hashes.HLEN(map, key));
    
    // Check full scan
    int expected = countMatches(list, 0, regex);
    String lastSeenMember = null;
    int count = 11;
    int total = scan(map, key, lastSeenMember, count, 250, regex);
    assertEquals(expected, total);

    // Check correctness of partial scans
    
    for(int i = 0; i < 100; i++) {
      int index = r.nextInt(list.size());
      String lastSeen =  list.get(index);
      String pattern = "^" + lastSeen.charAt(0) + ".*";
      expected = index == list.size() - 1? 0: countMatches(list, index + 1, pattern);
      total = scan(map, key, lastSeen, count, 250, pattern) ;
      assertEquals(expected, total);
    }
    
    // Check edge cases
    
    String before = "A"; // less than any values
    String after  = "zzzzzzzzzzzzzzzz"; // larger than any values
    expected = countMatches(list, 0, regex);
    
    total = scan(map, key, before, count, 250, regex);
    assertEquals(expected, total);
    total = scan(map, key, after, count, 250, regex);
    assertEquals(0, total);
    
    // Test buffer underflow - small buffer
    // buffer size is less than needed to keep 'count' members
    expected = countMatches(list, 0, regex);
    total = scan(map, key, before, count, 100, regex);
    assertEquals(expected, total);
    total = scan(map, key, after, count, 100, regex);
    assertEquals(0, total);
    
  }
  
  private int scan(BigSortedMap map, String key, String lastSeenMember, 
      int count, int bufferSize, String regex)
  {
    int total = 0;
    List<Pair<String>> result = null;
    // Check overall functionality - full scan
    while((result = Hashes.HSCAN(map, key, lastSeenMember, count, 200, regex)) != null) {
      total += result.size() - 1;
      lastSeenMember = result.get(result.size() - 1).getFirst();
    }
    return total;
  }
  
  //@Ignore
  @Test
  public void testScannerRandomMembersEdgeCases() throws IOException {
    System.out.println("Test Hashes HRANDFIELD API call (Edge cases)");
    // Load N elements
    int N = 10000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
    
    List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 100, false, 1105);// Required size is 1104 for 100 elements
    assertEquals(100, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 1104);// Required size is 1104 for 100 elements
    assertEquals(100, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 1103);// Required size is 1093 for 99 elements
    assertEquals(99, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 1092);// Required size is 1082 for 98 elements
    assertEquals(98, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 100);// Required size is 92 for 8 elements
    assertEquals(8, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 15);// Required size is 15 for 1 element
    assertEquals(1, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, false, 10);// Required size is 15 for 1 element
    assertEquals(0, result.size());
    
    // Now with values
    result = Hashes.HRANDFIELD(map, key, 100, true, 2205);// Required size is 2204 for 100 elements
    assertEquals(100, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 2204);// Required size is 2204 for 100 elements
    assertEquals(100, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 2182);// Required size is 2182 for 99 elements
    assertEquals(99, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 2160);// Required size is 2160 for 98 elements
    assertEquals(98, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 180);// Required size is 180 for 8 elements
    assertEquals(8, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 26);// Required size is 26 for 1 element
    assertEquals(1, result.size());
    
    result = Hashes.HRANDFIELD(map, key, 100, true, 10);// Required size is 26 for 1 element
    assertEquals(0, result.size());
  }
  
  //@Ignore
  @Test
  public void testScannerRandomMembers() throws IOException {
    System.out.println("Test Hashes HRANDFIELD API call");
    // Load N elements
    int N = 100000;
    int numIter = 1000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
        
    long start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 10, false, 4096);
      assertEquals(10, result.size());
      assertTrue(unique(result));
      for (Pair<String> p: result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
    
    // Check negatives
    start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, -10, false, 4096);
      assertEquals(10, result.size());
      for (Pair<String> p: result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
    
    // Now check with values
    start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, 10, true, 4096);
      assertEquals(10, result.size());
      assertTrue(unique(result));
      for (Pair<String> p: result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
    
    // Check negatives
    start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<Pair<String>> result = Hashes.HRANDFIELD(map, key, -10, true, 4096);
      assertEquals(10, result.size());
      for (Pair<String> p: result) {
        assertTrue(list.contains(p.getFirst()));
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
  }
  
  /**
   * Checks if list has all unique members. List must be sorted.
   * @param list
   * @return true/false
   */
  private boolean unique(List<Pair<String>> list) {
    if (list.size() <= 1) return true;
    
    for(int i = 1; i < list.size(); i++) {
      if (list.get(i-1).equals(list.get(i))) {
        return false;
      }
    }
    return true; 
  }
  
  
  @Before
  public void setUp() {
    map = new BigSortedMap(100000000);
  }
  
  @After
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
  
}
