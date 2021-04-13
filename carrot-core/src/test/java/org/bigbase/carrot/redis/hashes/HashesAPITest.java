package org.bigbase.carrot.redis.hashes;

import static org.junit.Assert.assertEquals;

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
  
  //@Ignore
  @Test
  public void testSscanNoRegex() {
    System.out.println("Test Sets HSCAN API call w/o regex pattern");
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
  
  //@Ignore
  @Test
  public void testSscanWithRegex() {
    System.out.println("Test Sets HSCAN API call with regex pattern");
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
