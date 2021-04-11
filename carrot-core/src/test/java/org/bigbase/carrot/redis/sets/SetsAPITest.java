package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SetsAPITest {
  BigSortedMap map;
  int valSize = 8;
  long n = 1000;

  
  static {
    //UnsafeAccess.debug = true;
  }
    
  @Test
  public void testSimpleCalls() throws IOException {
    System.out.println("Test Sets ADD/ISMEMBER/MEMBERS API calls");
    // Adding to set which does not exists
    int result = Sets.SADD(map, "key", "member1");
    assertEquals(1, result);
    // One more time - exists already
    result = Sets.SADD(map, "key", "member1");    
    assertEquals(0, result);
    // Positive result
    result = Sets.SISMEMBER(map, "key", "member1");
    assertEquals(1, result);
    // Negative result
    result = Sets.SISMEMBER(map, "key", "member2");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member1");
    assertEquals(0, result);
    // Add multiple members
    result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3"});
    // We expect only 2 new elements
    assertEquals(2, result);
    
    List<byte[]> members = Sets.SMEMBERS(map, "key".getBytes(), 1000);
    assertEquals(3, members.size());
    assertEquals("member1", new String(members.get(0)));
    assertEquals("member2", new String(members.get(1)));
    assertEquals("member3", new String(members.get(2)));
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member1");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member2");
    assertEquals(0, result);
    // Wrong key
    result = Sets.SISMEMBER(map, "key1", "member3");
    assertEquals(0, result);
        
    // Wrong key greater than "key"
    members = Sets.SMEMBERS(map, "key1".getBytes(), 1000);
    assertNull(members);
    
    // Wrong key less than "key"
    members = Sets.SMEMBERS(map, "kex".getBytes(), 1000);
    assertNull(members);
    
    // Small buffer - empty list
    members = Sets.SMEMBERS(map, "key".getBytes(), 10);
    assertEquals(0, members.size());
    // small buffer - list == 1
    members = Sets.SMEMBERS(map, "key".getBytes(), 20);
    assertEquals(1, members.size());
    
    // small buffer - list == 2
    members = Sets.SMEMBERS(map, "key".getBytes(), 30);
    assertEquals(2, members.size());
    
    // small buffer - list == 3
    members = Sets.SMEMBERS(map, "key".getBytes(), 40);
    assertEquals(3, members.size());
    
    
    // Add multiple members
    result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3", "member4", "member5"});
    // We expect only 2 new elements
    assertEquals(2, result);
    
    members = Sets.SMEMBERS(map, "key".getBytes(), 1000);
    assertEquals(5, members.size());
    assertEquals("member1", new String(members.get(0)));
    assertEquals("member2", new String(members.get(1)));
    assertEquals("member3", new String(members.get(2)));
    assertEquals("member4", new String(members.get(3)));
    assertEquals("member5", new String(members.get(4)));
    
  }
  
  @Test
  public void testMoveOperation() {
    System.out.println("Test Sets SMOVE API call");
    // Add multiple members
    int result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3", "member4", "member5"});
    // We expect only 2 new elements
    assertEquals(5, result);
    result = Sets.SMOVE(map, "key", "key1", "member1");
    assertEquals(1, result);
    assertEquals(4, (int)Sets.SCARD(map, "key"));
    assertEquals(1, (int)Sets.SCARD(map, "key1"));
    
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member1");
    // Now result = 0
    assertEquals(0, result);
    assertEquals(4, (int)Sets.SCARD(map, "key"));
    assertEquals(1, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member2");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(3, (int)Sets.SCARD(map, "key"));
    assertEquals(2, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member3");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(2, (int)Sets.SCARD(map, "key"));
    assertEquals(3, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member4");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(1, (int)Sets.SCARD(map, "key"));
    assertEquals(4, (int)Sets.SCARD(map, "key1"));
    // Try one more time
    result = Sets.SMOVE(map, "key", "key1", "member5");
    // Now result = 1
    assertEquals(1, result);
    assertEquals(0, (int)Sets.SCARD(map, "key"));
    assertEquals(5, (int)Sets.SCARD(map, "key1"));

  }
  
  @Test
  public void testMultipleMembersOperation() {
    System.out.println("Test Sets SMISMEMBER API call");
    // Add multiple members
    int result = Sets.SADD(map, "key" , new String[] {"member1", "member2", "member3", "member4", "member5"});
    assertEquals(5, result);
    byte[] res = Sets.SMISMEMBER(map, "key", new String[] {"member1", "member2", "member3", "member4", "member5"});
    // All must be 1
    assertEquals(5, res.length);
    for(int i=0; i < res.length; i++) {
      assertEquals(1, (int)res[i]);
    }
    
    res = Sets.SMISMEMBER(map, "key", new String[] {"member3", "member4", "member5"});
    // All must be 1
    assertEquals(3, res.length);
    for(int i=0; i < res.length; i++) {
      assertEquals(1, (int)res[i]);
    }
    
    res = Sets.SMISMEMBER(map, "key1", new String[] {"member1", "member2", "member3", "member4", "member5"});
    // All must be 1
    assertEquals(5, res.length);
    for(int i=0; i < res.length; i++) {
      assertEquals(0, (int)res[i]);
    }
    
    res = Sets.SMISMEMBER(map, "key", new String[] {"xember1", "xember2", "xember3", "member4", "member5"});
    // All must be 1
    assertEquals(5, res.length);
    assertEquals(0, (int)res[0]);
    assertEquals(0, (int)res[1]);
    assertEquals(0, (int)res[2]);
    assertEquals(1, (int)res[3]);
    assertEquals(1, (int)res[4]);
    
  }
  
  @Test
  public void testSscanNoRegex() {
    System.out.println("Test Sets SSCAN API call w/o regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    Random r = new Random();
    List<String> list = new ArrayList<String>();
    for (int i=0; i < X; i++) {
      String m = Utils.getRandomStr(r, 10);
      list.add(m);
      int res = Sets.SADD(map, key, m);
      assertEquals(1, res);
    }
    
    Collections.sort(list);
    
    // Check cardinality
    assertEquals(X, (int)Sets.SCARD(map, key));
    
    // Check full scan
    String lastSeenMember = null;
    int count = 11;
    int total = scan(map, key, lastSeenMember, count, 200, null);
    assertEquals(X, total);

    // Check correctness of partial scans
    
    for(int i = 0; i < 1000; i++) {
      int index = r.nextInt(list.size());
      String lastSeen =  list.get(index);
      int expected = list.size() - index - 1;
      total = scan(map, key, lastSeen, count, 200, null) ;
      assertEquals(expected, total);
    }
    
    // Check edge cases
    
    String before = "A";
    String after  = "zzzzzzzzzzzzzzzz";
    
    total = scan(map, key, before, count, 200, null);
    assertEquals(X, total);
    total = scan(map, key, after, count, 200, null);
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
  
  @Test
  public void testSscanWithRegex() {
    System.out.println("Test Sets SSCAN API call with regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    String regex = "^A.*";
    Random r = new Random();
    List<String> list = new ArrayList<String>();
    for (int i=0; i < X; i++) {
      String m = Utils.getRandomStr(r, 10);
      list.add(m);
      int res = Sets.SADD(map, key, m);
      assertEquals(1, res);
    }
    
    Collections.sort(list);
    
    // Check cardinality
    assertEquals(X, (int)Sets.SCARD(map, key));
    
    // Check full scan
    int expected = countMatches(list, 0, regex);
    String lastSeenMember = null;
    int count = 11;
    int total = scan(map, key, lastSeenMember, count, 200, regex);
    assertEquals(expected, total);

    // Check correctness of partial scans
    
    for(int i = 0; i < 100; i++) {
      int index = r.nextInt(list.size());
      String lastSeen =  list.get(index);
      String pattern = "^" + lastSeen.charAt(0) + ".*";
      expected = index == list.size() -1? 0: countMatches(list, index + 1, pattern);
      total = scan(map, key, lastSeen, count, 200, pattern) ;
      assertEquals(expected, total);
    }
    
    // Check edge cases
    
    String before = "A"; // less than any values
    String after  = "zzzzzzzzzzzzzzzz"; // larger than any values
    expected = countMatches(list, 0, regex);
    
    total = scan(map, key, before, count, 200, regex);
    assertEquals(expected, total);
    total = scan(map, key, after, count, 200, regex);
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
    List<String> result = null;
    // Check overall functionality - full scan
    while((result = Sets.SSCAN(map, key, lastSeenMember, count, 200, regex)) != null) {
      total += result.size() - 1;
      lastSeenMember = result.get(result.size() - 1);
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
