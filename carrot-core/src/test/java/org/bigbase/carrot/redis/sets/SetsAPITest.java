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
 *
 */
package org.bigbase.carrot.redis.sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class SetsAPITest {
  BigSortedMap map;
  int valSize = 8;
  long n = 1000;

  private List<String> loadData(String key, int n) {
    List<String> list = new ArrayList<String>();
    Random r = new Random();
    for (int i=0; i < n; i++) {
      String m = Utils.getRandomStr(r, 16);
      list.add(m);
      int res = Sets.SADD(map, key, m);
      if (res == 0) {
        /*DEBUG*/ System.out.println("Can not add "+ m + " it exists=" + Sets.SISMEMBER(map, key, m));
      }
      assertEquals(1, res);
      if (i % 100000 == 0) {
        System.out.println("Loaded "+ i);
      }
    }
    return list;
  }
  
  //@Ignore
  @Test
  public void runAllNoCompression() throws IOException {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 100; i++) {
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
    for (int i = 0; i < 100; i++) {
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
    
    setUp();
    testCardinalityPerformance();
    tearDown();
    setUp();
    testSimpleCalls();
    tearDown();
    setUp();
    testMoveOperation();
    tearDown();
    setUp();
    testMultipleMembersOperation();
    tearDown();
    setUp();
    testScannerRandomMembers();
    tearDown();
    setUp();
    testScannerRandomMembersDelete();
    tearDown();
    setUp();
    testScannerRandomMembersEdgeCases();
    tearDown();
    setUp();
    testScannerRandomMembersDeleteEdgeCases();
    tearDown();
    setUp();
    testScannerSkipRandom();
    tearDown();
    
    setUp();
    testScannerSkipRandomSingleScanner();
    tearDown();
    
    setUp();
    testSetScannerSkipSmall();
    tearDown();
    setUp();
    testSetScannerSkipLarge();
    tearDown();
    setUp();
    testSscanNoRegex();
    tearDown();
    setUp();
    testSscanWithRegex();
    tearDown();
    
  }
  
  @Ignore
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
    members = Sets.SMEMBERS(map, "key".getBytes(), 15);
    assertEquals(1, members.size());
    
    // small buffer - list == 2
    members = Sets.SMEMBERS(map, "key".getBytes(), 20);
    assertEquals(2, members.size());
    
    // small buffer - list == 3
    members = Sets.SMEMBERS(map, "key".getBytes(), 30);
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
  
  @Ignore
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
  
  @Ignore
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
  
  @Ignore
  @Test
  public void testSscanNoRegex() {
    System.out.println("Test Sets SSCAN API call w/o regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    Random r = new Random();
    List<String> list = loadData(key, X);
    
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
  
  @Ignore
  @Test
  public void testCardinalityPerformance() {
    System.out.println("Test Sets SCARD API call performance");
    // Load X elements
    int X = 200000;
    String key = "key";
    Random r = new Random();
    List<String> list = loadData(key, X);
    
    long total = 0;
    long start = System.currentTimeMillis();
    for(int i = 0; i < 1000; i++) {
      long card = Sets.SCARD(map, key);
      assertEquals(X, (int)card);
      total += card;
    }
    long end = System.currentTimeMillis();
    
    System.out.println("total="+ total+" time for "+ (1000 * X) +"="+ (end -start)+"ms");
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
    System.out.println("Test Sets SSCAN API call with regex pattern");
    // Load X elements
    int X = 10000;
    String key = "key";
    String regex = "^A.*";
    Random r = new Random();
    List<String> list = loadData(key, X);
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
  
  @Ignore
  @Test
  public void testSetScannerSkipSmall() throws IOException {
    System.out.println("Test Sets skip API call (small)");
    // Load X elements
    int X = 100;
    String key = "key";
    List<String> list = loadData(key, X);
    
    Collections.sort(list);
    
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int size = key.length();
    SetScanner scanner = Sets.getScanner(map, ptr, size, false);
    // Skip edge cases
    long skip = 0;
    long pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    long mPtr = scanner.memberAddress();
    int mSize = scanner.memberSize();
    String value = Utils.toString(mPtr, mSize);
    String expected = list.get((int)skip);
    assertEquals(expected, value);
    
    skip = 50;
    pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    mPtr = scanner.memberAddress();
    mSize = scanner.memberSize();
    value = Utils.toString(mPtr, mSize);
    expected = list.get((int)skip);
    assertEquals(expected, value);
    
    skip = X - 1;
    pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    mPtr = scanner.memberAddress();
    mSize = scanner.memberSize();
    value = Utils.toString(mPtr, mSize);
    expected = list.get((int)skip);
    assertEquals(expected, value);
    scanner.close();
    
  }
  
  @Ignore
  @Test
  public void testSetScannerSkipLarge() throws IOException {
    System.out.println("Test Sets skip API call (large) ");
    // Load X elements
    int X = 100000;
    String key = "key";
    List<String> list = loadData(key, X);
    
    Collections.sort(list);
    
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int size = key.length();
    SetScanner scanner = Sets.getScanner(map, ptr, size, false);
    // Skip edge cases
    long skip = 0;
    long pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    long mPtr = scanner.memberAddress();
    int mSize = scanner.memberSize();
    String value = Utils.toString(mPtr, mSize);
    String expected = list.get((int)skip);
    assertEquals(expected, value);
 
    skip = X/2 - 2;
    pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    mPtr = scanner.memberAddress();
    mSize = scanner.memberSize();
    value = Utils.toString(mPtr, mSize);
    expected = list.get((int)skip);
    assertEquals(expected, value);
    
    skip = X - 2;
    pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    mPtr = scanner.memberAddress();
    mSize = scanner.memberSize();
    value = Utils.toString(mPtr, mSize);
    expected = list.get((int)skip);
    assertEquals(expected, value);
    
    skip = X - 1;
    pos = scanner.skipTo(skip);
    assertEquals(skip, pos);
    
    mPtr = scanner.memberAddress();
    mSize = scanner.memberSize();
    value = Utils.toString(mPtr, mSize);
    expected = list.get((int)skip);
    assertEquals(expected, value);
    scanner.close();
    
  }
  
  @Ignore
  @Test
  public void testScannerSkipRandom() throws IOException {
    System.out.println("Test Sets skip API call (Random) ");
    // Load N elements
    int N = 1000000;
    int numIter = 1000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
    
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int size = key.length();
    
    Random r = new Random();
    
    long start = System.currentTimeMillis();
    for(int i = 0; i < 1000; i++) {
      SetScanner scanner = Sets.getScanner(map, ptr, size, false);
      int skipTo = r.nextInt(N);
      try {
        long pos = scanner.skipTo(skipTo);
        assertEquals(skipTo, (int) pos);
        long mPtr = scanner.memberAddress();
        int mSize = scanner.memberSize();
        String value = Utils.toString(mPtr, mSize);
        String expected = list.get((int)skipTo);
        assertEquals(expected, value);
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(numIter + " random skips for "+ N +" cardinality set time="+ (end - start)+"ms");
  }
  
  
  @Ignore
  @Test
  public void testScannerSkipRandomSingleScanner() throws IOException {
    System.out.println("Test Sets skip API call (Random Single Scanner) ");
    // Load N elements
    int N = 1000000;
    int numIter = 10000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
    
    long ptr = UnsafeAccess.allocAndCopy(key, 0, key.length());
    int size = key.length();
        
    long start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      SetScanner scanner = Sets.getScanner(map, ptr, size, false);
      try {
        long[] skips = Utils.randomDistinctArray(N, 10);
        for (int k = 0; k < skips.length; k++) {
          int skipTo = (int)skips[k];
          long pos = scanner.skipTo(skipTo);
          assertEquals(skipTo, (int) pos);
          long mPtr = scanner.memberAddress();
          int mSize = scanner.memberSize();
          String value = Utils.toString(mPtr, mSize);
          String expected = list.get((int)skipTo);
          assertEquals(expected, value);
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(numIter + " random skips x"+ 10 +" for "+ N +" cardinality set time="+ (end - start)+"ms");
  }
  
 
  @Ignore
  @Test
  public void testScannerRandomMembersEdgeCases() throws IOException {
    System.out.println("Test Sets SRANDMEMBER API call (Edge cases)");
    // Load N elements
    int N = 10000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
    
    List<String> result = Sets.SRANDMEMBER(map, key, 100, 1705);// Required size is 1704 for 100 elements
    assertEquals(100, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 1704);// Required size is 1104 for 100 elements
    assertEquals(100, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 1703);// Required size is 1093 for 99 elements
    assertEquals(99, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 1686);// Required size is 1082 for 98 elements
    assertEquals(98, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 100);// Required size is 89 for 5 elements
    assertEquals(5, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 21);// Required size is 21 for 1 element
    assertEquals(1, result.size());
    
    result = Sets.SRANDMEMBER(map, key, 100, 10);// Required size is 15 for 1 element
    assertEquals(0, result.size());
  }
  
  @Ignore
  @Test
  public void testScannerRandomMembers() throws IOException {
    System.out.println("Test Sets SRANDMEMBER API call");
    // Load N elements
    int N = 100000;
    int numIter = 1000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
        
    long start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<String> result = Sets.SRANDMEMBER(map, key, 10, 4096);
      assertEquals(10, result.size());
      assertTrue(Utils.unique(result));
      assertTrue(list.containsAll(result));
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
    
    // Check negatives
    start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<String> result = Sets.SRANDMEMBER(map, key, -10, 4096);
      assertEquals(10, result.size());
      assertTrue(list.containsAll(result));
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
  }
  
  @Ignore
  @Test
  public void testScannerRandomMembersDeleteEdgeCases() throws IOException {
    System.out.println("Test Sets SPOP API call (Edge cases)");
    // Load N elements
    int N = 10000;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
    
    List<String> result = Sets.SPOP(map, key, 100, 1705);// Required size is 1704 for 100 elements
    assertEquals(100, result.size());
    
    result = Sets.SPOP(map, key, 100, 1704);// Required size is 1704 for 100 elements
    assertEquals(100, result.size());
    
    result = Sets.SPOP(map, key, 100, 1687);// Required size is 1687 for 99 elements
    assertEquals(99, result.size());
    
    result = Sets.SPOP(map, key, 100, 1670);// Required size is 1670 for 98 elements
    assertEquals(98, result.size());
    
    result = Sets.SPOP(map, key, 100, 100);// Required size is 89 for 5 elements
    assertEquals(5, result.size());
    
    result = Sets.SPOP(map, key, 100, 21);// Required size is 21 for 1 element
    assertEquals(1, result.size());
    
    result = Sets.SPOP(map, key, 100, 10);// Required size is 21 for 1 element
    assertEquals(0, result.size());
  }
  
  @Ignore
  @Test
  public void testScannerRandomMembersDelete() throws IOException {
    System.out.println("Test Sets SPOP API call");
    // Load N elements
    int N = 100000;
    int numIter = 100;
    String key = "key";
    List<String> list = loadData(key, N);
    
    Collections.sort(list);
        
    long start = System.currentTimeMillis();
    for(int i = 0; i < numIter; i++) {
      List<String> result = Sets.SPOP(map, key, 10, 4096);
      assertEquals(10, result.size());
      assertTrue(Utils.unique(result));
      assertTrue(list.containsAll(result));
      // verify that they were deleted
      for (String s: result) {
        int res = Sets.SISMEMBER(map, key, s);
        assertEquals(0, res);
      }
      if (i % 100 == 0) {
        System.out.println("Skipped " + i);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
    
    // Check negatives
//    start = System.currentTimeMillis();
//    for(int i = 0; i < numIter; i++) {
//      List<String> result = Sets.SPOP(map, key, -10, 4096);
//      assertEquals(10, result.size());
//      assertTrue(list.containsAll(result));
//      // verify that they were deleted
//      for (String s: result) {
//        int res = Sets.SISMEMBER(map, key, s);
//        assertEquals(0, res);
//      }
//      if (i % 100 == 0) {
//        System.out.println("Skipped " + i);
//      }
//    }
//    end = System.currentTimeMillis();
//    System.out.println(numIter + " random members for "+ N +" cardinality set time="+ (end - start)+"ms");
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
  
  public void setUp() {
    map = new BigSortedMap(1000000000);
  }
  
  public void tearDown() {
    // Dispose
    map.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
}
