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
package org.bigbase.carrot.redis.sparse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.Key;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.Commons;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.junit.Ignore;
import org.junit.Test;

public class SparseBitmapsTest {
  BigSortedMap map;
  Key key, key2;
  long buffer;
  int bufferSize = 64;
  int keySize = 8;
  int N = 1000000;
  
  static {
    //UnsafeAccess.debug = true;
  }
    
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    key = getKey();
  }
  
  private void tearDown() {
    map.dispose();

    UnsafeAccess.free(key.address);
    if (key2 != null) {
      UnsafeAccess.free(key2.address);
      key2 = null;
    }
    UnsafeAccess.free(buffer);
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.memoryStats();
  }

  //@Ignore
  @Test
  public void runAllNoCompression() {
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
  public void runAllCompressionLZ4() {
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
  public void runAllCompressionLZ4HC() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC");
      allTests();
      BigSortedMap.printMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() {
    setUp();
    testSetBitGetBitLoop();
    tearDown();
    setUp();
    testPerformance();
    tearDown();
    setUp();
    testSparseLength();
    tearDown();
    setUp();
    testDeleteExists();
    tearDown();
    setUp();
    testBitCounts();
    tearDown();
    setUp();
    testBitcountPerformance();
    tearDown();    
    setUp();
    testBitPositions();
    tearDown();
    setUp();
    testBitGetRange();
    tearDown();
    setUp();
    testBitSetRange();
    tearDown();
  }
  
  @Ignore
  @Test
  public void testSetBitGetBitLoop() {
    
    System.out.println("Test SetBitGetBitLoop");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed="+ seed);
    
    int totalCount = 0;
    
    long start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong() / 2);
      int oldbit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (oldbit == 1) {
        continue;
      }
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      if (bit != 0) {
        System.out.println("FAILED i="+ i +" offset =" + offset);
      }
      assertEquals(0, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (bit != 1) {
        System.out.println("i="+ i + " offset=" + offset);
      }
      assertEquals(1, bit);
      totalCount++;
      if (totalCount % 10000 == 0) {
        System.out.println(totalCount);
      }
    }
    
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(totalCount, (int)count);

    /*DEBUG*/ System.out.println("totalCount=" + totalCount+ " N="+ N);
    /*DEBUG*/ System.out.println("Total RAM=" + UnsafeAccess.getAllocatedMemory());
    
    BigSortedMap.printMemoryAllocationStats();
    
    long end  = System.currentTimeMillis();
    
    System.out.println("Time for " + N + " new SetBit/GetBit/CountBits =" + (end - start) + "ms");
    
    Random rr = new Random();
    rr.setSeed(seed);
    
    start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(rr.nextLong() / 2);
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 0);
      assertEquals(1, bit);
      bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      assertEquals(0, bit);
      if ( i % 10000 == 0) {
        System.out.println(i);
      }
    }
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(0, (int)count);
    end  = System.currentTimeMillis();
    System.out.println("Time for " + N + " existing SetBit/GetBit/CountBits =" + (end - start) + "ms");
  }
  
  
  @Ignore
  @Test
  public void testPerformance() {
    
    System.out.println("Test Performance basic operations");
    long offset = 0;
    long max = Long.MIN_VALUE;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println();
    long start = System.currentTimeMillis();
    long expected = N;
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong() / 2);
      
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (bit == 1) {
        expected--;
      }
      if (offset > max) {
        max = offset;
      }
      if (i % 100000 == 0) {
        System.out.println("SetBit " + i);
      }
    }
    long end  = System.currentTimeMillis();    
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM    =" + memory);
    /*DEBUG*/ System.out.println("Total loaded =" + expected);
    
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);
    
    System.out.println("Time for " + N + " new SetBit=" + (end - start) + "ms");
    System.out.println("Compression ratio="+( ((double) max) / (8 * memory)));
    BigSortedMap.printMemoryAllocationStats();
    
    r.setSeed(seed);
    start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong() / 2);      
      int bit = SparseBitmaps.SGETBIT(map, key.address, key.length, offset); 
      assertEquals(1, bit);
      if (i % 100000 == 0) {
        System.out.println("GetBit " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Time for " + N + " GetBit=" + (end - start) + "ms");
    
    r.setSeed(seed);
    
    start = System.currentTimeMillis();
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong() / 2);      
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 0); 
      assertEquals(1, bit);
      if (i % 100000 == 0) {
        System.out.println("SetBit erase " + i);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Time for " + N + " SetBit erase=" + (end - start) + "ms");
    assertEquals(0, (int) BigSortedMap.countRecords(map));
    
  }
  
  @Ignore
  @Test
  public void testDeleteExists() {
    
    System.out.println("Test Delete/Exists basic operations");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println();
    long expected = N/10;
    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextLong() / 2);
      
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (bit == 1) {
        expected--;
      }
      if (i % 100000 == 0) {
        System.out.println("DeleteEixts " + i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM    =" + memory);
    /*DEBUG*/ System.out.println("Total loaded =" + expected);
    
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);
    
    assertTrue(SparseBitmaps.EXISTS(map, key.address, key.length));
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) BigSortedMap.countRecords(map));
  }
  
  @Ignore
  @Test
  public void testBitCounts() {
    
    System.out.println("Test Bit counts operations");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    TreeSet<Integer> bits = new TreeSet<Integer>();
    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextInt());
      bits.add((int)offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (i % 100000 == 0) {
        System.out.println("BitCounts " + i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM    =" + memory);
    /*DEBUG*/ System.out.println("Total loaded =" + bits.size());
    int size = bits.size();
    int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
    System.out.println("Edge cases ");
    // Test 1: no start, end limits
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(size, (int)count);
    
    assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));
    
    // Test 2: no end limit
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Long.MIN_VALUE, Commons.NULL_LONG);
    assertEquals(size, (int)count);
    
    // Test 3: no start limit
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Integer.MAX_VALUE);
    assertEquals(size, (int)count);
    
    // Test 4: end < start (both positive)
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, 100, 99);
    assertEquals(0, (int)count);
    
    // Test 5: end < start (both negative)
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -99, -100);
    assertEquals(0, (int)count);
    
    // Test 6: start and end cover all
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -2 * strlen, 2 * strlen);
    assertEquals(size, (int)count);
    
    // Test 7: negatives
    count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, -strlen, -1);
    assertEquals(size, (int)count);
    
    System.out.println("Edge cases start=end");

    // Test 8: start = end
    for (int i = 0; i < 100000; i++) {
      int index = r.nextInt(strlen);
      int expected = expectedNumber(bits, index, index);
      count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, index, index);
      assertEquals(expected, (int)count);
      
      if (i % 1000 == 0) {
        System.out.println("start=end "+ i);
      }
    }  
    System.out.println("Random tests");
    r.setSeed(seed);
    for (int i = 0; i < N/10; i++) {
      int x1 = r.nextInt(2 * strlen);
      x1 -= strlen;
      int x2 = r.nextInt(2 * strlen);
      x2 -= strlen;
      int start, end;
      if (x1 > x2) {
        end = x1; start = x2;
      } else {
        end = x2; start = x1;
      }
      int expected = expectedNumber(bits, start, end);
      long total = SparseBitmaps.SBITCOUNT(map, key.address, key.length, start, end);
      assertEquals(expected, (int) total);
      if (i % 1000 == 0) {
        System.out.println("random bc "+ i);
      }
    }
    
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) BigSortedMap.countRecords(map));
  }
  
  @Ignore
  @Test
  public void testBitPositions() {
    
    System.out.println("Test Bit position operations");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    TreeSet<Integer> bits = new TreeSet<Integer>();
    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextInt());
      bits.add((int)offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (i % 100000 == 0) {
        System.out.println("BitPos " + i);
      }
    }
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM    =" + memory);
    /*DEBUG*/ System.out.println("Total loaded =" + bits.size());
    int size = bits.size();
    int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));

    System.out.println("Edge cases ");
    // Test 1: no start, end limits bit = 1
    // bit == 1
    int bit = 1;
    long pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Commons.NULL_LONG, Commons.NULL_LONG);
    int expected = expectedPositionSet(bits, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);
    
    // bit == 0
    bit = 0;
    expected = expectedPositionUnSet(bits, Commons.NULL_LONG, Commons.NULL_LONG);
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);
    
    // Test 2: no end limit
    
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    expected = expectedPositionSet(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);    

    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    expected = expectedPositionUnSet(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
    assertEquals(expected, (int) pos);    

    // Test 3: no start limit
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    expected = expectedPositionSet(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    assertEquals(expected, (int) pos);
    
    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    expected = expectedPositionUnSet(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
    assertEquals(expected, (int) pos);
    
    // Test 4: end < start (both positive)
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, 100, 99);
    assertEquals(-1, (int) pos);
    
    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, 100, 99);
    assertEquals(-1, (int) pos);
    
    
    // Test 5: end < start (both negative)
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -99, -100);
    assertEquals(-1, (int) pos);
    
    // Test 6: start and end cover all
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -2 * strlen, 2 * strlen);
    assertEquals(bits.first(), (int) pos);
    
    bit = 0;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit, -2 * strlen, 2 * strlen);
    expected = expectedPositionUnSet(bits, -2 * strlen, 2 * strlen);
    assertEquals(expected, (int) pos);

    // Test 7: negatives
    bit = 1;
    pos = SparseBitmaps.SBITPOS(map, key.address, key.length, bit,  -strlen, -1);
    assertEquals(bits.first(), (int) pos);
    
    System.out.println("Edge cases start=end");

    // Test 8: start = end
    for (int i = 0; i < 100000; i++) {
      int index = r.nextInt(strlen);
      expected = expectedPositionSet(bits, index, index);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 1, index, index);
      assertEquals(expected, (int) pos);
      
      expected = expectedPositionUnSet(bits, index, index);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 0, index, index);
      assertEquals(expected, (int) pos);      
      if (i % 10000 == 0) {
        System.out.println("start=end "+ i);
      }
    }  
    
    System.out.println("Random tests");
    r.setSeed(seed);
    for (int i = 0; i < N/10; i++) {
      int x1 = r.nextInt(2 * strlen);
      x1 -= strlen;
      int x2 = r.nextInt(2 * strlen);
      x2 -= strlen;
      int start, end;
      if (x1 > x2) {
        end = x1; start = x2;
      } else {
        end = x2; start = x1;
      }
      expected = expectedPositionSet(bits, start, end);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 1, start, end);
      assertEquals(expected, (int) pos);
      expected = expectedPositionUnSet(bits, start, end);
      pos = SparseBitmaps.SBITPOS(map, key.address, key.length, 0, start, end);
      assertEquals(expected, (int) pos);
      if (i % 10000 == 0) {
        System.out.println("random bc "+ i);
      }
    }
    
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) BigSortedMap.countRecords(map));
  }

  
  @Ignore
  @Test
  public void testBitcountPerformance() {
    System.out.println("Test Bit counts performance");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println();
    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextInt());
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (i % 100000 == 0) {
        System.out.println("BitCounts " + i);
      }
    }
    long strlen = SparseBitmaps.SSTRLEN(map, key.address, key.length);
    
    System.out.println("Random tests");
    r.setSeed(seed);
    
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      int x1 = r.nextInt((int)strlen);
      int x2 = r.nextInt((int)strlen);
      int start, end;
      if (x1 > x2) {
        end = x1; start = x2;
      } else {
        end = x2; start = x1;
      }
      long total = SparseBitmaps.SBITCOUNT(map, key.address, key.length, start, end);
      if (i % 1000 == 0) {
        System.out.println("random bc "+ i);
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println("SBITCOUNT for bitmap="+ strlen+" long ="+ ((double) 1000 * 1000)/(endTime - startTime)+" RPS");
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    assertEquals(0, (int) BigSortedMap.countRecords(map));
  }
  
  /**
   * Get expected number of bits
   * @param set set
   * @param start from offset (inclusive)
   * @param end to offset (inclusive)
   * @return number of bits set
   */
  private int expectedNumber(NavigableSet<Integer> set, long start, long end) {
    
    int strlen = set.last()/ Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    
    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }
    
    if (start >= strlen) {
      return 0;
    }
    
    if (start < 0) {
      start = strlen + start;
    }
    
    if (start < 0) {
      start = 0;
    }
    
    if (end != Commons.NULL_LONG &&  end < 0) {
      end = strlen + end;
    }
    
    if (end < start || end < 0) {
      return 0;
    } 
    
    Iterator<Integer> it = set.iterator();
    int count = 0;
    while(it.hasNext()) {
      Integer v = it.next();
      if (between(start, end, v)) {
        count++;
      }
    }
    return count;
  }
  
  private boolean between(long start, long end, int value) {
    int off = value / Utils.BITS_PER_BYTE;
    return off >= start && off <= end;
  }
  
  /**
   * Get expected position for set bit
   * @param set set
   * @param start from offset (inclusive)
   * @param end to offset (inclusive)
   * @return forts set bit position
   */
  private int expectedPositionUnSet(NavigableSet<Integer> set, long start, long end) {
    
    if (set.isEmpty()) {
      return 0;
    }
    
    int strlen = set.last()/ Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    
    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }
    
    if (start >= strlen) {
      return -1;
    }
    
    if (start < 0) {
      start = strlen + start;
    }
    
    if (start < 0) {
      start = 0;
    }
    
    if (end != Commons.NULL_LONG &&  end < 0) {
      end = strlen + end;
    }
    
    if (end < start || end < 0) {
      return -1;
    } 
    
    long startOff = start * Utils.BITS_PER_BYTE;
    long endPos = (end + 1) * Utils.BITS_PER_BYTE;
    for (long off = startOff; off < endPos; off++) {
      if (set.contains((int) off)) {
        continue;
      }
      return (int) off;
    }
    
    return -1;
  }
  
 private int expectedPositionSet(NavigableSet<Integer> set, long start, long end) {
    
    if (set.isEmpty()) {
      return -1;
    }
    int strlen = set.last()/ Utils.BITS_PER_BYTE + 1;
    if (start == Commons.NULL_LONG) {
      start = 0;
    }
    
    if (end == Commons.NULL_LONG || end >= strlen) {
      end = strlen - 1;
    }
    
    if (start >= strlen) {
      return -1;
    }
    
    if (start < 0) {
      start = strlen + start;
    }
    
    if (start < 0) {
      start = 0;
    }
    
    if (end != Commons.NULL_LONG &&  end < 0) {
      end = strlen + end;
    }
    
    if (end < start || end < 0) {
      return -1;
    } 
        
    Integer v =  set.ceiling((int)(start * Utils.BITS_PER_BYTE));
    if (v != null && v < ((end + 1) * Utils.BITS_PER_BYTE)) {
      return v;
    }
    return -1;
  }

 @Ignore
 @Test
 public void testBitGetRange() {
   
   System.out.println("Test Bit Get Range operations");
   long offset = 0;
   Random r = new Random();
   long seed = r.nextLong();
   r.setSeed(seed);
   System.out.println("Test seed=" + seed);
   TreeSet<Integer> bits = new TreeSet<Integer>();
   
   for (int i = 0; i < N/10 ; i++) {
     offset = Math.abs(r.nextInt()/10);
     bits.add((int)offset);
     SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
     if (i % 100000 == 0) {
       System.out.println("BitGetRange " + i);
     }
   }
   long memory = UnsafeAccess.getAllocatedMemory();
   System.out.println("Total RAM    =" + memory);
   System.out.println("Total loaded =" + bits.size());
   
   int strlen = bits.last() / Utils.BITS_PER_BYTE + 1;
   assertEquals(strlen, (int) SparseBitmaps.SSTRLEN(map, key.address, key.length));
   // Check bit counts
   assertEquals(bits.size(), (int) SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG));
   long buffer = UnsafeAccess.malloc(strlen); // buffer size to fit all bitmap
   int bufferSize = strlen;
   
   System.out.println("Edge cases ");
   // Test 1: no start, end limits bit = 1
   int expected = expectedNumber(bits, Commons.NULL_LONG, Commons.NULL_LONG);
   long range = SparseBitmaps.SGETRANGE(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG, buffer, bufferSize);
   assertEquals(strlen, (int) range);
   long count = Utils.bitcount(buffer, (int)range);
   assertEquals(expected, (int) count);
   
   // Clear buffer
   UnsafeAccess.setMemory(buffer, bufferSize, (byte)0);
   
   // Test 2: no end limit
   
   expected = expectedNumber(bits, Long.MIN_VALUE + 1, Commons.NULL_LONG);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, Long.MIN_VALUE + 1, Commons.NULL_LONG, buffer, bufferSize);
   assertEquals(strlen, (int) range);
   count = Utils.bitcount(buffer, (int)range);
   assertEquals(expected, (int) count);
   // Clear buffer
   UnsafeAccess.setMemory(buffer, bufferSize, (byte)0);   

   // Test 3: no start limit
   expected = expectedNumber(bits, Commons.NULL_LONG, Long.MAX_VALUE >>> 4);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, Commons.NULL_LONG, Long.MAX_VALUE >>> 4, buffer, bufferSize);
   assertEquals(strlen, (int) range);
   count = Utils.bitcount(buffer, (int)range);
   assertEquals(expected, (int) count);
   // Clear buffer
   UnsafeAccess.setMemory(buffer, bufferSize, (byte)0); 
   
   // Test 4: end < start (both positive)
   
   expected = expectedNumber(bits, 100, 99);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, 100, 99, buffer, bufferSize);
   assertEquals(0, (int) range);
   
   // Test 5: end < start (both negative)
   expected = expectedNumber(bits, -99, -100);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, -99, -100, buffer, bufferSize);
   assertEquals(0, (int) range);
   
   // Test 6: start and end cover all
   expected = expectedNumber(bits, -2 * strlen, 2 * strlen);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, -2 * strlen, 2 * strlen, buffer, bufferSize);
   assertEquals(strlen, (int) range);
   count = Utils.bitcount(buffer, (int) range);
   assertEquals(expected, (int) count);
   // Clear buffer
   UnsafeAccess.setMemory(buffer, bufferSize, (byte)0); 

   // Test 7: negatives
   expected = expectedNumber(bits, -strlen, -1);
   range = SparseBitmaps.SGETRANGE(map, key.address, key.length, -strlen, -1, buffer, bufferSize);
   assertEquals(strlen, (int) range);
   count = Utils.bitcount(buffer, (int) range);
   assertEquals(expected, (int) count);
   // Clear buffer
   UnsafeAccess.setMemory(buffer, bufferSize, (byte) 0); 
   
   System.out.println("Edge cases start=end");

   // Test 8: start = end
   for (int i = 0; i < 1000; i++) {
     int index = r.nextInt(strlen);
     expected = expectedNumber(bits, index, index);
     range = SparseBitmaps.SGETRANGE(map, key.address, key.length, index, index, buffer, bufferSize);
     assertEquals(1, (int) range);
     count = Utils.bitcount(buffer, (int) range);
     assertEquals(expected, (int) count);
     // Clear first byte of a buffer
     UnsafeAccess.putByte(buffer, (byte) 0);
     
     if (i % 10000 == 0) {
       System.out.println("start=end "+ i);
     }
   }  
   
   // Test 9: random tests
   System.out.println("Random tests");
   r.setSeed(seed);
   for (int i = 0; i < 1000; i++) {
     int x1 = r.nextInt(2 * strlen);
     x1 -= strlen;
     int x2 = r.nextInt(2 * strlen);
     x2 -= strlen;
     int start, end;
     if (x1 > x2) {
       end = x1; start = x2;
     } else {
       end = x2; start = x1;
     }
     expected = expectedNumber(bits, start, end);
     range = SparseBitmaps.SGETRANGE(map, key.address, key.length, start, end, buffer, bufferSize);
     count = Utils.bitcount(buffer, (int) range);
     assertEquals(expected, (int) count);
     
     if (i % 100 == 0) {
       System.out.println("random bc "+ i);
     }
   }
   
   // Test 10: batch scanning
   
   System.out.println("Batch reading");
   
   int batchSize = strlen/100;
   int off = 0;
   long bitCount = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
   long bcount = 0;
   
   while (off < strlen) {
     long rr = SparseBitmaps.SGETRANGE(map, key.address, key.length, off, off + batchSize - 1, buffer, bufferSize);
     bcount += Utils.bitcount(buffer, (int) rr);
     off += batchSize;
   }
   
   assertEquals(bitCount, bcount);
   UnsafeAccess.free(buffer);
   
   SparseBitmaps.DELETE(map, key.address, key.length);
   assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
   assertEquals(0, (int) BigSortedMap.countRecords(map));
   
 } 
 
  @Ignore
  @Test
  public void testSparseLength() {
    System.out.println("Test testSparseLength");

    long offset;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    
    long max = - Long.MAX_VALUE;
    long start = System.currentTimeMillis();
    long totalCount = 0;
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong() / 2) ;
      int old = SparseBitmaps.SGETBIT(map, key.address, key.length, offset);
      if (old == 1) {
        continue;
      }
      if (offset > max) {
        max = offset;
      }
      int v = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1);
      assertEquals(0, v);
      totalCount++;
      long len = SparseBitmaps.SSTRLEN(map, key.address, key.length);
      long expectedlength = (max / Utils.BITS_PER_BYTE) + 1;
      assertEquals(expectedlength, len);
      if (i % 10000 == 0 && i > 0) {
        System.out.println(i);
      }
    }
    
    long end  = System.currentTimeMillis();

    System.out.println("\nTotal RAM=" + UnsafeAccess.getAllocatedMemory()+"\n");
    BigSortedMap.printMemoryAllocationStats();

    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(totalCount, count);
    System.out.println("Time for " + N + " SetBit/BitCount/StrLength =" + (end - start) + "ms");
    
  }
  
  @Ignore
  @Test
  public void testBitSetRange() {
    
    System.out.println("Test Bit Set Range operations");
    long offset = 0;
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("Test seed=" + seed);
    TreeSet<Integer> bits = new TreeSet<Integer>();
    System.out.println("Loading first sparse ");
    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextInt()/10);
      bits.add((int)offset);
      SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if ((i + 1) % 100000 == 0) {
        System.out.println("BitSetRange " + (i + 1));
      }
    }
    
    System.out.println("Loaded key1 " + bits.size() + " bits");
    
    key2 = getKey();
    System.out.println("Loading second sparse ");
    TreeSet<Integer> bits2 = new TreeSet<Integer>();

    for (int i = 0; i < N/10 ; i++) {
      offset = Math.abs(r.nextInt()/10);
      bits2.add((int)offset);
      SparseBitmaps.SSETBIT(map, key2.address, key2.length, offset, 1); 
      if ((i + 1) % 100000 == 0) {
        System.out.println("BitSetRange2 " + (i +1));
      }
    }
    System.out.println("Loaded key2 " + bits2.size() + " bits");

    long strlen1 = bits.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen1, SparseBitmaps.SSTRLEN(map, key.address, key.length));
    long strlen2 = bits2.last() / Utils.BITS_PER_BYTE + 1;
    assertEquals(strlen2, SparseBitmaps.SSTRLEN(map, key2.address, key2.length));  
    assertEquals(bits.size(), (int) SparseBitmaps.SBITCOUNT(map, key.address, key.length, 
      Commons.NULL_LONG, Commons.NULL_LONG));
    assertEquals(bits2.size(), (int) SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, 
      Commons.NULL_LONG, Commons.NULL_LONG));
    
    // Test 1: small overwrites <= BYTES_PER_CHUNK
    int bufferSize = (int) Math.max(strlen1, strlen2);
    long buffer = UnsafeAccess.malloc(bufferSize);
    long buffer2 = UnsafeAccess.malloc(bufferSize);
    
    int strlen = (int) Math.min(strlen1, strlen2);
    // Original bit count for key2 sparse
    long bc = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
    
    System.out.println("Running small overwrites sub-test");
    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK) + 1;
      int off = r.nextInt((int)strlen - len);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off, off + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer,  len,  (byte) 0);
      
      int rangeSize = (int) SparseBitmaps.SGETRANGE(map, key.address, key.length, 
        off, off + len - 1, buffer, bufferSize);  
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off, buffer, rangeSize);
      assertEquals(strlen2, size);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer2,  len,  (byte) 0);
      SparseBitmaps.SGETRANGE(map, key.address, key.length, 
        off, off + len - 1, buffer2, bufferSize); 
      
      assertTrue(Utils.compareTo(buffer, len, buffer2, len) == 0);
      
      long newbc = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      if ((i + 1) % 1000 == 0) {
        System.out.println("small "+ (i + 1));
      }
    }
    // Test 2: running larger overwrites
    // bc contains valid bit number
    System.out.println("Running larger overwrites sub-test");

    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK * 100) + 1;
      int off = r.nextInt((int)strlen - len);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off, off + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer,  len,  (byte) 0);     
      int rangeSize = (int) SparseBitmaps.SGETRANGE(map, key.address, key.length, 
        off, off + len - 1, buffer, bufferSize);  
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off, buffer, rangeSize);
      assertEquals(strlen2, size);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer2,  len,  (byte) 0);
      SparseBitmaps.SGETRANGE(map, key.address, key.length, 
        off, off + len - 1, buffer2, bufferSize); 
      
      assertTrue(Utils.compareTo(buffer, len, buffer2, len) == 0);
      long newbc = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      if ((i + 1) % 1000 == 0) {
        System.out.println("large "+ (i + 1));
      }
    }
    
    // Test 3: running larger overwrites out of key2 sparse range
    // bc contains valid bit number
    System.out.println("Running larger overwrites out of range (but intersects) sub-test");
        
    for (int i = 0; i < 10000; i++) {
      int len = r.nextInt(SparseBitmaps.BYTES_PER_CHUNK * 100) + 1;
      int off = r.nextInt((int)strlen - len);
      long off2 = Math.abs(r.nextLong()) % (long)(strlen2);
      long before = SparseBitmaps.SSTRLEN(map, key2.address, key2.length);
      long rbc1 = SparseBitmaps.SBITCOUNT(map, key.address, key.length, off, off + len - 1);
      long rbc2 = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, off2, off2 + len - 1);
      // Clear first len bytes of buffer
      UnsafeAccess.setMemory(buffer, len, (byte) 0);
      
      int rangeSize = (int) SparseBitmaps.SGETRANGE(map, key.address, key.length, 
        off, off + len - 1, buffer, bufferSize);  
      assertEquals(len, rangeSize);
      long size = SparseBitmaps.SSETRANGE(map, key2.address, key2.length, off2, buffer, rangeSize);
      // Clear first len bytes of buffer2
      UnsafeAccess.setMemory(buffer2, len, (byte) 0);
      
      long rangeSize2 = SparseBitmaps.SGETRANGE(map, key2.address, key2.length, off2, off2 + rangeSize - 1, buffer2, bufferSize);
      if (rangeSize2 < rangeSize) {
        // Requested range can be smaller than original b/c of a trailing zeros
        // Clear last rangeSize - size2 bytes in buffer2 
        UnsafeAccess.setMemory(buffer2 + rangeSize2, rangeSize - rangeSize2, (byte) 0);
        assertEquals(size, off2 + rangeSize2);
      } else {
        assertEquals(rangeSize, (int)rangeSize2);
      }
      // Compare two ranges read after write
      int res = Utils.compareTo(buffer, rangeSize, buffer2, rangeSize);
      if (res != 0) {
        System.err.println(i);
        System.err.println("strlen1=" + strlen1);
        System.err.println("strlen2=" + strlen2);
        System.err.println("    off=" + off);
        System.err.println("   off2=" + off2);
        System.err.println("    len=" + len);
        System.err.println(" before=" + before);
        System.err.println("  after=" + size);
      }
      assertTrue(res == 0);
      long newbc = SparseBitmaps.SBITCOUNT(map, key2.address, key2.length, Commons.NULL_LONG, Commons.NULL_LONG);
      if (newbc != bc + rbc1 - rbc2) {
        System.err.println(i);
      }
      assertEquals(bc + rbc1 - rbc2, newbc);
      bc = newbc;
      strlen2 = size;
      if ((i + 1) % 1000 == 0) {
        System.out.println("large out "+ (i + 1));
      }
    } 
    
    UnsafeAccess.free(buffer);
    UnsafeAccess.free(buffer2);
    
    SparseBitmaps.DELETE(map, key.address, key.length);
    assertFalse(SparseBitmaps.EXISTS(map, key.address, key.length));
    SparseBitmaps.DELETE(map, key2.address, key2.length);
    assertFalse(SparseBitmaps.EXISTS(map, key2.address, key2.length));
    assertEquals(0, (int) BigSortedMap.countRecords(map));
  }
  
  public static void main(String[] args) {
    SparseBitmapsTest test = new SparseBitmapsTest();
    test.runAllNoCompression();
    test.runAllCompressionLZ4();
  }
}
