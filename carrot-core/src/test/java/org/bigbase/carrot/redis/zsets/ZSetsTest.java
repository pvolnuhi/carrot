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
package org.bigbase.carrot.redis.zsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.DataBlock;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.util.Bytes;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;
import org.bigbase.carrot.util.Utils;
import org.bigbase.carrot.util.Value;
import org.bigbase.carrot.util.ValueScore;
import org.junit.Ignore;
import org.junit.Test;

public class ZSetsTest {
  BigSortedMap map;
  Key key;
  long buffer;
  int bufferSize = 64;
  int fieldSize = 16;
  long n = 1000000;
  List<Value> fields;
  List<Double> scores;
  int maxScore = 100000;
  
  static {
    //UnsafeAccess.setMallocDebugEnabled(true);
    //UnsafeAccess.setMallocDebugStackTraceEnabled(true);
  }
  
  private List<Value> getFields(long n) {
    List<Value> keys = new ArrayList<Value>();
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("KEYS SEED=" + seed);
    byte[] buf = new byte[fieldSize/2];
    for (int i = 0; i < n; i++) {
      r.nextBytes(buf);
      long ptr = UnsafeAccess.malloc(fieldSize);
      // Make values compressible
      UnsafeAccess.copy(buf, 0, ptr, buf.length);
      UnsafeAccess.copy(buf, 0, ptr + buf.length, buf.length);
      keys.add(new Value(ptr, fieldSize));
    }
    return keys;
  }
    
  private List<Double> getScores(long n) {
    List<Double> scores = new ArrayList<Double>();
    Random r = new Random(1);
    for (int i = 0; i < n; i++) {
      scores.add((double)r.nextInt(maxScore));
    }
    return scores;
  }
  
  private Key getKey() {
    long ptr = UnsafeAccess.malloc(fieldSize);
    byte[] buf = new byte[fieldSize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, fieldSize);
    return key = new Key(ptr, fieldSize);
  }
  
  private void setUp() {
    map = new BigSortedMap(1000000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    fields = getFields(n);
    scores = getScores(n);
    Utils.sortKeys(fields);
    for(int i = 1; i < n; i++) {
      Key prev = fields.get(i-1);
      Key cur = fields.get(i);
      int res = Utils.compareTo(prev.address, prev.length, cur.address, cur.length);
      if (res == 0) {
        System.out.println("Found duplicate");
        fail();
      }
    }
  }
  
  @Ignore
  @Test
  public void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < 10; i++) {
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL");
      allTests();
      BigSortedMap.printGlobalMemoryAllocationStats();
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
      BigSortedMap.printGlobalMemoryAllocationStats();
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
      BigSortedMap.printGlobalMemoryAllocationStats();
      UnsafeAccess.mallocStats.printStats();
    }
  }
  
  private void allTests() {
    setUp();
    testAddGetScore();
    tearDown();
    setUp();
    testAddRemove();
    tearDown();
    setUp();
    testAddDeleteMulti();
    tearDown();
  }
  
  @Ignore
  @Test
  public void testAddGetScoreMulti () {
    System.out.println("Test ZSet Add Get Score Multi");
    map = new BigSortedMap();
    int total = 3000;
    Key key = getKey();
    long[] elemPtrs = new long[total];
    int[] elemSizes = new int[total];
    double[] scores = new double[total];
    int len = scores.length;
    List<Value> fields = getFields(len);
    List<Double> scl = getScores(len); 
    for(int i = 0; i < len; i++) {
      elemPtrs[i] = fields.get(i).address;
      elemSizes[i] = fields.get(i).length;
      scores[i] = scl.get(i);      
    }
        
    long start = System.nanoTime();
    long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
    long end = System.nanoTime();
    System.out.println("call time=" + (end - start)/1000 + "micros");
    assertEquals(total, (int) num);
    assertEquals(total, (int) ZSets.ZCARD(map, key.address, key.length));
        
    for (int i = 0; i < total; i++) {
      Double res = ZSets.ZSCORE(map, key.address, key.length, elemPtrs[i], elemSizes[i]);
      assertEquals(scores[i], res);
    }

    BigSortedMap.printGlobalMemoryAllocationStats();
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
    map.dispose();
    UnsafeAccess.free(key.address);
    fields.stream().forEach(x -> UnsafeAccess.free(x.address));
    UnsafeAccess.mallocStats.printStats();
  }
  
  //@Ignore
  @Test
  public void testAddGetScoreMultiOpt () {
    System.out.println("Test ZSet Add Get Score Multi (Optimized version)");
    map = new BigSortedMap();
    int total = 30000;
    Key key = getKey();

    List<Value> fields = getFields(total);
    List<Double> scl = getScores(total); 
    List<ValueScore> list = new ArrayList<ValueScore>();
    for (int i = 0; i < total; i++) {
      Value v = fields.get(i);
      double score = scl.get(i);
      list.add( new ValueScore(v.address, v.length, score));
    }
        
    long start = System.nanoTime();
    long num = ZSets.ZADD_NEW(map, key.address, key.length, list);
    long end = System.nanoTime();
    System.out.println("call time=" + (end - start)/1000 + "micros");
    assertEquals(total, (int) num);
    assertEquals(total, (int) ZSets.ZCARD(map, key.address, key.length));
        
    for (int i = 0; i < total; i++) {
      Value v = fields.get(i);
      Double res = ZSets.ZSCORE(map, key.address, key.length, v.address, v.length);
      assertEquals(scl.get(i), res);
    }

    BigSortedMap.printGlobalMemoryAllocationStats();
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int) ZSets.ZCARD(map, key.address, key.length));
    map.dispose();
    UnsafeAccess.free(key.address);
    fields.stream().forEach(x -> UnsafeAccess.free(x.address));
    UnsafeAccess.mallocStats.printStats();
  }
  
  @Ignore
  @Test
  public void testAddGetScore () {
    System.out.println("Test ZSet Add Get Score");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = this.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int)num);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + (2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - (2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3)) +
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printGlobalMemoryAllocationStats();

    
    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      Double res = ZSets.ZSCORE(map, key.address, key.length, fields.get(i).address, fields.get(i).length);
      assertEquals(this.scores.get(i), res);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    end = System.currentTimeMillis();
    System.out.println(" Time for " + n+ " ZSCORE="+(end -start)+"ms");
    BigSortedMap.printGlobalMemoryAllocationStats();
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)ZSets.ZCARD(map, key.address, key.length));
 
  }
  
  @Ignore
  @Test
  public void testAddRemove() {
    System.out.println("Test ZSet Add Remove");
    Key key = getKey();
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = this.scores.get(i);
      long num = ZSets.ZADD(map, key.address, key.length, scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int)num);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
      + " for "+ n + " " + (2 * (fieldSize + Utils.SIZEOF_DOUBLE) + 3) + " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - (2 * (fieldSize + Utils.SIZEOF_DOUBLE)+ 3)) 
      + " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printGlobalMemoryAllocationStats();

    assertEquals(n, ZSets.ZCARD(map, key.address, key.length));
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      long n = ZSets.ZREM(map, key.address, key.length, elemPtrs, elemSizes);
      assertEquals(1, (int) n);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Time for " + n + " ZREM="+(end -start)+"ms");
    assertEquals(0, (int)map.countRecords());
    assertEquals(0, (int)ZSets.ZCARD(map, key.address, key.length));
    ZSets.DELETE(map, key.address, key.length);
    assertEquals(0, (int)map.countRecords());
    assertEquals(0, (int)ZSets.ZCARD(map, key.address, key.length));

  }
  
  @Ignore
  @Test
  public void testAddDeleteMulti() {
    System.out.println("Test ZSet Add Delete Multi");
    long[] elemPtrs = new long[1];
    int[] elemSizes = new int[1];
    double[] scores = new double[1];
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      scores[0] = this.scores.get(i);
      long num = ZSets.ZADD(map, elemPtrs[0], elemSizes[0], scores, elemPtrs, elemSizes, true);
      assertEquals(1, (int)num);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    int setSize = DataBlock.RECORD_TOTAL_OVERHEAD + fieldSize /*part of a key*/ + 
        6/*4 + 1 + 1 - additional key overhead */ + Utils.SIZEOF_DOUBLE + fieldSize + 3;
        
    long end = System.currentTimeMillis();
    System.out.println("Total allocated memory ="+ BigSortedMap.getGlobalAllocatedMemory() 
    + " for "+ n + " " + setSize + " byte values. Overhead="+ 
        ((double)BigSortedMap.getGlobalAllocatedMemory()/n - setSize) +
    " bytes per value. Time to load: "+(end -start)+"ms");
    
    BigSortedMap.printGlobalMemoryAllocationStats();
    
    start = System.currentTimeMillis();
    for (int i =0; i < n; i++) {
      elemPtrs[0] = fields.get(i).address;
      elemSizes[0] = fields.get(i).length;
      boolean res  = ZSets.DELETE(map, elemPtrs[0], elemSizes[0]);
      assertEquals(true, res);
      if ((i+1) % 100000 == 0) {
        System.out.println(i+1);
      }
    }
    end = System.currentTimeMillis();
    System.out.println("Time for " + n + " DELETE="+(end -start)+"ms");
    assertEquals(0, (int)map.countRecords());

  }
  
  private void tearDown() {
    // Dispose
    map.dispose();
    if (key != null) {
      UnsafeAccess.free(key.address);
      key = null;
    }
    for (Key k: fields) {
      UnsafeAccess.free(k.address);
    }
    UnsafeAccess.free(buffer);
    UnsafeAccess.mallocStats.printStats();
    BigSortedMap.printGlobalMemoryAllocationStats();
  }
}
