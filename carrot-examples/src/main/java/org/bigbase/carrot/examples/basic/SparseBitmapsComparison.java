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
 */

package org.bigbase.carrot.examples.basic;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.bigbase.carrot.BigSortedMap;
import org.bigbase.carrot.compression.CodecFactory;
import org.bigbase.carrot.compression.CodecType;
import org.bigbase.carrot.redis.sparse.SparseBitmaps;
import org.bigbase.carrot.redis.util.Commons;
import org.bigbase.carrot.util.Key;
import org.bigbase.carrot.util.UnsafeAccess;

/**
 * Carrot sparse bitmaps can be more memory efficient then Redis bitmaps
 * when bit population count is small (< 0.05) 
 * 
 * The Test runs sparse bitmap tests with different population counts
 * and measure compression relative to Redis bitmap.
 * 
 *   Note:
 *   
 *   We do not take into account how Redis allocate memory for bitmap
 *   
 * Result (for LZ4HC compression):
 * 
 * population        COMPRESSION
 * count (dencity)
 * 
 * dencity=1.0E-6    5993
 * 
 * dencity=1.0E-5    647 
 * 
 * dencity=1.0E-4    118
 * 
 * dencity=0.001     27
 * 
 * dencity=0.01      4.2          
 * 
 * dencity=0.02      2.5
 * 
 * dencity=0.03      2.0
 * 
 * dencity=0.04      1.6
 * 
 * dencity=0.05      1.43
 * 
 * dencity=0.075     1.2
 * 
 * dencity=0.1       1
 * 
 * 
 * Notes: COMPRESSION = sizeUncompressedBitmap/Test_consumed_RAM
 * 
 * sizeUncompressedBitmap - size of an uncompressed bitmap, which can hold all the bits
 * Test_consumed_RAM - RAM consumed by test.
 * 
 * @author vrodionov
 *
 */

public class SparseBitmapsComparison {
  static BigSortedMap map;
  static Key key;
  static long buffer;
  static int bufferSize = 64;
  static int keySize = 8;
  static int N = 1000000;
  static int delta = 100;
  static double dencity = 0.01;
  
  static double[] dencities = 
      new double[] {/*0.000001, 0.00001, 0.0001, 0.001,*/ 0.01 /*, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1*/};
  
  static {
    UnsafeAccess.debug = true;
  }
    
  private static Key getKey() {
    long ptr = UnsafeAccess.malloc(keySize);
    byte[] buf = new byte[keySize];
    Random r = new Random();
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println("SEED=" + seed);
    r.nextBytes(buf);
    UnsafeAccess.copy(buf, 0, ptr, keySize);
    return new Key(ptr, keySize);
  }
  
  private static void setUp() {
    map = new BigSortedMap(10000000);
    buffer = UnsafeAccess.mallocZeroed(bufferSize); 
    key = getKey();
  }
  
  private static void tearDown() {
    map.dispose();

    UnsafeAccess.free(key.address);
    UnsafeAccess.free(buffer);
  }

  private static void runAllNoCompression() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.NONE));
    System.out.println();
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];
      System.out.println("*************** RUN = " + (i + 1) +" Compression=NULL, dencity=" + dencity);
      allTests();
 
    }
  }
  
  private static void runAllCompressionLZ4() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4));
    System.out.println();
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];
      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4, dencity=" + dencity);
      allTests();
    }
  }
  
  private static void runAllCompressionLZ4HC() {
    BigSortedMap.setCompressionCodec(CodecFactory.getInstance().getCodec(CodecType.LZ4HC));
    System.out.println();
    for (int i = 0; i < dencities.length; i++) {
      dencity = dencities[i];

      System.out.println("*************** RUN = " + (i + 1) +" Compression=LZ4HC, dencity="+ dencity);
      allTests();
    }
  }
  
  private static void allTests() {
    setUp();
    testPerformance();
    tearDown();
  }
   
  private static void testPerformance() {
    
    System.out.println("\nTest Performance\n");
    long offset= 0;
    long MAX =  (long)(N / dencity);
    Random r = new Random();
    
    long start = System.currentTimeMillis();
    long expected = N;
    for (int i = 0; i < N ; i++) {
      offset = Math.abs(r.nextLong()) % MAX;
      int bit = SparseBitmaps.SSETBIT(map, key.address, key.length, offset, 1); 
      if (bit == 1) {
        expected--;
      }
    }
    long end  = System.currentTimeMillis();    
    long memory = UnsafeAccess.getAllocatedMemory();
    /*DEBUG*/ System.out.println("Total RAM=" + memory+ " MAX=" + MAX+"\n");
    
    long count = SparseBitmaps.SBITCOUNT(map, key.address, key.length, Commons.NULL_LONG, Commons.NULL_LONG);
    assertEquals(expected, count);
    
    System.out.println("Time for " + N + " population dencity="+ dencity + 
     " bitmap size=" + (MAX) +  " new SetBit=" + (end - start) + "ms");
    System.out.println("COMPRESSION ratio ="+( ((double)MAX) / (8 * memory)));
    BigSortedMap.printMemoryAllocationStats();

  }
  
  public static void main(String[] args) {
    runAllNoCompression();
    runAllCompressionLZ4();
    runAllCompressionLZ4HC();
  }
}
