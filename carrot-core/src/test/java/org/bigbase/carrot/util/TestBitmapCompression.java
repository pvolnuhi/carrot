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
package org.bigbase.carrot.util;

import java.util.Random;

import org.junit.Test;

public class TestBitmapCompression {

  
  @Test
  public void testBitmapCodecs() {
    
    byte[] arr = null;
    double startFraction = 0.001; 
    double incr = 0.001;
    System.out.println("\n*********** 16 bits ***********\n");
    double stopFraction = 0.16;

    double fraction = startFraction;
    while(fraction <= stopFraction) {
      arr = new byte[256 * 256];
      populate(arr, fraction);
      int size = calculateSize16(arr);
      int diffSize = calculateSize16Diff(arr);
      System.out.println(fraction + " : " + size + " comp ratio = " + ((double)256 * 256)/ size + " diff size=" 
        + diffSize + " diff comp ratio = " + ((double)256 * 256)/ diffSize);
      fraction += incr;
    }
    
    fraction = startFraction;
    stopFraction = 0.20;
    System.out.println("\n******** 8 bits ***********\n");
    while(fraction <= stopFraction) {
      arr = new byte[256 * 256];
      populate(arr, fraction);
      int size = calculateSize8(arr);
      int sizeDiff = calculateSize8Diff(arr);
      System.out.println(fraction + " : " + size + " comp ratio = " + ((double)256 * 256)/ size + " diff size=" + 
          sizeDiff + " diff comp ratio=" + ((double)256 * 256)/ sizeDiff);
      fraction += incr;
    }
    
    fraction = startFraction;
    stopFraction = 0.25;

    System.out.println("\n********* 4 bits ************\n");
    while(fraction <= stopFraction) {
      arr = new byte[256 * 256];
      populate(arr, fraction);
      int size = calculateSize4(arr);
      System.out.println(fraction + " : " + size + " comp ratio = " + ((double)256 * 256)/ size );
      fraction += incr;
    }
    
    fraction = startFraction;
    stopFraction = 0.3;
    System.out.println("\n********** 2 bits ***********\n");
    while(fraction <= stopFraction) {
      arr = new byte[256 * 256];
      populate(arr, fraction);
      int size = calculateSize2(arr);
      System.out.println(fraction + " : " + size + " comp ratio = " + ((double)256 * 256)/ size);
      fraction += incr;
    }
//    
//    fraction = startFraction;
//    stopFraction = 0.4;
//    System.out.println("1 bits");
//    while(fraction <= stopFraction) {
//      arr = new byte[256 * 256];
//      populate(arr, fraction);
//      int size = calculateSize1(arr);
//      System.out.println(fraction + " : " + size + " comp ratio = " + ((double)256 * 256)/ size);
//      fraction += incr;
//    }
  }
  
  
  private static int calculateSize1(byte[] arr) {
    // Calculate by chunk of size 2 - 1 bit
    int size = 0;
    for (int i = 0; i < arr.length; i += 2) {
      int sz = bitCount(arr, i, 2) == 0? 1: 3;
      size += sz;
    }
    return size; 
  }
  
  private static int calculateSize2(byte[] arr) {
    // Calculate by chunk of size 4 - 2 bits
    int size = 0;
    for (int i = 0; i < arr.length; i += 4) {
      int sz = bitCount(arr, i, 4) == 0? 1: 5;
      size += sz;
    }
    return size; 
  }
  
  private static int calculateSize4(byte[] arr) {
    // Calculate by chunk of size 16 - 4 bits
    int size = 0;
    for (int i = 0; i < arr.length; i += 16) {
      int bitCount = bitCount(arr, i, 16);
      // when bitCount == 0, we encode chunk into just 1 bit - 0
      // when bitCount > 0, first bit is always 1, followed by 2 bits:
      // 00 - 1 bit set
      // 01 - 2 bits set
      // 10 - 3 bits set
      // 11 - raw 16 bits chunk
      int sz = bitCount * 4 + (bitCount == 0? 1: 3); /* to keep # bits*/;
      if (sz >= 16 + 3) {
        size += 16 + 3;
      } else {
        size += sz;
      }
    }
    return size;
  }
  
  
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size
   */
  private static int calculateSize8(byte[] arr) {
    // Calculate by chunk of size 256 - 8 bits
    int size = 0;
    int maxBitCount = 0;
    for (int i = 0; i < arr.length; i += 256) {
      int bitCount = bitCount(arr, i, 256);
      if (bitCount > maxBitCount) {
        maxBitCount = bitCount;
      }
      int sz = bitCount * 8 + (bitCount == 0? 1: 6) /* we need only 5 bits to keep max elements in a compressed 
      256 bits chunk - 31; first bit is always 1, total 5+1 = 6 bits*/;
      if (sz >= 256 + 6) {
        size += 256 + 6;
      } else {
        size += sz;
      }
    }
    //System.out.println("max bit count per 256 = "+ maxBitCount);
    return size;
  }
  
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size
   */
  private static int calculateSize8Diff(byte[] arr) {
    // Calculate by chunk of size 256 - 8 bits
    int size = 0;
    int maxBits = 0;
    int maxBitCount = 0;
    int maxMax  = 0;
    int bitCountMax = 0;
    for (int i = 0; i < arr.length; i += 256) {
      int[] index = indexArray(arr, i, 256);
      if (index.length > 0) {
        int[] diffArray = diffArray(index);
        int max = max(diffArray);
        int lead = Integer.numberOfLeadingZeros(max);
        int bits = 32  - lead;
        //System.out.print(bits + " ");
        if (bits > maxBits) {
          maxBits = bits;
          maxMax = max;
          bitCountMax = diffArray.length;
        }
        
        // For 256 bit slices we use 6 bit to keep number of elements ( 1's)
        // We do not use DIFF encoding unless we have at least 4 bits
        int numBits = diffArray.length;
        int sz;
        if (numBits > maxBitCount) {
          maxBitCount = numBits;
        }
        if (numBits >= 4) {
        // 7 bits - is 1xxxxxx, last 6 bits  are total bit count (bits set)
        // in a 256 bit block set
        // first bit == 1, when bit count > 0   
          
          sz = 7 + 3 /*keeps bits, max = 8 */ /*+ Utils.sizeUVInt(diffArray[0])*/ + 
          (diffArray.length) * bits;
        } else {
          sz = 7 + diffArray.length * 8;// 
        }
        if ( sz > 256 + 7) {
          sz = 256 + 7;
        }
        size += sz;
      } else {
        size += 1; // just length = 0, one bit = 0 
      }
    }
    //System.out.println();
    //System.out.println("diff bits=" + maxBits + " max bit count=" + maxBitCount + " maxMax=" + maxMax + 
    //  " bitCountMax=" + bitCountMax);
    return size;
  }
  
  /**
   * For testing only array size = 64K
   * @param arr array represents bits: 0, 1
   * @return compressed size in bits
   */
  private static int calculateSize16(byte[] arr) {
    // 16 bits
    int bitCount = bitCount(arr, 0, arr.length);
    int size = bitCount * 16 + 12 /* max bits in compressed block is 4096 */;
    if (size > arr.length) {
      size = arr.length;
    }
    return size;
  }
  
  
  private static int bitCount(byte[] arr, int offset, int chunkSize) {
    int count = 0;
    for (int i = offset; i < offset + chunkSize; i++) {
      count += arr[i];
    }
    return count;
  }
  
  private static int calculateSize16Diff(byte[] arr) {
     int[] index = indexArray(arr, 0, arr.length);
     int[] diffArray = diffArray(index);
     int max = max(diffArray);
     int lead = Integer.numberOfLeadingZeros(max);
     int bits = 32 - lead;
     //System.out.println("max = "+ max + " lead =" + lead + " bits=" + bits);
     int size = 14 /*number of elements*/ + 
          /* first element*/ + 4 /* bits number */ + (diffArray.length) * bits;
     return size;
  }
  
  private static int[] diffArray(int[] arr) {
    int[] diff = new int[arr.length];
    diff[0] = arr[0];
    for (int i = 1; i < arr.length; i++) {
      diff[i] = arr[i] - arr[i - 1];
    }
    return diff;
  }
  
  private static int[] indexArray(byte[] arr, int offset, int chunkSize) {
    int count  = bitCount(arr, offset, chunkSize);
    int[] index = new int[count];
    int j = 0;
    for (int i = offset; i < offset + chunkSize; i++) {
      if (arr[i] == 0) continue;
      index[j++] = i - offset;
    }
    return index;
  }
  
  static int max(int[] arr) {
    /// skip first element
    int max = 0;
    for (int i = 0; i < arr.length; i++) {
      if (arr[i] > max) max = arr[i];
    }
    return max;
  }
  
  /**
   * Populates byte array for a given bit fraction
   * @param arr array to populate
   * @param bitFraction fraction of 1's
   */
  private static void populate(byte[] arr, double bitFraction) {
    Random r = new Random();
    for (int i = 0; i < arr.length; i++) {
      double d = r.nextDouble();
      if (d < bitFraction) {
        arr[i] = 1;
      }
    }
  }
}
